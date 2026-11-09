import json
import os
import random
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest

import peewee
from playhouse import generation_cutover as gc
from playhouse.generation_cutover import (
    CHANGELOG_TABLE,
    CutoverAborted,
    CutoverError,
    CutoverSqliteDatabase,
    GenerationCutover,
    GenerationSqlitePool,
    PHASE_CATCHUP,
    PHASE_DRAIN,
    PHASE_SNAPSHOT,
    PHASE_VERIFY,
    TRIGGER_PREFIX,
    apply_changelog_batch,
    database_checksums,
    diff_data,
    diff_schema,
    install_changelog,
    max_changelog_seq,
)


SCHEMA_SQL = (
    'CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER, b BLOB)',
    'CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, '
    'name TEXT NOT NULL UNIQUE, tag TEXT)',
    'CREATE TABLE scratch (data TEXT)',  # No explicit primary key.
    'CREATE TABLE comp (a INTEGER, b INTEGER, v TEXT, '
    'PRIMARY KEY (a, b)) WITHOUT ROWID',
)

USER_TABLES = ('kv', 'items', 'scratch', 'comp')


class SimulatedCrash(RuntimeError):
    """Raised by test hooks to simulate a migration process dying mid-run."""


class BaseCutoverTestCase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix='cutover-test-')
        self.pool = GenerationSqlitePool(
            self.directory, stem='appdb', max_connections=8,
            acquire_timeout=15.0)
        self.pool.initialize()
        with self.pool.acquire() as conn:
            for statement in SCHEMA_SQL:
                conn.execute(statement)

    def tearDown(self):
        self.pool.close_all()
        shutil.rmtree(self.directory, ignore_errors=True)

    # -- helpers -------------------------------------------------------------

    def gen_path(self, generation):
        return self.pool.generation_path(generation)

    def open_file(self, generation):
        conn = sqlite3.connect(self.gen_path(generation),
                               isolation_level=None)
        conn.execute('PRAGMA foreign_keys = OFF')
        return conn

    def file_checksums(self, generation):
        conn = self.open_file(generation)
        try:
            return database_checksums(conn)
        finally:
            conn.close()

    def assert_generations_identical(self, source, target):
        src = self.open_file(source)
        dst = self.open_file(target)
        try:
            self.assertEqual(diff_schema(src, dst), [])
            self.assertEqual(diff_data(src, dst), [])
            self.assertEqual(database_checksums(src),
                             database_checksums(dst))
        finally:
            dst.close()
            src.close()

    def assert_no_cdc_objects(self, generation):
        conn = self.open_file(generation)
        try:
            names = [row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'")]
            self.assertFalse(
                any(name.startswith(TRIGGER_PREFIX) for name in names))
            tables = [row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'")]
            self.assertFalse(
                any(table.startswith('_cutover_') for table in tables))
        finally:
            conn.close()

    def rows(self, generation, table):
        conn = self.open_file(generation)
        try:
            return conn.execute(
                'SELECT * FROM "%s"' % table).fetchall()
        finally:
            conn.close()

    def seed_mixed_workload(self, start=0, count=100):
        """Write INSERT/UPDATE/DELETE statements touching every table."""
        with self.pool.acquire() as conn:
            for i in range(start, start + count):
                conn.execute(
                    'INSERT OR REPLACE INTO kv (k, v, b) VALUES (?, ?, ?)',
                    ('k%d' % (i % 40), i, bytes([i % 256])))
                if i % 3 == 0:
                    conn.execute('UPDATE kv SET v = ? WHERE k = ?',
                                 (-i, 'k%d' % (i % 40)))
                conn.execute(
                    'INSERT INTO items (name, tag) VALUES (?, ?)',
                    ('item%d' % i, 'even' if i % 2 == 0 else 'odd'))
                if i % 7 == 0 and i >= 7:
                    conn.execute('DELETE FROM items WHERE name = ?',
                                 ('item%d' % (i - 7),))
                conn.execute('INSERT INTO scratch (data) VALUES (?)',
                             (json.dumps({'i': i}),))
                conn.execute(
                    'INSERT OR REPLACE INTO comp (a, b, v) VALUES (?, ?, ?)',
                    (i % 9, i % 5, 'v%d' % i))
                if i % 11 == 0:
                    conn.execute('DELETE FROM comp WHERE a = ? AND b = ?',
                                 (i % 9, i % 5))
                if i % 5 == 0:
                    conn.execute('DELETE FROM kv WHERE k = ?',
                                 ('k%d' % ((i - 1) % 40),))


class TestCdcBuildingBlocks(BaseCutoverTestCase):
    def test_trigger_sql_and_changelog_roundtrip(self):
        source = self.open_file(1)
        try:
            source.execute('BEGIN IMMEDIATE')
            install_changelog(source)
            source.commit()
            source.execute("INSERT INTO kv (k, v, b) VALUES ('a', 1, X'01ff')")
            source.execute("UPDATE kv SET v = 2 WHERE k = 'a'")
            source.execute("INSERT INTO scratch (data) VALUES ('s1')")
            source.execute("INSERT INTO comp (a, b, v) VALUES (1, 2, 'x')")
            source.execute("DELETE FROM kv WHERE k = 'a'")
            entries = source.execute(
                'SELECT tbl, op, pk_cols, pk_vals, body FROM %s ORDER BY seq'
                % CHANGELOG_TABLE).fetchall()
        finally:
            source.close()

        ops = [(row[0], row[1]) for row in entries]
        self.assertEqual(ops, [
            ('kv', 'I'), ('kv', 'U'), ('scratch', 'I'),
            ('comp', 'I'), ('kv', 'D')])
        # Blob is hex-encoded inside a marker object (hex() is upper-case).
        insert_body = json.loads(entries[0][4])
        self.assertEqual(insert_body['b'],
                         {'__cutover_blob__': '01FF'})
        # Table without a primary key falls back to the rowid.
        self.assertEqual(json.loads(entries[2][2]), [])
        self.assertEqual(len(json.loads(entries[2][3])), 1)
        # Composite WITHOUT ROWID key is captured column-wise.
        comp_entry = entries[3]
        self.assertEqual(json.loads(comp_entry[2]), ['a', 'b'])
        self.assertEqual(json.loads(comp_entry[3]), [1, 2])

        # Replaying the full changelog onto an empty schema-only target
        # reproduces every surviving row.
        target_path = os.path.join(self.directory, 'replay.db')
        source = self.open_file(1)
        try:
            batch = source.execute(
                'SELECT seq, tbl, op, pk_cols, pk_vals, body FROM %s '
                'ORDER BY seq' % CHANGELOG_TABLE).fetchall()
        finally:
            source.close()

        target = sqlite3.connect(target_path, isolation_level=None)
        try:
            target.execute('PRAGMA foreign_keys = OFF')
            for statement in SCHEMA_SQL:
                target.execute(statement)
            gc.ensure_replay_table(target)
            apply_changelog_batch(target, batch)
            # Re-applying the whole batch is strictly idempotent: the
            # no-primary-key scratch table must not gain duplicate rows.
            apply_changelog_batch(target, batch)
            self.assertEqual(
                target.execute('SELECT data FROM scratch').fetchall(),
                [('s1',)])
            self.assertEqual(
                target.execute('SELECT a, b, v FROM comp').fetchall(),
                [(1, 2, 'x')])
            self.assertEqual(
                target.execute('SELECT count(*) FROM kv').fetchone(),
                (0,))
        finally:
            target.close()


class TestQuietCutover(BaseCutoverTestCase):
    def test_full_cutover_and_committed_rerun_is_noop(self):
        self.seed_mixed_workload(count=200)

        # Writes issued after CDC install (but before the snapshot) land in
        # the changelog and exercise the replay + watermark path.
        def before_phase(phase, state):
            if phase == PHASE_SNAPSHOT:
                self.seed_mixed_workload(start=1000, count=100)

        cutover = GenerationCutover(self.pool, migration_id='m1',
                                    batch_size=50,
                                    hooks={'before_phase': before_phase})
        state = cutover.run()

        self.assertEqual(state['phase'], 'committed')
        self.assertEqual(state['target_generation'], 2)
        self.assertEqual(self.pool.active_generation, 2)
        self.assertEqual(os.readlink(self.pool.link_path),
                         os.path.basename(self.gen_path(2)))
        # Old generation file is retained for rollback.
        self.assertTrue(os.path.exists(self.gen_path(1)))
        self.assert_generations_identical(1, 2)
        self.assert_no_cdc_objects(2)

        source = self.open_file(1)
        try:
            tip = max_changelog_seq(source)
        finally:
            source.close()
        self.assertEqual(state['applied_seq'], tip)
        self.assertGreater(tip, 0)

        # Re-running a committed migration is a no-op in the same generation.
        rerun = GenerationCutover(self.pool, migration_id='m1')
        rerun_state = rerun.run()
        self.assertEqual(rerun_state['target_generation'], 2)
        self.assertEqual(rerun.rows_applied, 0)


class TestOnlineCutover(BaseCutoverTestCase):
    def _writer(self, stop_event, status):
        rng = random.Random(1234)
        i = 0
        try:
            while not stop_event.is_set():
                # Stop requesting work once the cutover starts draining so
                # the comparison below reflects the exact switch point.
                if self.pool.draining:
                    break
                lease = self.pool.acquire()
                try:
                    # A lease handed out after the commit belongs to the new
                    # generation: stop instead of writing post-switch data.
                    if lease.generation != 1:
                        break
                    conn = lease.conn
                    conn.execute(
                        'INSERT OR REPLACE INTO kv (k, v, b) VALUES (?, ?, ?)',
                        ('k%d' % (i % 50), i, bytes([i % 256])))
                    if i % 3 == 0:
                        conn.execute('UPDATE kv SET v = ? WHERE k = ?',
                                     (-i, 'k%d' % (i % 50)))
                    conn.execute(
                        'INSERT INTO items (name, tag) VALUES (?, ?)',
                        ('item%d' % i, 'tag%d' % rng.randint(0, 3)))
                    if i >= 10 and i % 9 == 0:
                        conn.execute('DELETE FROM items WHERE name = ?',
                                     ('item%d' % (i - 9),))
                    conn.execute('INSERT INTO scratch (data) VALUES (?)',
                                 (json.dumps({'i': i}),))
                    conn.execute(
                        'INSERT OR REPLACE INTO comp (a, b, v) '
                        'VALUES (?, ?, ?)', (i % 7, i % 3, 'v%d' % i))
                    if i % 13 == 0:
                        conn.execute(
                            'DELETE FROM comp WHERE a = ? AND b = ?',
                            (i % 7, i % 3))
                finally:
                    lease.release()
                i += 1
                status['iterations'] = i
                time.sleep(0.002)
        except Exception as exc:
            status['error'] = exc

    def test_cutover_while_writer_runs(self):
        stop_event = threading.Event()
        status = {'iterations': 0}
        writer = threading.Thread(target=self._writer,
                                  args=(stop_event, status))
        writer.start()
        try:
            time.sleep(0.1)  # Let the writer accumulate some traffic.
            cutover = GenerationCutover(
                self.pool, migration_id='online', batch_size=64,
                verify_rounds=200, catchup_poll=0.01, drain_timeout=15.0)
            state = cutover.run()
            self.assertEqual(state['phase'], 'committed')
            self.assertEqual(self.pool.active_generation, 2)
        finally:
            stop_event.set()
            writer.join(timeout=10)
        self.assertFalse(writer.is_alive())
        self.assertNotIn('error', status)
        self.assertGreater(status['iterations'], 10)

        # The writer stopped at the drain boundary, so the two generations
        # must be byte-for-byte equivalent in content at the switch point.
        self.assert_generations_identical(1, 2)

        # New leases target generation 2, and post-switch writes never touch
        # the retained old generation.
        lease = self.pool.acquire()
        try:
            self.assertEqual(lease.generation, 2)
            lease.conn.execute(
                "INSERT INTO kv (k, v, b) VALUES ('post-switch', 1, X'')")
        finally:
            lease.release()
        gen1 = sqlite3.connect(self.gen_path(1))
        gen2 = sqlite3.connect(self.gen_path(2))
        try:
            self.assertEqual(gen1.execute(
                "SELECT count(*) FROM kv WHERE k = 'post-switch'").fetchone(),
                (0,))
            self.assertEqual(gen2.execute(
                "SELECT count(*) FROM kv WHERE k = 'post-switch'").fetchone(),
                (1,))
        finally:
            gen2.close()
            gen1.close()
        self.assertTrue(os.path.exists(self.gen_path(1)))


class TestResumeAfterInterruption(BaseCutoverTestCase):
    def _make_hooks(self, crash_phase=None, crash_after_snapshot=False,
                    crash_at_batch_seq=None, seed_count=400):
        seeded = {'done': False}
        crashes = {'count': 0}

        def before_phase(phase, state):
            if phase == PHASE_SNAPSHOT and not seeded['done']:
                self.seed_mixed_workload(count=seed_count)
                seeded['done'] = True
                if crash_phase == PHASE_SNAPSHOT:
                    crashes['count'] += 1
                    raise SimulatedCrash('crash before snapshot')
            if crash_phase is not None and phase == crash_phase \
                    and phase != PHASE_SNAPSHOT and crashes['count'] == 0:
                crashes['count'] += 1
                raise SimulatedCrash('crash in phase %s' % phase)

        def after_phase(phase, state):
            if phase != PHASE_SNAPSHOT:
                return
            if crash_at_batch_seq is not None:
                # Post-snapshot writes are what replay must catch up.
                self.seed_mixed_workload(start=10_000, count=100)
            if crash_after_snapshot and crashes['count'] == 0:
                crashes['count'] += 1
                raise SimulatedCrash('crash after snapshot')

        def after_batch(applied_seq, state):
            if crash_at_batch_seq is not None \
                    and applied_seq >= crash_at_batch_seq \
                    and crashes['count'] == 0:
                crashes['count'] += 1
                raise SimulatedCrash('crash mid-batch at seq %d'
                                     % applied_seq)

        return {'before_phase': before_phase,
                'after_phase': after_phase,
                'after_batch': after_batch}, crashes

    def _changelog_tip(self):
        conn = self.open_file(1)
        try:
            return max_changelog_seq(conn)
        finally:
            conn.close()

    def _run_with_crash_and_resume(self, hooks, crashes):
        first = GenerationCutover(self.pool, migration_id='resume',
                                  batch_size=50, hooks=hooks)
        with self.assertRaises(SimulatedCrash):
            first.run()
        self.assertEqual(crashes['count'], 1)

        # A brand-new coordinator resumes from persisted state with no
        # knowledge of the previous process.
        second = GenerationCutover(self.pool, migration_id='resume',
                                   batch_size=50)
        state = second.run()
        self.assertEqual(state['phase'], 'committed')
        self.assertEqual(state['target_generation'], 2)
        self.assertEqual(state['applied_seq'], self._changelog_tip())
        self.assert_generations_identical(1, 2)
        return first, second, state

    def _assert_no_batch_replayed_twice(self, first, second, state):
        # Only sequences newer than the snapshot are replayed, and no
        # committed batch is applied again after resume.
        expected = self._changelog_tip() - state['snapshot_seq']
        self.assertEqual(first.rows_applied + second.rows_applied,
                         expected)

    def test_resume_across_every_phase(self):
        for crash_phase in (PHASE_SNAPSHOT, PHASE_CATCHUP,
                            PHASE_VERIFY, PHASE_DRAIN):
            with self.subTest(crash_phase=crash_phase):
                self._reset_pool()
                hooks, crashes = self._make_hooks(crash_phase=crash_phase)
                first, second, state = \
                    self._run_with_crash_and_resume(hooks, crashes)
                # No committed batch is ever replayed after resume.
                self._assert_no_batch_replayed_twice(first, second, state)

    def test_resume_after_snapshot_redo_and_mid_batch_crash(self):
        with self.subTest(crash='after_snapshot_redo'):
            self._reset_pool()
            hooks, crashes = self._make_hooks(crash_after_snapshot=True)
            first, second, state = \
                self._run_with_crash_and_resume(hooks, crashes)
            self.assertTrue(os.path.exists(self.gen_path(2)))
            self._assert_no_batch_replayed_twice(first, second, state)

        with self.subTest(crash='mid_batch'):
            self._reset_pool()
            hooks, crashes = self._make_hooks(crash_at_batch_seq=100)
            first, second, state = \
                self._run_with_crash_and_resume(hooks, crashes)
            self.assertGreater(first.rows_applied, 0)
            self._assert_no_batch_replayed_twice(first, second, state)
            self.assert_generations_identical(1, 2)

    def _reset_pool(self):
        self.pool.close_all()
        shutil.rmtree(self.directory, ignore_errors=True)
        os.makedirs(self.directory)
        self.pool = GenerationSqlitePool(
            self.directory, stem='appdb', max_connections=8,
            acquire_timeout=15.0)
        self.pool.initialize()
        with self.pool.acquire() as conn:
            for statement in SCHEMA_SQL:
                conn.execute(statement)


class TestAbortConditions(BaseCutoverTestCase):
    def test_abort_when_lease_remains_active(self):
        self.seed_mixed_workload(count=50)
        held = self.pool.acquire()  # Simulate a long-running connection.
        try:
            cutover = GenerationCutover(
                self.pool, migration_id='blocked', batch_size=50,
                drain_timeout=0.3)
            started = time.time()
            with self.assertRaises(CutoverAborted):
                cutover.run()
            self.assertLess(time.time() - started, 5)
        finally:
            held.release()

        # Old generation keeps serving and the link was never moved.
        self.assertEqual(self.pool.active_generation, 1)
        self.assertEqual(os.readlink(self.pool.link_path),
                         os.path.basename(self.gen_path(1)))
        lease = self.pool.acquire()
        try:
            self.assertEqual(lease.generation, 1)
            lease.conn.execute(
                "INSERT INTO kv (k, v) VALUES ('after-abort', 1)")
        finally:
            lease.release()

        # Releasing the lease lets a resumed run complete successfully.
        rerun = GenerationCutover(self.pool, migration_id='blocked',
                                  batch_size=50, drain_timeout=5.0)
        state = rerun.run()
        self.assertEqual(state['phase'], 'committed')
        self.assertEqual(self.pool.active_generation, 2)
        self.assert_generations_identical(1, 2)

    def test_abort_on_checksum_mismatch_then_fresh_migration(self):
        self.seed_mixed_workload(count=80)

        def before_final_compare(state):
            corrupt = sqlite3.connect(state['target_path'],
                                      isolation_level=None)
            try:
                corrupt.execute('PRAGMA foreign_keys = OFF')
                corrupt.execute("DELETE FROM kv WHERE k = 'k1'")
            finally:
                corrupt.close()

        cutover = GenerationCutover(
            self.pool, migration_id='corrupt', batch_size=50,
            drain_timeout=5.0,
            hooks={'before_final_compare': before_final_compare})
        with self.assertRaises(CutoverAborted):
            cutover.run()

        self.assertEqual(self.pool.active_generation, 1)
        self.assertEqual(os.readlink(self.pool.link_path),
                         os.path.basename(self.gen_path(1)))
        # The "lost" row is still safely served by the old generation.
        with self.pool.acquire() as conn:
            count = conn.execute(
                "SELECT count(*) FROM kv WHERE k = 'k1'").fetchone()[0]
        self.assertEqual(count, 1)

        # A new migration id reuses the not-yet-committed generation 2 and
        # succeeds (a generation number is only consumed at commit time).
        retry = GenerationCutover(self.pool, migration_id='retry',
                                  batch_size=50, drain_timeout=5.0)
        state = retry.run()
        self.assertEqual(state['phase'], 'committed')
        self.assertEqual(state['target_generation'], 2)
        self.assert_generations_identical(1, 2)

    def test_abort_on_schema_drift(self):
        self.seed_mixed_workload(count=40)

        def before_phase(phase, state):
            if phase == PHASE_DRAIN:
                # A DDL statement sneaks in after the snapshot was taken.
                admin = sqlite3.connect(state['source_path'],
                                        isolation_level=None)
                try:
                    admin.execute('CREATE TABLE late_table (id INTEGER)')
                    admin.execute('INSERT INTO late_table VALUES (1)')
                finally:
                    admin.close()

        cutover = GenerationCutover(
            self.pool, migration_id='drift', batch_size=50,
            drain_timeout=5.0, hooks={'before_phase': before_phase})
        with self.assertRaises(CutoverAborted):
            cutover.run()
        self.assertEqual(self.pool.active_generation, 1)
        with self.pool.acquire() as conn:
            self.assertEqual(
                conn.execute('SELECT count(*) FROM late_table').fetchone(),
                (1,))


class TestPeeweeIntegration(BaseCutoverTestCase):
    def test_orm_traffic_survives_cutover(self):
        database = CutoverSqliteDatabase(self.pool)

        class Base(peewee.Model):
            class Meta:
                database = None

        class Widget(Base):
            name = peewee.TextField()
            score = peewee.IntegerField(default=0)

        # Bind the model to the pool-backed database (nested class bodies
        # cannot reference the function-local database by bare name).
        database.bind([Widget], bind_refs=False, bind_backrefs=False)
        database.connect()
        database.create_tables([Widget])
        Widget.insert_many([{'name': 'w%d' % i, 'score': i}
                            for i in range(25)]).execute()
        # Connections are request-scoped: return the lease before cutover so
        # the old generation can drain.
        database.close()

        cutover = GenerationCutover(self.pool, migration_id='orm',
                                    batch_size=32, drain_timeout=10.0)
        state = cutover.run()
        self.assertEqual(state['phase'], 'committed')

        # Reconnecting transparently leases a connection on the new generation.
        database.connect()
        Widget.create(name='post-cutover', score=99)
        self.assertEqual(Widget.select().count(), 26)
        database.close()

        gen1 = sqlite3.connect(self.gen_path(1))
        gen2 = sqlite3.connect(self.gen_path(2))
        try:
            self.assertEqual(gen1.execute(
                'SELECT count(*) FROM widget').fetchone(), (25,))
            self.assertEqual(gen2.execute(
                'SELECT count(*) FROM widget').fetchone(), (26,))
            self.assertEqual(gen2.execute(
                "SELECT score FROM widget WHERE name = 'post-cutover'"
            ).fetchone(), (99,))
        finally:
            gen2.close()
            gen1.close()


class TestRollback(BaseCutoverTestCase):
    def test_rollback_repoints_at_retained_generation(self):
        self.seed_mixed_workload(count=60)
        cutover = GenerationCutover(self.pool, migration_id='rb',
                                    batch_size=50, drain_timeout=10.0)
        cutover.run()
        self.assertEqual(self.pool.active_generation, 2)

        cutover.rollback()
        self.assertEqual(self.pool.active_generation, 1)
        self.assertEqual(os.readlink(self.pool.link_path),
                         os.path.basename(self.gen_path(1)))
        lease = self.pool.acquire()
        try:
            self.assertEqual(lease.generation, 1)
            self.assertGreater(
                lease.conn.execute('SELECT count(*) FROM kv').fetchone()[0],
                0)
        finally:
            lease.release()

        # A rolled-back migration cannot be re-run.
        with self.assertRaises(CutoverError):
            GenerationCutover(self.pool, migration_id='rb').run()


if __name__ == '__main__':
    unittest.main()
