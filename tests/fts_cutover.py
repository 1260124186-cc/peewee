import os
import shutil
import tempfile
import threading
import unittest

from peewee import *
from playhouse.fts_cutover import CutoverStage
from playhouse.fts_cutover import CutoverStateError
from playhouse.fts_cutover import CutoverValidationError
from playhouse.fts_cutover import FTS4_INDEX
from playhouse.fts_cutover import FTS5_INDEX
from playhouse.fts_cutover import FTSCutover
from playhouse.sqlite_ext import FTS5Model
from playhouse.sqlite_ext import FTSModel
from playhouse.sqlite_ext import SearchField

from .base import skip_unless


# Placeholder database; every test rebinds the models to its own temp file.
database = SqliteDatabase(':memory:')


class Doc(Model):
    title = TextField()
    body = TextField()

    class Meta:
        database = database
        table_name = 'doc'


class DocFTS4(FTSModel):
    title = SearchField()
    body = SearchField()

    class Meta:
        database = database
        table_name = 'doc_fts4'
        options = {'tokenize': 'porter'}


class DocFTS5(FTS5Model):
    title = SearchField()
    body = SearchField()

    class Meta:
        database = database
        table_name = 'doc_fts5'
        options = {'tokenize': 'porter'}


@skip_unless(FTS5Model.fts5_installed(), 'requires fts5')
class BaseCutoverTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='pw-fts-cutover-')
        self.db_path = os.path.join(self.tmpdir, 'cutover.db')
        self.db = SqliteDatabase(self.db_path, timeout=30)
        for model in (Doc, DocFTS4, DocFTS5):
            model.bind(self.db)
        self.db.connect()
        # The legacy deployment: main table plus the FTS4 index, no FTS5.
        self.db.create_tables([Doc, DocFTS4])

    def tearDown(self):
        if not self.db.is_closed():
            self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def new_cutover(self):
        return FTSCutover(self.db, Doc, DocFTS4, DocFTS5)

    def reopen_database(self):
        """Simulate a new process opening the same database file."""
        self.db.close()
        self.db = SqliteDatabase(self.db_path, timeout=30)
        for model in (Doc, DocFTS4, DocFTS5):
            model.bind(self.db)
        self.db.connect()
        return self.db

    def seed(self, n=20):
        for i in range(1, n + 1):
            self.db.execute_sql(
                'INSERT INTO doc(id, title, body) VALUES (?, ?, ?)',
                (i, 'title %d alpha' % i, 'body %d gamma delta' % i))
            self.db.execute_sql(
                'INSERT INTO doc_fts4(rowid, title, body) VALUES (?, ?, ?)',
                (i, 'title %d alpha' % i, 'body %d gamma delta' % i))

    def assertIndexesMatchMain(self, cutover):
        counts = cutover.index_counts()
        self.assertEqual(counts['main'], counts['fts4'])
        self.assertEqual(counts['main'], counts['fts5'])
        self.assertEqual(
            cutover._rowid_diff(Doc._meta.table_name,
                                DocFTS5._meta.table_name), [])
        self.assertEqual(
            cutover._rowid_diff(DocFTS5._meta.table_name,
                                Doc._meta.table_name), [])
        cutover._check_contents(DocFTS5._meta.table_name, batch_size=16)
        cutover._check_contents(DocFTS4._meta.table_name, batch_size=16)


class TestCutoverLifecycle(BaseCutoverTestCase):
    def test_install_is_idempotent_and_starts_on_fts4(self):
        cutover = self.new_cutover()
        self.assertEqual(cutover.install(), CutoverStage.PREPARED)
        # Re-running install must not error or create a second state row.
        self.assertEqual(cutover.install(), CutoverStage.PREPARED)
        self.assertTrue(self.db.table_exists('doc_fts5'))
        self.assertEqual(self.db.execute_sql(
            'SELECT COUNT(*) FROM doc_fts_cutover_state').fetchone()[0], 1)
        self.assertEqual(cutover.active_index, FTS4_INDEX.name)
        self.assertEqual(cutover.backfill_cursor, 0)
        self.assertIs(cutover.active_model, DocFTS4)

    def test_prepared_stage_writes_fts4_only(self):
        cutover = self.new_cutover()
        cutover.install()
        cutover.index_document(100, title='alpha', body='beta')
        self.assertEqual(cutover.index_counts(),
                         {'main': 0, 'fts4': 1, 'fts5': 0})
        # The search entry keeps serving the legacy index.
        self.assertEqual(cutover.search('alpha'), [100])

    def test_backfill_populates_fts5_but_reads_stay_on_fts4(self):
        self.seed(20)
        cutover = self.new_cutover()
        cutover.install()
        cutover.backfill(batch_size=7)

        self.assertEqual(cutover.stage, CutoverStage.BACKFILLED)
        self.assertEqual(cutover.active_index, FTS4_INDEX.name)
        self.assertEqual(cutover.backfill_cursor, 20)
        self.assertIndexesMatchMain(cutover)
        self.assertEqual(cutover.search('gamma'),
                         list(range(1, 21)))

    def test_backfill_resumes_from_cursor_after_crash(self):
        self.seed(50)
        cutover = self.new_cutover()
        cutover.install()

        batches = []

        def crash_after_two(cursor, size):
            batches.append((cursor, size))
            if len(batches) == 2:
                raise RuntimeError('simulated process kill')

        with self.assertRaises(RuntimeError):
            cutover.backfill(batch_size=10, progress=crash_after_two)

        # The third batch never committed: cursor and rows agree at 20.
        self.assertEqual(cutover.stage, CutoverStage.BACKFILLING)
        self.assertEqual(cutover.backfill_cursor, 20)
        self.assertEqual(cutover.index_counts()['fts5'], 20)

        # A brand-new coordinator over the same file resumes the run.
        cutover2 = self.new_cutover()
        stage = cutover2.run(batch_size=10)
        self.assertEqual(stage, CutoverStage.SWITCHED)
        self.assertIndexesMatchMain(cutover2)
        rowids = self.db.execute_sql(
            'SELECT DISTINCT rowid FROM doc_fts5').fetchall()
        self.assertEqual(len(rowids), 50)  # no duplicate documents

    def test_backfill_resumes_after_full_connection_restart(self):
        self.seed(35)
        cutover = self.new_cutover()
        cutover.install()
        with self.assertRaises(RuntimeError):
            cutover.backfill(
                batch_size=10,
                progress=lambda cursor, size: (_ for _ in ()).throw(
                    RuntimeError('killed')) if cursor >= 20 else None)
        self.assertEqual(cutover.backfill_cursor, 20)

        self.reopen_database()
        cutover2 = self.new_cutover()
        self.assertEqual(cutover2.stage, CutoverStage.BACKFILLING)
        self.assertEqual(cutover2.run(batch_size=10),
                         CutoverStage.SWITCHED)
        self.assertIndexesMatchMain(cutover2)

    def test_rerunning_completed_pipeline_is_a_noop(self):
        self.seed(15)
        cutover = self.new_cutover()
        cutover.run(batch_size=6)
        self.assertEqual(cutover.stage, CutoverStage.SWITCHED)
        cutover.run(batch_size=6)
        self.assertEqual(cutover.stage, CutoverStage.SWITCHED)
        self.assertIndexesMatchMain(cutover)

    def test_dual_write_crud_while_backfilling_converges(self):
        self.seed(10)
        cutover = self.new_cutover()
        cutover.install()
        with self.assertRaises(RuntimeError):
            cutover.backfill(
                batch_size=4,
                progress=lambda cursor, size: (_ for _ in ()).throw(
                    RuntimeError('killed')) if cursor >= 4 else None)
        self.assertEqual(cutover.stage, CutoverStage.BACKFILLING)

        # Creates, updates and deletes while the backfill is partially done.
        with self.db.atomic():
            self.db.execute_sql(
                'INSERT INTO doc(id, title, body) VALUES (?, ?, ?)',
                (100, 'new alpha', 'fresh gamma'))
            cutover.index_document(100, title='new alpha',
                                   body='fresh gamma')
        with self.db.atomic():
            self.db.execute_sql(
                'UPDATE doc SET title = ?, body = ? WHERE id = 1',
                ('updated alpha', 'updated delta'))
            cutover.index_document(1, title='updated alpha',
                                   body='updated delta')
        with self.db.atomic():
            self.db.execute_sql('DELETE FROM doc WHERE id = 2')
            cutover.remove_document(2)

        cutover.run(batch_size=4)
        self.assertEqual(cutover.stage, CutoverStage.SWITCHED)
        self.assertIndexesMatchMain(cutover)
        # The update is visible from both indexes, the delete from neither.
        self.assertEqual(cutover.search_index('fts4', 'updated'), [1])
        self.assertEqual(cutover.search_index('fts5', 'updated'), [1])
        self.assertNotIn(2, cutover.search('delta'))
        self.assertIn(100, cutover.search('fresh'))

    def test_dual_write_failure_rolls_back_main_and_both_indexes(self):
        self.seed(5)
        cutover = self.new_cutover()
        cutover.run(batch_size=8)
        # Roll the pointer back to dual-write for the outage simulation.
        cutover.State.update(stage=CutoverStage.BACKFILLED,
                             active_index=FTS4_INDEX.name).execute()
        before = cutover.index_counts()

        original_execute = self.db.execute_sql

        def fail_fts5(sql, params=None):
            if 'doc_fts5' in sql and 'INSERT' in sql:
                raise OperationalError('simulated FTS5 outage')
            return original_execute(sql, params)

        def app_create():
            with self.db.atomic():
                self.db.execute_sql(
                    'INSERT INTO doc(id, title, body) VALUES (?, ?, ?)',
                    (99, 'ghost alpha', 'ghost beta'))
                cutover.index_document(99, title='ghost alpha',
                                       body='ghost beta')

        self.db.execute_sql = fail_fts5
        try:
            self.assertRaises(OperationalError, app_create)
        finally:
            self.db.execute_sql = original_execute

        # Nothing partial survived: main row and both indexes rolled back.
        self.assertEqual(cutover.index_counts(), before)
        self.assertEqual(self.db.execute_sql(
            'SELECT COUNT(*) FROM doc WHERE id = 99').fetchone()[0], 0)
        # Search still returns the full, pre-failure result set.
        self.assertEqual(len(cutover.search('alpha')), 5)

        def fail_fts4(sql, params=None):
            if 'doc_fts4' in sql and 'INSERT' in sql:
                raise OperationalError('simulated FTS4 outage')
            return original_execute(sql, params)

        def app_create_fts4_outage():
            with self.db.atomic():
                self.db.execute_sql(
                    'INSERT INTO doc(id, title, body) VALUES (?, ?, ?)',
                    (98, 'ghost alpha', 'ghost beta'))
                cutover.index_document(98, title='ghost alpha',
                                       body='ghost beta')

        self.db.execute_sql = fail_fts4
        try:
            self.assertRaises(OperationalError, app_create_fts4_outage)
        finally:
            self.db.execute_sql = original_execute
        self.assertEqual(cutover.index_counts(), before)

    def test_remove_document_hits_both_indexes(self):
        self.seed(8)
        cutover = self.new_cutover()
        cutover.run(batch_size=4)
        cutover.State.update(stage=CutoverStage.BACKFILLED).execute()
        with self.db.atomic():
            self.db.execute_sql('DELETE FROM doc WHERE id = 3')
            cutover.remove_document(3)
        cutover.validate()
        self.assertIndexesMatchMain(cutover)
        self.assertEqual(
            cutover.search_index('fts4', 'gamma'),
            [i for i in range(1, 9) if i != 3])
        self.assertEqual(
            cutover.search_index('fts5', 'gamma'),
            [i for i in range(1, 9) if i != 3])

    def test_validation_detects_missing_fts5_row(self):
        self.seed(12)
        cutover = self.new_cutover()
        cutover.backfill(batch_size=5)
        self.db.execute_sql('DELETE FROM doc_fts5 WHERE rowid = 4')
        with self.assertRaises(CutoverValidationError):
            cutover.validate()
        # A failed validation never advances the stage or the read pointer.
        self.assertEqual(cutover.stage, CutoverStage.BACKFILLED)
        self.assertEqual(cutover.active_index, FTS4_INDEX.name)

    def test_validation_detects_orphan_and_content_drift(self):
        self.seed(12)
        cutover = self.new_cutover()
        cutover.backfill(batch_size=5)

        self.db.execute_sql(
            'INSERT INTO doc_fts5(rowid, title, body) VALUES (?, ?, ?)',
            (999, 'orphan', 'row'))
        with self.assertRaises(CutoverValidationError):
            cutover.validate()
        self.db.execute_sql('DELETE FROM doc_fts5 WHERE rowid = 999')

        self.db.execute_sql(
            'INSERT OR REPLACE INTO doc_fts5(rowid, title, body) '
            'VALUES (?, ?, ?)', (5, 'tampered alpha', 'tampered delta'))
        with self.assertRaises(CutoverValidationError):
            cutover.validate()
        self.assertEqual(cutover.stage, CutoverStage.BACKFILLED)

    def test_validation_search_term_parity(self):
        self.seed(12)
        cutover = self.new_cutover()
        cutover.backfill(batch_size=5)
        cutover.validate(search_terms=['alpha', 'gamma', 'delta'])

        self.db.execute_sql(
            'INSERT OR REPLACE INTO doc_fts5(rowid, title, body) '
            'VALUES (?, ?, ?)', (6, 'different alpha', 'body 6 gamma delta'))
        with self.assertRaises(CutoverValidationError):
            cutover.validate(search_terms=['different'])

    def test_switch_requires_successful_validation(self):
        self.seed(10)
        cutover = self.new_cutover()
        cutover.backfill(batch_size=4)
        with self.assertRaises(CutoverStateError):
            cutover.switch_reads()
        self.assertEqual(cutover.active_index, FTS4_INDEX.name)

    def test_switch_is_atomic_and_idempotent(self):
        self.seed(20)
        cutover = self.new_cutover()
        cutover.backfill(batch_size=6)
        cutover.validate()
        self.assertEqual(cutover.switch_reads(), CutoverStage.SWITCHED)
        self.assertEqual(cutover.active_index, FTS5_INDEX.name)
        self.assertIs(cutover.active_model, DocFTS5)
        # Both entry points expose identical result sets at the switch.
        for term in ('alpha', 'gamma', 'delta'):
            self.assertEqual(cutover.search_index('fts4', term),
                             cutover.search_index('fts5', term))
        self.assertEqual(cutover.search('alpha'),
                         cutover.search_index('fts5', 'alpha'))
        # Re-entering the switch after a crash is harmless.
        self.assertEqual(cutover.switch_reads(), CutoverStage.SWITCHED)

    def test_cleanup_blocked_before_switch(self):
        self.seed(6)
        cutover = self.new_cutover()
        cutover.install()
        with self.assertRaises(CutoverStateError):
            cutover.cleanup_old_index()
        cutover.backfill(batch_size=3)
        with self.assertRaises(CutoverStateError):
            cutover.cleanup_old_index()
        cutover.validate()
        with self.assertRaises(CutoverStateError):
            cutover.cleanup_old_index()
        self.assertTrue(self.db.table_exists('doc_fts4'))

    def test_cleanup_drops_fts4_only_after_switch(self):
        self.seed(9)
        cutover = self.new_cutover()
        cutover.run(batch_size=4)
        self.assertEqual(cutover.stage, CutoverStage.SWITCHED)
        self.assertEqual(cutover.cleanup_old_index(), CutoverStage.DONE)
        self.assertFalse(self.db.table_exists('doc_fts4'))

        # Post-cleanup writes target FTS5 and remain fully searchable.
        with self.db.atomic():
            self.db.execute_sql(
                'INSERT INTO doc(id, title, body) VALUES (?, ?, ?)',
                (500, 'postcut alpha', 'postcut gamma'))
            cutover.index_document(500, title='postcut alpha',
                                   body='postcut gamma')
        self.assertEqual(cutover.search('postcut'), [500])
        # Idempotent cleanup after a re-run.
        self.assertEqual(cutover.cleanup_old_index(), CutoverStage.DONE)

    def test_run_pipeline_end_to_end(self):
        self.seed(30)
        cutover = self.new_cutover()
        stage = cutover.run(batch_size=8, search_terms=['alpha', 'gamma'])
        self.assertEqual(stage, CutoverStage.SWITCHED)
        self.assertIndexesMatchMain(cutover)
        self.assertEqual(cutover.search('alpha'), list(range(1, 31)))
        self.assertEqual(cutover.cleanup_old_index(), CutoverStage.DONE)

    def test_resume_between_validation_and_switch(self):
        self.seed(14)
        cutover = self.new_cutover()
        cutover.backfill(batch_size=5)
        cutover.validate()
        self.assertEqual(cutover.stage, CutoverStage.VALIDATED)

        # Process exits after validation but before the switch. A new
        # process must continue at the switch, not re-run earlier stages.
        self.reopen_database()
        cutover2 = self.new_cutover()
        self.assertEqual(cutover2.stage, CutoverStage.VALIDATED)
        self.assertEqual(cutover2.active_index, FTS4_INDEX.name)
        self.assertEqual(cutover2.run(batch_size=5), CutoverStage.SWITCHED)
        self.assertEqual(cutover2.active_index, FTS5_INDEX.name)
        self.assertIndexesMatchMain(cutover2)

    def test_search_never_serves_a_partial_index(self):
        self.seed(25)
        cutover = self.new_cutover()
        cutover.install()
        # While FTS5 is half-built, the gateway only exposes complete FTS4.
        with self.assertRaises(RuntimeError):
            cutover.backfill(
                batch_size=5,
                progress=lambda cursor, size: (_ for _ in ()).throw(
                    RuntimeError('killed')) if cursor >= 10 else None)
        self.assertEqual(cutover.index_counts()['fts5'], 10)
        self.assertEqual(len(cutover.search('alpha')), 25)
        self.assertEqual(cutover.active_index, FTS4_INDEX.name)

        cutover.run(batch_size=5)
        # After the switch the same entry serves the complete FTS5 index.
        self.assertEqual(cutover.active_index, FTS5_INDEX.name)
        self.assertEqual(len(cutover.search('alpha')), 25)

    def test_concurrent_connections_during_backfill(self):
        self.seed(100)
        cutover = self.new_cutover()
        cutover.install()
        errors = []

        def worker(worker_id):
            try:
                wdb = SqliteDatabase(self.db_path, timeout=30)
                wdb.connect()
                wgw = FTSCutover(wdb, Doc, DocFTS4, DocFTS5)
                try:
                    for j in range(10):
                        rowid = 1000 + worker_id * 100 + j
                        title = 'w%d alpha %d' % (worker_id, j)
                        body = 'w%d gamma' % worker_id
                        with wdb.atomic(lock_type='IMMEDIATE'):
                            wdb.execute_sql(
                                'INSERT INTO doc(id, title, body) '
                                'VALUES (?, ?, ?)', (rowid, title, body))
                            wgw.index_document(rowid, title=title, body=body)
                finally:
                    wdb.close()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(4)]
        for thread in threads:
            thread.start()
        cutover.backfill(batch_size=10)
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        cutover.run(batch_size=10)
        self.assertEqual(cutover.stage, CutoverStage.SWITCHED)
        self.assertIndexesMatchMain(cutover)
        self.assertEqual(cutover.index_counts()['main'], 140)


if __name__ == '__main__':
    unittest.main()
