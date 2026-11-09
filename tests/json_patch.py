import json
import os
import shutil
import tempfile
import threading

from peewee import SqliteDatabase
from playhouse.json_patch import (
    DocumentExistsError,
    DocumentNotFoundError,
    DocumentView,
    IllegalPathError,
    InvalidPatchError,
    JsonPatchError,
    JsonPatchStore,
    PathConflictError,
    StaleGenerationError,
    add,
    checksum_of,
    parse_pointer,
    paths_overlap,
    pointer_to_string,
    remove,
    replay_bundle,
    replace)

from .base import DatabaseTestCase


INITIAL = {
    'unmanaged': {'keep': True},
    'services': {
        'a': {'timeout': 10},
        'b': {'timeout': 20}}}


class TestJsonPatchStore(DatabaseTestCase):
    def setUp(self):
        super(TestJsonPatchStore, self).setUp()
        self._stores = []
        self._extra_dbs = []
        self._temp_dirs = []

    def tearDown(self):
        for store in self._stores:
            self.database.drop_tables(store.models, safe=True)
        for db in self._extra_dbs:
            if not db.is_closed():
                db.close()
        for directory in self._temp_dirs:
            shutil.rmtree(directory, ignore_errors=True)
        super(TestJsonPatchStore, self).tearDown()

    def create_store(self, **kwargs):
        store = JsonPatchStore(self.database, **kwargs)
        self._stores.append(store)
        return store

    def create_doc(self, store=None, doc_id='cfg', data=None):
        store = store or self.create_store()
        return store, store.create_document(doc_id,
                                            data if data is not None
                                            else json.loads(json.dumps(INITIAL)))

    def file_database(self):
        directory = tempfile.mkdtemp(prefix='jsonp-test-')
        self._temp_dirs.append(directory)
        path = os.path.join(directory, 'docs.db')
        db = SqliteDatabase(
            path, timeout=30,
            pragmas=[('busy_timeout', 30000), ('journal_mode', 'wal')])
        self._extra_dbs.append(db)
        return db, path

    #
    # Pointer helpers.
    #

    def test_pointer_parsing_and_escaping(self):
        self.assertEqual(parse_pointer('/a/b/c'), ('a', 'b', 'c'))
        self.assertEqual(parse_pointer('/a~0b/c~1d'), ('a~b', 'c/d'))
        self.assertEqual(pointer_to_string(('a~b', 'c/d')), '/a~0b/c~1d')
        for bad in ('', 'a/b', '/a~2', '/a~', 123, None):
            self.assertRaises(IllegalPathError, parse_pointer, bad)

    def test_paths_overlap(self):
        self.assertTrue(paths_overlap(('a',), ('a',)))
        self.assertTrue(paths_overlap(('a',), ('a', 'b')))
        self.assertTrue(paths_overlap(('a', 'b'), ('a',)))
        self.assertFalse(paths_overlap(('a', 'b'), ('a', 'c')))
        self.assertFalse(paths_overlap(('a',), ('b',)))

    #
    # Document creation and basic commits.
    #

    def test_create_document_validates_input(self):
        store = self.create_store()
        self.assertRaises(InvalidPatchError, store.create_document, '', {})
        self.assertRaises(InvalidPatchError, store.create_document, 'x', [])
        store.create_document('cfg', {'k': 'v'})
        self.assertRaises(DocumentExistsError, store.create_document,
                          'cfg', {})
        view = store.read('cfg')
        self.assertEqual(view.generation, 0)
        self.assertEqual(view.snapshot_generation, 0)
        self.assertEqual(view.data, {'k': 'v'})
        self.assertRaises(DocumentNotFoundError, store.read, 'missing')

    def test_commit_records_patch_and_bumps_generation(self):
        store, _ = self.create_doc()
        view = store.commit('cfg', [replace('/services/a/timeout', 30)], 0)
        self.assertEqual(view.generation, 1)
        self.assertEqual(view.data['services']['a']['timeout'], 30)
        # Unmanaged keys are preserved untouched.
        self.assertEqual(view.data['unmanaged'], {'keep': True})
        self.assertEqual(view.data['services']['b'], {'timeout': 20})

        patches = store.list_patches('cfg')
        self.assertEqual(len(patches), 1)
        self.assertEqual(patches[0], {
            'op': 'replace',
            'path': '/services/a/timeout',
            'value': 30,
            'base_generation': 0,
            'generation': 1,
            'seq': 0})

        store.commit('cfg', [add('/services/c', {'timeout': 40})], 1)
        store.commit('cfg', [remove('/services/b')], 2)
        view = store.read('cfg')
        self.assertEqual(view.generation, 3)
        self.assertNotIn('b', view.data['services'])
        self.assertEqual(view.data['services']['c'], {'timeout': 40})
        self.assertTrue(store.verify_integrity('cfg'))

    def test_add_replace_remove_semantics(self):
        store = self.create_store()
        store.create_document('d', {'a': {'existing': 1}})
        # add creates a new key and may also replace an existing one.
        view = store.commit('d', [add('/a/new', 2), add('/a/existing', 9)], 0)
        self.assertEqual(view.data['a'], {'existing': 9, 'new': 2})
        # replace on a missing key is illegal.
        self.assertRaises(IllegalPathError, store.commit, 'd',
                          [replace('/a/nope', 1)], 1)
        # remove on a missing key is illegal.
        self.assertRaises(IllegalPathError, store.commit, 'd',
                          [remove('/a/nope')], 1)

    def test_multi_op_commit_merges_disjoint_paths(self):
        store, _ = self.create_doc()
        view = store.commit('cfg', [
            replace('/services/a/timeout', 1),
            replace('/services/b/timeout', 2),
            add('/services/c', {'timeout': 3})], 0)
        self.assertEqual(view.generation, 1)
        self.assertEqual(view.data['services']['a']['timeout'], 1)
        self.assertEqual(view.data['services']['b']['timeout'], 2)
        self.assertEqual(view.data['services']['c'], {'timeout': 3})
        # All ops share the generation, ordered by seq.
        patches = store.list_patches('cfg')
        self.assertEqual([p['seq'] for p in patches], [0, 1, 2])
        self.assertTrue(all(p['generation'] == 1 for p in patches))

    #
    # Whole-commit failures.
    #

    def test_illegal_paths_abort_whole_commit(self):
        store, _ = self.create_doc()
        bad_batches = [
            [add('services/a', 1)],                          # no leading "/"
            [add('/a~2/b', 1)],                              # bad escape
            [replace('', 1)],                                # root path
            [replace('/services/missing/x', 1)],             # missing parent
            [replace('/services/a/missing', 1)],             # missing target
            [remove('/services/a/missing')],                 # missing target
            [add('/services/a/timeout/child', 1)],           # parent scalar
            [{'op': 'frobnicate', 'path': '/x', 'value': 1}],
            [{'op': 'add', 'path': '/x'}],                   # missing value
            [add('/x', object())],                           # not JSON
            'not-a-list',
            [],
        ]
        for batch in bad_batches:
            self.assertRaises(JsonPatchError, store.commit, 'cfg', batch, 0)
            view = store.read('cfg')
            self.assertEqual(view.generation, 0)
            self.assertEqual(view.data, INITIAL)
            self.assertEqual(store.Patch.select().count(), 0)

    def test_invalid_base_generation(self):
        store, _ = self.create_doc()
        for bad in (-1, '1', 1.5, True, None):
            self.assertRaises(InvalidPatchError, store.commit, 'cfg',
                              [add('/x', 1)], bad)

    def test_intra_commit_overlap_aborts(self):
        store, _ = self.create_doc()
        overlapping = [
            [add('/services/a', 1), replace('/services/a/timeout', 2)],
            [replace('/services/a/timeout', 1),
             replace('/services/a/timeout', 2)],
            [add('/services/a', {}), add('/services/a/timeout', 1)],
        ]
        for batch in overlapping:
            self.assertRaises(PathConflictError, store.commit, 'cfg',
                              batch, 0)
        self.assertEqual(store.read('cfg').generation, 0)
        self.assertEqual(store.Patch.select().count(), 0)

    #
    # Optimistic concurrency between two services.
    #

    def test_disjoint_stale_commits_merge(self):
        store, _ = self.create_doc()
        # Both services read generation 0.
        store.commit('cfg', [replace('/services/a/timeout', 1)], 0)
        view = store.commit('cfg', [replace('/services/b/timeout', 2)], 0)
        self.assertEqual(view.generation, 2)
        self.assertEqual(view.data['services']['a']['timeout'], 1)
        self.assertEqual(view.data['services']['b']['timeout'], 2)
        self.assertEqual(view.data['unmanaged'], {'keep': True})

    def test_same_path_conflict_rolls_back_whole_commit(self):
        store, _ = self.create_doc()
        store.commit('cfg', [replace('/services/a/timeout', 1)], 0)
        # Stale base 0, overlapping path -> conflict, and the unrelated op in
        # the same batch must be rolled back too.
        self.assertRaises(PathConflictError, store.commit, 'cfg', [
            replace('/services/a/timeout', 2),
            add('/services/c', {'timeout': 3})], 0)
        view = store.read('cfg')
        self.assertEqual(view.generation, 1)
        self.assertNotIn('c', view.data['services'])
        self.assertEqual(store.Patch.select().count(), 1)

    def test_future_generation_rejected(self):
        store, _ = self.create_doc()
        self.assertRaises(StaleGenerationError, store.commit, 'cfg',
                          [add('/x', 1)], 99)

    def test_stale_base_after_compaction_rejected(self):
        store, _ = self.create_doc()
        store.commit('cfg', [replace('/services/a/timeout', 1)], 0)
        self.assertEqual(store.compact('cfg'), 1)
        self.assertRaises(StaleGenerationError, store.commit, 'cfg',
                          [add('/x', 1)], 0)
        # A fresh read gives a usable base.
        view = store.read('cfg')
        view = store.commit('cfg', [add('/x', 1)], view.generation)
        self.assertEqual(view.generation, 2)

    #
    # Snapshot compaction and crash recovery.
    #

    def test_compaction_preserves_json_and_deletes_patches(self):
        store, _ = self.create_doc()
        for i in range(4):
            view = store.read('cfg')
            store.commit('cfg', [add('/services/n%d' % i, i)],
                         view.generation)
        before = store.read('cfg')
        self.assertEqual(store.Patch.select().count(), 4)

        snapshot_generation = store.compact('cfg')
        self.assertEqual(snapshot_generation, before.generation)
        self.assertEqual(store.Patch.select().count(), 0)
        self.assertEqual(store.Snapshot.select().count(), 2)

        after = store.read('cfg')
        # The snapshot pointer advances, but the materialized JSON and the
        # head generation are unchanged.
        self.assertEqual(after.data, before.data)
        self.assertEqual(after.generation, before.generation)
        self.assertEqual(after.snapshot_generation, before.generation)
        self.assertTrue(store.verify_integrity('cfg'))

        # Life continues after compaction.
        after = store.commit('cfg', [add('/services/late', True)],
                             after.generation)
        self.assertEqual(after.data['services']['late'], True)
        self.assertEqual(store.compact('cfg'), after.generation)
        self.assertEqual(store.read('cfg').data, after.data)
        self.assertTrue(store.verify_integrity('cfg'))

    def test_compaction_threshold_and_idempotency(self):
        store, _ = self.create_doc()
        store.commit('cfg', [add('/x', 1)], 0)
        self.assertIsNone(store.compact('cfg', min_patches=5))
        self.assertEqual(store.Snapshot.select().count(), 1)
        self.assertEqual(store.Patch.select().count(), 1)
        # Re-running an already-finished compaction is a no-op.
        self.assertEqual(store.compact('cfg'), 1)
        self.assertIsNone(store.compact('cfg'))
        self.assertEqual(store.Snapshot.select().count(), 2)

    def test_compact_all(self):
        store = self.create_store()
        store.create_document('d1', {'k': 1})
        store.create_document('d2', {'k': 2})
        store.commit('d1', [add('/x', 1)], 0)
        store.commit('d2', [add('/y', 2)], 0)
        result = store.compact_all()
        self.assertEqual(result, {'d1': 1, 'd2': 1})
        self.assertEqual(store.Patch.select().count(), 0)

    def test_crash_between_snapshot_and_cleanup(self):
        db, path = self.file_database()
        store = JsonPatchStore(db)
        store.create_document('cfg', json.loads(json.dumps(INITIAL)))
        for i in range(3):
            view = store.read('cfg')
            store.commit('cfg', [add('/services/n%d' % i, i)],
                         view.generation)
        before = store.read('cfg')

        # Phase 1 commits, phase 2 (cleanup) never runs.
        crashed_generation = store._compact_write_snapshot('cfg')
        self.assertEqual(crashed_generation, before.generation)
        self.assertEqual(store.Patch.select().count(), 3)
        db.close()

        # A brand-new process/connection opens the same file.
        db2, _ = self.file_database()
        # Reuse the path rather than creating a second temp database.
        db2.init(path)
        store2 = JsonPatchStore(db2)
        recovered = store2.restore('cfg')
        self.assertEqual(recovered.data, before.data)
        self.assertEqual(recovered.generation, before.generation)
        self.assertTrue(store2.verify_integrity('cfg'))

        # Re-running compaction needs no new snapshot but still finishes the
        # interrupted cleanup phase.
        self.assertIsNone(store2.compact('cfg'))
        self.assertEqual(store2.Patch.select().count(), 0)
        self.assertEqual(store2.read('cfg').data, before.data)
        self.assertTrue(store2.verify_integrity('cfg'))

    def test_process_restart_reproduces_identical_json(self):
        db, path = self.file_database()
        store = JsonPatchStore(db)
        store.create_document('cfg', json.loads(json.dumps(INITIAL)))
        for i in range(3):
            view = store.read('cfg')
            store.commit('cfg', [add('/services/n%d' % i, i)],
                         view.generation)
        store.compact('cfg')
        for i in range(2):
            view = store.read('cfg')
            store.commit('cfg', [add('/services/post%d' % i, i)],
                         view.generation)
        before = store.read('cfg')
        canonical_before = json.dumps(before.data, sort_keys=True,
                                      separators=(',', ':'))
        db.close()

        db2, _ = self.file_database()
        db2.init(path)
        store2 = JsonPatchStore(db2)
        restored = store2.restore('cfg')
        self.assertEqual(restored.generation, before.generation)
        self.assertEqual(restored.data, before.data)
        canonical_after = json.dumps(restored.data, sort_keys=True,
                                     separators=(',', ':'))
        self.assertEqual(canonical_after, canonical_before)
        self.assertTrue(store2.verify_integrity('cfg'))

    #
    # Export / import consistency.
    #

    def test_export_matches_read_generation(self):
        store, _ = self.create_doc()
        store.commit('cfg', [replace('/services/a/timeout', 1)], 0)
        store.commit('cfg', [add('/services/c', {'timeout': 3})], 1)
        view = store.read('cfg')
        bundle = store.export_patches('cfg')

        self.assertEqual(bundle['doc_id'], 'cfg')
        self.assertEqual(bundle['generation'], view.generation)
        self.assertEqual(bundle['checksum'], checksum_of(view.data))
        self.assertEqual(bundle['data'], view.data)
        # Replaying the exported snapshot + patch tail gives identical JSON.
        self.assertEqual(replay_bundle(bundle), view.data)
        self.assertEqual(
            json.dumps(replay_bundle(bundle), sort_keys=True,
                       separators=(',', ':')),
            json.dumps(view.data, sort_keys=True, separators=(',', ':')))

    def test_export_after_compaction(self):
        store, _ = self.create_doc()
        store.commit('cfg', [add('/x', 1)], 0)
        store.compact('cfg')
        store.commit('cfg', [add('/y', 2)], 1)
        view = store.read('cfg')
        bundle = store.export_patches('cfg')
        self.assertEqual(bundle['snapshot']['generation'], 1)
        self.assertEqual(len(bundle['patches']), 1)
        self.assertEqual(replay_bundle(bundle), view.data)

    def test_import_bundle_roundtrip(self):
        store, _ = self.create_doc()
        store.commit('cfg', [replace('/services/a/timeout', 1)], 0)
        store.commit('cfg', [add('/services/c', {'timeout': 3})], 1)
        bundle = store.export_patches('cfg')

        imported_store = self.create_store(table_prefix='jsonp_imp')
        view = imported_store.import_bundle(bundle)
        self.assertIsInstance(view, DocumentView)
        self.assertEqual(view.generation, bundle['generation'])
        self.assertEqual(view.data, bundle['data'])
        self.assertTrue(imported_store.verify_integrity('cfg'))
        self.assertEqual(imported_store.export_patches('cfg')['patches'],
                         bundle['patches'])

    def test_verify_integrity_detects_tampering(self):
        store, _ = self.create_doc()
        store.commit('cfg', [add('/x', 1)], 0)
        self.assertTrue(store.verify_integrity('cfg'))

        row = store.Patch.get()
        row.value = json.dumps(999)
        row.save()
        self.assertRaises(JsonPatchError, store.verify_integrity, 'cfg')

        # Restore the patch, then tamper with the head checksum instead.
        row.value = json.dumps(1)
        row.save()
        self.assertTrue(store.verify_integrity('cfg'))
        doc = store.Document.get_by_id('cfg')
        doc.checksum = '0' * 64
        doc.save()
        self.assertRaises(JsonPatchError, store.verify_integrity, 'cfg')

    #
    # Real multi-connection concurrency.
    #

    def test_threaded_disjoint_writers_all_commit(self):
        db, path = self.file_database()
        n_threads, n_commits = 6, 8
        initial = json.loads(json.dumps(INITIAL))
        # Pre-create each worker's root object so its per-key patches have an
        # existing parent (RFC 6902 does not create missing parents).
        initial.update({'t%d' % i: {} for i in range(n_threads)})
        bootstrap = JsonPatchStore(db)
        bootstrap.create_document('cfg', initial)
        db.close()

        errors = []
        barrier = threading.Barrier(n_threads)

        def worker(tid):
            worker_db, _ = self.file_database()
            worker_db.init(path)
            store = JsonPatchStore(worker_db)
            barrier.wait()
            try:
                # Every worker deliberately keeps the original base
                # generation; the paths are disjoint so all must merge.
                base = store.read('cfg').generation
                for j in range(n_commits):
                    store.commit('cfg', [add('/t%d/k%d' % (tid, j), j)],
                                 base)
            except Exception as exc:
                errors.append(exc)
            finally:
                worker_db.close()

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(n_threads)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        checker_db, _ = self.file_database()
        checker_db.init(path)
        checker = JsonPatchStore(checker_db)
        view = checker.read('cfg')
        self.assertEqual(view.generation, n_threads * n_commits)
        for tid in range(n_threads):
            for j in range(n_commits):
                self.assertEqual(view.data['t%d' % tid]['k%d' % j], j)
        # Original and unmanaged content survives.
        self.assertEqual(view.data['unmanaged'], INITIAL['unmanaged'])
        self.assertTrue(checker.verify_integrity('cfg'))

    def test_threaded_same_path_single_winner(self):
        db, path = self.file_database()
        bootstrap = JsonPatchStore(db)
        bootstrap.create_document('cfg', {'shared': {}})
        db.close()

        n_threads = 8
        outcomes = []
        lock = threading.Lock()
        barrier = threading.Barrier(n_threads)

        def worker(tid):
            worker_db, _ = self.file_database()
            worker_db.init(path)
            store = JsonPatchStore(worker_db)
            barrier.wait()
            try:
                store.commit('cfg', [add('/shared/x', tid)], 0)
                with lock:
                    outcomes.append('ok')
            except PathConflictError:
                with lock:
                    outcomes.append('conflict')
            finally:
                worker_db.close()

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(n_threads)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(outcomes.count('ok'), 1)
        self.assertEqual(outcomes.count('conflict'), n_threads - 1)

        checker_db, _ = self.file_database()
        checker_db.init(path)
        checker = JsonPatchStore(checker_db)
        view = checker.read('cfg')
        self.assertEqual(view.generation, 1)
        self.assertIn(view.data['shared']['x'], range(n_threads))
        self.assertTrue(checker.verify_integrity('cfg'))
