import datetime
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
import uuid

from peewee import SqliteDatabase
from playhouse.model_versioning import ManualNameConflictError
from playhouse.model_versioning import MissingReferenceError
from playhouse.model_versioning import ModelVersionStore
from playhouse.model_versioning import NameConflictError
from playhouse.model_versioning import VerificationError
from playhouse.model_versioning import VersioningError
from playhouse.model_versioning import main as mv_main
from playhouse.model_versioning import schema_from_database


# ---------------------------------------------------------------------------
# Schema fixtures
# ---------------------------------------------------------------------------

def users_v1():
    return {
        'columns': [
            {'name': 'id', 'type': 'auto', 'primary_key': True,
             'nullable': False},
            {'name': 'user_name', 'type': 'varchar', 'max_length': 40,
             'nullable': False},
        ],
    }


def notes_v1():
    return {
        'columns': [
            {'name': 'id', 'type': 'auto', 'primary_key': True,
             'nullable': False},
            {'name': 'user_id', 'type': 'integer', 'nullable': False,
             'fk': {'table': 'users', 'column': 'id'}},
            {'name': 'body', 'type': 'text', 'nullable': True},
        ],
    }


def schema_v1():
    return {'tables': {'users': users_v1(), 'notes': notes_v1()}}


def schema_v2_rename_column():
    users = {
        'columns': [
            {'name': 'id', 'type': 'auto', 'primary_key': True,
             'nullable': False},
            {'name': 'display_name', 'type': 'varchar', 'max_length': 40,
             'nullable': False},
        ],
    }
    return {'tables': {'users': users, 'notes': notes_v1()}}


def schema_v3_rename_column_again():
    users = {
        'columns': [
            {'name': 'id', 'type': 'auto', 'primary_key': True,
             'nullable': False},
            {'name': 'full_name', 'type': 'varchar', 'max_length': 40,
             'nullable': False},
        ],
    }
    return {'tables': {'users': users, 'notes': notes_v1()}}


def isolated_import(pkg_dir):
    """Import a package directory under a unique throwaway parent name."""
    parent_name = '_mvtest_%s' % uuid.uuid4().hex
    parent = types.ModuleType(parent_name)
    parent.__path__ = [os.path.dirname(pkg_dir)]
    sys.modules[parent_name] = parent
    full_name = '%s.%s' % (parent_name, os.path.basename(pkg_dir))
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        full_name, os.path.join(pkg_dir, '__init__.py'),
        submodule_search_locations=[pkg_dir])
    module = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = module
    spec.loader.exec_module(module)
    return parent_name, module


class ModelVersioningTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix='mv_store_')
        self.store = ModelVersionStore(self.root)
        self._imports = []

    def tearDown(self):
        for parent_name in self._imports:
            for key in [key for key in sys.modules
                        if key == parent_name
                        or key.startswith(parent_name + '.')]:
                del sys.modules[key]
        shutil.rmtree(self.root, ignore_errors=True)

    def import_default(self):
        parent_name, module = isolated_import(self.root)
        self._imports.append(parent_name)
        return module

    def active_version(self):
        with open(os.path.join(self.root, '_current.json')) as handle:
            return json.load(handle)['active']

    def assert_no_half_finished(self):
        entries = os.listdir(self.root)
        self.assertFalse(
            any(entry.startswith('.staging_') for entry in entries),
            'staging directory left behind: %r' % entries)
        self.assertFalse(
            '_journal.json' in entries,
            'journal left behind after generation settled')

    def generate_v1(self):
        return self.store.generate(schema=schema_v1())

    # -- happy path ---------------------------------------------------------

    def test_first_generation_and_default_export(self):
        result = self.generate_v1()
        self.assertTrue(result.switched)
        self.assertIsNone(result.previous)

        module = self.import_default()
        self.assertEqual(module.__version_package__, result.active)
        self.assertEqual(module.Users._meta.table_name, 'users')
        self.assertEqual(
            module.Users._meta.fields['user_name'].column_name,
            'user_name')
        # Foreign keys are resolved to real model classes, not deferred.
        self.assertIs(module.Notes._meta.fields['user'].rel_model,
                      module.Users)
        self.assertIn('Users', module.__all__)
        self.assertIn('Notes', module.__all__)

    def test_column_rename_creates_field_alias(self):
        self.generate_v1()
        result = self.store.generate(
            schema=schema_v2_rename_column(),
            renames={'columns': {
                'users': {'user_name': 'display_name'}}})
        self.assertTrue(result.switched)

        module = self.import_default()
        users = module.Users
        # The new field maps to the new column.
        self.assertEqual(
            users._meta.fields['display_name'].column_name, 'display_name')
        # Old field name is still usable and points at the same field.
        self.assertIs(users.user_name, users.display_name)

        instance = users()
        instance.display_name = 'alice'
        self.assertEqual(instance.user_name, 'alice')
        instance.user_name = 'bob'
        self.assertEqual(instance.display_name, 'bob')

        module.database.initialize(SqliteDatabase(':memory:'))
        alias_sql, params = users.select().where(
            users.user_name == 'carol').sql()
        direct_sql, _ = users.select().where(
            users.display_name == 'carol').sql()
        self.assertEqual(alias_sql, direct_sql)
        self.assertIn('"display_name"', alias_sql)
        self.assertNotIn('user_name', alias_sql)
        self.assertEqual(params, ['carol'])

    def test_table_rename_creates_class_alias(self):
        self.generate_v1()
        new_schema = {'tables': {
            'accounts': users_v1(),
            'notes': {
                'columns': [
                    {'name': 'id', 'type': 'auto', 'primary_key': True,
                     'nullable': False},
                    {'name': 'user_id', 'type': 'integer', 'nullable': False,
                     'fk': {'table': 'accounts', 'column': 'id'}},
                    {'name': 'body', 'type': 'text', 'nullable': True},
                ],
            },
        }}
        result = self.store.generate(
            schema=new_schema,
            renames={'tables': {'users': 'accounts'}})
        self.assertTrue(result.switched)

        module = self.import_default()
        self.assertEqual(module.Accounts._meta.table_name, 'accounts')
        # The old class name keeps resolving to the current class.
        self.assertIs(module.Users, module.Accounts)
        self.assertIn('Users', module.__all__)
        # The dependent foreign key follows the renamed target.
        self.assertIs(module.Notes._meta.fields['user'].rel_model,
                      module.Accounts)

    def test_alias_accumulates_across_three_versions(self):
        self.generate_v1()
        self.store.generate(
            schema=schema_v2_rename_column(),
            renames={'columns': {
                'users': {'user_name': 'display_name'}}})
        third = self.store.generate(
            schema=schema_v3_rename_column_again(),
            renames={'columns': {
                'users': {'display_name': 'full_name'}}})
        self.assertTrue(third.switched)

        module = self.import_default()
        users = module.Users
        # Both historical names resolve to the current field.
        self.assertIs(users.user_name, users.full_name)
        self.assertIs(users.display_name, users.full_name)
        self.assertEqual(
            users._meta.fields['full_name'].column_name, 'full_name')

        manifest_path = os.path.join(
            self.root, third.active, '_manifest.json')
        with open(manifest_path) as handle:
            manifest = json.load(handle)
        self.assertEqual(
            manifest['aliases']['fields']['Users'],
            {'display_name': 'full_name', 'user_name': 'full_name'})

    def test_old_packages_are_retained(self):
        first = self.generate_v1()
        second = self.store.generate(
            schema=schema_v2_rename_column(),
            renames={'columns': {
                'users': {'user_name': 'display_name'}}})
        versions = self.store.list_versions()
        self.assertIn(first.active, versions)
        self.assertIn(second.active, versions)
        self.assertEqual(len(versions), 2)

    def test_idempotent_rerun_creates_no_new_version(self):
        first = self.generate_v1()
        second = self.store.generate(schema=schema_v1())
        self.assertFalse(second.switched)
        self.assertEqual(second.active, first.active)
        self.assertEqual(len(self.store.list_versions()), 1)
        self.assert_no_half_finished()

    # -- abort conditions ---------------------------------------------------

    def test_class_name_collision_aborts_and_keeps_old(self):
        self.generate_v1()
        conflicting = {'tables': dict(schema_v1()['tables'], **{
            'audits': {
                'class_name': 'Users',
                'columns': [
                    {'name': 'id', 'type': 'auto', 'primary_key': True,
                     'nullable': False},
                    {'name': 'note', 'type': 'text', 'nullable': True},
                ],
            },
        })}
        with self.assertRaises(NameConflictError):
            self.store.generate(schema=conflicting)
        self.assertEqual(self.active_version(), self.store.list_versions()[0])
        module = self.import_default()
        self.assertEqual(module.Users._meta.table_name, 'users')
        self.assert_no_half_finished()

    def test_field_name_collision_aborts_and_keeps_old(self):
        self.generate_v1()
        conflicting = schema_v2_rename_column()
        # Two columns normalize onto the same python attribute.
        conflicting['tables']['users']['columns'][1]['field_name'] = 'id'
        with self.assertRaises(NameConflictError):
            self.store.generate(schema=conflicting)
        module = self.import_default()
        self.assertEqual(
            module.Users._meta.fields['user_name'].column_name, 'user_name')
        self.assert_no_half_finished()

    def test_manual_class_name_is_preserved_then_conflict_aborts(self):
        self.store.register_manual_class('LegacyUsers', table='users')
        result = self.generate_v1()
        module = self.import_default()
        self.assertIs(module.LegacyUsers, module.Users)

        # A different table trying to claim the manual name must abort.
        hijack = {'tables': dict(schema_v1()['tables'], **{
            'audits': {
                'class_name': 'LegacyUsers',
                'columns': [
                    {'name': 'id', 'type': 'auto', 'primary_key': True,
                     'nullable': False},
                ],
            },
        })}
        with self.assertRaises(ManualNameConflictError):
            self.store.generate(schema=hijack)
        self.assertEqual(self.active_version(), result.active)
        self.assert_no_half_finished()

    def test_manual_field_name_is_preserved_then_conflict_aborts(self):
        self.store.register_manual_field('users', 'login',
                                         column='user_name')
        self.generate_v1()
        module = self.import_default()
        self.assertIs(module.Users.login, module.Users.user_name)

        # A new column trying to claim the manual attribute name aborts.
        hijack = schema_v1()
        hijack['tables']['users']['columns'].append({
            'name': 'last_login', 'type': 'varchar', 'max_length': 20,
            'nullable': True, 'field_name': 'login',
        })
        with self.assertRaises(ManualNameConflictError):
            self.store.generate(schema=hijack)
        self.assert_no_half_finished()

    def test_missing_active_package_aborts(self):
        result = self.generate_v1()
        ghost = 'v_19990101000000_ghostghost'
        self.store._atomic_json(
            os.path.join(self.root, '_current.json'),
            {'active': ghost, 'previous': None, 'history': []})
        with self.assertRaises(MissingReferenceError):
            self.store.generate(schema=schema_v1())
        # Pointer is untouched and the old validated package still exists.
        self.assertEqual(self.active_version(), ghost)
        self.assertTrue(
            os.path.isdir(os.path.join(self.root, result.active)))

    def test_corrupt_pointer_without_journal_aborts(self):
        self.generate_v1()
        with open(os.path.join(self.root, '_current.json'), 'w') as handle:
            handle.write('{this is not json')
        with self.assertRaises(VersioningError):
            self.store.generate(schema=schema_v1())
        self.assert_no_half_finished()

    def test_dropped_table_aborts_without_hint_succeeds_with_hint(self):
        self.generate_v1()
        dropped = {'tables': {'users': users_v1()}}
        with self.assertRaises(MissingReferenceError):
            self.store.generate(schema=dropped)
        self.assert_no_half_finished()

        # Declaring the drop as a rename carries the old names forward.
        renamed = {'tables': {'users': users_v1(), 'memos': notes_v1()}}
        result = self.store.generate(
            schema=renamed,
            renames={'tables': {'notes': 'memos'}})
        self.assertTrue(result.switched)
        module = self.import_default()
        self.assertIs(module.Notes, module.Memos)
        self.assertEqual(module.Memos._meta.table_name, 'memos')

    def test_verification_failure_keeps_old_package(self):
        first = self.generate_v1()

        class BrokenStore(ModelVersionStore):
            def _render_generated(self, prepared):
                return super(BrokenStore, self)._render_generated(
                    prepared) + '\nraise RuntimeError("boom")\n'

        broken = BrokenStore(self.root)
        with self.assertRaises(VerificationError):
            broken.generate(
                schema=schema_v2_rename_column(),
                renames={'columns': {
                    'users': {'user_name': 'display_name'}}})
        self.assertEqual(self.active_version(), first.active)
        module = self.import_default()
        self.assertEqual(module.__version_package__, first.active)
        self.assert_no_half_finished()

    # -- crash recovery / rollback -----------------------------------------

    def _stage_orphan_version(self, version, schema, renames=None):
        """Run the internal prepare/write pipeline without touching pointer."""
        prepared = self.store._prepare(schema)
        previous = self.store._load_manifest(self.store._active_version())
        manual = self.store._read_json(
            os.path.join(self.root, '_manual.json'))
        plan = self.store._plan_aliases(prepared, previous, manual,
                                        renames or {})
        digest = self.store._content_hash(prepared, plan, manual)
        staging = self.store._path('.staging_' + version)
        self.store._write_version(staging, version, prepared, plan, digest,
                                  renames or {})
        self.store._verify_version(staging, version, prepared, plan)
        os.replace(staging, self.store._path(version))
        return prepared, plan, digest

    def test_recover_from_promoted_journal(self):
        first = self.generate_v1()
        orphan = 'v_19990101000000_aaaaaaaaaa'
        self._stage_orphan_version(
            orphan, schema_v2_rename_column(),
            renames={'columns': {'users': {'user_name': 'display_name'}}})
        self.store._atomic_json(os.path.join(self.root, '_journal.json'), {
            'state': 'promoted', 'version': orphan,
            'staging': self.store._path('.staging_' + orphan),
            'previous': first.active,
        })

        self.store._recover()

        self.assertFalse(os.path.exists(self.store._path(orphan)))
        self.assertFalse(
            os.path.exists(os.path.join(self.root, '_journal.json')))
        self.assertEqual(self.active_version(), first.active)
        # A rerun converges and finally completes the intended switch.
        result = self.store.generate(
            schema=schema_v2_rename_column(),
            renames={'columns': {'users': {'user_name': 'display_name'}}})
        self.assertTrue(result.switched)
        module = self.import_default()
        self.assertIs(module.Users.user_name, module.Users.display_name)
        self.assert_no_half_finished()

    def test_recover_from_switched_journal(self):
        self.generate_v1()
        orphan = 'v_19990101000000_bbbbbbbbbb'
        self._stage_orphan_version(
            orphan, schema_v2_rename_column(),
            renames={'columns': {'users': {'user_name': 'display_name'}}})
        self.store._atomic_json(os.path.join(self.root, '_current.json'), {
            'active': orphan, 'previous': None, 'history': []})
        self.store._atomic_json(os.path.join(self.root, '_journal.json'), {
            'state': 'switched', 'version': orphan, 'staging': None,
            'previous': None,
        })

        self.store._recover()

        self.assertFalse(
            os.path.exists(os.path.join(self.root, '_journal.json')))
        module = self.import_default()
        self.assertEqual(module.__version_package__, orphan)
        self.assertIs(module.Users.user_name, module.Users.display_name)
        self.assert_no_half_finished()

    def test_corrupt_compat_layer_rolls_back_on_rerun(self):
        first = self.generate_v1()
        second = self.store.generate(
            schema=schema_v2_rename_column(),
            renames={'columns': {
                'users': {'user_name': 'display_name'}}})

        # Simulate a damaged compatibility layer on the active package.
        compat_path = os.path.join(self.root, second.active, '_compat.py')
        with open(compat_path, 'w') as handle:
            handle.write('def broken(:\n')

        # Rerun with another rename: recovery must restore the previous good
        # package, then complete the new generation from that baseline.
        third = self.store.generate(
            schema=schema_v3_rename_column_again(),
            renames={'columns': {'users': {'user_name': 'full_name'}}})
        self.assertTrue(third.switched)

        module = self.import_default()
        self.assertEqual(module.__version_package__, third.active)
        # The v1 name still resolves because aliases accumulate.
        self.assertIs(module.Users.user_name, module.Users.full_name)
        self.assertEqual(
            module.Users._meta.fields['full_name'].column_name, 'full_name')
        # The broken package and the first package both remain on disk.
        self.assertTrue(
            os.path.isdir(os.path.join(self.root, first.active)))
        self.assertTrue(
            os.path.isdir(os.path.join(self.root, second.active)))
        self.assert_no_half_finished()

    def test_explicit_rollback_restores_previous_export(self):
        from unittest import mock
        from playhouse import model_versioning as mv

        class FrozenDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.datetime(2026, 1, 1, 0, 0, 0,
                                         tzinfo=datetime.timezone.utc)

        with mock.patch.object(mv.datetime, 'datetime', FrozenDateTime):
            first = self.generate_v1()
            second = self.store.generate(
                schema=schema_v2_rename_column(),
                renames={'columns': {
                    'users': {'user_name': 'display_name'}}})
            result = self.store.rollback()
            self.assertTrue(result.switched)
            self.assertEqual(result.active, first.active)
            module = self.import_default()
            self.assertEqual(module.__version_package__, first.active)
            self.assertFalse(hasattr(module.Users, 'display_name'))
            self.assertIn('user_name', module.Users._meta.fields)
            self.assert_no_half_finished()
            # Rolling forward again reuses the retained, identical package.
            again = self.store.generate(
                schema=schema_v2_rename_column(),
                renames={'columns': {
                    'users': {'user_name': 'display_name'}}})
        self.assertTrue(again.switched)
        self.assertEqual(again.active, second.active)
        self.assertEqual(len(self.store.list_versions()), 2)
        self.assert_no_half_finished()

    def test_two_services_see_the_same_active_package(self):
        self.generate_v1()
        self.store.generate(
            schema=schema_v2_rename_column(),
            renames={'columns': {
                'users': {'user_name': 'display_name'}}})
        service_a = self.import_default()
        service_b = self.import_default()
        self.assertEqual(service_a.__version_package__,
                         service_b.__version_package__)
        self.assertEqual(service_a.__version_package__, self.active_version())
        for module in (service_a, service_b):
            self.assertEqual(module.Users._meta.table_name, 'users')
            self.assertIs(module.Users.user_name,
                          module.Users.display_name)

    # -- structural coverage ------------------------------------------------

    def test_foreign_keys_and_self_reference(self):
        from peewee import DeferredForeignKey
        deferred_before = set(DeferredForeignKey._unresolved)
        schema = {'tables': {
            'categories': {
                'columns': [
                    {'name': 'id', 'type': 'auto', 'primary_key': True,
                     'nullable': False},
                    {'name': 'parent_id', 'type': 'integer', 'nullable': True,
                     'fk': {'table': 'categories', 'column': 'id'}},
                    {'name': 'label', 'type': 'varchar', 'max_length': 20,
                     'nullable': False},
                ],
            },
            'items': {
                'columns': [
                    {'name': 'id', 'type': 'auto', 'primary_key': True,
                     'nullable': False},
                    {'name': 'category_id', 'type': 'integer',
                     'nullable': False,
                     'fk': {'table': 'categories', 'column': 'id'}},
                ],
            },
        }}
        result = self.store.generate(schema=schema)
        module = self.import_default()
        self.assertIs(module.Categories.parent.rel_model, module.Categories)
        self.assertIs(module.Items.category.rel_model, module.Categories)
        # The generated package must not itself leave foreign keys deferred
        # (the process-global set may hold leftovers from other modules).
        self.assertEqual(
            set(DeferredForeignKey._unresolved) - deferred_before, set())
        self.assertEqual(result.active, self.active_version())

    def test_composite_primary_key_and_no_pk_tables(self):
        schema = {'tables': {
            'memberships': {
                'columns': [
                    {'name': 'group_id', 'type': 'integer',
                     'primary_key': True, 'nullable': False},
                    {'name': 'user_id', 'type': 'integer',
                     'primary_key': True, 'nullable': False},
                ],
            },
            'settings': {
                'columns': [
                    {'name': 'key', 'type': 'varchar', 'max_length': 20,
                     'nullable': False},
                    {'name': 'value', 'type': 'text', 'nullable': True},
                ],
            },
        }}
        self.store.generate(schema=schema)
        module = self.import_default()
        self.assertTrue(module.Memberships._meta.composite_key)
        self.assertEqual(
            set(module.Memberships._meta.primary_key.field_names),
            {'group_id', 'user_id'})
        self.assertFalse(module.Settings._meta.primary_key)

    def test_schema_from_database_matches_pwiz_names(self):
        db_path = os.path.join(self.root, 'introspect.db')
        database = SqliteDatabase(db_path)
        database.execute_sql(
            'CREATE TABLE users (id INTEGER PRIMARY KEY, '
            'user_name VARCHAR(40) NOT NULL)')
        database.execute_sql(
            'CREATE TABLE notes (id INTEGER PRIMARY KEY, '
            'user_id INTEGER NOT NULL REFERENCES users(id))')
        database.close()

        descriptor = schema_from_database(SqliteDatabase(db_path))
        users = descriptor['tables']['users']
        self.assertEqual(users['class_name'], 'Users')
        self.assertEqual(
            users['columns'][1]['field_name'], 'user_name')
        notes = descriptor['tables']['notes']
        note_columns = dict(
            (column['name'], column) for column in notes['columns'])
        self.assertEqual(note_columns['user_id']['field_name'], 'user')
        self.assertEqual(
            note_columns['user_id']['fk'],
            {'table': 'users', 'column': 'id'})

        # The introspected descriptor drives a normal generation.
        result = self.store.generate(
            schema=descriptor)
        module = self.import_default()
        self.assertEqual(module.__version_package__, result.active)
        self.assertIs(module.Notes._meta.fields['user'].rel_model,
                      module.Users)

    def test_sqlite_database_roundtrip_with_rename(self):
        db_path = os.path.join(self.root, 'app.db')
        database = SqliteDatabase(db_path)
        database.execute_sql(
            'CREATE TABLE users (id INTEGER PRIMARY KEY, '
            'user_name VARCHAR(40) NOT NULL)')
        database.execute_sql(
            "INSERT INTO users (user_name) VALUES ('alice')")
        database.close()

        first = self.store.generate(database=SqliteDatabase(db_path))
        module = self.import_default()
        module.database.initialize(SqliteDatabase(db_path))
        self.assertEqual(module.Users.get().user_name, 'alice')

        # Rename the column in the live database, then regenerate with hint.
        migrator = SqliteDatabase(db_path)
        migrator.execute_sql(
            'ALTER TABLE users RENAME COLUMN user_name TO display_name')
        migrator.close()

        second = self.store.generate(
            database=SqliteDatabase(db_path),
            renames={'columns': {
                'users': {'user_name': 'display_name'}}})
        self.assertTrue(second.switched)
        module = self.import_default()
        module.database.initialize(SqliteDatabase(db_path))

        row = module.Users.get()
        self.assertEqual(row.display_name, 'alice')
        # Old code using the old field name keeps working against the DB.
        self.assertEqual(row.user_name, 'alice')
        module.Users.create(**{'user_name': 'bob'})
        self.assertEqual(
            module.Users.select().where(
                module.Users.user_name == 'bob').count(), 1)
        self.assertEqual(first.active != second.active, True)

    def test_cli_generate(self):
        descriptor_path = os.path.join(self.root, 'schema.json')
        with open(descriptor_path, 'w') as handle:
            json.dump(schema_v1(), handle)
        exit_code = mv_main([
            'generate', '--root', self.root,
            '--schema', descriptor_path,
        ])
        self.assertEqual(exit_code, 0)
        module = self.import_default()
        self.assertEqual(module.Users._meta.table_name, 'users')
        self.assert_no_half_finished()


if __name__ == '__main__':
    unittest.main()
