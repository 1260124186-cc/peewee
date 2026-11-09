"""
Versioned, crash-safe generation of peewee model packages.

Two services may share the same auto-generated peewee models. Overwriting a
single module in place breaks every still-running service as soon as a table
or column is renamed. This module instead generates an immutable, versioned
package on every run, records a persistent rename-compatibility layer (old
class names and old field names keep working) and only flips the stable
default export once the new package has been verified to import and to map
onto the current schema.

Store layout::

    <root>/
      __init__.py          # stable default export, resolves _current.json
      _current.json        # pointer to the active version (atomically swapped)
      _journal.json        # exists only while a switch is in flight
      _manual.json         # hand-maintained names that must keep resolving
      _lock                # inter-process lock (flock where available)
      v_20261109_103000_<hash>/
        __init__.py        # public surface: models + compatibility aliases
        _generated.py      # generated peewee models (database is a Proxy)
        _compat.py         # old class / field name compatibility layer
        _manifest.json     # full schema + alias snapshot of this version
      .staging_<version>/  # verified in place, never reachable via export

Switching is journaled and atomic. Re-running after a crash, a broken
compatibility layer or a failed switch always converges: the default export
(and therefore both services) resolves to exactly one complete, validated
package, with no half-finished staging directories left behind.
"""

import contextlib
import datetime
import hashlib
import importlib
import importlib.util
import json
import os
import py_compile
import re
import shutil
import sys
import tempfile
import types
import uuid
from collections import OrderedDict
from collections import namedtuple

from peewee import AutoField
from peewee import BareField
from peewee import BigAutoField
from peewee import BigIntegerField
from peewee import BlobField
from peewee import BooleanField
from peewee import CharField
from peewee import DateField
from peewee import DateTimeField
from peewee import DecimalField
from peewee import DeferredForeignKey
from peewee import DoubleField
from peewee import FloatField
from peewee import IntegerField
from peewee import Model
from peewee import SmallIntegerField
from peewee import SqliteDatabase
from peewee import TextField
from peewee import TimeField
from peewee import UUIDField
from peewee import make_snake_case
from playhouse.reflection import Introspector
from playhouse.reflection import RESERVED_WORDS


# ---------------------------------------------------------------------------
# File names and constants
# ---------------------------------------------------------------------------

CURRENT_POINTER = '_current.json'
JOURNAL_FILE = '_journal.json'
MANUAL_REGISTRY = '_manual.json'
LOCK_FILE = '_lock'
STAGING_PREFIX = '.staging_'
MANIFEST_NAME = '_manifest.json'
GENERATED_MODULE = '_generated'
COMPAT_MODULE = '_compat'
MAX_HISTORY = 20


GenerationResult = namedtuple('GenerationResult',
                              ['version', 'active', 'switched', 'previous'])


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class VersioningError(Exception):
    """Base class for model versioning failures. The pointer never moved."""


class NameConflictError(VersioningError):
    """Two different objects contend for the same exported Python name."""


class ManualNameConflictError(VersioningError):
    """A hand-maintained name would be claimed by a different object."""


class MissingReferenceError(VersioningError):
    """A previous package / historical name cannot be resolved."""


class VerificationError(VersioningError):
    """A generated package failed import or schema-mapping verification."""


# ---------------------------------------------------------------------------
# Field alias descriptor (imported by every generated _compat.py)
# ---------------------------------------------------------------------------

class FieldAlias(object):
    """Expose a renamed field under its old Python name.

    Class-level access returns the real current field, so query expressions
    such as ``Model.old_field == value`` keep working and compile against the
    current column. Instance access proxies reads and writes to the current
    attribute. This is intentionally not a :class:`Field` subclass, so the
    model metaclass never registers a second field for the same column.
    """
    def __init__(self, target_name):
        self._target_name = target_name

    def __get__(self, instance, owner):
        if instance is None:
            return getattr(owner, self._target_name)
        return getattr(instance, self._target_name)

    def __set__(self, instance, value):
        setattr(instance, self._target_name, value)

    def __repr__(self):
        return '<FieldAlias -> %s>' % self._target_name


# ---------------------------------------------------------------------------
# Naming helpers (mirror the pwiz / reflection naming conventions)
# ---------------------------------------------------------------------------

def make_model_name(table, snake_case=True):
    if snake_case:
        table = make_snake_case(table)
    cleaned = re.sub(r'[^\w]+', '', table)
    name = ''.join(part.title() for part in cleaned.split('_'))
    if not name or not name[0].isalpha():
        name = 'T' + name
    return name


def make_field_name(column, is_foreign_key=False, snake_case=True):
    column = column.strip()
    if snake_case:
        column = make_snake_case(column)
    column = column.lower()
    if is_foreign_key:
        column = re.sub('_id$', '', column) or column
    column = re.sub(r'[^\w]+', '_', column)
    if column in RESERVED_WORDS:
        column += '_'
    if column and column[0].isdigit():
        column = '_' + column
    return column


# Descriptor column type -> peewee field class used when rendering source.
FIELD_TYPE_MAP = {
    'auto': AutoField,
    'bigauto': BigAutoField,
    'integer': IntegerField,
    'bigint': BigIntegerField,
    'smallint': SmallIntegerField,
    'varchar': CharField,
    'char': CharField,
    'text': TextField,
    'bool': BooleanField,
    'boolean': BooleanField,
    'float': FloatField,
    'double': DoubleField,
    'decimal': DecimalField,
    'date': DateField,
    'datetime': DateTimeField,
    'time': TimeField,
    'blob': BlobField,
    'uuid': UUIDField,
    'bare': BareField,
}

# Reverse map used when turning an introspected database into a descriptor.
FIELD_CLASS_TO_TYPE = {
    AutoField: 'auto',
    BigAutoField: 'bigauto',
    IntegerField: 'integer',
    BigIntegerField: 'bigint',
    SmallIntegerField: 'smallint',
    CharField: 'varchar',
    TextField: 'text',
    BooleanField: 'bool',
    FloatField: 'float',
    DoubleField: 'double',
    DecimalField: 'decimal',
    DateField: 'date',
    DateTimeField: 'datetime',
    TimeField: 'time',
    BlobField: 'blob',
    UUIDField: 'uuid',
    BareField: 'bare',
}


def schema_from_database(database, table_names=None, include_views=False,
                         schema=None):
    """Introspect a live database into the framework-neutral descriptor.

    The descriptor carries the exact class/field names that pwiz would
    generate, so database-driven and descriptor-driven generations produce
    identical packages.
    """
    introspector = Introspector.from_database(database, schema=schema)
    metadata = introspector.introspect(table_names=table_names,
                                       include_views=include_views)
    tables = OrderedDict()
    for table in sorted(metadata.columns):
        columns = []
        primary_keys = set(metadata.primary_keys[table])
        for column_name, column in metadata.columns[table].items():
            entry = {
                'name': column_name,
                'field_name': column.name,
                'type': FIELD_CLASS_TO_TYPE.get(column.field_class, 'bare'),
                'nullable': bool(column.nullable),
                'primary_key': column_name in primary_keys,
            }
            extra = column.extra_parameters or {}
            for key in ('max_length', 'max_digits', 'decimal_places'):
                if isinstance(extra.get(key), int):
                    entry[key] = extra[key]
            if column.is_foreign_key():
                entry['fk'] = {
                    'table': column.foreign_key.dest_table,
                    'column': column.foreign_key.dest_column,
                }
                entry['type'] = 'integer'
            columns.append(entry)
        tables[table] = {
            'class_name': metadata.model_names[table],
            'columns': columns,
        }
    return {'tables': tables}


# ---------------------------------------------------------------------------
# Inter-process lock
# ---------------------------------------------------------------------------

try:
    import fcntl

    class _FileLock(object):
        def __init__(self, path):
            self._path = path
            self._handle = None

        def __enter__(self):
            self._handle = open(self._path, 'a+')
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
            return self

        def __exit__(self, *exc_info):
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None

except ImportError:  # pragma: no cover - Windows fallback.
    class _FileLock(object):
        def __init__(self, path):
            self._path = path

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            pass


# ---------------------------------------------------------------------------
# Static content of the stable default export
# ---------------------------------------------------------------------------

DEFAULT_INIT = '''\
"""Stable default export of the versioned peewee model store.

The active version is read from ``_current.json`` at import time and swapped
atomically by the generator, so every service importing this package sees
exactly the same validated model set.
"""
import importlib as _importlib
import json as _json
import os as _os

_HERE = _os.path.dirname(__file__)


def _load_active():
    pointer_path = _os.path.join(_HERE, "_current.json")
    try:
        with open(pointer_path) as handle:
            active = _json.load(handle)["active"]
    except (FileNotFoundError, KeyError, ValueError) as exc:
        raise ImportError("No active model version in %r" % _HERE) from exc
    return _importlib.import_module("%s.%s" % (__name__, active))


_active = _load_active()
__version_package__ = _active.__version__
__all__ = list(getattr(_active, "__all__", []))
for _exported_name in __all__:
    globals()[_exported_name] = getattr(_active, _exported_name)


def __getattr__(_name):
    return getattr(_active, _name)
'''


class ModelVersionStore(object):
    """Generate, verify and atomically switch versioned model packages."""

    def __init__(self, root):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        init_path = os.path.join(self.root, '__init__.py')
        if not os.path.exists(init_path):
            self._atomic_write(init_path, DEFAULT_INIT)
        if not os.path.exists(self._path(LOCK_FILE)):
            open(self._path(LOCK_FILE), 'a+').close()
        if not os.path.exists(self._path(MANUAL_REGISTRY)):
            self._atomic_json(self._path(MANUAL_REGISTRY),
                              {'classes': {}, 'fields': {}})

    # -- paths / low level helpers -----------------------------------------

    def _path(self, name):
        return os.path.join(self.root, name)

    @contextlib.contextmanager
    def _locked(self):
        with _FileLock(self._path(LOCK_FILE)):
            yield

    @staticmethod
    def _atomic_write(path, text):
        directory = os.path.dirname(path) or '.'
        fd, tmp = tempfile.mkstemp(dir=directory, prefix='.tmp_')
        try:
            with os.fdopen(fd, 'w') as handle:
                handle.write(text)
            os.replace(tmp, path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def _atomic_json(self, path, payload):
        self._atomic_write(path, json.dumps(payload, indent=2, sort_keys=True))

    @staticmethod
    def _read_json(path):
        with open(path) as handle:
            return json.load(handle)

    def _remove_journal(self):
        path = self._path(JOURNAL_FILE)
        if os.path.exists(path):
            os.unlink(path)

    def _remove_tree(self, path):
        if path and os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)

    # -- manual name registry ----------------------------------------------

    def register_manual_class(self, name, table=None):
        """Record a hand-maintained class export that must keep resolving.

        ``table`` is the database table backing the name (``None`` means the
        name must remain a generated class name verbatim).
        """
        registry = self._read_json(self._path(MANUAL_REGISTRY))
        registry['classes'][name] = table
        self._atomic_json(self._path(MANUAL_REGISTRY), registry)

    def register_manual_field(self, table, name, column=None):
        """Record a hand-maintained field attribute on ``table``'s model."""
        registry = self._read_json(self._path(MANUAL_REGISTRY))
        registry['fields'].setdefault(table, {})[name] = column
        self._atomic_json(self._path(MANUAL_REGISTRY), registry)

    # -- schema preparation / collision detection --------------------------

    @staticmethod
    def _prepare(schema):
        """Normalize the descriptor and reject same-name contention."""
        prepared = OrderedDict()
        class_owner = {}
        for table in sorted(schema['tables']):
            spec = schema['tables'][table]
            class_name = spec.get('class_name') or make_model_name(table)
            if class_name in class_owner:
                raise NameConflictError(
                    'Class name %r is claimed by both tables %r and %r.' % (
                        class_name, class_owner[class_name], table))
            class_owner[class_name] = table

            columns = OrderedDict()
            field_owner = {}
            for column in spec['columns']:
                column_name = column['name']
                is_fk = 'fk' in column
                field_name = column.get('field_name') or make_field_name(
                    column_name, is_fk)
                if field_name in field_owner:
                    raise NameConflictError(
                        'Field name %r on table %r is claimed by both '
                        'columns %r and %r.' % (
                            field_name, table,
                            field_owner[field_name], column_name))
                field_owner[field_name] = column_name
                entry = dict(column)
                entry['field'] = field_name
                columns[column_name] = entry
            prepared[table] = OrderedDict([
                ('class_name', class_name),
                ('columns', columns),
            ])
        return prepared

    def _plan_aliases(self, prepared, previous, manual, renames):
        """Compute every name the new package must export.

        Historical names accumulate across versions: a name renamed several
        times still resolves to the current object. Any name two different
        objects contend for aborts the switch. Hand-maintained names are
        claimed before historical ones, so any contention on them always
        surfaces as :class:`ManualNameConflictError`.
        """
        table_renames = renames.get('tables', {})       # old table -> new
        column_renames = renames.get('columns', {})     # new table -> old->new
        new_to_old_table = dict(
            (new_name, old_name)
            for old_name, new_name in table_renames.items())

        # --- class-level exports ------------------------------------------
        class_exports = {}
        manual_class_names = set(manual.get('classes', {}))

        def declare_class(name, current_class, conflict_exc):
            if name in manual_class_names:
                conflict_exc = ManualNameConflictError
            existing = class_exports.get(name)
            if existing is not None and existing != current_class:
                raise conflict_exc(
                    'Class export %r contends between %r and %r.' % (
                        name, existing, current_class))
            class_exports[name] = current_class

        # 1. Names generated from the current schema.
        for info in prepared.values():
            declare_class(info['class_name'], info['class_name'],
                          NameConflictError)

        # 2. Hand-maintained names.
        for name, target_table in manual.get('classes', {}).items():
            if target_table is None:
                if name not in class_exports:
                    raise MissingReferenceError(
                        'Manual class name %r does not resolve to any '
                        'generated class.' % name)
                continue
            new_table = table_renames.get(target_table, target_table)
            if new_table not in prepared:
                raise MissingReferenceError(
                    'Manual class name %r references missing table %r.'
                    % (name, target_table))
            declare_class(name, prepared[new_table]['class_name'],
                          ManualNameConflictError)

        # 3. Historical names carried over from the previous package.
        if previous is not None:
            prev_schema = previous['schema']
            class_to_table = dict(
                (info['class_name'], table)
                for table, info in prev_schema.items())

            def lineage_table(old_table):
                new_table = table_renames.get(old_table, old_table)
                if new_table not in prepared:
                    raise MissingReferenceError(
                        'Previous table %r no longer exists; cannot carry '
                        'its old class names forward. Provide a table '
                        'rename hint or retire the dependent services.'
                        % old_table)
                return new_table

            for old_table, info in prev_schema.items():
                new_table = lineage_table(old_table)
                declare_class(info['class_name'],
                              prepared[new_table]['class_name'],
                              NameConflictError)
            for alias, target_class in previous['aliases']['classes'].items():
                old_table = class_to_table.get(target_class)
                if old_table is None:
                    raise MissingReferenceError(
                        'Previous compatibility alias %r points at class %r '
                        'which is missing from the previous package.'
                        % (alias, target_class))
                new_table = lineage_table(old_table)
                declare_class(alias,
                              prepared[new_table]['class_name'],
                              NameConflictError)

        # --- field-level exports, per table -------------------------------
        field_aliases = {}
        for new_table, info in prepared.items():
            class_name = info['class_name']
            attr_exports = {}
            col_map = column_renames.get(new_table, {})
            manual_attr_names = set(
                manual.get('fields', {}).get(new_table, {}))

            def declare_field(attr, current_field, conflict_exc):
                if attr in manual_attr_names:
                    conflict_exc = ManualNameConflictError
                existing = attr_exports.get(attr)
                if existing is not None and existing != current_field:
                    raise conflict_exc(
                        'Field export %r on %r contends between %r and %r.'
                        % (attr, class_name, existing, current_field))
                attr_exports[attr] = current_field

            # 1. Current field names.
            for column in info['columns'].values():
                declare_field(column['field'], column['field'],
                              NameConflictError)

            # 2. Hand-maintained field names (column target follows renames).
            for attr, target_column in manual.get('fields', {}) \
                    .get(new_table, {}).items():
                if target_column is None:
                    if attr not in attr_exports:
                        raise MissingReferenceError(
                            'Manual field %r on table %r does not resolve.'
                            % (attr, new_table))
                    continue
                target_column = col_map.get(target_column, target_column)
                if target_column not in info['columns']:
                    raise MissingReferenceError(
                        'Manual field %r references missing column %r on '
                        'table %r.' % (attr, target_column, new_table))
                declare_field(attr,
                              info['columns'][target_column]['field'],
                              ManualNameConflictError)

            # 3. Historical field names.
            old_table = new_to_old_table.get(new_table, new_table)
            if previous is not None and old_table in previous['schema']:
                prev_info = previous['schema'][old_table]
                prev_col_to_field = dict(
                    (col, data['field'])
                    for col, data in prev_info['columns'].items())
                prev_field_to_col = dict(
                    (data['field'], col)
                    for col, data in prev_info['columns'].items())

                def lineage_column(old_column):
                    new_column = col_map.get(old_column, old_column)
                    if new_column not in info['columns']:
                        raise MissingReferenceError(
                            'Previous column %r of table %r no longer '
                            'exists; cannot carry its old field name '
                            'forward. Provide a column rename hint.'
                            % (old_column, old_table))
                    return new_column

                for old_column, old_field in prev_col_to_field.items():
                    new_column = lineage_column(old_column)
                    declare_field(old_field,
                                  info['columns'][new_column]['field'],
                                  NameConflictError)
                for alias, target_field in previous['aliases']['fields'].get(
                        prev_info['class_name'], {}).items():
                    old_column = prev_field_to_col.get(target_field)
                    if old_column is None:
                        raise MissingReferenceError(
                            'Previous field alias %r on %r references a '
                            'missing field.' % (
                                alias, prev_info['class_name']))
                    new_column = lineage_column(old_column)
                    declare_field(alias,
                                  info['columns'][new_column]['field'],
                                  NameConflictError)

            field_aliases[class_name] = OrderedDict(
                (attr, field) for attr, field in sorted(attr_exports.items())
                if attr != field)

        class_aliases = OrderedDict(
            (name, current) for name, current in sorted(class_exports.items())
            if name != current)
        return OrderedDict([
            ('classes', class_aliases),
            ('fields', field_aliases),
        ])

    # -- source rendering ---------------------------------------------------

    @staticmethod
    def _topological_order(prepared):
        tables = set(prepared)
        result = []
        seen = set()
        visiting = set()

        def visit(table):
            if table in seen:
                return
            if table in visiting:
                return  # cycle: emitted later, DeferredForeignKey resolves it
            visiting.add(table)
            for column in prepared[table]['columns'].values():
                fk = column.get('fk')
                if fk and fk['table'] in tables and fk['table'] != table:
                    visit(fk['table'])
            visiting.discard(table)
            seen.add(table)
            result.append(table)

        for table in sorted(tables):
            visit(table)
        return result

    def _render_generated(self, prepared):
        lines = [
            'from peewee import *',
            '',
            '# The concrete database is bound by the services, e.g.',
            '# "<package>.database.initialize(SqliteDatabase(\'app.db\'))".',
            'database = Proxy()',
            '',
            '',
            'class BaseModel(Model):',
            '    class Meta:',
            '        database = database',
            '',
        ]
        emitted = set()
        for table in self._topological_order(prepared):
            info = prepared[table]
            class_name = info['class_name']
            pk_columns = [col for col, data in info['columns'].items()
                          if data.get('primary_key')]
            lines.append('class %s(BaseModel):' % class_name)
            body = []
            for column_name, column in info['columns'].items():
                if len(pk_columns) == 1 and column_name == pk_columns[0]:
                    body.append('    %s' % self._render_primary_key(column))
                else:
                    body.append('    %s' % self._render_column(
                        column, table, prepared, emitted,
                        composite_pk=len(pk_columns) > 1))
            if not body:
                body.append('    pass')
            lines.extend(body)
            lines.append('')
            lines.append('    class Meta:')
            lines.append("        table_name = '%s'" % table)
            if not pk_columns:
                lines.append('        primary_key = False')
            elif len(pk_columns) > 1:
                pk_fields = ', '.join(
                    repr(info['columns'][col]['field']) for col in pk_columns)
                lines.append('        primary_key = CompositeKey(%s)'
                             % pk_fields)
            lines.append('')
            emitted.add(table)
        return '\n'.join(lines) + '\n'

    @staticmethod
    def _base_kwargs(column, composite_pk=False):
        kwargs = OrderedDict()
        kwargs['column_name'] = repr(column['name'])
        if column.get('nullable'):
            kwargs['null'] = 'True'
        for key in ('max_length', 'max_digits', 'decimal_places'):
            if column.get(key) is not None:
                kwargs[key] = repr(column[key])
        if column.get('unique'):
            kwargs['unique'] = 'True'
        if composite_pk:
            kwargs['primary_key'] = 'False'
        return kwargs

    def _render_primary_key(self, column):
        field_name = column['field']
        field_class = FIELD_TYPE_MAP.get(column.get('type'), IntegerField)
        if field_class not in (AutoField, BigAutoField):
            field_class = IntegerField if field_class is not BigAutoField \
                else BigAutoField
            kwargs = self._base_kwargs(column)
            kwargs['primary_key'] = 'True'
        else:
            kwargs = self._base_kwargs(column)
        return '%s = %s(%s)' % (
            field_name, field_class.__name__,
            ', '.join('%s=%s' % item for item in kwargs.items()))

    def _render_column(self, column, table, prepared, emitted,
                       composite_pk=False):
        field_name = column['field']
        fk = column.get('fk')
        if fk and fk.get('table') in prepared:
            dest_table = fk['table']
            dest_info = prepared[dest_table]
            dest_pk = next((col for col, data in dest_info['columns'].items()
                            if data.get('primary_key')), None)
            dest_field = (dest_info['columns'][fk['column']]['field']
                          if fk['column'] in dest_info['columns']
                          else (dest_info['columns'][dest_pk]['field']
                                if dest_pk else 'id'))
            kwargs = self._base_kwargs(column, composite_pk)
            kwargs['field'] = repr(dest_field)
            arguments = ', '.join('%s=%s' % item for item in kwargs.items())
            if dest_table == table:
                return '%s = ForeignKeyField(%s, %s)' % (
                    field_name, repr('self'), arguments)
            if dest_table in emitted:
                return '%s = ForeignKeyField(%s, %s)' % (
                    field_name, dest_info['class_name'], arguments)
            # Forward reference: the metaclass resolves it once the target
            # model class is created.
            return '%s = DeferredForeignKey(%r, %s)' % (
                field_name, dest_info['class_name'], arguments)

        field_class = FIELD_TYPE_MAP.get(column.get('type'), BareField)
        kwargs = self._base_kwargs(column, composite_pk)
        return '%s = %s(%s)' % (
            field_name, field_class.__name__,
            ', '.join('%s=%s' % item for item in kwargs.items()))

    @staticmethod
    def _render_compat(plan):
        class_aliases = dict(plan['classes'])
        field_aliases = dict(
            (class_name, dict(mapping))
            for class_name, mapping in plan['fields'].items())
        return '\n'.join([
            '"""Rename compatibility layer - regenerated, do not edit."""',
            'from playhouse.model_versioning import FieldAlias',
            'from . import _generated',
            '',
            'CLASS_ALIASES = %r' % class_aliases,
            'FIELD_ALIASES = %r' % field_aliases,
            '',
            'for _old_name, _new_name in CLASS_ALIASES.items():',
            '    globals()[_old_name] = getattr(_generated, _new_name)',
            '',
            'for _class_name, _mapping in FIELD_ALIASES.items():',
            '    _model = getattr(_generated, _class_name)',
            '    for _old_name, _new_name in _mapping.items():',
            '        setattr(_model, _old_name, FieldAlias(_new_name))',
            '',
            '__all__ = sorted(CLASS_ALIASES)',
            '',
        ])

    @staticmethod
    def _render_version_init(version, export_names):
        return '\n'.join([
            'from .%s import *  # noqa' % GENERATED_MODULE,
            'from . import %s  # noqa' % GENERATED_MODULE,
            'from .%s import *  # noqa' % COMPAT_MODULE,
            'from . import %s  # noqa' % COMPAT_MODULE,
            '',
            '__version__ = %r' % version,
            '__all__ = %r' % sorted(export_names),
            '',
        ])

    # -- staging / verification --------------------------------------------

    def _write_version(self, directory, version, prepared, plan, digest,
                       renames):
        os.makedirs(directory, exist_ok=True)
        export_names = [info['class_name'] for info in prepared.values()]
        export_names.extend(plan['classes'].keys())
        manifest = OrderedDict([
            ('version', version),
            ('created_at', datetime.datetime.now(
                datetime.timezone.utc).isoformat()),
            ('generator', 'playhouse.model_versioning'),
            ('content_hash', digest),
            ('renames', renames),
            ('schema', prepared),
            ('aliases', plan),
        ])
        self._atomic_write(
            os.path.join(directory, GENERATED_MODULE + '.py'),
            self._render_generated(prepared))
        self._atomic_write(
            os.path.join(directory, COMPAT_MODULE + '.py'),
            self._render_compat(plan))
        self._atomic_write(
            os.path.join(directory, '__init__.py'),
            self._render_version_init(version, export_names))
        self._atomic_json(os.path.join(directory, MANIFEST_NAME), manifest)

    @staticmethod
    def _isolated_import(pkg_dir, package_name):
        """Import a package under a unique, throwaway parent name."""
        parent_name = '_mvverify_%s' % uuid.uuid4().hex
        parent = types.ModuleType(parent_name)
        parent.__path__ = [os.path.dirname(pkg_dir)]
        sys.modules[parent_name] = parent
        full_name = '%s.%s' % (parent_name, package_name)
        spec = importlib.util.spec_from_file_location(
            full_name, os.path.join(pkg_dir, '__init__.py'),
            submodule_search_locations=[pkg_dir])
        module = importlib.util.module_from_spec(spec)
        sys.modules[full_name] = module
        spec.loader.exec_module(module)
        return parent_name, full_name, module

    @staticmethod
    def _discard_import(parent_name):
        prefix = parent_name + '.'
        for key in [key for key in sys.modules
                    if key == parent_name or key.startswith(prefix)]:
            del sys.modules[key]

    def _verify_version(self, pkg_dir, version, prepared, plan):
        for filename in ('__init__.py',
                         GENERATED_MODULE + '.py',
                         COMPAT_MODULE + '.py'):
            try:
                py_compile.compile(os.path.join(pkg_dir, filename),
                                   doraise=True)
            except py_compile.PyCompileError as exc:
                raise VerificationError(
                    'Generated package %r does not compile: %s'
                    % (version, exc))

        # DeferredForeignKey tracks unresolved fields in a process-global set;
        # only entries introduced by this very import are relevant here.
        deferred_before = set(DeferredForeignKey._unresolved)
        parent_name = None
        try:
            try:
                parent_name, _full, module = self._isolated_import(
                    pkg_dir, version)
            except BaseException as exc:
                raise VerificationError(
                    'Generated package %r cannot be imported: %r'
                    % (version, exc))

            unresolved = DeferredForeignKey._unresolved - deferred_before
            if unresolved:
                raise VerificationError(
                    'Generated package %r has unresolved foreign keys.'
                    % version)

            # Every current table / column must map onto the package.
            for table, info in prepared.items():
                cls = getattr(module, info['class_name'], None)
                if not (isinstance(cls, type) and issubclass(cls, Model)):
                    raise VerificationError(
                        'Missing model class %r for table %r.'
                        % (info['class_name'], table))
                if cls._meta.table_name != table:
                    raise VerificationError(
                        'Class %r maps to table %r, expected %r.'
                        % (info['class_name'], cls._meta.table_name, table))
                for column_name, column in info['columns'].items():
                    field = cls._meta.fields.get(column['field'])
                    if field is None:
                        raise VerificationError(
                            'Missing field %r on %r.'
                            % (column['field'], info['class_name']))
                    if field.column_name != column_name:
                        raise VerificationError(
                            'Field %r maps to column %r, expected %r.'
                            % (column['field'], field.column_name,
                               column_name))

            # Compatibility aliases must resolve to the current objects.
            for old_name, new_name in plan['classes'].items():
                if getattr(module, old_name) is not getattr(module, new_name):
                    raise VerificationError(
                        'Class alias %r does not resolve to %r.'
                        % (old_name, new_name))
            for class_name, mapping in plan['fields'].items():
                cls = getattr(module, class_name)
                for old_name, new_name in mapping.items():
                    if getattr(cls, old_name) is not getattr(cls, new_name):
                        raise VerificationError(
                            'Field alias %r.%s does not resolve to %r.'
                            % (class_name, old_name, new_name))
                self._verify_field_behavior(cls, mapping)
            return module
        finally:
            # Drop only the entries this verification introduced; other
            # callers in the same process keep their deferred state.
            DeferredForeignKey._unresolved.clear()
            DeferredForeignKey._unresolved.update(deferred_before)
            if parent_name is not None:
                self._discard_import(parent_name)

    @staticmethod
    def _verify_field_behavior(cls, mapping):
        if not mapping:
            return
        probe = SqliteDatabase(':memory:')
        cls._meta.database.initialize(probe)
        try:
            instance = cls()
            for old_name, new_name in mapping.items():
                setattr(instance, new_name, 1)
                if getattr(instance, old_name) != 1:
                    raise VerificationError(
                        'Field alias %r.%s instance read failed.'
                        % (cls.__name__, old_name))
                setattr(instance, old_name, 2)
                if getattr(instance, new_name) != 2:
                    raise VerificationError(
                        'Field alias %r.%s instance write failed.'
                        % (cls.__name__, old_name))
                query, _ = cls.select().where(
                    getattr(cls, old_name) == 1).sql()
                current_field = cls._meta.fields[new_name]
                if current_field.column_name not in query:
                    raise VerificationError(
                        'Query through alias %r.%s does not target column '
                        '%r.' % (cls.__name__, old_name,
                                 current_field.column_name))
        finally:
            cls._meta.database.initialize(None)

    def _verify_against_manifest(self, pkg_dir, manifest):
        prepared = manifest['schema']
        plan = manifest['aliases']
        self._verify_version(pkg_dir, manifest['version'], prepared, plan)

    def _package_is_valid(self, version):
        pkg_dir = self._path(version)
        manifest_path = os.path.join(pkg_dir, MANIFEST_NAME)
        if not os.path.isdir(pkg_dir) or not os.path.exists(manifest_path):
            return False
        try:
            manifest = self._read_json(manifest_path)
            self._verify_against_manifest(pkg_dir, manifest)
        except (VersioningError, ValueError, OSError):
            return False
        return True

    def _verify_default_export(self, expected_version):
        parent_name = None
        try:
            parent_name, _full, module = self._isolated_import(
                self.root, os.path.basename(self.root.rstrip(os.sep)))
            resolved = getattr(module, '__version_package__', None)
            if resolved != expected_version:
                raise VerificationError(
                    'Default export resolves to %r, expected %r.'
                    % (getattr(module, '__version_package__', None),
                       expected_version))
        except VerificationError:
            raise
        except BaseException as exc:
            raise VerificationError(
                'Default export cannot be imported: %r' % exc)
        finally:
            if parent_name is not None:
                self._discard_import(parent_name)

    # -- pointer / journal --------------------------------------------------

    def _read_pointer(self):
        path = self._path(CURRENT_POINTER)
        if not os.path.exists(path):
            return None
        return self._read_json(path)

    def _read_pointer_safe(self):
        try:
            return self._read_pointer()
        except (ValueError, OSError):
            return None

    def _write_pointer(self, active, previous, history=None):
        path = self._path(CURRENT_POINTER)
        if active is None:
            if os.path.exists(path):
                os.unlink(path)
            return
        history = list(history or [])
        if previous and previous not in history:
            history.insert(0, previous)
        payload = OrderedDict([
            ('active', active),
            ('previous', previous),
            ('history', history[:MAX_HISTORY]),
            ('updated_at', datetime.datetime.now(
                datetime.timezone.utc).isoformat()),
        ])
        self._atomic_json(path, payload)

    def _active_version(self):
        pointer = self._read_pointer()
        return pointer['active'] if pointer else None

    def _load_manifest(self, version):
        if version is None:
            return None
        path = self._path(os.path.join(version, MANIFEST_NAME))
        if not os.path.exists(path):
            raise MissingReferenceError(
                'Active version %r is missing its manifest; the previous '
                'package is not usable for building a compatibility layer.'
                % version)
        return self._read_json(path)

    def list_versions(self):
        """Return all versioned package directories currently on disk."""
        return sorted(
            entry for entry in os.listdir(self.root)
            if entry.startswith('v_')
            and os.path.isdir(os.path.join(self.root, entry)))

    # -- crash recovery -----------------------------------------------------

    def _recover(self):
        journal_path = self._path(JOURNAL_FILE)
        if os.path.exists(journal_path):
            try:
                journal = self._read_json(journal_path)
            except (ValueError, OSError):
                journal = None
            if journal is not None:
                self._reconcile_journal(journal)
        if os.path.exists(journal_path):
            os.unlink(journal_path)

        # Half-finished staging directories are never reachable via export.
        for entry in os.listdir(self.root):
            if entry.startswith(STAGING_PREFIX):
                self._remove_tree(os.path.join(self.root, entry))

        pointer_path = self._path(CURRENT_POINTER)
        if os.path.exists(pointer_path):
            try:
                pointer = self._read_pointer()
            except (ValueError, OSError):
                # Journal reconciliation had a chance to rewrite it; a still
                # unreadable pointer means history cannot be reconstructed.
                raise VersioningError(
                    '%s is corrupt and no journal could restore it; '
                    'refusing to guess the active version.'
                    % CURRENT_POINTER)
        else:
            pointer = None
        if pointer is not None and not self._package_is_valid(
                pointer['active']):
            previous = pointer.get('previous')
            if previous and self._package_is_valid(previous):
                self._write_pointer(previous, None,
                                    pointer.get('history', []))
            else:
                raise MissingReferenceError(
                    'Active package %r is broken and no validated previous '
                    'package is available; refusing to continue.'
                    % pointer['active'])

    def _reconcile_journal(self, journal):
        state = journal.get('state')
        version = journal.get('version')
        previous = journal.get('previous')
        staging = journal.get('staging')
        pointer = self._read_pointer_safe()
        active = pointer['active'] if pointer else None

        if state == 'prepared':
            # Crashed before promotion: only staging data exists.
            self._remove_tree(staging)
            self._remove_journal()
            return

        if state == 'promoted':
            # The version directory exists but the pointer was never swapped.
            self._remove_tree(self._path(version))
            self._remove_tree(staging)
            self._remove_journal()
            return

        if state == 'switched':
            if active == version and self._package_is_valid(version):
                self._remove_journal()
                return
            if self._package_is_valid(version):
                # Swap happened after the journal was written but did not
                # survive: finish it.
                self._write_pointer(version, previous,
                                    pointer.get('history', []) if pointer
                                    else [])
                self._remove_journal()
                return
            if previous and self._package_is_valid(previous):
                self._write_pointer(previous, None,
                                    pointer.get('history', []) if pointer
                                    else [])
                self._remove_tree(self._path(version))
                self._remove_journal()
                return
            raise VersioningError(
                'Interrupted switch cannot be reconciled: neither %r nor '
                '%r validate.' % (version, previous))

        # Unknown / corrupt journal state: clean up defensively.
        self._remove_tree(staging)
        self._remove_journal()

    # -- public API ---------------------------------------------------------

    def generate(self, schema=None, database=None, renames=None,
                 table_names=None, include_views=False):
        """Generate a new versioned package and switch the default export.

        Either a framework-neutral ``schema`` descriptor or a live peewee
        ``database`` must be given. ``renames`` records database renames the
        generator cannot infer::

            {'tables': {'old_table': 'new_table'},
             'columns': {'new_table': {'old_column': 'new_column'}}}

        On any naming conflict, manual-name clash, missing reference or
        verification failure the pointer is left untouched and the previous
        package remains the default export.
        """
        renames = renames or {}
        with self._locked():
            self._recover()

            if database is not None:
                schema = schema_from_database(
                    database, table_names=table_names,
                    include_views=include_views)
            if schema is None:
                raise ValueError('Provide either a schema descriptor or a '
                                 'database.')

            prepared = self._prepare(schema)
            previous_version = self._active_version()
            previous = self._load_manifest(previous_version)
            if previous_version and previous is None:
                raise MissingReferenceError(
                    'Active version %r cannot be read; refusing to switch.'
                    % previous_version)
            manual = self._read_json(self._path(MANUAL_REGISTRY))
            plan = self._plan_aliases(prepared, previous, manual, renames)
            digest = self._content_hash(prepared, plan, manual)

            if previous is not None and previous.get('content_hash') == digest:
                return GenerationResult(
                    version=previous_version, active=previous_version,
                    switched=False, previous=previous_version)

            version = 'v_%s_%s' % (
                datetime.datetime.now(datetime.timezone.utc)
                .strftime('%Y%m%d%H%M%S'),
                digest[:10])
            staging = self._path(STAGING_PREFIX + version)
            try:
                self._write_version(staging, version, prepared, plan, digest,
                                    renames)
                self._verify_version(staging, version, prepared, plan)
            except BaseException:
                self._remove_tree(staging)
                raise

            journal = OrderedDict([
                ('state', 'prepared'),
                ('version', version),
                ('staging', staging),
                ('previous', previous_version),
            ])
            self._atomic_json(self._path(JOURNAL_FILE), journal)
            target = self._path(version)
            adopted = False
            try:
                if os.path.exists(target):
                    # A retained package with the same version id already
                    # exists: adopt it when it is the same validated content,
                    # otherwise give the new package a unique suffix.
                    manifest_path = os.path.join(target, MANIFEST_NAME)
                    existing_manifest = (
                        self._read_json(manifest_path)
                        if os.path.exists(manifest_path) else None)
                    if (existing_manifest is not None
                            and existing_manifest.get('content_hash') == digest
                            and self._package_is_valid(version)):
                        self._remove_tree(staging)
                        adopted = True
                    else:
                        version = '%s_%s' % (version, uuid.uuid4().hex[:6])
                        target = self._path(version)
                        os.replace(staging, target)
                else:
                    os.replace(staging, target)
            except BaseException:
                self._remove_tree(staging)
                self._remove_journal()
                raise

            journal['version'] = version
            journal['staging'] = None
            journal['state'] = 'promoted'
            self._atomic_json(self._path(JOURNAL_FILE), journal)
            current_pointer = self._read_pointer_safe()
            history = (current_pointer.get('history', [])
                       if current_pointer else [])
            journal['state'] = 'switched'
            self._atomic_json(self._path(JOURNAL_FILE), journal)
            try:
                self._write_pointer(version, previous_version, history)
                self._verify_default_export(version)
            except BaseException:
                # Put the old export back before surfacing the failure.
                if (previous_version
                        and self._package_is_valid(previous_version)):
                    self._write_pointer(previous_version, None, history)
                    self._verify_default_export(previous_version)
                else:
                    self._write_pointer(None, None)
                # Only discard a package this run created; an adopted
                # pre-existing package must remain on disk untouched.
                if not adopted:
                    self._remove_tree(target)
                self._remove_journal()
                raise
            self._remove_journal()
            return GenerationResult(
                version=version, active=version, switched=True,
                previous=previous_version)

    def rollback(self, version=None):
        """Atomically point the default export back at a prior version."""
        with self._locked():
            self._recover()
            pointer = self._read_pointer()
            if pointer is None:
                raise VersioningError('No active version to roll back.')
            target = version or pointer.get('previous')
            if not target or not self._package_is_valid(target):
                raise MissingReferenceError(
                    'Rollback target %r is not a validated package.'
                    % target)
            journal = OrderedDict([
                ('state', 'switched'),
                ('version', target),
                ('staging', None),
                ('previous', pointer['active']),
            ])
            self._atomic_json(self._path(JOURNAL_FILE), journal)
            self._write_pointer(target, pointer['active'],
                                pointer.get('history', []))
            self._verify_default_export(target)
            self._remove_journal()
            return GenerationResult(
                version=target, active=target, switched=True,
                previous=pointer['active'])

    def load_active(self):
        """Import the stable default export as a regular Python package."""
        parent = os.path.dirname(self.root)
        package_name = os.path.basename(self.root.rstrip(os.sep))
        inserted = parent not in sys.path
        if inserted:
            sys.path.insert(0, parent)
        try:
            return importlib.import_module(package_name)
        finally:
            if inserted and parent in sys.path:
                sys.path.remove(parent)

    @staticmethod
    def _content_hash(prepared, plan, manual):
        payload = json.dumps(
            {'schema': prepared, 'aliases': plan, 'manual': manual},
            sort_keys=True, default=str)
        return hashlib.sha256(payload.encode('utf-8')).hexdigest()


# ---------------------------------------------------------------------------
# Command line entry point
# ---------------------------------------------------------------------------

def _parse_rename_table(values):
    mapping = {}
    for value in values:
        old, new = value.split('=', 1)
        mapping[old.strip()] = new.strip()
    return mapping


def _parse_rename_column(values):
    mapping = {}
    for value in values:
        location, new_column = value.split('=', 1)
        table, old_column = location.split('.', 1)
        mapping.setdefault(table.strip(), {})[old_column.strip()] = \
            new_column.strip()
    return mapping


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(
        prog='model_versioning',
        description='Versioned peewee model generation with rename '
                    'compatibility layers.')
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    generate = subparsers.add_parser('generate')
    generate.add_argument('--root', required=True)
    generate.add_argument('--schema', help='Path to a schema descriptor JSON')
    generate.add_argument('--engine')
    generate.add_argument('--database')
    generate.add_argument('--schema-name')
    generate.add_argument('--tables')
    generate.add_argument('--rename-table', action='append', default=[])
    generate.add_argument('--rename-column', action='append', default=[])

    rollback = subparsers.add_parser('rollback')
    rollback.add_argument('--root', required=True)
    rollback.add_argument('--version')

    register = subparsers.add_parser('register-manual')
    register.add_argument('--root', required=True)
    register.add_argument('--kind', choices=('class', 'field'),
                          required=True)
    register.add_argument('--name', required=True)
    register.add_argument('--table')
    register.add_argument('--column')

    args = parser.parse_args(argv)
    store = ModelVersionStore(args.root)

    if args.command == 'generate':
        renames = {
            'tables': _parse_rename_table(args.rename_table),
            'columns': _parse_rename_column(args.rename_column),
        }
        if args.schema:
            with open(args.schema) as handle:
                schema = json.load(handle)
            result = store.generate(schema=schema, renames=renames)
        else:
            if not args.engine or not args.database:
                parser.error('generate requires --schema or --engine and '
                             '--database')
            from pwiz import make_introspector
            introspector = make_introspector(
                args.engine, args.database,
                **({'schema': args.schema_name} if args.schema_name else {}))
            database = introspector.metadata.database
            table_names = (
                [item.strip()
                 for item in args.tables.split(',') if item.strip()]
                if args.tables else None)
            result = store.generate(database=database, renames=renames,
                                    table_names=table_names)
        print('active=%s switched=%s previous=%s' % (
            result.active, result.switched, result.previous))
        return 0

    if args.command == 'rollback':
        result = store.rollback(version=args.version)
        print('active=%s' % result.active)
        return 0

    if args.command == 'register-manual':
        if args.kind == 'class':
            store.register_manual_class(args.name, table=args.table)
        else:
            if not args.table:
                parser.error('manual field registration requires --table')
            store.register_manual_field(args.table, args.name,
                                        column=args.column)
        print('registered %s %r' % (args.kind, args.name))
        return 0

    parser.error('unknown command')


if __name__ == '__main__':
    sys.exit(main())
