"""
Zero-downtime SQLite database generation cutover.

This module implements an online migration workflow that copies a live SQLite
database to a new "generation" file while a connection pool keeps serving
read/write traffic from the old generation:

1. A trigger-based change-data-capture (CDC) changelog is installed inside the
   source database. Because the triggers run inside the same transactions as
   the user writes, every INSERT/UPDATE/DELETE is recorded durably and in
   order before the snapshot is taken.
2. A transactionally-consistent snapshot is copied to the target generation
   file using the SQLite online backup API.
3. Changelog rows produced after the snapshot are replayed onto the target in
   batches, using a persisted high-water-mark so an interrupted replay never
   re-applies a committed batch and never loses a write.
4. Schema and per-table row checksums are compared between the generations.
5. The pool drains the old generation (no new leases are handed out, existing
   leases are awaited), a final replay + verification runs in the guaranteed
   quiet window, and an atomic symlink switch makes new connections open the
   new generation. The old file is retained for rollback.

Every phase transition is persisted in a JSON state file, so re-running a
migration after a crash resumes in the same target generation and at the same
watermark. A failed verification or a drain timeout aborts the cutover and
leaves the old generation serving traffic.
"""

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time

from peewee import SqliteDatabase

logger = logging.getLogger('playhouse.generation_cutover')

CHANGELOG_TABLE = '_cutover_changelog'
APPLIED_TABLE = '_cutover_applied'
TRIGGER_PREFIX = 'cutover_'
# Key used to encode BLOB values, which json_object() cannot store directly.
BLOB_MARKER = '__cutover_blob__'

PHASE_INIT = 'init'
PHASE_SNAPSHOT = 'snapshot'
PHASE_CATCHUP = 'catchup'
PHASE_VERIFY = 'verify'
PHASE_DRAIN = 'drain'
PHASE_COMMITTED = 'committed'
PHASE_ROLLED_BACK = 'rolled_back'
PHASES = (PHASE_INIT, PHASE_SNAPSHOT, PHASE_CATCHUP, PHASE_VERIFY,
          PHASE_DRAIN, PHASE_COMMITTED)

DEFAULT_PRAGMAS = (
    ('journal_mode', 'WAL'),
    ('foreign_keys', 1),
    ('busy_timeout', 30000),
)


class CutoverError(Exception):
    """Base class for generation cutover errors."""


class CutoverAborted(CutoverError):
    """Raised when a cutover is safely aborted with the old generation active.

    The source database is untouched and continues to serve traffic; the
    migration state is kept so the operator can inspect or retry the run.
    """


class PoolTimeout(CutoverError):
    """Raised when a connection lease cannot be acquired in time."""


class MaxConnectionsExceeded(PoolTimeout):
    """Raised when the pool has reached its connection cap."""


# ---------------------------------------------------------------------------
# Identifier / value encoding helpers
# ---------------------------------------------------------------------------

def quote_ident(name):
    """Quote a SQLite identifier (table/column/trigger name)."""
    return '"%s"' % str(name).replace('"', '""')


def quote_string(value):
    """Quote a string literal for embedding in SQL text."""
    return "'%s'" % str(value).replace("'", "''")


def _blob_safe_expr(row_ref, column):
    """Return an SQL expression that encodes a column value as JSON safely.

    BLOB values cannot be passed to json_object(); they are wrapped in a
    marker object carrying their hex encoding. Everything else is passed
    through unchanged so type affinity is preserved on replay.
    """
    ref = '%s.%s' % (row_ref, quote_ident(column))
    return ("CASE WHEN typeof(%s) = 'blob' "
            "THEN json_object('%s', hex(%s)) ELSE %s END"
            % (ref, BLOB_MARKER, ref, ref))


def _decode_json(value):
    """Inverse of the trigger-side JSON encoding (recursively decode blobs)."""
    if isinstance(value, dict):
        if len(value) == 1 and BLOB_MARKER in value:
            return bytes.fromhex(value[BLOB_MARKER])
        return {key: _decode_json(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_decode_json(val) for val in value]
    return value


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------

def list_user_tables(conn):
    """Return ``(name, create_sql)`` for ordinary, non-virtual user tables."""
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
        "AND name NOT LIKE '\\_cutover\\_%' ESCAPE '\\' "
        "ORDER BY name").fetchall()
    tables = []
    for name, sql in rows:
        if re.match(r'\s*create\s+virtual\s+table', sql or '', re.IGNORECASE):
            continue
        tables.append((name, sql))
    return tables


def table_info(conn, table):
    """Return PRAGMA table_info rows for ``table``."""
    return conn.execute('PRAGMA table_info(%s)' % quote_ident(table)).fetchall()


def table_columns(conn, table):
    """Return the ordered column names of ``table``."""
    return [row[1] for row in table_info(conn, table)]


def primary_key_columns(info):
    """Return primary-key column names in key order (empty for no explicit PK)."""
    pk_rows = sorted((row for row in info if row[5]), key=lambda row: row[5])
    return [row[1] for row in pk_rows]


def is_without_rowid(create_sql):
    return bool(re.search(r'without\s+rowid', create_sql or '', re.IGNORECASE))


# ---------------------------------------------------------------------------
# Change-data-capture: changelog table + per-table triggers
# ---------------------------------------------------------------------------

def build_trigger_sql(table, action, columns, pk_columns):
    """Build the CREATE TRIGGER statement that records row changes.

    Each changelog row stores the ordered primary-key columns/values (the
    rowid is used for tables that declare no primary key) plus a JSON image
    of the whole row, so changes can be replayed without parsing SQL.
    """
    assert action in ('INSERT', 'UPDATE', 'DELETE')
    row_ref = 'OLD' if action == 'DELETE' else 'NEW'
    suffix = {'INSERT': 'ins', 'UPDATE': 'upd', 'DELETE': 'del'}[action]
    trigger_name = '%s%s_%s' % (TRIGGER_PREFIX, table, suffix)

    if pk_columns:
        pk_cols_sql = 'json_array(%s)' % ', '.join(
            quote_string(col) for col in pk_columns)
        pk_vals_sql = 'json_array(%s)' % ', '.join(
            _blob_safe_expr(row_ref, col) for col in pk_columns)
    else:
        # No explicit primary key: fall back to the implicit rowid.
        pk_cols_sql = 'json_array()'
        pk_vals_sql = 'json_array(%s.rowid)' % row_ref

    body_pairs = ', '.join(
        '%s, %s' % (quote_string(col), _blob_safe_expr(row_ref, col))
        for col in columns)
    return (
        'CREATE TRIGGER IF NOT EXISTS %s AFTER %s ON %s BEGIN '
        'INSERT INTO %s (tbl, op, pk_cols, pk_vals, body) VALUES '
        '(%s, %s, %s, %s, json_object(%s)); END;'
        % (quote_ident(trigger_name), action, quote_ident(table),
           quote_ident(CHANGELOG_TABLE), quote_string(table),
           quote_string(action[0]), pk_cols_sql, pk_vals_sql, body_pairs))


def install_changelog(conn, tables=None):
    """Create the changelog table and CDC triggers.

    Runs inside the caller's transaction so trigger installation is atomic
    and every subsequent write is guaranteed to be captured.
    """
    conn.execute(
        'CREATE TABLE IF NOT EXISTS %s ('
        'seq INTEGER PRIMARY KEY AUTOINCREMENT, '
        'tbl TEXT NOT NULL, op TEXT NOT NULL, '
        'pk_cols TEXT NOT NULL, pk_vals TEXT NOT NULL, body TEXT NOT NULL)'
        % quote_ident(CHANGELOG_TABLE))
    if tables is None:
        tables = [name for name, _ in list_user_tables(conn)]
    for table in tables:
        info = table_info(conn, table)
        columns = [row[1] for row in info]
        pk_columns = primary_key_columns(info)
        for action in ('INSERT', 'UPDATE', 'DELETE'):
            conn.execute(build_trigger_sql(table, action, columns,
                                           pk_columns))
    return tables


def uninstall_changelog(conn, tables=None):
    """Drop CDC triggers and the changelog table (used for cleanup/rollback)."""
    if tables is None:
        tables = [name for name, _ in list_user_tables(conn)]
    for table in tables:
        for suffix in ('ins', 'upd', 'del'):
            conn.execute('DROP TRIGGER IF EXISTS %s' % quote_ident(
                '%s%s_%s' % (TRIGGER_PREFIX, table, suffix)))
    conn.execute('DROP TABLE IF EXISTS %s' % quote_ident(CHANGELOG_TABLE))


def strip_cdc_objects(conn):
    """Remove CDC triggers and changelog copied into a snapshot target."""
    # Escape the underscore in the prefix so it is matched literally.
    pattern = TRIGGER_PREFIX.replace('_', '\\_') + '%'
    names = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' "
        "AND name LIKE ? ESCAPE '\\'", (pattern,)).fetchall()
    for (name,) in names:
        conn.execute('DROP TRIGGER IF EXISTS %s' % quote_ident(name))
    conn.execute('DROP TABLE IF EXISTS %s' % quote_ident(CHANGELOG_TABLE))
    conn.execute('DROP TABLE IF EXISTS %s' % quote_ident(APPLIED_TABLE))


def max_changelog_seq(conn):
    row = conn.execute(
        'SELECT COALESCE(MAX(seq), 0) FROM %s'
        % quote_ident(CHANGELOG_TABLE)).fetchone()
    return row[0]


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

def apply_changelog_batch(conn, batch):
    """Apply one changelog batch transactionally and idempotently.

    Every applied sequence is recorded in ``_cutover_applied`` inside the same
    transaction as its row change, so re-applying a batch after a crash between
    the commit and the external watermark save skips every duplicate entry --
    including plain INSERTs into tables without a primary key. INSERT/UPDATE
    use the full row image with INSERT OR REPLACE; DELETE removes the row by
    its primary key (or rowid).
    """
    ensure_replay_table(conn)
    conn.execute('BEGIN IMMEDIATE')
    try:
        seqs = [entry[0] for entry in batch]
        placeholders = ', '.join('?' for _ in seqs)
        already = {row[0] for row in conn.execute(
            'SELECT seq FROM %s WHERE seq IN (%s)'
            % (quote_ident(APPLIED_TABLE), placeholders), seqs)}
        for seq, table, op, pk_cols, pk_vals, body in batch:
            if seq in already:
                continue
            row = _decode_json(json.loads(body))
            if op in ('I', 'U'):
                columns = list(row)
                col_placeholders = ', '.join('?' for _ in columns)
                conn.execute(
                    'INSERT OR REPLACE INTO %s (%s) VALUES (%s)' % (
                        quote_ident(table),
                        ', '.join(quote_ident(c) for c in columns),
                        col_placeholders),
                    [row[col] for col in columns])
            else:
                pk_columns = _decode_json(json.loads(pk_cols))
                pk_values = _decode_json(json.loads(pk_vals))
                if pk_columns:
                    where = ' AND '.join('%s = ?' % quote_ident(c)
                                         for c in pk_columns)
                    conn.execute('DELETE FROM %s WHERE %s'
                                 % (quote_ident(table), where), pk_values)
                else:
                    conn.execute('DELETE FROM %s WHERE rowid = ?'
                                 % quote_ident(table), pk_values)
            conn.execute('INSERT INTO %s (seq) VALUES (?)'
                         % quote_ident(APPLIED_TABLE), (seq,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Schema comparison and row checksums
# ---------------------------------------------------------------------------

def _normalize_sql(sql):
    return re.sub(r'\s+', ' ', (sql or '').strip().rstrip(';')).lower()


def _cdc_owned(name):
    return name.startswith('_cutover_') or name.startswith(TRIGGER_PREFIX)


def ensure_replay_table(conn):
    """Create the target-side table tracking already-replayed sequences."""
    conn.execute('CREATE TABLE IF NOT EXISTS %s (seq INTEGER PRIMARY KEY)'
                 % quote_ident(APPLIED_TABLE))


def schema_objects(conn):
    """Return a normalized view of user-owned schema objects."""
    objects = {}
    for obj_type, name, sql in conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\'").fetchall():
        if _cdc_owned(name):
            continue
        objects.setdefault(obj_type, {})[name] = _normalize_sql(sql)
    # Column definitions are compared explicitly as well, so an equivalent
    # statement rendered differently can never hide a real structural drift.
    columns = {}
    for name, _ in list_user_tables(conn):
        columns[name] = [
            (row[1], (row[2] or '').upper(), row[3], row[4], row[5])
            for row in table_info(conn, name)]
    return {'objects': objects, 'columns': columns}


def diff_schema(source, target):
    """Return a list of human-readable schema differences (empty == equal)."""
    diffs = []
    src_schema = schema_objects(source)
    dst_schema = schema_objects(target)
    if src_schema['objects'] != dst_schema['objects']:
        src_objs = src_schema['objects']
        dst_objs = dst_schema['objects']
        for obj_type in set(src_objs) | set(dst_objs):
            src_names = src_objs.get(obj_type, {})
            dst_names = dst_objs.get(obj_type, {})
            for name in set(src_names) | set(dst_names):
                if src_names.get(name) != dst_names.get(name):
                    diffs.append('schema %s %r differs: %r != %r'
                                 % (obj_type, name, src_names.get(name),
                                    dst_names.get(name)))
    if src_schema['columns'] != dst_schema['columns']:
        for table in set(src_schema['columns']) | set(dst_schema['columns']):
            if src_schema['columns'].get(table) != \
                    dst_schema['columns'].get(table):
                diffs.append('column definitions of %r differ (%r != %r)'
                             % (table, src_schema['columns'].get(table),
                                dst_schema['columns'].get(table)))
    return diffs


def table_checksum(conn, table, create_sql):
    """Compute ``(row_count, sha256_hexdigest)`` over a table deterministically."""
    digest = hashlib.sha256()
    info = table_info(conn, table)
    pk_columns = primary_key_columns(info)
    if is_without_rowid(create_sql) and pk_columns:
        order_by = ', '.join(quote_ident(c) for c in pk_columns)
    else:
        order_by = 'rowid'
    count = 0
    cursor = conn.execute(
        'SELECT * FROM %s ORDER BY %s'
        % (quote_ident(table), order_by))
    for row in cursor:
        count += 1
        digest.update(repr(tuple(row)).encode('utf-8'))
        digest.update(b'\n')
    return count, digest.hexdigest()


def database_checksums(conn):
    """Return ``{table: (row_count, checksum)}`` for all user tables."""
    return {
        name: table_checksum(conn, name, sql)
        for name, sql in list_user_tables(conn)}


def diff_data(source, target):
    """Return a list of checksum differences (empty == data identical)."""
    diffs = []
    source_sums = database_checksums(source)
    target_sums = database_checksums(target)
    for table in sorted(set(source_sums) | set(target_sums)):
        if source_sums.get(table) != target_sums.get(table):
            diffs.append('table %r checksum differs: %r != %r'
                         % (table, source_sums.get(table),
                            target_sums.get(table)))
    return diffs


def _diff_snapshots(src_schema, dst_schema, src_sums, dst_sums):
    """Compare precomputed schema payloads and per-table checksums."""
    diffs = []
    if src_schema['objects'] != dst_schema['objects']:
        src_objs, dst_objs = src_schema['objects'], dst_schema['objects']
        for obj_type in sorted(set(src_objs) | set(dst_objs)):
            src_names = src_objs.get(obj_type, {})
            dst_names = dst_objs.get(obj_type, {})
            for name in sorted(set(src_names) | set(dst_names)):
                if src_names.get(name) != dst_names.get(name):
                    diffs.append('schema %s %r differs: %r != %r'
                                 % (obj_type, name, src_names.get(name),
                                    dst_names.get(name)))
    if src_schema['columns'] != dst_schema['columns']:
        diffs.append('column definitions differ')
    for table in sorted(set(src_sums) | set(dst_sums)):
        if src_sums.get(table) != dst_sums.get(table):
            diffs.append('table %r checksum differs: %r != %r'
                         % (table, src_sums.get(table),
                            dst_sums.get(table)))
    return diffs


# ---------------------------------------------------------------------------
# Persisted migration state
# ---------------------------------------------------------------------------

def _fsync_directory(path):
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


class StateStore(object):
    """Atomic JSON state file shared by the pool and the migrator."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.data = None

    def load(self):
        with self.lock:
            if self.data is not None:
                return self.data
            if not os.path.exists(self.path):
                return None
            with open(self.path, 'r', encoding='utf-8') as fh:
                self.data = json.load(fh)
            return self.data

    def save(self, data):
        with self.lock:
            tmp_path = '%s.tmp' % self.path
            with open(tmp_path, 'w', encoding='utf-8') as fh:
                json.dump(data, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.path)
            _fsync_directory(os.path.dirname(self.path) or '.')
            self.data = data
            return data


# ---------------------------------------------------------------------------
# Generation-aware connection pool with leases and draining
# ---------------------------------------------------------------------------

class Lease(object):
    """A checked-out connection bound to a specific database generation."""

    __slots__ = ('_pool', 'conn', 'generation', 'path', 'token',
                 'checked_out', '_released')

    def __init__(self, pool, conn, generation, path, token):
        self._pool = pool
        self.conn = conn
        self.generation = generation
        self.path = path
        self.token = token
        self.checked_out = time.time()
        self._released = False

    def release(self):
        if not self._released:
            self._released = True
            self._pool._release(self)

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def __repr__(self):
        return ('<Lease token=%s generation=%s path=%s>'
                % (self.token, self.generation, self.path))


class GenerationSqlitePool(object):
    """A SQLite connection pool whose active file can be switched online.

    Connections are opened through a symlink that always points at the active
    generation file. Every checked-out connection is wrapped in a :class:`Lease`
    tagged with its generation, allowing the cutover coordinator to drain the
    old generation before swapping the symlink.
    """

    def __init__(self, directory, stem='appdb', max_connections=20,
                 pragmas=None, connect_timeout=30.0, acquire_timeout=30.0):
        self.directory = os.path.abspath(directory)
        self.stem = stem
        self.max_connections = max_connections
        self.pragmas = list(DEFAULT_PRAGMAS if pragmas is None else pragmas)
        self.connect_timeout = connect_timeout
        self.acquire_timeout = acquire_timeout

        self.link_path = os.path.join(self.directory, '%s.db' % stem)
        self.state_path = os.path.join(self.directory, '%s.cutover.json' % stem)
        self.store = StateStore(self.state_path)

        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._idle = {}          # generation -> list[sqlite3.Connection]
        self._leases = {}        # token -> Lease
        self._conn_generation = {}
        self._token_seq = 0
        self._draining = False
        self._drain_generation = None

    # -- path / state helpers ------------------------------------------------

    def generation_path(self, generation):
        return os.path.join(self.directory,
                            '%s.g%d.db' % (self.stem, generation))

    @property
    def active_generation(self):
        with self._lock:
            data = self.store.load()
            return data['active_generation'] if data else None

    @property
    def draining(self):
        """Whether the pool is currently draining the old generation."""
        with self._lock:
            return self._draining

    @property
    def active_path(self):
        return self.generation_path(self.active_generation)

    def initialize(self):
        """Bootstrap generation 1 and the state file (idempotent)."""
        os.makedirs(self.directory, exist_ok=True)
        with self._lock:
            data = self.store.load()
            if data is not None:
                target = self.generation_path(data['active_generation'])
                if not os.path.exists(self.link_path):
                    self._replace_link(target)
                return data
            path = self.generation_path(1)
            conn = self._open_raw(1, path)
            conn.close()
            self._replace_link(path)
            data = {'version': 1, 'active_generation': 1,
                    'migrations': {}, 'history': []}
            self.store.save(data)
            return data

    def _replace_link(self, target_path):
        tmp_link = '%s.linktmp.%d' % (self.link_path, os.getpid())
        try:
            os.unlink(tmp_link)
        except OSError:
            pass
        os.symlink(os.path.basename(target_path), tmp_link)
        os.replace(tmp_link, self.link_path)
        _fsync_directory(self.directory)

    # -- raw connections -----------------------------------------------------

    def _apply_pragmas(self, conn):
        for key, value in self.pragmas:
            conn.execute('PRAGMA %s = %s' % (key, value))

    def _open_raw(self, generation, path):
        # Leases may be released by a different thread than the one that
        # opened the connection, so disable sqlite3's thread affinity check.
        conn = sqlite3.connect(path, timeout=self.connect_timeout,
                               isolation_level=None, check_same_thread=False)
        self._apply_pragmas(conn)
        self._conn_generation[id(conn)] = generation
        return conn

    def _raw_close(self, conn):
        self._conn_generation.pop(id(conn), None)
        try:
            conn.close()
        except sqlite3.Error:
            logger.debug('Error closing connection.', exc_info=True)

    # -- lease management ----------------------------------------------------

    def _generation_size(self, generation):
        return len(self._idle.get(generation, [])) + sum(
            1 for lease in self._leases.values()
            if lease.generation == generation)

    def _try_acquire(self):
        generation = self.store.load()['active_generation']
        idle = self._idle.setdefault(generation, [])
        while idle:
            conn = idle.pop()
            try:
                conn.total_changes
            except sqlite3.Error:
                self._raw_close(conn)
                continue
            self._token_seq += 1
            lease = Lease(self, conn, generation,
                          self.generation_path(generation), self._token_seq)
            self._leases[lease.token] = lease
            return lease
        if self.max_connections and \
                self._generation_size(generation) >= self.max_connections:
            return None
        conn = self._open_raw(generation, self.generation_path(generation))
        self._token_seq += 1
        lease = Lease(self, conn, generation,
                      self.generation_path(generation), self._token_seq)
        self._leases[lease.token] = lease
        return lease

    def acquire(self, timeout=None):
        """Acquire a lease on the active generation, waiting while draining."""
        timeout = self.acquire_timeout if timeout is None else timeout
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if not self._draining:
                    lease = self._try_acquire()
                    if lease is not None:
                        return lease
                    if time.monotonic() >= deadline:
                        raise MaxConnectionsExceeded(
                            'Exceeded maximum connections.')
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if self._draining:
                        raise PoolTimeout(
                            'Timed out waiting for generation cutover.')
                    raise MaxConnectionsExceeded(
                        'Exceeded maximum connections.')
                self._cond.wait(min(remaining, 0.5))

    def _release(self, lease):
        with self._cond:
            current = self._leases.pop(lease.token, None)
            if current is None:
                return
            active = self.store.load()['active_generation']
            stale_generation = (
                lease.generation != active
                or (self._draining
                    and lease.generation == self._drain_generation))
            if stale_generation:
                self._raw_close(lease.conn)
            else:
                idle = self._idle.setdefault(lease.generation, [])
                if self.max_connections and \
                        len(idle) >= self.max_connections:
                    self._raw_close(lease.conn)
                else:
                    idle.append(lease.conn)
            self._cond.notify_all()

    def active_lease_count(self, generation=None):
        with self._lock:
            if generation is None:
                return len(self._leases)
            return sum(1 for lease in self._leases.values()
                       if lease.generation == generation)

    def close_idle(self):
        with self._lock:
            for generation, conns in list(self._idle.items()):
                for conn in conns:
                    self._raw_close(conn)
            self._idle.clear()

    def close_all(self):
        """Close every idle and leased connection (mainly for tests/shutdown)."""
        with self._lock:
            for conns in list(self._idle.values()):
                for conn in conns:
                    self._raw_close(conn)
            self._idle.clear()
            for lease in list(self._leases.values()):
                self._raw_close(lease.conn)
            self._leases.clear()
            self._cond.notify_all()

    # -- drain / switch protocol used by the migrator ------------------------

    def begin_drain(self, generation):
        """Stop handing out leases for ``generation`` and close its idle conns."""
        with self._cond:
            self._draining = True
            self._drain_generation = generation
            for conn in self._idle.pop(generation, []):
                self._raw_close(conn)
            self._cond.notify_all()

    def wait_leases_released(self, generation, timeout):
        """Wait until no lease of ``generation`` remains.

        Returns ``True`` on drain success, ``False`` if the timeout elapsed
        with active leases still present.
        """
        deadline = time.monotonic() + timeout
        with self._cond:
            while self.active_lease_count(generation) > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(min(remaining, 0.1))
            return True

    def commit_switch(self, generation, path):
        """Atomically point the active link at ``path`` and resume serving."""
        with self._cond:
            self._replace_link(path)
            data = self.store.load()
            data['active_generation'] = generation
            self.store.save(data)
            self._draining = False
            self._drain_generation = None
            for old_generation, conns in list(self._idle.items()):
                if old_generation != generation:
                    for conn in conns:
                        self._raw_close(conn)
                    self._idle.pop(old_generation, None)
            self._cond.notify_all()

    def abort_drain(self):
        """Cancel a drain and resume service on the current generation."""
        with self._cond:
            self._draining = False
            self._drain_generation = None
            self._cond.notify_all()

    def rollback_switch(self, generation, path):
        """Point the active link back at a previously retained generation."""
        with self._cond:
            self._replace_link(path)
            data = self.store.load()
            data['active_generation'] = generation
            self.store.save(data)
            for current, conns in list(self._idle.items()):
                if current != generation:
                    for conn in conns:
                        self._raw_close(conn)
                    self._idle.pop(current, None)
            self._cond.notify_all()


# ---------------------------------------------------------------------------
# Cutover coordinator (resumable state machine)
# ---------------------------------------------------------------------------

def _connect_admin(path, foreign_keys=True):
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.execute('PRAGMA busy_timeout = 30000')
    conn.execute('PRAGMA foreign_keys = %d' % (1 if foreign_keys else 0))
    return conn


class GenerationCutover(object):
    """Run and resume an online cutover between two database generations.

    A run is described by a migration id stored in the pool's state file.
    Re-creating the coordinator and calling :meth:`run` after a crash resumes
    the same migration in the same target generation at its persisted
    watermark.

    ``hooks`` may contain ``before_phase(phase, state)``,
    ``after_phase(phase, state)``, ``after_batch(applied_seq, state)`` and
    ``before_final_compare(state)`` callables, which are useful for tests and
    for instrumenting long migrations.
    """

    def __init__(self, pool, migration_id='default', batch_size=200,
                 catchup_poll=0.05, verify_rounds=100, drain_timeout=30.0,
                 hooks=None):
        self.pool = pool
        self.migration_id = migration_id
        self.batch_size = batch_size
        self.catchup_poll = catchup_poll
        self.verify_rounds = verify_rounds
        self.drain_timeout = drain_timeout
        self.hooks = hooks or {}
        # Observability counters (reset per coordinator instance).
        self.rows_applied = 0
        self.batches_applied = 0

    # -- state plumbing ------------------------------------------------------

    def _hook(self, name, *args):
        callback = self.hooks.get(name)
        if callback is not None:
            callback(*args)

    def _load_or_create_state(self):
        store = self.pool.store
        with store.lock:
            data = store.load()
            if data is None:
                raise CutoverError('Pool is not initialized; call '
                                   'GenerationSqlitePool.initialize() first.')
            existing = data['migrations'].get(self.migration_id)
            if existing is not None:
                return existing
            source_generation = data['active_generation']
            target_generation = source_generation + 1
            state = {
                'id': self.migration_id,
                'phase': PHASE_INIT,
                'source_generation': source_generation,
                'target_generation': target_generation,
                'source_path': self.pool.generation_path(source_generation),
                'target_path': self.pool.generation_path(target_generation),
                'snapshot_seq': 0,
                'applied_seq': 0,
                'tables': [],
            }
            data['migrations'][self.migration_id] = state
            store.save(data)
            return state

    def _save(self, state):
        self.pool.store.save(self.pool.store.data)

    # -- phase: install CDC --------------------------------------------------

    def _do_init(self, state):
        self._hook('before_phase', PHASE_INIT, state)
        conn = _connect_admin(state['source_path'])
        try:
            conn.execute('BEGIN IMMEDIATE')
            try:
                tables = install_changelog(conn, state.get('tables') or None)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        finally:
            conn.close()
        state['tables'] = tables
        state['phase'] = PHASE_SNAPSHOT
        self._save(state)
        self._hook('after_phase', PHASE_INIT, state)

    # -- phase: online snapshot ----------------------------------------------

    def _do_snapshot(self, state):
        self._hook('before_phase', PHASE_SNAPSHOT, state)
        target_path = state['target_path']
        for suffix in ('', '-wal', '-shm'):
            partial = target_path + suffix
            if os.path.exists(partial):
                os.unlink(partial)
        source = _connect_admin(state['source_path'])
        try:
            target = sqlite3.connect(target_path, timeout=30.0,
                                     isolation_level=None)
            try:
                # One-shot online backup: Connection.backup() is invoked on
                # the SOURCE with the destination as argument. Pages dirtied
                # while the backup runs are re-copied, so the result is a
                # consistent snapshot.
                source.backup(target)
                target.execute('PRAGMA journal_mode = WAL')
                target.execute('PRAGMA foreign_keys = OFF')
                # The watermark must come from the frozen snapshot itself,
                # not from the live source: a transaction committing while
                # the backup runs may be newer than the snapshot yet still
                # visible to a post-backup read on the source, which would
                # otherwise cause its changes to be skipped during replay.
                state['snapshot_seq'] = max_changelog_seq(target)
                strip_cdc_objects(target)
                ensure_replay_table(target)
                target.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            finally:
                target.close()
            # Replay starts strictly after the sequences already included in
            # the snapshot, otherwise pre-snapshot changes would be applied
            # twice. The target-side _cutover_applied table still guards the
            # narrow window between a batch commit and this watermark save.
            state['applied_seq'] = state['snapshot_seq']
        finally:
            source.close()
        state['phase'] = PHASE_CATCHUP
        self._save(state)
        self._hook('after_phase', PHASE_SNAPSHOT, state)

    # -- phase: changelog replay ---------------------------------------------

    def _replay_to_tip(self, state):
        source = _connect_admin(state['source_path'])
        target = _connect_admin(state['target_path'], foreign_keys=False)
        try:
            while True:
                tip = max_changelog_seq(source)
                if tip <= state['applied_seq']:
                    return
                batch = source.execute(
                    'SELECT seq, tbl, op, pk_cols, pk_vals, body FROM %s '
                    'WHERE seq > ? ORDER BY seq LIMIT ?'
                    % quote_ident(CHANGELOG_TABLE),
                    (state['applied_seq'], self.batch_size)).fetchall()
                if not batch:
                    return
                apply_changelog_batch(target, batch)
                state['applied_seq'] = batch[-1][0]
                self.rows_applied += len(batch)
                self.batches_applied += 1
                self._save(state)
                self._hook('after_batch', state['applied_seq'], state)
        finally:
            target.close()
            source.close()

    def _do_catchup(self, state):
        self._hook('before_phase', PHASE_CATCHUP, state)
        self._replay_to_tip(state)
        state['phase'] = PHASE_VERIFY
        self._save(state)
        self._hook('after_phase', PHASE_CATCHUP, state)

    # -- verification --------------------------------------------------------

    def _read_snapshot(self, conn):
        conn.execute('BEGIN')
        try:
            schema = schema_objects(conn)
            checksums = database_checksums(conn)
        finally:
            conn.rollback()
        return schema, checksums

    def _verify_once(self, state):
        """Catch up and compare at a changelog-quiet point.

        Returns a list of diffs; an empty list means the generations matched
        at a single source changelog sequence.
        """
        self._replay_to_tip(state)
        source = _connect_admin(state['source_path'])
        target = _connect_admin(state['target_path'])
        try:
            w1 = max_changelog_seq(source)
            source.execute('BEGIN')
            try:
                w2 = max_changelog_seq(source)
                if w2 != w1:
                    source.rollback()
                    return None  # write in flight, caller retries
                src_schema, src_sums = (
                    schema_objects(source), database_checksums(source))
            finally:
                source.rollback()
            if state['applied_seq'] < w2:
                return None
            target.execute('BEGIN')
            try:
                dst_schema, dst_sums = (
                    schema_objects(target), database_checksums(target))
            finally:
                target.rollback()
        finally:
            target.close()
            source.close()
        return _diff_snapshots(src_schema, dst_schema, src_sums, dst_sums)

    def _do_verify(self, state):
        self._hook('before_phase', PHASE_VERIFY, state)
        last_diffs = None
        for _ in range(self.verify_rounds):
            diffs = self._verify_once(state)
            if diffs is None:
                time.sleep(self.catchup_poll)
                continue
            if not diffs:
                state['phase'] = PHASE_DRAIN
                self._save(state)
                self._hook('after_phase', PHASE_VERIFY, state)
                return
            last_diffs = diffs
            time.sleep(self.catchup_poll)
        # Persistent differences are reported, but the authoritative gate is
        # the post-drain comparison, where no writes can be in flight.
        logger.warning('Pre-drain verification not quiet yet: %s', last_diffs)
        state['phase'] = PHASE_DRAIN
        self._save(state)

    # -- phase: drain, final compare, switch ---------------------------------

    def _final_compare(self, state):
        source = _connect_admin(state['source_path'])
        target = _connect_admin(state['target_path'])
        try:
            tip = max_changelog_seq(source)
            if state['applied_seq'] != tip:
                return ['target watermark %d != source changelog tip %d'
                        % (state['applied_seq'], tip)]
            src_schema, src_sums = self._read_snapshot(source)
            dst_schema, dst_sums = self._read_snapshot(target)
            return _diff_snapshots(src_schema, dst_schema,
                                   src_sums, dst_sums)
        finally:
            target.close()
            source.close()

    def _do_drain(self, state):
        self._hook('before_phase', PHASE_DRAIN, state)
        state['phase'] = PHASE_DRAIN
        self._save(state)
        self.pool.begin_drain(state['source_generation'])
        try:
            if not self.pool.wait_leases_released(
                    state['source_generation'], self.drain_timeout):
                raise CutoverAborted(
                    'Cutover aborted: %d active lease(s) on generation %d '
                    'after %.1fs; old generation remains active.'
                    % (self.pool.active_lease_count(
                        state['source_generation']),
                       state['source_generation'], self.drain_timeout))
            # No new source leases exist, hence no writes can commit: replay
            # the final tail and run the authoritative verification.
            self._replay_to_tip(state)
            self._hook('before_final_compare', state)
            diffs = self._final_compare(state)
            if diffs:
                raise CutoverAborted(
                    'Cutover aborted: verification mismatch after drain: %s; '
                    'old generation remains active.' % '; '.join(diffs))
            # Verification passed: remove the replay ledger so the switched
            # generation contains only user-owned objects.
            admin = _connect_admin(state['target_path'],
                                   foreign_keys=False)
            try:
                admin.execute('DROP TABLE IF EXISTS %s'
                              % quote_ident(APPLIED_TABLE))
                admin.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            finally:
                admin.close()
            self.pool.commit_switch(state['target_generation'],
                                    state['target_path'])
        except BaseException:
            self.pool.abort_drain()
            raise
        state['phase'] = PHASE_COMMITTED
        data = self.pool.store.data
        data['history'].append({
            'id': state['id'],
            'source_generation': state['source_generation'],
            'target_generation': state['target_generation'],
            'committed_at': time.time(),
        })
        self._save(state)
        self._hook('after_phase', PHASE_COMMITTED, state)

    # -- public API ----------------------------------------------------------

    def run(self):
        """Execute (or resume) the migration, returning its persisted state."""
        state = self._load_or_create_state()
        phase = state['phase']
        if phase == PHASE_COMMITTED:
            logger.info('Migration %r already committed at generation %d.',
                        self.migration_id, state['target_generation'])
            return state
        if phase == PHASE_ROLLED_BACK:
            raise CutoverError(
                'Migration %r was rolled back; start a new migration id.'
                % self.migration_id)
        ordered = (PHASE_INIT, PHASE_SNAPSHOT, PHASE_CATCHUP,
                   PHASE_VERIFY, PHASE_DRAIN)
        handlers = {
            PHASE_INIT: self._do_init,
            PHASE_SNAPSHOT: self._do_snapshot,
            PHASE_CATCHUP: self._do_catchup,
            PHASE_VERIFY: self._do_verify,
            PHASE_DRAIN: self._do_drain,
        }
        for current in ordered:
            if ordered.index(current) < ordered.index(phase):
                continue
            handlers[current](state)
            if state['phase'] == PHASE_COMMITTED:
                break
        return state

    def rollback(self):
        """Repoint the pool at the retained source generation."""
        state = self.pool.store.data['migrations'].get(self.migration_id)
        if state is None:
            raise CutoverError('Unknown migration %r.' % self.migration_id)
        if state['phase'] != PHASE_COMMITTED:
            raise CutoverError(
                'Cannot roll back migration %r in phase %r.'
                % (self.migration_id, state['phase']))
        if not os.path.exists(state['source_path']):
            raise CutoverError(
                'Retained source file is missing: %s' % state['source_path'])
        self.pool.rollback_switch(state['source_generation'],
                                  state['source_path'])
        state['phase'] = PHASE_ROLLED_BACK
        self._save(state)
        return state


# ---------------------------------------------------------------------------
# Peewee integration
# ---------------------------------------------------------------------------

class CutoverSqliteDatabase(SqliteDatabase):
    """A peewee database that vends connections from a generation pool.

    Models use this class exactly like a regular :class:`SqliteDatabase`;
    ``connect()`` leases a connection from the pool and ``close()`` returns
    the lease instead of closing the underlying sqlite3 connection, so the
    cutover coordinator can track and drain every connection the ORM uses.
    """

    def __init__(self, pool, **kwargs):
        self._generation_pool = pool
        # sqlite3.Connection objects expose no __dict__, so lease lookup is
        # keyed by connection id rather than stashed on the connection.
        self._cutover_leases = {}
        kwargs.pop('database', None)
        kwargs.pop('pragmas', None)
        super(CutoverSqliteDatabase, self).__init__(
            'generation-cutover', pragmas={}, **kwargs)

    def _connect(self):
        lease = self._generation_pool.acquire()
        conn = lease.conn
        self._cutover_leases[id(conn)] = lease
        try:
            self._add_conn_hooks(conn)
        except Exception:
            self._cutover_leases.pop(id(conn), None)
            lease.release()
            raise
        return conn

    def _close(self, conn):
        lease = self._cutover_leases.pop(id(conn), None)
        if lease is not None:
            lease.release()
        else:
            super(CutoverSqliteDatabase, self)._close(conn)
