"""
Zero-downtime cutover of a full-text search index from FTS4 to FTS5.

The migration is driven by a durable, singleton state row that records which
stage the cutover is in and which index currently serves reads. Every stage
transition is committed inside SQLite, so a process killed mid-backfill,
mid-dual-write or mid-switch can re-enter :meth:`FTSCutover.run` and resume
from the exact stage it died in, without duplicate rows or lost changes.

Stage flow::

    prepared -> backfilling -> backfilled -> validated -> switched -> done

* ``prepared``: the FTS5 virtual table and the state table exist. Reads and
  writes still target FTS4 only.
* ``backfilling``: dual-write is enabled (every change hits both indexes in
  one transaction) while the main table is copied into FTS5 in checkpointed
  batches. Reads still target FTS4, which stays fully serviceable.
* ``backfilled``: both indexes track the main table; reads still target FTS4.
* ``validated``: FTS5 passed the native integrity check, rowid-set and
  content comparison against the main table (and FTS4).
* ``switched``: the read pointer atomically flipped to FTS5.
* ``done``: the FTS4 virtual table has been dropped. Cleanup is only legal
  after the switch, so the old index remains available until reads moved.

The FTS4 and FTS5 tables are expected to be regular (content-storing)
virtual tables whose ``rowid`` matches the main table's integer primary key.
Writes use plain ``INSERT OR REPLACE`` / ``DELETE ... WHERE rowid = ?`` SQL,
which is supported by both extension versions and is naturally idempotent.
"""

import time

from peewee import BigIntegerField
from peewee import IntegerField
from peewee import Model
from peewee import TextField
from playhouse.sqlite_ext import FTS5Model
from playhouse.sqlite_ext import FTSModel
from playhouse.sqlite_ext import RowIDField


class CutoverStage(object):
    PREPARED = 'prepared'
    BACKFILLING = 'backfilling'
    BACKFILLED = 'backfilled'
    VALIDATED = 'validated'
    SWITCHED = 'switched'
    DONE = 'done'

    ALL = (PREPARED, BACKFILLING, BACKFILLED, VALIDATED, SWITCHED, DONE)


class FTS4_INDEX(object):
    name = 'fts4'


class FTS5_INDEX(object):
    name = 'fts5'


# Stages at which a document write must be applied to both indexes.
_DUAL_WRITE_STAGES = (
    CutoverStage.BACKFILLING,
    CutoverStage.BACKFILLED,
    CutoverStage.VALIDATED)

# Stages at which the old index may still be present and writable.
_FTS4_LIVE_STAGES = (
    CutoverStage.PREPARED,
    CutoverStage.BACKFILLING,
    CutoverStage.BACKFILLED,
    CutoverStage.VALIDATED)

# Stages at which the new index receives writes (dual-write or sole index).
_FTS5_LIVE_STAGES = (
    CutoverStage.BACKFILLING,
    CutoverStage.BACKFILLED,
    CutoverStage.VALIDATED,
    CutoverStage.SWITCHED,
    CutoverStage.DONE)


class FTSCutoverError(Exception):
    pass


class CutoverStateError(FTSCutoverError):
    """Raised when an operation is illegal in the current cutover stage."""


class CutoverValidationError(FTSCutoverError):
    """Raised when the new FTS5 index fails the pre-switch validation."""


def _quote(identifier):
    return '"%s"' % identifier.replace('"', '""')


class FTSCutover(object):
    """
    Coordinate an FTS4 -> FTS5 cutover for one main table.

    :param database: a :class:`SqliteDatabase` holding all three tables.
    :param main_model: regular model, source of truth, with an integer PK.
    :param fts4_model: existing content-storing FTS4 virtual model.
    :param fts5_model: content-storing FTS5 virtual model mirroring the
        FTS4 columns; its table is created by :meth:`install` if missing.
    :param state_table_name: name of the singleton state table.
    :param fts_columns: optional explicit list of indexed column names;
        derived from ``fts5_model`` when omitted.
    """

    def __init__(self, database, main_model, fts4_model, fts5_model,
                 state_table_name=None, fts_columns=None):
        if not (isinstance(fts4_model, type) and
                issubclass(fts4_model, FTSModel)):
            raise CutoverStateError('fts4_model must be an FTSModel subclass.')
        if not (isinstance(fts5_model, type) and
                issubclass(fts5_model, FTS5Model)):
            raise CutoverStateError('fts5_model must be an FTS5Model '
                                    'subclass.')

        self.database = database
        self.main_model = main_model
        self.fts4_model = fts4_model
        self.fts5_model = fts5_model

        for model in (main_model, fts4_model, fts5_model):
            model.bind(database)

        self.main_table = main_model._meta.table_name
        self.fts4_table = fts4_model._meta.table_name
        self.fts5_table = fts5_model._meta.table_name
        self.pk_column = main_model._meta.primary_key.column_name

        if fts_columns is None:
            fts_columns = [
                f.column_name for f in fts5_model._meta.sorted_fields
                if not isinstance(f, RowIDField)]
        self.fts_columns = tuple(fts_columns)

        fts4_columns = {f.column_name for f in fts4_model._meta.sorted_fields
                        if not isinstance(f, RowIDField)}
        missing = [c for c in self.fts_columns if c not in fts4_columns]
        if missing:
            raise CutoverStateError(
                'FTS4 model is missing indexed column(s): %s' %
                ', '.join(missing))

        self.state_table_name = (state_table_name or
                                 '%s_fts_cutover_state' % self.main_table)
        self.State = self._build_state_model()

        # Pre-rendered SQL, all identifiers are quoted, all values bound.
        self._main_select_sql = self._render_main_select()
        self._fts4_upsert_sql = self._render_upsert(self.fts4_table)
        self._fts5_upsert_sql = self._render_upsert(self.fts5_table)
        self._fts4_delete_sql = self._render_delete(self.fts4_table)
        self._fts5_delete_sql = self._render_delete(self.fts5_table)

    # ------------------------------------------------------------------
    # Setup / state plumbing
    # ------------------------------------------------------------------

    def _build_state_model(self):
        class Meta(object):
            database = self.database
            table_name = self.state_table_name

        attrs = {
            'id': IntegerField(primary_key=True),  # singleton, always 1
            'stage': TextField(),
            'active_index': TextField(),
            'backfill_cursor': IntegerField(),
            'updated_at': BigIntegerField(),
            'Meta': Meta}
        return type('FTSCutoverState', (Model,), attrs)

    def install(self):
        """Create the FTS5 table and state row if they do not exist yet."""
        self.database.create_tables([self.State], safe=True)
        if not self.database.table_exists(self.fts5_table):
            self.fts5_model.create_table()

        with self.database.atomic(lock_type='IMMEDIATE'):
            if self._state_row_locked() is None:
                self.State.create(
                    id=1,
                    stage=CutoverStage.PREPARED,
                    active_index=FTS4_INDEX.name,
                    backfill_cursor=0,
                    updated_at=int(time.time()))
        return self.stage

    def _state_row_locked(self):
        # Called inside a transaction; the singleton is fetched by PK.
        return self.State.select().where(self.State.id == 1).first()

    def _state(self):
        row = self.State.select().where(self.State.id == 1).first()
        if row is None:
            raise CutoverStateError(
                'Cutover is not installed for table "%s"; call install() '
                'first.' % self.main_table)
        return row

    @property
    def stage(self):
        return self._state().stage

    @property
    def backfill_cursor(self):
        return self._state().backfill_cursor

    @property
    def active_index(self):
        return self._state().active_index

    @property
    def active_model(self):
        if self.active_index == FTS5_INDEX.name:
            return self.fts5_model
        return self.fts4_model

    def _set_state(self, **fields):
        fields['updated_at'] = int(time.time())
        (self.State
         .update(**fields)
         .where(self.State.id == 1)
         .execute())

    # ------------------------------------------------------------------
    # Rendered SQL
    # ------------------------------------------------------------------

    def _render_main_select(self):
        columns = [self.pk_column] + list(self.fts_columns)
        select_cols = ', '.join(_quote(c) for c in columns)
        return (
            'SELECT %s FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT ?' % (
                select_cols,
                _quote(self.main_table),
                _quote(self.pk_column),
                _quote(self.pk_column)))

    def _render_upsert(self, table):
        columns = ('rowid',) + self.fts_columns
        column_sql = ', '.join(_quote(c) for c in columns)
        placeholders = ', '.join('?' for _ in columns)
        return 'INSERT OR REPLACE INTO %s(%s) VALUES (%s)' % (
            _quote(table), column_sql, placeholders)

    def _render_delete(self, table):
        return 'DELETE FROM %s WHERE rowid = ?' % _quote(table)

    # ------------------------------------------------------------------
    # Dual-write gateway
    # ------------------------------------------------------------------

    def index_document(self, rowid, **values):
        """
        Insert/replace one document in the indexes that are live for the
        current stage. FTS4 and FTS5 writes run in one transaction (a
        savepoint when called inside the application's own transaction), so
        a failure on either side rolls the whole change back and the active
        search index never holds half of a document.
        """
        rowid = int(rowid)
        params = [rowid] + [values.get(column) for column in self.fts_columns]
        with self.database.atomic():
            stage = self.stage
            if stage in _FTS4_LIVE_STAGES:
                self.database.execute_sql(self._fts4_upsert_sql, params)
            if stage in _FTS5_LIVE_STAGES:
                self.database.execute_sql(self._fts5_upsert_sql, params)

    def remove_document(self, rowid):
        """Delete one document from every live index, atomically."""
        rowid = int(rowid)
        with self.database.atomic():
            stage = self.stage
            if stage in _FTS4_LIVE_STAGES:
                self.database.execute_sql(self._fts4_delete_sql, (rowid,))
            if stage in _FTS5_LIVE_STAGES:
                self.database.execute_sql(self._fts5_delete_sql, (rowid,))

    def sync_document(self, rowid):
        """Re-read a main-table row and upsert its current values."""
        columns = [self.pk_column] + list(self.fts_columns)
        select_cols = ', '.join(_quote(c) for c in columns)
        row = self.database.execute_sql(
            'SELECT %s FROM %s WHERE %s = ?' % (
                select_cols,
                _quote(self.main_table),
                _quote(self.pk_column)),
            (int(rowid),)).fetchone()
        if row is None:
            self.remove_document(rowid)
        else:
            self.index_document(row[0], **dict(zip(self.fts_columns, row[1:])))

    # ------------------------------------------------------------------
    # Resumable backfill
    # ------------------------------------------------------------------

    def backfill(self, batch_size=500, progress=None):
        """
        Copy the main table into FTS5 in idempotent, checkpointed batches.

        Each batch is one IMMEDIATE transaction: rows are upserted by rowid
        and the high-water cursor is advanced in the same commit. A crash
        only loses the uncommitted batch; re-running re-reads rows above the
        persisted cursor and re-applies ``INSERT OR REPLACE``, which cannot
        create duplicates.
        """
        self.install()

        with self.database.atomic(lock_type='IMMEDIATE'):
            row = self._state_row_locked()
            if row.stage == CutoverStage.PREPARED:
                # From here on, application writes must reach both indexes.
                self._set_state(stage=CutoverStage.BACKFILLING)
            elif row.stage != CutoverStage.BACKFILLING:
                return row.stage

        while True:
            with self.database.atomic(lock_type='IMMEDIATE'):
                row = self._state_row_locked()
                if row.stage != CutoverStage.BACKFILLING:
                    return row.stage
                cursor = row.backfill_cursor
                batch = self.database.execute_sql(
                    self._main_select_sql, (cursor, batch_size)).fetchall()
                if not batch:
                    self._set_state(
                        stage=CutoverStage.BACKFILLED,
                        backfill_cursor=cursor)
                    return CutoverStage.BACKFILLED

                for values in batch:
                    rowid = values[0]
                    params = [rowid] + list(values[1:])
                    self.database.execute_sql(
                        self._fts5_upsert_sql, params)

                new_cursor = batch[-1][0]
                self._set_state(backfill_cursor=new_cursor)

            if progress is not None:
                progress(new_cursor, len(batch))

    # ------------------------------------------------------------------
    # Validation gate
    # ------------------------------------------------------------------

    def validate(self, search_terms=(), batch_size=500):
        """
        Prove FTS5 is a complete, faithful projection of the main table
        before any read traffic is switched to it.

        Checks, all failing with :class:`CutoverValidationError` while the
        stage (and read pointer) stay untouched:

        1. native FTS5 ``integrity-check``;
        2. row counts and rowid-set equality (both directions);
        3. batched value-by-value content comparison main vs FTS5 and
           main vs FTS4 (NULL and empty string normalized);
        4. optional MATCH result parity between FTS4 and FTS5.
        """
        row = self._state()
        if row.stage not in (CutoverStage.BACKFILLED,
                             CutoverStage.VALIDATED):
            raise CutoverStateError(
                'Validation requires stage "%s", current stage is "%s".' % (
                    CutoverStage.BACKFILLED, row.stage))

        # 1. Native structural integrity.
        try:
            self.fts5_model.integrity_check()
        except Exception as exc:
            raise CutoverValidationError(
                'FTS5 integrity-check failed: %s' % exc)

        # 2. Counts and rowid sets.
        main_count = self._count(self.main_table)
        fts5_count = self._count(self.fts5_table)
        if main_count != fts5_count:
            raise CutoverValidationError(
                'Row count mismatch: main table %d vs FTS5 %d.' % (
                    main_count, fts5_count))

        missing = self._rowid_diff(self.main_table, self.fts5_table)
        if missing:
            raise CutoverValidationError(
                'FTS5 is missing %d rowid(s), e.g. %s.' % (
                    len(missing), missing[:5]))
        extra = self._rowid_diff(self.fts5_table, self.main_table)
        if extra:
            raise CutoverValidationError(
                'FTS5 contains %d orphan rowid(s), e.g. %s.' % (
                    len(extra), extra[:5]))

        # 3. Content comparison against the main table and FTS4.
        self._check_contents(self.fts5_table, batch_size)
        self._check_contents(self.fts4_table, batch_size)

        # 4. Optional search parity.
        for term in search_terms:
            fts4_hits = self._match_rowids(self.fts4_table, term, False)
            fts5_hits = self._match_rowids(self.fts5_table, term, True)
            if fts4_hits != fts5_hits:
                raise CutoverValidationError(
                    'Search result divergence for %r: FTS4 %r vs FTS5 %r.' % (
                        term, fts4_hits, fts5_hits))

        with self.database.atomic(lock_type='IMMEDIATE'):
            if self.stage == CutoverStage.BACKFILLED:
                self._set_state(stage=CutoverStage.VALIDATED)
        return True

    def _count(self, table):
        return self.database.execute_sql(
            'SELECT COUNT(*) FROM %s' % _quote(table)).fetchone()[0]

    def _rowid_diff(self, left_table, right_table):
        sql = (
            'SELECT %s FROM %s EXCEPT SELECT rowid FROM %s' % (
                _quote(self.pk_column) if left_table == self.main_table
                else 'rowid',
                _quote(left_table),
                _quote(right_table)))
        return [r[0] for r in self.database.execute_sql(sql).fetchall()]

    def _check_contents(self, fts_table, batch_size):
        # One JOINed read per batch: main values followed by indexed values.
        main_columns = ', '.join(
            'm.%s' % _quote(column) for column in self.fts_columns)
        fts_columns = ', '.join(
            'f.%s' % _quote(column) for column in self.fts_columns)
        sql = (
            'SELECT m.%s, %s, %s FROM %s AS m JOIN %s AS f '
            'ON f.rowid = m.%s WHERE m.%s > ? '
            'ORDER BY m.%s ASC LIMIT ?' % (
                _quote(self.pk_column),
                main_columns,
                fts_columns,
                _quote(self.main_table),
                _quote(fts_table),
                _quote(self.pk_column),
                _quote(self.pk_column),
                _quote(self.pk_column)))
        width = len(self.fts_columns)
        cursor = 0
        while True:
            batch = self.database.execute_sql(
                sql, (cursor, batch_size)).fetchall()
            if not batch:
                return
            for values in batch:
                rowid, payload = values[0], values[1:]
                expected = tuple(
                    '' if v is None else v for v in payload[:width])
                indexed = tuple(
                    '' if v is None else v for v in payload[width:])
                if indexed != expected:
                    raise CutoverValidationError(
                        'Content mismatch at rowid %s in %s: %r != %r.' % (
                            rowid, fts_table, indexed, expected))
            cursor = batch[-1][0]

    # ------------------------------------------------------------------
    # Atomic read-pointer switch and cleanup
    # ------------------------------------------------------------------

    def switch_reads(self):
        """
        Flip the read pointer to FTS5 in a single committed update.

        Only legal after :meth:`validate` succeeded. Because every search
        goes through the one persisted pointer row, readers either all see
        FTS4 (crash before commit) or all see FTS5 (crash after commit);
        there is no observable mixed state.
        """
        with self.database.atomic(lock_type='IMMEDIATE'):
            row = self._state_row_locked()
            if row.stage in (CutoverStage.SWITCHED, CutoverStage.DONE):
                return row.stage
            if row.stage != CutoverStage.VALIDATED:
                raise CutoverStateError(
                    'Cannot switch reads at stage "%s": validation has not '
                    'passed.' % row.stage)
            self._set_state(
                stage=CutoverStage.SWITCHED,
                active_index=FTS5_INDEX.name)
            return CutoverStage.SWITCHED

    def cleanup_old_index(self):
        """Drop the FTS4 virtual table. Legal only after the switch."""
        with self.database.atomic(lock_type='IMMEDIATE'):
            row = self._state_row_locked()
            if row.stage == CutoverStage.DONE:
                return CutoverStage.DONE
            if row.stage != CutoverStage.SWITCHED:
                raise CutoverStateError(
                    'Refusing to drop FTS4 at stage "%s": the read pointer '
                    'must be switched first.' % row.stage)
            self.fts4_model.drop_table(safe=True)
            self._set_state(stage=CutoverStage.DONE)
            return CutoverStage.DONE

    # ------------------------------------------------------------------
    # Idempotent orchestration / crash recovery
    # ------------------------------------------------------------------

    def run(self, batch_size=500, progress=None, search_terms=()):
        """
        Drive the cutover forward from whatever stage is persisted.

        Safe to call repeatedly, including from a fresh process after a
        crash: already-finished stages are skipped via the state row, the
        backfill resumes at its high-water cursor, and every write is
        idempotent by rowid.
        """
        self.install()
        if self.stage in (CutoverStage.PREPARED,
                          CutoverStage.BACKFILLING):
            self.backfill(batch_size=batch_size, progress=progress)
        if self.stage == CutoverStage.BACKFILLED:
            self.validate(search_terms=search_terms,
                          batch_size=batch_size)
        if self.stage == CutoverStage.VALIDATED:
            self.switch_reads()
        return self.stage

    # Explicit alias making the recovery intent obvious at call sites.
    recover = run

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def _match_rowids(self, table, term, is_fts5):
        if is_fts5:
            term = FTS5Model.clean_query(term)
        rows = self.database.execute_sql(
            'SELECT rowid FROM %s WHERE %s MATCH ? ORDER BY rowid' % (
                _quote(table), _quote(table)),
            (term,)).fetchall()
        return [row[0] for row in rows]

    def search(self, term):
        """
        Return the matching rowids from the active index.

        The active index is resolved from the persisted pointer on every
        call, so a switch takes effect for all gateway callers atomically.
        """
        if self.active_index == FTS5_INDEX.name:
            return self._match_rowids(self.fts5_table, term, True)
        return self._match_rowids(self.fts4_table, term, False)

    def search_index(self, index_name, term):
        """Search one explicit index ('fts4' or 'fts5'), for diagnostics."""
        if index_name == FTS5_INDEX.name:
            return self._match_rowids(self.fts5_table, term, True)
        if index_name == FTS4_INDEX.name:
            return self._match_rowids(self.fts4_table, term, False)
        raise CutoverStateError('Unknown index %r' % index_name)

    def index_counts(self):
        """Diagnostic snapshot of row counts in all three tables."""
        return {
            'main': self._count(self.main_table),
            'fts4': self._count(self.fts4_table),
            'fts5': self._count(self.fts5_table)}
