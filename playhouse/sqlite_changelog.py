import datetime

from peewee import *
from playhouse.sqlite_ext import JSONField

import time


class BaseChangeLog(Model):
    timestamp = DateTimeField(constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')])
    action = TextField()
    table = TextField()
    primary_key = IntegerField()
    changes = JSONField()


class ChangeLogCursorError(Exception):
    "Raised when a cursor operation would violate the cursor invariants."


class UnknownConsumer(ChangeLogCursorError):
    "No lease has ever been registered for the given downstream consumer."


class ConsumerExists(ChangeLogCursorError):
    "A lease was already registered for the given downstream consumer."


class UnknownGeneration(ChangeLogCursorError):
    "An acknowledgement referenced a generation that does not exist."


class GenerationRetired(ChangeLogCursorError):
    "An acknowledgement referenced a generation that has been retired."


# Backwards / convenience alias: a retired generation is no longer valid.
InvalidGeneration = GenerationRetired


class LeaseExpired(ChangeLogCursorError):
    "An acknowledgement referenced a generation whose lease has expired."


class PositionRegression(ChangeLogCursorError):
    "An acknowledgement moved the cursor backwards."


class DuplicateConfirmation(ChangeLogCursorError):
    "The same event was confirmed by two different instances / generations."


class ChangeLog(object):
    # Model class that will serve as the base for the changelog. This model
    # will be subclassed and mapped to your application database.
    base_model = BaseChangeLog

    # Template for the triggers that handle updating the changelog table.
    # table: table name
    # action: insert / update / delete
    # new_old: NEW or OLD (OLD is for DELETE)
    # primary_key: table primary key column name
    # column_array: output of build_column_array()
    # change_table: changelog table name
    template = """CREATE TRIGGER IF NOT EXISTS %(table)s_changes_%(action)s
    AFTER %(action)s ON %(table)s
    BEGIN
        INSERT INTO %(change_table)s
            ("action", "table", "primary_key", "changes")
        SELECT
            '%(action)s', '%(table)s', %(new_old)s."%(primary_key)s", "changes"
        FROM (
            SELECT json_group_object(
                col,
                json_array(
                    case when json_valid("oldval") then json("oldval")
                        else "oldval" end,
                    case when json_valid("newval") then json("newval")
                        else "newval" end)
                ) AS "changes"
            FROM (
                SELECT json_extract(value, '$[0]') as "col",
                       json_extract(value, '$[1]') as "oldval",
                       json_extract(value, '$[2]') as "newval"
                FROM json_each(json_array(%(column_array)s))
                WHERE "oldval" IS NOT "newval"
            )
        );
    END;"""

    drop_template = 'DROP TRIGGER IF EXISTS %(table)s_changes_%(action)s'

    _actions = ('INSERT', 'UPDATE', 'DELETE')

    def __init__(self, db, table_name='changelog'):
        self.db = db
        self.table_name = table_name

    def _build_column_array(self, model, use_old, use_new, skip_fields=None):
        # Builds a list of SQL expressions for each field we are tracking. This
        # is used as the data source for change tracking in our trigger.
        col_array = []
        for field in model._meta.sorted_fields:
            if field.primary_key:
                continue

            if skip_fields is not None and field.name in skip_fields:
                continue

            column = field.column_name
            new = 'NULL' if not use_new else 'NEW."%s"' % column
            old = 'NULL' if not use_old else 'OLD."%s"' % column

            if isinstance(field, JSONField):
                # Ensure that values are cast to JSON so that the serialization
                # is preserved when calculating the old / new.
                if use_old: old = 'json(%s)' % old
                if use_new: new = 'json(%s)' % new

            col_array.append("json_array('%s', %s, %s)" % (column, old, new))

        return ', '.join(col_array)

    def trigger_sql(self, model, action, skip_fields=None):
        assert action in self._actions
        use_old = action != 'INSERT'
        use_new = action != 'DELETE'
        cols = self._build_column_array(model, use_old, use_new, skip_fields)
        return self.template % {
            'table': model._meta.table_name,
            'action': action,
            'new_old': 'NEW' if action != 'DELETE' else 'OLD',
            'primary_key': model._meta.primary_key.column_name,
            'column_array': cols,
            'change_table': self.table_name}

    def drop_trigger_sql(self, model, action):
        assert action in self._actions
        return self.drop_template % {
            'table': model._meta.table_name,
            'action': action}

    @property
    def model(self):
        if not hasattr(self, '_changelog_model'):
            class ChangeLog(self.base_model):
                class Meta:
                    database = self.db
                    table_name = self.table_name
            self._changelog_model = ChangeLog

        return self._changelog_model

    def cursors(self, clock=None):
        # Return a cursor / lease manager bound to this changelog. The
        # optional ``clock`` is a zero-argument callable returning a naive
        # datetime (used to make lease expiry deterministic in tests).
        return ChangeLogCursors(self, clock=clock)

    def install(self, model, skip_fields=None, drop=True, insert=True,
                update=True, delete=True, create_table=True):
        ChangeLog = self.model
        if create_table:
            ChangeLog.create_table()

        actions = list(zip((insert, update, delete), self._actions))
        if drop:
            for _, action in actions:
                self.db.execute_sql(self.drop_trigger_sql(model, action))

        for enabled, action in actions:
            if enabled:
                sql = self.trigger_sql(model, action, skip_fields)
                self.db.execute_sql(sql)


class ChangeLogCursors(object):
    """Persistent per-consumer cursors over a :class:`ChangeLog`.

    Each downstream consumer owns a persistent lease. Every rolling
    takeover persists a new *generation* (代际) that starts at the
    previous generation's last confirmed position:

    - ``register`` creates generation 1 at position 0.
    - ``takeover`` records a new generation that starts exactly where the
      active generation left off. The old generation stays valid (state
      ``retiring``) until the new generation has confirmed consecutively.
    - ``ack`` confirms one event. Positions must be monotonic; receipts
      make the same event un-confirmable by two instances / generations.
    - ``cleanup`` only deletes events at or below the minimum position of
      every *valid* (non-retired) generation, across all consumers.

    Every mutating operation runs in a transaction, so a rejected
    acknowledgement leaves the original records untouched, and an
    interrupted takeover or cleanup resumes with the same generation and
    position when re-run.
    """
    # Generation states.
    ACTIVE = 'active'          # the sole, current generation
    TAKING_OVER = 'taking_over'  # new generation warming up
    RETIRING = 'retiring'      # previous generation, still valid
    RETIRED = 'retired'        # invalidated generation

    VALID_STATES = (ACTIVE, TAKING_OVER, RETIRING)

    DEFAULT_LEASE_SECONDS = 300
    DEFAULT_CONFIRMATIONS = 1

    def __init__(self, changelog, clock=None):
        self.changelog = changelog
        self.db = changelog.db
        self.table_name = changelog.table_name
        self._clock = clock or datetime.datetime.now

    # ------------------------------------------------------------------
    # Schema / models
    # ------------------------------------------------------------------

    @property
    def event_model(self):
        return self.changelog.model

    @property
    def models(self):
        if not hasattr(self, '_models'):
            db = self.db
            base = self.table_name

            class BaseCursorModel(Model):
                class Meta:
                    database = db

            class Lease(BaseCursorModel):
                consumer = TextField()
                generation = IntegerField()
                owner = TextField()
                position = BigIntegerField(default=0)
                state = TextField()
                consecutive = IntegerField(default=0)
                confirmations = IntegerField(
                    default=self.DEFAULT_CONFIRMATIONS)
                lease_seconds = IntegerField(null=True)
                lease_expires = DateTimeField(null=True)
                created_at = DateTimeField(default=datetime.datetime.now)
                updated_at = DateTimeField(default=datetime.datetime.now)

                class Meta:
                    table_name = '%s_lease' % base
                    indexes = ((('consumer', 'generation'), True),)

            class Receipt(BaseCursorModel):
                consumer = TextField()
                generation = IntegerField()
                owner = TextField()
                position = BigIntegerField()
                created_at = DateTimeField(default=datetime.datetime.now)

                class Meta:
                    table_name = '%s_receipt' % base
                    # One event may only ever be confirmed once for a
                    # consumer -- across every instance and generation.
                    indexes = ((('consumer', 'position'), True),
                               (('consumer', 'generation', 'position'),
                                True))

            class CleanupJob(BaseCursorModel):
                watermark = BigIntegerField()
                state = TextField(default='pending')
                created_at = DateTimeField(default=datetime.datetime.now)
                finished_at = DateTimeField(null=True)

                class Meta:
                    table_name = '%s_cleanup' % base

            self._models = {
                'lease': Lease,
                'receipt': Receipt,
                'cleanup': CleanupJob}
        return self._models

    @property
    def lease_model(self):
        return self.models['lease']

    @property
    def receipt_model(self):
        return self.models['receipt']

    @property
    def cleanup_model(self):
        return self.models['cleanup']

    def create_tables(self):
        "Create the lease, receipt and cleanup-ledger tables."
        for model in self.models.values():
            model.create_table()

    def _now(self):
        return self._clock()

    def _with_lock_retry(self, operation):
        # Run a short transactional operation, retrying transient SQLite
        # "database is locked" errors. Each retry starts a fresh
        # transaction (the failed one was rolled back), so two instances
        # racing to confirm the same event serialise here and the loser
        # deterministically sees the winner's receipt.
        last_exc = None
        for attempt in range(20):
            try:
                return operation()
            except OperationalError as exc:
                last_exc = exc
                if 'locked' not in str(exc).lower():
                    raise
                time.sleep(0.005 * (attempt + 1))
        raise last_exc

    def _expiry(self, lease_seconds):
        if lease_seconds is None:
            return None
        return self._now() + datetime.timedelta(seconds=lease_seconds)

    def _lease_expired(self, lease):
        if lease.lease_expires is None:
            return False
        return lease.lease_expires <= self._now()

    # ------------------------------------------------------------------
    # Leases: register / takeover / renew
    # ------------------------------------------------------------------

    def _leases(self, consumer):
        Lease = self.lease_model
        return (Lease
                .select()
                .where(Lease.consumer == consumer)
                .order_by(Lease.generation.desc()))

    def lease(self, consumer, generation):
        "Return the lease row for a (consumer, generation), or ``None``."
        Lease = self.lease_model
        return Lease.get_or_none(Lease.consumer == consumer,
                                 Lease.generation == generation)

    def current(self, consumer):
        "Return the newest valid (non-retired) lease for the consumer."
        for lease in self._leases(consumer):
            if lease.state != self.RETIRED:
                return lease
        return None

    def register(self, consumer, owner='default', lease_seconds=None):
        """Register a new downstream consumer, creating generation 1.

        The lease starts at position 0 (no events confirmed).
        """
        return self._with_lock_retry(
            lambda: self._register_once(consumer, owner, lease_seconds))

    def _register_once(self, consumer, owner, lease_seconds):
        Lease = self.lease_model
        with self.db.atomic():
            if Lease.select().where(Lease.consumer == consumer).exists():
                raise ConsumerExists(
                    'consumer %r is already registered, use takeover()'
                    % consumer)
            return Lease.create(
                consumer=consumer,
                generation=1,
                owner=owner,
                position=0,
                state=self.ACTIVE,
                consecutive=0,
                confirmations=self.DEFAULT_CONFIRMATIONS,
                lease_seconds=lease_seconds,
                lease_expires=self._expiry(lease_seconds))

    def takeover(self, consumer, owner, lease_seconds=None,
                 confirmations=None):
        """Persist a new generation that takes over ``consumer``.

        Returns a ``(generation, position)`` tuple: the new generation
        number and the position (the old generation's last confirmed
        position) the new instance must continue from.

        The old generation is only marked ``retiring`` -- it stays valid
        until the new generation confirms consecutively (see ``ack``).

        Re-running an interrupted takeover restores the same generation
        and position: an in-progress ``taking_over`` lease is resumed.
        If that lease has expired it may be adopted (owner reassigned),
        but its generation and position are always preserved.
        """
        if lease_seconds is None:
            lease_seconds = self.DEFAULT_LEASE_SECONDS
        if confirmations is None:
            confirmations = self.DEFAULT_CONFIRMATIONS
        return self._with_lock_retry(
            lambda: self._takeover_once(consumer, owner, lease_seconds,
                                        confirmations))

    def _takeover_once(self, consumer, owner, lease_seconds, confirmations):
        Lease = self.lease_model
        with self.db.atomic():
            pending = (Lease
                       .select()
                       .where(Lease.consumer == consumer,
                              Lease.state == self.TAKING_OVER)
                       .order_by(Lease.generation.desc())
                       .first())
            if pending is not None:
                # Interrupted takeover: resume / adopt the very same
                # generation and position.
                if pending.owner != owner:
                    if not self._lease_expired(pending):
                        raise ChangeLogCursorError(
                            'generation %s for %r is still owned by %r' %
                            (pending.generation, consumer, pending.owner))
                    pending.owner = owner
                pending.lease_seconds = lease_seconds
                pending.lease_expires = self._expiry(lease_seconds)
                pending.updated_at = self._now()
                pending.save()
                return pending.generation, pending.position

            active = (Lease
                      .select()
                      .where(Lease.consumer == consumer,
                             Lease.state == self.ACTIVE)
                      .order_by(Lease.generation.desc())
                      .first())
            if active is None:
                raise UnknownConsumer(
                    'unknown consumer %r, call register() first' % consumer)

            active.state = self.RETIRING
            active.updated_at = self._now()
            active.save()

            new_lease = Lease.create(
                consumer=consumer,
                generation=active.generation + 1,
                owner=owner,
                position=active.position,
                state=self.TAKING_OVER,
                consecutive=0,
                confirmations=confirmations,
                lease_seconds=lease_seconds,
                lease_expires=self._expiry(lease_seconds))
            return new_lease.generation, new_lease.position

    def renew(self, consumer, generation, owner, lease_seconds=None):
        "Extend the lease on a valid generation."
        return self._with_lock_retry(
            lambda: self._renew_once(consumer, generation, owner,
                                     lease_seconds))

    def _renew_once(self, consumer, generation, owner, lease_seconds):
        with self.db.atomic():
            lease = self.lease(consumer, generation)
            if lease is None:
                raise UnknownGeneration(
                    'unknown generation %s for %r' % (generation, consumer))
            if lease.state == self.RETIRED:
                raise GenerationRetired(
                    'generation %s for %r is retired' %
                    (generation, consumer))
            if lease.owner != owner:
                raise ChangeLogCursorError(
                    'generation %s for %r is owned by %r' %
                    (generation, consumer, lease.owner))
            if self._lease_expired(lease):
                raise LeaseExpired(
                    'lease for generation %s of %r has expired' %
                    (generation, consumer))
            if lease_seconds is None:
                lease_seconds = lease.lease_seconds
            lease.lease_seconds = lease_seconds
            lease.lease_expires = self._expiry(lease_seconds)
            lease.updated_at = self._now()
            lease.save()
            return lease.lease_expires

    # ------------------------------------------------------------------
    # Acknowledgement
    # ------------------------------------------------------------------

    def ack(self, consumer, generation, owner, position):
        """Confirm that event ``position`` was consumed.

        Rules (any violation aborts the transaction and leaves the
        stored records untouched):

        - the generation must exist, not be retired, and be unexpired;
        - the position must not regress the stored cursor;
        - the event must exist in the changelog;
        - a given event can only be confirmed once: a retry by the same
          instance is an idempotent no-op, while a confirmation of the
          same event by another instance / generation raises
          :class:`DuplicateConfirmation`.

        Once a taking-over generation confirms consecutively it becomes
        active and every older generation is retired.
        """
        if position < 0:
            raise ChangeLogCursorError('position must be non-negative')
        return self._with_lock_retry(
            lambda: self._ack_once(consumer, generation, owner, position))

    def _ack_once(self, consumer, generation, owner, position):
        Lease = self.lease_model
        Receipt = self.receipt_model
        Event = self.event_model

        with self.db.atomic():
            lease = self.lease(consumer, generation)
            if lease is None:
                raise UnknownGeneration(
                    'unknown generation %s for %r' % (generation, consumer))
            if lease.state == self.RETIRED:
                raise GenerationRetired(
                    'generation %s for %r is retired' %
                    (generation, consumer))
            if lease.owner != owner:
                raise ChangeLogCursorError(
                    'generation %s for %r is owned by %r' %
                    (generation, consumer, lease.owner))
            if self._lease_expired(lease):
                raise LeaseExpired(
                    'lease for generation %s of %r has expired' %
                    (generation, consumer))

            if position < lease.position:
                raise PositionRegression(
                    'position %s is behind stored position %s' %
                    (position, lease.position))

            if position == lease.position:
                if position == 0:
                    return 0
                # Idempotent retry of the last confirmation.
                receipt = Receipt.get_or_none(
                    Receipt.consumer == consumer,
                    Receipt.position == position)
                if (receipt is not None and
                        receipt.generation == generation and
                        receipt.owner == owner):
                    return position
                raise DuplicateConfirmation(
                    'event %s for %r was already confirmed by another '
                    'instance' % (position, consumer))

            # Advancing the cursor: the event must exist...
            if not Event.select().where(Event.id == position).exists():
                raise ChangeLogCursorError(
                    'event %s does not exist in the changelog' % position)

            # ...and must never have been confirmed by another instance
            # or generation.
            existing = Receipt.get_or_none(
                Receipt.consumer == consumer,
                Receipt.position == position)
            if existing is not None:
                raise DuplicateConfirmation(
                    'event %s for %r was already confirmed by generation '
                    '%s (%r)' % (position, consumer, existing.generation,
                                 existing.owner))

            try:
                Receipt.create(consumer=consumer,
                               generation=generation,
                               owner=owner,
                               position=position)
            except IntegrityError:
                # Concurrent confirmation of the same event.
                raise DuplicateConfirmation(
                    'event %s for %r was confirmed concurrently' %
                    (position, consumer))

            # Conditional advance -- refuses to overwrite a cursor that
            # another transaction moved in the meantime.
            updated = (Lease
                       .update(position=position,
                               consecutive=Lease.consecutive + 1,
                               lease_expires=self._expiry(lease.lease_seconds),
                               updated_at=self._now())
                       .where(Lease.id == lease.id,
                              Lease.position == lease.position)
                       .execute())
            if updated != 1:
                raise ChangeLogCursorError(
                    'cursor for generation %s of %r moved concurrently' %
                    (generation, consumer))

            lease = self.lease(consumer, generation)
            if (lease.state == self.TAKING_OVER and
                    lease.consecutive >= lease.confirmations):
                # New generation has confirmed consecutively: the old
                # generations are now invalid.
                (Lease
                 .update(state=self.RETIRED, updated_at=self._now())
                 .where(Lease.consumer == consumer,
                        Lease.generation < generation,
                        Lease.state != self.RETIRED)
                 .execute())
                (Lease
                 .update(state=self.ACTIVE, updated_at=self._now())
                 .where(Lease.id == lease.id)
                 .execute())
            return position

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def position(self, consumer, generation=None):
        """Last confirmed position visible to a reader.

        Defaults to the newest valid generation, so both an instance
        resuming after takeover and a steady-state reader see the same
        boundary as :meth:`cleanup` uses.
        """
        if generation is None:
            lease = self.current(consumer)
            if lease is None:
                raise UnknownConsumer(
                    'no valid lease for consumer %r' % consumer)
            return lease.position
        lease = self.lease(consumer, generation)
        if lease is None:
            raise UnknownGeneration(
                'unknown generation %s for %r' % (generation, consumer))
        if lease.state == self.RETIRED:
            raise GenerationRetired(
                'generation %s for %r is retired' % (generation, consumer))
        return lease.position

    def _read_query(self, consumer, generation=None, n=None):
        Event = self.event_model
        pos = self.position(consumer, generation)
        query = (Event
                 .select(Event.id, Event.timestamp, Event.action, Event.table,
                         Event.primary_key, Event.changes)
                 .where(Event.id > pos)
                 .order_by(Event.id))
        if n is not None:
            query = query.limit(n)
        return query

    def read(self, consumer, generation=None, n=None):
        """Read pending events as changelog model instances.

        Both read entry points (:meth:`read` and :meth:`fetch`) are built
        from one query, so they always expose the same event range.
        """
        return list(self._read_query(consumer, generation, n))

    def fetch(self, consumer, generation=None, n=None):
        """Read pending events as raw tuples (second read entry point).

        Uses the exact same SQL / parameters as :meth:`read`.
        """
        query = self._read_query(consumer, generation, n)
        sql, params = query.sql()
        cursor = self.db.execute_sql(sql, params)
        return cursor.fetchall()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def valid_leases(self):
        "All leases that still count as valid downstreams."
        Lease = self.lease_model
        return [lease for lease in
                Lease.select().order_by(Lease.consumer, Lease.generation)
                if lease.state in self.VALID_STATES]

    def safe_position(self):
        """Minimum confirmed position over every valid generation.

        Retired generations do not count; generations mid-takeover do.
        With no registered consumers the safe position is 0, so nothing
        is deleted.
        """
        positions = [lease.position for lease in self.valid_leases()]
        return min(positions) if positions else 0

    def cleanup(self):
        """Delete events safely consumed by every valid downstream.

        Returns the watermark applied. An interrupted cleanup is
        resumed: an unfinished cleanup-ledger row pins the rerun to the
        same watermark even if cursors advanced in the meantime.
        """
        return self._with_lock_retry(self._cleanup_once)

    def _cleanup_once(self):
        Event = self.event_model
        Receipt = self.receipt_model
        CleanupJob = self.cleanup_model

        with self.db.atomic():
            job = (CleanupJob
                   .select()
                   .where(CleanupJob.state == 'pending')
                   .order_by(CleanupJob.id)
                   .first())
            if job is not None:
                # Resume the interrupted cleanup with the same position.
                watermark = job.watermark
            else:
                watermark = self.safe_position()
                job = CleanupJob.create(watermark=watermark,
                                        state='pending')

            Event.delete().where(Event.id <= watermark).execute()
            # Keep the receipt *at* the watermark: a generation that took
            # over at exactly this position never confirmed the event
            # itself, so the receipt is still needed to detect a
            # duplicate confirmation of it after the event row is gone.
            Receipt.delete().where(Receipt.position < watermark).execute()

            job.state = 'done'
            job.finished_at = self._now()
            job.save()
            return watermark
