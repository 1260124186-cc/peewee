import datetime

from peewee import *
from playhouse.sqlite_changelog import ChangeLog
from playhouse.sqlite_changelog import ChangeLogCursors
from playhouse.sqlite_changelog import ChangeLogCursorError
from playhouse.sqlite_changelog import ConsumerExists
from playhouse.sqlite_changelog import DuplicateConfirmation
from playhouse.sqlite_changelog import GenerationRetired
from playhouse.sqlite_changelog import LeaseExpired
from playhouse.sqlite_changelog import PositionRegression
from playhouse.sqlite_changelog import UnknownConsumer
from playhouse.sqlite_changelog import UnknownGeneration

from .base import ModelTestCase
from .base import TestModel
from .base import requires_models
from .base import skip_unless


database = SqliteDatabase(':memory:', pragmas={'foreign_keys': 1})


class Person(TestModel):
    name = TextField()
    dob = DateField()


class Note(TestModel):
    person = ForeignKeyField(Person, on_delete='CASCADE')
    content = TextField()
    timestamp = TimestampField()
    status = IntegerField(default=0)


class CT1(TestModel):
    f1 = TextField()
    f2 = IntegerField(null=True)
    f3 = FloatField()
    fi = IntegerField()


class CT2(TestModel):
    data = JSONField()  # Diff of json?


changelog = ChangeLog(database)
CL = changelog.model


class TestChangeLog(ModelTestCase):
    database = database
    requires = [Person, Note]

    def setUp(self):
        super(TestChangeLog, self).setUp()
        changelog.install(Person)
        changelog.install(Note, skip_fields=['timestamp'])
        self.last_index = 0

    def assertChanges(self, changes, last_index=None):
        last_index = last_index or self.last_index
        query = (CL
                 .select(CL.action, CL.table, CL.changes)
                 .order_by(CL.id)
                 .offset(last_index))
        accum = list(query.tuples())
        self.last_index += len(accum)
        self.assertEqual(accum, changes)

    def test_changelog(self):
        huey = Person.create(name='huey', dob=datetime.date(2010, 5, 1))
        zaizee = Person.create(name='zaizee', dob=datetime.date(2013, 1, 1))
        self.assertChanges([
            ('INSERT', 'person', {'name': [None, 'huey'],
                                  'dob': [None, '2010-05-01']}),
            ('INSERT', 'person', {'name': [None, 'zaizee'],
                                  'dob': [None, '2013-01-01']})])

        zaizee.dob = datetime.date(2013, 2, 2)
        zaizee.save()
        self.assertChanges([
            ('UPDATE', 'person', {'dob': ['2013-01-01', '2013-02-02']})])

        zaizee.name = 'zaizee-x'
        zaizee.dob = datetime.date(2013, 3, 3)
        zaizee.save()

        huey.save()  # No changes.

        self.assertChanges([
            ('UPDATE', 'person', {'name': ['zaizee', 'zaizee-x'],
                                  'dob': ['2013-02-02', '2013-03-03']}),
            ('UPDATE', 'person', {})])

        zaizee.delete_instance()
        self.assertChanges([
            ('DELETE', 'person', {'name': ['zaizee-x', None],
                                  'dob': ['2013-03-03', None]})])

        nh1 = Note.create(person=huey, content='huey1', status=1)
        nh2 = Note.create(person=huey, content='huey2', status=2)
        self.assertChanges([
            ('INSERT', 'note', {'person_id': [None, huey.id],
                                'content': [None, 'huey1'],
                                'status': [None, 1]}),
            ('INSERT', 'note', {'person_id': [None, huey.id],
                                'content': [None, 'huey2'],
                                'status': [None, 2]})])

        nh1.content = 'huey1-x'
        nh1.status = 0
        nh1.save()

        mickey = Person.create(name='mickey', dob=datetime.date(2009, 8, 1))
        nh2.person = mickey
        nh2.save()

        self.assertChanges([
            ('UPDATE', 'note', {'content': ['huey1', 'huey1-x'],
                                'status': [1, 0]}),
            ('INSERT', 'person', {'name': [None, 'mickey'],
                                  'dob': [None, '2009-08-01']}),
            ('UPDATE', 'note', {'person_id': [huey.id, mickey.id]})])

        mickey.delete_instance()
        self.assertChanges([
            ('DELETE', 'note', {'person_id': [mickey.id, None],
                                'content': ['huey2', None],
                                'status': [2, None]}),
            ('DELETE', 'person', {'name': ['mickey', None],
                                  'dob': ['2009-08-01', None]})])

    @requires_models(CT1)
    def test_changelog_details(self):
        changelog.install(CT1, skip_fields=['fi'], insert=False, delete=False)

        c1 = CT1.create(f1='v1', f2=1, f3=1.5, fi=0)
        self.assertChanges([])

        CT1.update(f1='v1-x', f2=2, f3=2.5, fi=1).execute()
        self.assertChanges([
            ('UPDATE', 'ct1', {
                'f1': ['v1', 'v1-x'],
                'f2': [1, 2],
                'f3': [1.5, 2.5]})])

        c1.f2 = None
        c1.save()  # Overwrites previously-changed fields.
        self.assertChanges([('UPDATE', 'ct1', {
            'f1': ['v1-x', 'v1'],
            'f2': [2, None],
            'f3': [2.5, 1.5]})])

        c1.delete_instance()
        self.assertChanges([])

    @requires_models(CT2)
    def test_changelog_jsonfield(self):
        changelog.install(CT2)

        ca = CT2.create(data={'k1': 'v1'})
        cb = CT2.create(data=['i0', 'i1', 'i2'])
        cc = CT2.create(data='hello')

        self.assertChanges([
            ('INSERT', 'ct2', {'data': [None, {'k1': 'v1'}]}),
            ('INSERT', 'ct2', {'data': [None, ['i0', 'i1', 'i2']]}),
            ('INSERT', 'ct2', {'data': [None, 'hello']})])

        ca.data['k1'] = 'v1-x'
        cb.data.append('i3')
        cc.data = 'world'

        ca.save()
        cb.save()
        cc.save()

        self.assertChanges([
            ('UPDATE', 'ct2', {'data': [{'k1': 'v1'}, {'k1': 'v1-x'}]}),
            ('UPDATE', 'ct2', {'data': [['i0', 'i1', 'i2'],
                                        ['i0', 'i1', 'i2', 'i3']]}),
            ('UPDATE', 'ct2', {'data': ['hello', 'world']})])

        cc.data = 13.37
        cc.save()
        self.assertChanges([('UPDATE', 'ct2', {'data': ['world', 13.37]})])

        ca.delete_instance()
        self.assertChanges([
            ('DELETE', 'ct2', {'data': [{'k1': 'v1-x'}, None]})])


class FakeClock(object):
    "Deterministic clock for lease expiry tests."
    def __init__(self, dt=None):
        self.dt = dt or datetime.datetime(2026, 1, 1, 12, 0, 0)

    def __call__(self):
        return self.dt

    def advance(self, seconds):
        self.dt += datetime.timedelta(seconds=seconds)


class TestChangeLogCursors(ModelTestCase):
    database = database
    requires = [Person]

    def setUp(self):
        super(TestChangeLogCursors, self).setUp()
        changelog.install(Person)
        self.clock = FakeClock()
        self.cursors = ChangeLogCursors(changelog, clock=self.clock)
        self.cursors.create_tables()

    def create_events(self, n):
        "Create ``n`` changelog events and return their ids."
        ids = []
        for i in range(n):
            p = Person.create(name='p%s' % i, dob=datetime.date(2020, 1, 1))
            ids.append(p.id)
        return ids

    def event_ids(self):
        return [row.id for row in CL.select(CL.id).order_by(CL.id)]

    def assertReadParity(self, consumer, generation=None, n=None,
                         expected=None):
        # The two read entry points must expose the same event range.
        rows = self.cursors.read(consumer, generation, n)
        tuples = self.cursors.fetch(consumer, generation, n)
        self.assertEqual([r.id for r in rows], [t[0] for t in tuples])
        for row, tup in zip(rows, tuples):
            self.assertEqual((row.id, row.action, row.table,
                              row.primary_key),
                             (tup[0], tup[2], tup[3], tup[4]))
        if expected is not None:
            self.assertEqual([r.id for r in rows], expected)
        return [r.id for r in rows]

    # -- basics --------------------------------------------------------

    def test_register_read_ack_cleanup(self):
        self.create_events(3)

        self.assertEqual(self.cursors.safe_position(), 0)
        # No consumers registered: cleanup is a no-op.
        self.assertEqual(self.cursors.cleanup(), 0)
        self.assertEqual(len(self.event_ids()), 3)

        self.cursors.register('idx', owner='old', lease_seconds=None)
        self.assertEqual(self.cursors.position('idx'), 0)
        self.assertReadParity('idx', expected=[1, 2, 3])
        self.assertReadParity('idx', n=2, expected=[1, 2])

        for pos in (1, 2, 3):
            self.assertEqual(self.cursors.ack('idx', 1, 'old', pos), pos)
        self.assertEqual(self.cursors.position('idx'), 3)
        self.assertReadParity('idx', expected=[])

        self.assertEqual(self.cursors.cleanup(), 3)
        self.assertEqual(self.event_ids(), [])
        self.assertReadParity('idx', expected=[])

    def test_register_twice_fails(self):
        self.cursors.register('idx')
        with self.assertRaises(ConsumerExists):
            self.cursors.register('idx')

    def test_ack_unknown_consumer_generation_and_event(self):
        self.create_events(2)
        # Unknown consumer / generation.
        with self.assertRaises(UnknownGeneration):
            self.cursors.ack('nope', 1, 'old', 1)

        self.cursors.register('idx', owner='old', lease_seconds=None)
        with self.assertRaises(UnknownGeneration):
            self.cursors.ack('idx', 99, 'old', 1)

        # Owner mismatch / non-existent event also stop and preserve.
        with self.assertRaises(ChangeLogCursorError):
            self.cursors.ack('idx', 1, 'intruder', 1)
        with self.assertRaises(ChangeLogCursorError):
            self.cursors.ack('idx', 1, 'old', 42)
        lease = self.cursors.lease('idx', 1)
        self.assertEqual(lease.position, 0)

    # -- takeover ------------------------------------------------------

    def test_takeover_starts_at_last_position(self):
        self.create_events(4)
        self.cursors.register('idx', owner='old', lease_seconds=None)
        for pos in (1, 2):
            self.cursors.ack('idx', 1, 'old', pos)

        generation, position = self.cursors.takeover(
            'idx', 'new', lease_seconds=None)
        self.assertEqual((generation, position), (2, 2))

        # Old generation stays valid until new gen confirms; new gen is
        # the current one, starting at the old last confirmed position.
        old = self.cursors.lease('idx', 1)
        new = self.cursors.lease('idx', 2)
        self.assertEqual(old.state, self.cursors.RETIRING)
        self.assertEqual(new.state, self.cursors.TAKING_OVER)
        self.assertEqual(new.position, 2)
        self.assertEqual(self.cursors.current('idx').generation, 2)
        self.assertEqual(self.cursors.position('idx'), 2)
        self.assertEqual(
            sorted((l.generation, l.position)
                   for l in self.cursors.valid_leases()),
            [(1, 2), (2, 2)])

        # Both generations (and both read entries) see the same range.
        self.assertReadParity('idx', 1, expected=[3, 4])
        self.assertReadParity('idx', 2, expected=[3, 4])
        self.assertReadParity('idx', expected=[3, 4])

        # Cleanup is bounded by BOTH valid generations.
        self.assertEqual(self.cursors.safe_position(), 2)
        self.assertEqual(self.cursors.cleanup(), 2)
        self.assertEqual(self.event_ids(), [3, 4])
        self.assertReadParity('idx', 1, expected=[3, 4])
        self.assertReadParity('idx', 2, expected=[3, 4])

    def test_takeover_unknown_consumer(self):
        with self.assertRaises(UnknownConsumer):
            self.cursors.takeover('nope', 'new')

    def test_old_generation_still_commits_during_overlap(self):
        self.create_events(4)
        self.cursors.register('idx', owner='old', lease_seconds=None)
        self.cursors.ack('idx', 1, 'old', 1)
        self.cursors.ack('idx', 1, 'old', 2)
        self.cursors.takeover('idx', 'new', lease_seconds=None)

        # The old instance is still allowed to confirm while retiring.
        self.assertEqual(self.cursors.ack('idx', 1, 'old', 3), 3)
        self.assertEqual(self.cursors.safe_position(), 2)
        self.assertEqual(self.cursors.cleanup(), 2)
        self.assertEqual(self.event_ids(), [3, 4])

    def test_same_event_confirmed_by_two_instances_stops(self):
        self.create_events(4)
        self.cursors.register('idx', owner='old', lease_seconds=None)
        self.cursors.ack('idx', 1, 'old', 1)
        self.cursors.ack('idx', 1, 'old', 2)
        # Barrier of 2 keeps the old generation valid past the new
        # generation's first confirmation.
        self.cursors.takeover('idx', 'new', lease_seconds=None,
                              confirmations=2)

        Receipt = self.cursors.receipt_model

        # Old confirms 3 first; new confirming the same event halts.
        self.cursors.ack('idx', 1, 'old', 3)
        with self.assertRaises(DuplicateConfirmation):
            self.cursors.ack('idx', 2, 'new', 3)
        self.assertEqual(self.cursors.lease('idx', 1).position, 3)
        self.assertEqual(self.cursors.lease('idx', 2).position, 2)
        self.assertEqual(
            Receipt.select()
            .where(Receipt.consumer == 'idx', Receipt.position == 3)
            .count(), 1)

        # Reverse ordering: new confirms 4 first (barrier not yet met),
        # then the old instance confirms 4 -> halt with records intact.
        self.cursors.ack('idx', 2, 'new', 4)
        with self.assertRaises(DuplicateConfirmation):
            self.cursors.ack('idx', 1, 'old', 4)
        self.assertEqual(self.cursors.lease('idx', 1).position, 3)
        self.assertEqual(self.cursors.lease('idx', 2).position, 4)
        self.assertEqual(
            Receipt.select()
            .where(Receipt.consumer == 'idx', Receipt.position == 4)
            .count(), 1)

        # The rejected acks did not retire anything.
        self.assertEqual(self.cursors.lease('idx', 1).state,
                         self.cursors.RETIRING)
        self.assertEqual(self.cursors.lease('idx', 2).state,
                         self.cursors.TAKING_OVER)

    def test_boundary_receipt_survives_cleanup(self):
        # After cleanup at the takeover boundary, the receipt at the
        # boundary is retained, so the new generation can still be
        # stopped if it tries to confirm the event the old generation
        # already confirmed.
        self.create_events(3)
        self.cursors.register('idx', owner='old', lease_seconds=None)
        self.cursors.ack('idx', 1, 'old', 1)
        self.cursors.ack('idx', 1, 'old', 2)
        self.cursors.takeover('idx', 'new', lease_seconds=None)
        self.assertEqual(self.cursors.cleanup(), 2)
        self.assertEqual(self.event_ids(), [3])

        Receipt = self.cursors.receipt_model
        self.assertEqual(
            Receipt.select()
            .where(Receipt.consumer == 'idx', Receipt.position == 2)
            .count(), 1)
        # New generation re-confirming the inherited boundary: halt.
        with self.assertRaises(DuplicateConfirmation):
            self.cursors.ack('idx', 2, 'new', 2)
        # Old instance retrying its own confirmation is idempotent.
        self.assertEqual(self.cursors.ack('idx', 1, 'old', 2), 2)
        # The next event still flows normally to the new generation.
        self.cursors.ack('idx', 2, 'new', 3)

    def test_same_event_idempotent_retry(self):
        self.create_events(3)
        self.cursors.register('idx', owner='old', lease_seconds=None)
        self.cursors.ack('idx', 1, 'old', 1)
        self.cursors.ack('idx', 1, 'old', 2)

        Receipt = self.cursors.receipt_model
        # Retrying the last confirmation with the same instance is a
        # no-op (no duplicate receipt, cursor unchanged).
        self.assertEqual(self.cursors.ack('idx', 1, 'old', 2), 2)
        self.assertEqual(self.cursors.lease('idx', 1).position, 2)
        self.assertEqual(
            Receipt.select().where(Receipt.consumer == 'idx').count(), 2)

    def test_position_regression_stops(self):
        self.create_events(3)
        self.cursors.register('idx', owner='old', lease_seconds=None)
        self.cursors.ack('idx', 1, 'old', 1)
        self.cursors.ack('idx', 1, 'old', 2)
        with self.assertRaises(PositionRegression):
            self.cursors.ack('idx', 1, 'old', 1)
        self.assertEqual(self.cursors.lease('idx', 1).position, 2)

    def test_consecutive_confirmation_retires_old_generation(self):
        self.create_events(4)
        self.cursors.register('idx', owner='old', lease_seconds=None)
        self.cursors.ack('idx', 1, 'old', 1)
        self.cursors.ack('idx', 1, 'old', 2)

        # Barrier of 2: one confirmation is not enough.
        self.cursors.takeover('idx', 'new', lease_seconds=None,
                              confirmations=2)
        self.cursors.ack('idx', 2, 'new', 3)
        self.assertEqual(self.cursors.lease('idx', 1).state,
                         self.cursors.RETIRING)
        self.assertEqual(self.cursors.lease('idx', 2).state,
                         self.cursors.TAKING_OVER)

        self.cursors.ack('idx', 2, 'new', 4)
        self.assertEqual(self.cursors.lease('idx', 2).state,
                         self.cursors.ACTIVE)
        self.assertEqual(self.cursors.lease('idx', 1).state,
                         self.cursors.RETIRED)

        # Once retired, the old generation's acks stop and preserve.
        with self.assertRaises(GenerationRetired):
            self.cursors.ack('idx', 1, 'old', 3)
        self.assertEqual(self.cursors.lease('idx', 1).position, 2)

        # The retired generation no longer pins the safe position.
        self.assertEqual(self.cursors.safe_position(), 4)
        self.assertEqual(self.cursors.cleanup(), 4)
        self.assertEqual(self.event_ids(), [])

        # Next takeover continues from generation 2's position.
        generation, position = self.cursors.takeover(
            'idx', 'newer', lease_seconds=None)
        self.assertEqual((generation, position), (3, 4))

    # -- lease expiry --------------------------------------------------

    def test_expired_generation_ack_stops(self):
        self.create_events(2)
        self.cursors.register('idx', owner='old', lease_seconds=10)
        self.cursors.ack('idx', 1, 'old', 1)

        self.clock.advance(11)
        with self.assertRaises(LeaseExpired):
            self.cursors.ack('idx', 1, 'old', 2)
        with self.assertRaises(LeaseExpired):
            self.cursors.renew('idx', 1, 'old')
        self.assertEqual(self.cursors.lease('idx', 1).position, 1)

        # A live lease can be renewed and keep working.
        clock2 = FakeClock()
        cursors2 = ChangeLogCursors(changelog, clock=clock2)
        cursors2.register('other', owner='o', lease_seconds=10)
        cursors2.ack('other', 1, 'o', 1)
        clock2.advance(5)
        cursors2.renew('other', 1, 'o')
        clock2.advance(8)
        cursors2.ack('other', 1, 'o', 2)  # renewed lease still valid

    # -- resumability --------------------------------------------------

    def test_takeover_resume_same_generation_and_position(self):
        self.create_events(3)
        self.cursors.register('idx', owner='old', lease_seconds=10)
        self.cursors.ack('idx', 1, 'old', 1)

        self.assertEqual(self.cursors.takeover('idx', 'new'), (2, 1))
        Lease = self.cursors.lease_model
        # Re-running the interrupted takeover restores gen+position.
        self.assertEqual(self.cursors.takeover('idx', 'new'), (2, 1))
        self.assertEqual(
            Lease.select().where(Lease.consumer == 'idx').count(), 2)

        # Another owner cannot steal a live in-progress takeover.
        with self.assertRaises(ChangeLogCursorError):
            self.cursors.takeover('idx', 'intruder')
        self.assertEqual(self.cursors.lease('idx', 2).owner, 'new')

        # After expiry another instance adopts the SAME generation and
        # position.
        self.clock.advance(600)
        self.assertEqual(
            self.cursors.takeover('idx', 'new2'), (2, 1))
        lease = self.cursors.lease('idx', 2)
        self.assertEqual(lease.owner, 'new2')
        self.assertEqual(lease.position, 1)
        self.assertEqual(lease.generation, 2)

    def test_cleanup_resume_same_watermark(self):
        self.create_events(4)
        self.cursors.register('a', owner='oa', lease_seconds=None)
        self.cursors.register('b', owner='ob', lease_seconds=None)
        for pos in (1, 2, 3, 4):
            self.cursors.ack('a', 1, 'oa', pos)
        self.cursors.ack('b', 1, 'ob', 1)
        self.cursors.ack('b', 1, 'ob', 2)
        self.assertEqual(self.cursors.safe_position(), 2)

        CleanupJob = self.cursors.cleanup_model
        # Simulate a crash after the cleanup ledger pinned watermark 2
        # but before the delete ran.
        CleanupJob.create(watermark=2, state='pending')

        # Both consumers advance past 2 before the rerun: the resumed
        # cleanup must still finish at the persisted position 2...
        for pos in (3, 4):
            self.cursors.ack('b', 1, 'ob', pos)
        self.assertEqual(self.cursors.cleanup(), 2)
        self.assertEqual(self.event_ids(), [3, 4])

        # ...and the next run applies the newly computed boundary.
        self.assertEqual(self.cursors.cleanup(), 4)
        self.assertEqual(self.event_ids(), [])

    def test_cleanup_interrupted_before_delete_is_idempotent(self):
        # Interruption after the ledger row is written but before the
        # delete: rerun performs the delete, and a second rerun against
        # the same boundary deletes nothing new.
        self.create_events(2)
        self.cursors.register('a', owner='oa', lease_seconds=None)
        self.cursors.ack('a', 1, 'oa', 1)
        self.cursors.ack('a', 1, 'oa', 2)

        CleanupJob = self.cursors.cleanup_model
        CleanupJob.create(watermark=2, state='pending')
        self.assertEqual(self.cursors.cleanup(), 2)
        self.assertEqual(self.event_ids(), [])
        self.assertEqual(self.cursors.cleanup(), 2)
        self.assertEqual(self.event_ids(), [])

    # -- multiple consumers -------------------------------------------

    def test_cleanup_covers_all_valid_downstreams(self):
        self.create_events(4)
        self.cursors.register('a', owner='oa', lease_seconds=None)
        self.cursors.register('b', owner='ob', lease_seconds=None)

        for pos in (1, 2, 3, 4):
            self.cursors.ack('a', 1, 'oa', pos)
        self.cursors.ack('b', 1, 'ob', 1)
        self.assertEqual(self.cursors.cleanup(), 1)
        self.assertEqual(self.event_ids(), [2, 3, 4])

        # A takeover on consumer "a" pins its new generation at 4 while
        # consumer b is at 1: cleanup must still wait for b.
        self.cursors.takeover('a', 'oa2', lease_seconds=None)
        self.assertEqual(self.cursors.safe_position(), 1)
        self.assertEqual(self.cursors.cleanup(), 1)
        self.assertEqual(self.event_ids(), [2, 3, 4])

        for pos in (2, 3, 4):
            self.cursors.ack('b', 1, 'ob', pos)
        self.assertEqual(self.cursors.cleanup(), 4)
        self.assertEqual(self.event_ids(), [])
