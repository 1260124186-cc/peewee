"""
Path-scoped JSON patch store with generations and snapshot compaction.

Two services may edit the same JSON document concurrently. Instead of
last-write-wins overwriting the whole document, every commit records one
patch row per target path together with the document generation it was
based on and the generation it produced.

Guarantees:

* Patches targeting non-overlapping JSON-pointer paths merge cleanly; keys
  that no patch manages are never touched.
* A commit fails as a whole (nothing is written, generation is not bumped)
  when:
    - a path is syntactically or semantically illegal,
    - two ops inside the commit overlap,
    - a patch confirmed after the caller's base generation overlaps one of
      the commit's paths (same-path conflict / stale generation),
    - the base generation is unknown to the store.
* Confirmed patches are periodically compacted into a snapshot. Recovery
  always replays "latest snapshot + later patches", so a crash in the middle
  of compaction or a process restart reproduces byte-identical JSON.
* Document reads and patch exports run in a single transaction and always
  report the same generation; a SHA-256 checksum makes divergence
  detectable.

Only JSON objects are navigable: every parent segment of a path must be an
existing object. The supported ops are the object-oriented subset of
RFC 6902: ``add`` (set a key, creating or replacing it), ``replace`` (set an
existing key) and ``remove`` (delete an existing key).
"""

import copy
import datetime
import hashlib
import json

from peewee import *


#
# Errors. Any of these aborts the whole commit.
#

class JsonPatchError(Exception):
    """Base class for every error raised by the patch store."""


class DocumentNotFoundError(JsonPatchError):
    """The requested document id is unknown."""


class DocumentExistsError(JsonPatchError):
    """A document with the given id already exists."""


class InvalidPatchError(JsonPatchError):
    """An op is malformed (unknown op, missing value, bad payload type)."""


class IllegalPathError(JsonPatchError):
    """A JSON pointer is malformed or cannot be resolved on the document."""


class PathConflictError(JsonPatchError):
    """An overlapping path was already confirmed at a newer generation."""


class StaleGenerationError(JsonPatchError):
    """The base generation predates confirmed patches or the snapshot."""


#
# RFC 6901 JSON-pointer helpers.
#

def parse_pointer(pointer):
    """Split a JSON pointer into its unescaped key tokens.

    :raises IllegalPathError: if ``pointer`` is not a string pointer, is the
        root pointer ``""`` or contains an invalid ``~`` escape.
    """
    if not isinstance(pointer, str):
        raise IllegalPathError('JSON pointer must be a string, got %r.'
                               % type(pointer).__name__)
    if pointer == '':
        raise IllegalPathError('Root path "" is not managed; patches must '
                               'target a key below the document root.')
    if not pointer.startswith('/'):
        raise IllegalPathError('JSON pointer %r must start with "/".'
                               % pointer)
    tokens = []
    for raw in pointer.split('/')[1:]:
        chars = []
        i = 0
        while i < len(raw):
            ch = raw[i]
            if ch == '~':
                if i + 1 >= len(raw) or raw[i + 1] not in ('0', '1'):
                    raise IllegalPathError(
                        'Invalid escape in JSON pointer %r: "~" must be '
                        'followed by "0" or "1".' % pointer)
                chars.append('~' if raw[i + 1] == '0' else '/')
                i += 2
            else:
                chars.append(ch)
                i += 1
        tokens.append(''.join(chars))
    return tuple(tokens)


def pointer_to_string(tokens):
    """Render key tokens back into a canonical JSON pointer."""
    parts = []
    for token in tokens:
        parts.append('/' + token.replace('~', '~0').replace('/', '~1'))
    return ''.join(parts)


def paths_overlap(tokens_a, tokens_b):
    """Return True if two token paths are equal or one contains the other."""
    shorter = tokens_a if len(tokens_a) <= len(tokens_b) else tokens_b
    longer = tokens_b if shorter is tokens_a else tokens_a
    return tuple(longer[:len(shorter)]) == tuple(shorter)


def checksum_of(data):
    """Canonical SHA-256 checksum of a JSON-compatible value."""
    canonical = json.dumps(data, sort_keys=True, separators=(',', ':'),
                           ensure_ascii=False)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


#
# Op constructors.
#

def add(path, value):
    """Build an ``add`` op (create or replace an object key)."""
    return {'op': 'add', 'path': path, 'value': value}


def replace(path, value):
    """Build a ``replace`` op (the key must already exist)."""
    return {'op': 'replace', 'path': path, 'value': value}


def remove(path):
    """Build a ``remove`` op (the key must already exist)."""
    return {'op': 'remove', 'path': path}


class DocumentView(object):
    """Materialized document state at a single generation."""
    __slots__ = ('doc_id', 'generation', 'snapshot_generation', 'data')

    def __init__(self, doc_id, generation, snapshot_generation, data):
        self.doc_id = doc_id
        self.generation = generation
        self.snapshot_generation = snapshot_generation
        self.data = data

    def __repr__(self):
        return ('<DocumentView doc_id=%r generation=%d '
                'snapshot_generation=%d>' % (self.doc_id, self.generation,
                                             self.snapshot_generation))

    def __eq__(self, other):
        if not isinstance(other, DocumentView):
            return NotImplemented
        return (self.doc_id == other.doc_id and
                self.generation == other.generation and
                self.snapshot_generation == other.snapshot_generation and
                self.data == other.data)


def replay_bundle(bundle):
    """Pure function: rebuild JSON from an exported snapshot + patch list."""
    data = copy.deepcopy(bundle['snapshot']['data'])
    for opdef in bundle['patches']:
        tokens = parse_pointer(opdef['path'])
        value = opdef.get('value')
        JsonPatchStore.apply_op(data, opdef['op'], tokens, value)
    return data


class JsonPatchStore(object):
    """Persist JSON documents as a snapshot plus a generation-ordered log.

    :param Database database: peewee database holding the store tables.
    :param str table_prefix: prefix for the three backing tables.
    """

    VALID_OPS = ('add', 'replace', 'remove')

    def __init__(self, database, table_prefix='jsonp'):
        self.database = database
        self.table_prefix = table_prefix
        # Alias so the nested class bodies below don't shadow the name with
        # their own ``database`` assignment.
        db = database

        class Base(Model):
            class Meta:
                database = db

        class JsonDocument(Base):
            doc_id = CharField(max_length=255, primary_key=True)
            generation = IntegerField(default=0)
            checksum = CharField(max_length=64)
            created_at = DateTimeField(default=datetime.datetime.now)

            class Meta:
                table_name = '%s_document' % table_prefix

        class JsonSnapshot(Base):
            doc_id = CharField(max_length=255, index=True)
            generation = IntegerField()
            data = TextField()
            checksum = CharField(max_length=64)
            created_at = DateTimeField(default=datetime.datetime.now)

            class Meta:
                table_name = '%s_snapshot' % table_prefix
                indexes = ((('doc_id', 'generation'), True),)

        class JsonPatch(Base):
            doc_id = CharField(max_length=255, index=True)
            path = TextField()
            op = CharField(max_length=16)
            value = TextField(null=True)
            base_generation = IntegerField()
            generation = IntegerField()
            seq = IntegerField()
            created_at = DateTimeField(default=datetime.datetime.now)

            class Meta:
                table_name = '%s_patch' % table_prefix
                indexes = (
                    (('doc_id', 'generation'), False),
                    (('doc_id', 'generation', 'seq'), True),
                )

        self.Document = JsonDocument
        self.Snapshot = JsonSnapshot
        self.Patch = JsonPatch
        self.models = (JsonDocument, JsonSnapshot, JsonPatch)
        database.create_tables(self.models, safe=True)

    #
    # Transaction helpers.
    #

    def _write_txn(self):
        # BEGIN IMMEDIATE serializes writers on SQLite so two services never
        # interleave a read/check/write sequence.
        if isinstance(self.database, SqliteDatabase):
            return self.database.atomic(lock_type='IMMEDIATE')
        return self.database.atomic()

    def _read_txn(self):
        # A deferred transaction gives a consistent cross-table view for the
        # duration of the read/export.
        return self.database.atomic()

    #
    # Document lifecycle.
    #

    def create_document(self, doc_id, data):
        """Register a new document with its generation-0 snapshot."""
        if not isinstance(doc_id, str) or not doc_id:
            raise InvalidPatchError('doc_id must be a non-empty string.')
        if not isinstance(data, dict):
            raise InvalidPatchError('Document root must be a JSON object.')
        # Fail early on non-JSON-serializable payloads.
        serialized = json.dumps(data)
        with self._write_txn():
            if self.Document.select().where(
                    self.Document.doc_id == doc_id).exists():
                raise DocumentExistsError(
                    'Document %r already exists.' % doc_id)
            self.Document.create(doc_id=doc_id, generation=0,
                                 checksum=checksum_of(data))
            self.Snapshot.create(doc_id=doc_id, generation=0,
                                 data=serialized, checksum=checksum_of(data))
        return self.read(doc_id)

    def _get_doc(self, doc_id):
        try:
            return self.Document.get_by_id(doc_id)
        except DoesNotExist:
            raise DocumentNotFoundError('Unknown document %r.' % doc_id)

    #
    # Reads.
    #

    def _latest_snapshot(self, doc_id):
        return (self.Snapshot
                .select()
                .where(self.Snapshot.doc_id == doc_id)
                .order_by(self.Snapshot.generation.desc())
                .first())

    def _materialize(self, doc_id):
        """Rebuild the document from its latest snapshot and patch tail."""
        snapshot = self._latest_snapshot(doc_id)
        if snapshot is None:
            raise JsonPatchError(
                'No snapshot found for document %r; the store is corrupt.'
                % doc_id)
        data = json.loads(snapshot.data)
        query = (self.Patch
                 .select()
                 .where((self.Patch.doc_id == doc_id) &
                        (self.Patch.generation > snapshot.generation))
                 .order_by(self.Patch.generation, self.Patch.seq))
        for row in query:
            tokens = parse_pointer(row.path)
            value = json.loads(row.value) if row.value is not None else None
            self.apply_op(data, row.op, tokens, value)
        return data, snapshot.generation

    def read(self, doc_id):
        """Return the current document state and its generation."""
        with self._read_txn():
            doc = self._get_doc(doc_id)
            data, snapshot_generation = self._materialize(doc_id)
            return DocumentView(doc_id, doc.generation,
                                snapshot_generation, data)

    def list_patches(self, doc_id):
        """Return the confirmed patch tail in replay order."""
        with self._read_txn():
            self._get_doc(doc_id)
            snapshot = self._latest_snapshot(doc_id)
            snapshot_generation = snapshot.generation if snapshot else 0
            query = (self.Patch
                     .select()
                     .where((self.Patch.doc_id == doc_id) &
                            (self.Patch.generation > snapshot_generation))
                     .order_by(self.Patch.generation, self.Patch.seq))
            return [self._patch_to_dict(row) for row in query]

    #
    # Commit path.
    #

    @staticmethod
    def _navigate_parent(data, tokens, pointer):
        """Resolve the parent object of ``tokens``' last key."""
        node = data
        for key in tokens[:-1]:
            if not isinstance(node, dict):
                raise IllegalPathError(
                    'Path %r crosses a non-object value.' % pointer)
            if key not in node:
                raise IllegalPathError(
                    'Parent key %r does not exist for path %r.'
                    % (key, pointer))
            node = node[key]
        if not isinstance(node, dict):
            raise IllegalPathError(
                'Parent of path %r is not a JSON object.' % pointer)
        return node, tokens[-1]

    @staticmethod
    def apply_op(data, op, tokens, value):
        """Apply one validated op in place on ``data``."""
        pointer = pointer_to_string(tokens)
        parent, key = JsonPatchStore._navigate_parent(data, tokens, pointer)
        if op == 'remove':
            if key not in parent:
                raise IllegalPathError(
                    'Cannot remove missing path %r.' % pointer)
            del parent[key]
        else:
            parent[key] = value

    def _validate_op(self, opdef, state):
        if not isinstance(opdef, dict):
            raise InvalidPatchError('Each op must be an object, got %r.'
                                    % type(opdef).__name__)
        op = opdef.get('op')
        if op not in self.VALID_OPS:
            raise InvalidPatchError('Unknown op %r; expected one of %r.'
                                    % (op, self.VALID_OPS))
        path = opdef.get('path')
        tokens = parse_pointer(path)
        if op in ('add', 'replace'):
            if 'value' not in opdef:
                raise InvalidPatchError(
                    'Op %r on path %r requires a "value".' % (op, path))
            # Reject payloads that cannot be round-tripped through JSON.
            try:
                json.dumps(opdef['value'])
            except (TypeError, ValueError):
                raise InvalidPatchError(
                    'Value at path %r is not JSON-serializable.' % path)
        pointer = pointer_to_string(tokens)
        parent, key = self._navigate_parent(state, tokens, pointer)
        if op in ('replace', 'remove') and key not in parent:
            raise IllegalPathError(
                'Cannot %s missing path %r.' % (op, pointer))
        value = opdef['value'] if op != 'remove' else None
        return op, tokens, pointer, value

    def commit(self, doc_id, ops, base_generation):
        """Atomically apply a batch of non-overlapping ops.

        :param str doc_id: target document.
        :param list ops: op dicts built by :func:`add`, :func:`replace` or
            :func:`remove`.
        :param int base_generation: generation returned by the caller's last
            :meth:`read` / :meth:`export_patches`.
        :returns: the new :class:`DocumentView`.
        :raises JsonPatchError: on any validation/conflict failure; the
            document and its generation are left untouched.
        """
        if not isinstance(ops, (list, tuple)) or not ops:
            raise InvalidPatchError('ops must be a non-empty list.')
        if (not isinstance(base_generation, int) or
                isinstance(base_generation, bool) or
                base_generation < 0):
            raise InvalidPatchError(
                'base_generation must be a non-negative integer.')

        with self._write_txn():
            doc = self._get_doc(doc_id)
            head_generation = doc.generation
            snapshot = self._latest_snapshot(doc_id)
            snapshot_generation = snapshot.generation if snapshot else 0

            if base_generation > head_generation:
                raise StaleGenerationError(
                    'Base generation %d is ahead of head generation %d.'
                    % (base_generation, head_generation))
            if base_generation < snapshot_generation:
                raise StaleGenerationError(
                    'Base generation %d predates snapshot generation %d; '
                    're-read the document before committing.'
                    % (base_generation, snapshot_generation))

            state, _ = self._materialize(doc_id)
            parsed = []
            for opdef in ops:
                op, tokens, pointer, value = self._validate_op(opdef, state)
                for previous in parsed:
                    if paths_overlap(previous[1], tokens):
                        raise PathConflictError(
                            'Paths %r and %r overlap within the same commit.'
                            % (previous[2], pointer))
                parsed.append((op, tokens, pointer, value))

            # Every patch confirmed after the caller's base generation is a
            # potential conflict. Non-overlapping paths are allowed to merge.
            newer_patches = (self.Patch
                             .select(self.Patch.path)
                             .where((self.Patch.doc_id == doc_id) &
                                    (self.Patch.generation > base_generation)))
            for row in newer_patches:
                committed_tokens = parse_pointer(row.path)
                for _, tokens, pointer, _ in parsed:
                    if paths_overlap(committed_tokens, tokens):
                        raise PathConflictError(
                            'Path %r was modified at a newer generation; '
                            'base generation %d is stale for this path.'
                            % (pointer, base_generation))

            new_data = copy.deepcopy(state)
            for op, tokens, _, value in parsed:
                self.apply_op(new_data, op, tokens, value)

            new_generation = head_generation + 1
            rows = [{
                'doc_id': doc_id,
                'path': pointer,
                'op': op,
                'value': json.dumps(value) if op != 'remove' else None,
                'base_generation': base_generation,
                'generation': new_generation,
                'seq': seq,
            } for seq, (op, _, pointer, value) in enumerate(parsed)]
            self.Patch.insert_many(rows).execute()

            doc.generation = new_generation
            doc.checksum = checksum_of(new_data)
            doc.save()

            return DocumentView(doc_id, new_generation,
                                snapshot_generation, new_data)

    #
    # Snapshot compaction.
    #

    def _compact_write_snapshot(self, doc_id, min_patches=1):
        """Phase 1: persist a snapshot at the current head generation."""
        with self._write_txn():
            doc = self._get_doc(doc_id)
            snapshot = self._latest_snapshot(doc_id)
            snapshot_generation = snapshot.generation if snapshot else 0
            pending = (self.Patch
                       .select()
                       .where((self.Patch.doc_id == doc_id) &
                              (self.Patch.generation > snapshot_generation))
                       .count())
            if pending < min_patches:
                return None
            data, _ = self._materialize(doc_id)
            # IGNORE keeps the phase idempotent if a previous run crashed
            # after inserting this exact snapshot row.
            (self.Snapshot
             .insert(doc_id=doc_id, generation=doc.generation,
                     data=json.dumps(data), checksum=checksum_of(data))
             .on_conflict_ignore()
             .execute())
            return doc.generation

    def _compact_delete_patches(self, doc_id, generation):
        """Phase 2: drop patches now captured by the snapshot."""
        with self._write_txn():
            (self.Patch
             .delete()
             .where((self.Patch.doc_id == doc_id) &
                    (self.Patch.generation <= generation))
             .execute())

    def compact(self, doc_id, min_patches=1):
        """Compact confirmed patches into a snapshot.

        The two phases commit independently. A crash between them leaves the
        old patches in place; recovery simply ignores patches at or below the
        new snapshot generation, and re-running :meth:`compact` finishes the
        cleanup.

        :returns: the snapshot generation, or ``None`` when there were fewer
            than ``min_patches`` pending patches.
        """
        generation = self._compact_write_snapshot(doc_id, min_patches)
        # Always reconcile the patch tail against the latest snapshot: when
        # a previous run crashed after writing the snapshot, no new snapshot
        # is needed but the interrupted cleanup still has to finish.
        snapshot = self._latest_snapshot(doc_id)
        if snapshot is not None:
            self._compact_delete_patches(doc_id, snapshot.generation)
        return generation

    def compact_all(self, min_patches=1):
        """Compact every document with pending patches; returns a mapping."""
        result = {}
        for doc in self.Document.select():
            generation = self.compact(doc.doc_id, min_patches)
            if generation is not None:
                result[doc.doc_id] = generation
        return result

    def restore(self, doc_id):
        """Rebuild the document from its snapshot and patch tail.

        This is the crash/re-entry entry point: it performs no writes and
        always returns the state implied by the durable snapshot plus the
        surviving patches.
        """
        return self.read(doc_id)

    #
    # Export / import and integrity checks.
    #

    @staticmethod
    def _patch_to_dict(row):
        return {
            'op': row.op,
            'path': row.path,
            'value': json.loads(row.value) if row.value is not None else None,
            'base_generation': row.base_generation,
            'generation': row.generation,
            'seq': row.seq,
        }

    def export_patches(self, doc_id):
        """Return a self-contained, generation-consistent export bundle.

        The bundle's ``generation`` matches :meth:`read`, and feeding it to
        :func:`replay_bundle` reproduces the exact document JSON.
        """
        with self._read_txn():
            doc = self._get_doc(doc_id)
            snapshot = self._latest_snapshot(doc_id)
            if snapshot is None:
                raise JsonPatchError(
                    'No snapshot found for document %r.' % doc_id)
            patches = [self._patch_to_dict(row) for row in (
                self.Patch
                .select()
                .where((self.Patch.doc_id == doc_id) &
                       (self.Patch.generation > snapshot.generation))
                .order_by(self.Patch.generation, self.Patch.seq))]
            data, _ = self._materialize(doc_id)
            return {
                'doc_id': doc_id,
                'generation': doc.generation,
                'checksum': doc.checksum,
                'snapshot': {
                    'generation': snapshot.generation,
                    'data': json.loads(snapshot.data),
                    'checksum': snapshot.checksum,
                },
                'patches': patches,
                # Materialized view captured in the same transaction, so the
                # exported log and the readable model cannot diverge.
                'data': data,
            }

    def import_bundle(self, bundle, doc_id=None):
        """Load an export bundle into a fresh document and verify it."""
        doc_id = doc_id or bundle['doc_id']
        with self._write_txn():
            if self.Document.select().where(
                    self.Document.doc_id == doc_id).exists():
                raise DocumentExistsError(
                    'Document %r already exists.' % doc_id)
            snapshot = bundle['snapshot']
            self.Document.create(
                doc_id=doc_id, generation=bundle['generation'],
                checksum=bundle['checksum'])
            self.Snapshot.create(
                doc_id=doc_id, generation=snapshot['generation'],
                data=json.dumps(snapshot['data']),
                checksum=snapshot['checksum'])
            rows = [{
                'doc_id': doc_id,
                'path': opdef['path'],
                'op': opdef['op'],
                'value': (json.dumps(opdef['value'])
                          if opdef['op'] != 'remove' else None),
                'base_generation': opdef['base_generation'],
                'generation': opdef['generation'],
                'seq': opdef['seq'],
            } for opdef in bundle['patches']]
            if rows:
                self.Patch.insert_many(rows).execute()
        view = self.read(doc_id)
        if checksum_of(view.data) != bundle['checksum']:
            raise JsonPatchError(
                'Imported document checksum does not match the bundle.')
        return view

    def verify_integrity(self, doc_id):
        """Check that snapshot, patches, head generation and checksum agree.

        :returns: True when the store is internally consistent.
        :raises JsonPatchError: describing the first inconsistency found.
        """
        with self._read_txn():
            doc = self._get_doc(doc_id)
            snapshot = self._latest_snapshot(doc_id)
            if snapshot is None:
                raise JsonPatchError('Missing snapshot for %r.' % doc_id)
            if checksum_of(json.loads(snapshot.data)) != snapshot.checksum:
                raise JsonPatchError(
                    'Snapshot checksum mismatch for %r.' % doc_id)

            # Only patches newer than the snapshot participate in replay;
            # older rows may linger legitimately when compaction crashed
            # between its snapshot and cleanup phases.
            generations = [row.generation for row in (
                self.Patch
                .select(self.Patch.generation)
                .distinct()
                .where((self.Patch.doc_id == doc_id) &
                       (self.Patch.generation > snapshot.generation))
                .order_by(self.Patch.generation))]
            expected = list(range(snapshot.generation + 1,
                                  doc.generation + 1))
            if generations != expected:
                raise JsonPatchError(
                    'Patch generations %r do not form the expected tail %r.'
                    % (generations, expected))

            data, _ = self._materialize(doc_id)
            if checksum_of(data) != doc.checksum:
                raise JsonPatchError(
                    'Head checksum mismatch for %r; replayed JSON diverges.'
                    % doc_id)
            return True
