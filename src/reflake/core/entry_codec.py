"""The single entry model and codec for manifests and trees.

Every content leaf in a manifest line or tree line is a positional JSON array
tagged by kind:

    ["b",  name, hash, size, mtime_ns]
    ["m",  name, hash, size, mtime_ns, source_uri]
    ["bp", name, hash, size, mtime_ns, footer]
    ["mp", name, hash, size, mtime_ns, source_uri, footer]

``m``/``mp`` entries may carry one trailing field, the source object's
ETag/version marker (``source_etag``).  It is optional so local-file sources
(which have none) keep their short shape, and it is what makes
``identity verify --drift`` possible without reading source bytes.

Subtree pointers (tree nodes only) are short lines:

    ["t", name, hash]
    ["s", name, hash]

``name`` is the full logical path in manifests and a single path component in
tree nodes — the codec is agnostic; builders guarantee the convention.
``Entry`` is the one record type for both layers (replacing the former
``LeafRecord`` / ``ManifestEntry`` / ``TreeEntry`` trio): kind derivation,
shape dispatch, field rules, validation, encoding, and decoding live here
exactly once, so adding a leaf attribute touches this module, not every
serializer in the codebase.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import msgspec

KIND_BLOB = "b"
KIND_META = "m"
KIND_BP = "bp"
KIND_MP = "mp"
KIND_TREE = "t"
KIND_SHARD = "s"

LEAF_KINDS = frozenset({KIND_BLOB, KIND_META, KIND_BP, KIND_MP})
SUBTREE_KINDS = frozenset({KIND_TREE, KIND_SHARD})
SUPPORTED_KINDS = frozenset({KIND_TREE, KIND_SHARD, *LEAF_KINDS})

_ARITY_BY_KIND = {
    KIND_BLOB: (5, 5),
    KIND_META: (6, 7),
    KIND_BP: (6, 6),
    KIND_MP: (7, 8),
}

# Compiled once: strict lowercase-hex check at C speed (~4x faster than a
# per-character frozenset scan, identical strictness). Hot: runs per entry.
_HEX_RE = re.compile(r"[0-9a-f]{64}\Z")


class CorruptEntryError(ValueError):
    """A line that is not parseable JSON at all.

    Distinct from a well-formed payload that fails validation (plain
    ``ValueError``), so readers can report "corrupt file" vs "invalid
    entry" with line context.
    """


def _is_hex_digest(value: str) -> bool:
    return _HEX_RE.match(value) is not None


def _validate_entry_path(path: str) -> None:
    # Manual scan, not PurePosixPath (~10x faster; ~0.3us vs ~3us per entry).
    # Equivalent rules: normalized relative POSIX path, no empty/dot parts.
    if not path or path.startswith("/") or path.endswith("/"):
        raise ValueError("Entry path must be a normalized relative path")
    if "\\" in path or "//" in path:
        raise ValueError("Entry path must use normalized POSIX separators")
    for part in path.split("/"):
        if part in ("", ".", ".."):
            raise ValueError("Entry path must be a normalized relative path")


@dataclass(frozen=True)
class Entry:
    """One addressable node: subtree pointer or content leaf.

    ``path`` is the full logical path in manifest contexts and a single
    path component in tree-node contexts. ``identity_mode`` is derived from
    ``kind`` (``None`` for subtree pointers, which carry no identity).
    """

    path: str
    kind: str
    hash: str
    size: int = 0
    mtime_ns: int = 0
    source_uri: str | None = None
    footer: str | None = None
    #: Source object's ETag/version marker for pointer entries (optional;
    #: only S3 sources have one). Used for metadata-only drift checks.
    source_etag: str | None = None

    def __post_init__(self) -> None:
        _validate_entry_path(self.path)
        if self.kind not in SUPPORTED_KINDS:
            raise ValueError(f"Unsupported entry kind: {self.kind}")
        if not _is_hex_digest(self.hash):
            raise ValueError("Entry hash must be a 64-character hex digest")
        if self.size < 0:
            raise ValueError("Entry size cannot be negative")
        if self.mtime_ns < 0:
            raise ValueError("Entry mtime_ns cannot be negative")
        if self.kind in SUBTREE_KINDS:
            if (
                self.size
                or self.mtime_ns
                or self.source_uri
                or self.footer
                or self.source_etag
            ):
                raise ValueError("Subtree entries only carry a path and hash")
            return
        validate_leaf(
            self.kind,
            source_uri=self.source_uri,
            footer=self.footer,
            source_etag=self.source_etag,
        )
        if self.footer is not None and not _is_hex_digest(self.footer):
            raise ValueError(
                "Parquet entries must carry a footer hash (64 hex chars)"
            )
        if self.source_uri is not None and not self.source_uri.strip():
            raise ValueError("Entry source_uri cannot be empty")

    @property
    def is_subtree(self) -> bool:
        return self.kind in SUBTREE_KINDS

    @property
    def is_leaf(self) -> bool:
        return not self.is_subtree

    @property
    def identity_mode(self) -> str | None:
        """``content`` for blob-backed leaves, ``pointer`` for source-pointer
        leaves, ``None`` for subtree pointers."""
        if self.kind in SUBTREE_KINDS:
            return None
        return "content" if self.kind in (KIND_BLOB, KIND_BP) else "pointer"

    @property
    def identity_value(self) -> str:
        return self.hash

    @property
    def blob_hash(self) -> str | None:
        return self.hash if self.identity_mode == "content" else None

    @property
    def is_verified(self) -> bool:
        """True when the entry is backed by a canonical content blob.

        Pointer entries are *unverifiable*: their identity is derived from
        path and size, not from content bytes. Use ``promote`` to read the
        source blob, compute a content hash, and promote the entry.
        """
        return self.identity_mode == "content"

    def serialize(self) -> str:
        if self.kind in SUBTREE_KINDS:
            return msgspec.json.encode(
                [self.kind, self.path, self.hash]
            ).decode("utf-8")
        return encode_leaf(self)

    @staticmethod
    def parse(line: str | bytes) -> Entry:
        """Parse one tree/manifest line (subtree pointer or leaf)."""
        try:
            payload = msgspec.json.decode(line)
        except (msgspec.DecodeError, ValueError) as error:
            raise CorruptEntryError("Corrupt entry payload") from error
        if not isinstance(payload, list) or not payload:
            raise ValueError("Entry payload must be a JSON array")
        kind = str(payload[0])
        if kind not in SUPPORTED_KINDS:
            raise ValueError(f"Entry payload has an unsupported shape: {kind}")
        if kind in SUBTREE_KINDS:
            try:
                name, hash_value = str(payload[1]), str(payload[2])
            except IndexError as error:
                raise ValueError(
                    "Entry payload is missing required fields"
                ) from error
            return Entry(path=name, kind=kind, hash=hash_value)
        return _entry_from_leaf_payload(payload)

    @staticmethod
    def with_identity(
        path: str,
        hash_value: str,
        size: int,
        mtime_ns: int,
        identity_mode: str,
        *,
        source_uri: str | None = None,
        footer: str | None = None,
        source_etag: str | None = None,
    ) -> Entry:
        """Build a leaf entry from an identity mode (manifest-style)."""
        return Entry(
            path=path,
            kind=leaf_kind_for(identity_mode, has_footer=footer is not None),
            hash=hash_value,
            size=size,
            mtime_ns=mtime_ns,
            source_uri=source_uri,
            footer=footer,
            source_etag=source_etag,
        )


def leaf_kind_for(identity_mode: str, *, has_footer: bool) -> str:
    """Map ``(identity_mode, footer presence)`` to a leaf kind."""
    if identity_mode == "content":
        return KIND_BP if has_footer else KIND_BLOB
    if identity_mode == "pointer":
        return KIND_MP if has_footer else KIND_META
    raise ValueError("identity_mode must be one of: content, pointer")


def validate_leaf(
    kind: str,
    *,
    source_uri: str | None,
    footer: str | None,
    source_etag: str | None = None,
) -> None:
    """Enforce the per-kind field rules shared by all entry uses."""
    if kind not in LEAF_KINDS:
        raise ValueError(f"Unsupported leaf kind: {kind}")
    if kind in (KIND_BLOB, KIND_BP):
        if source_uri is not None:
            raise ValueError("Blob-backed entries cannot carry source_uri")
        if source_etag is not None:
            raise ValueError("Blob-backed entries cannot carry source_etag")
    else:
        if not source_uri:
            raise ValueError("Source-pointer entries must carry a non-empty source_uri")
        if source_etag is not None and not source_etag.strip():
            raise ValueError("Entry source_etag cannot be empty")
    if kind in (KIND_BP, KIND_MP):
        if not footer:
            raise ValueError("Parquet entries must carry a footer")
    elif footer is not None:
        raise ValueError("Non-parquet entries cannot carry a footer")


def encode_leaf(record: Entry) -> str:
    """Serialize a validated leaf entry into its positional JSON line."""
    return encode_leaf_parts(
        record.kind,
        record.path,
        record.hash,
        record.size,
        record.mtime_ns,
        record.source_uri,
        record.footer,
        record.source_etag,
    )


def encode_leaf_parts(
    kind: str,
    name: str,
    hash_value: str,
    size: int,
    mtime_ns: int,
    source_uri: str | None = None,
    footer: str | None = None,
    source_etag: str | None = None,
) -> str:
    """Serialize leaf fields straight to a JSON line (hot path).

    Takes plain values instead of an ``Entry`` so producers with guaranteed
    inputs (digest output, stat values, walk names, closed kind branches)
    pay no object construction or validation — the same posture as the old
    unvalidated record on this path. Callers must pass well-formed values;
    anything parsed or user-supplied goes through ``Entry`` instead.

    The trailing ``source_etag`` is written only for pointer kinds that have
    one, so local-file pointers keep the shorter line shape.
    """
    payload: list[object] = [kind, name, hash_value, size, mtime_ns]
    if kind in (KIND_META, KIND_MP):
        payload.append(source_uri)
    if kind in (KIND_BP, KIND_MP):
        payload.append(footer)
    if kind in (KIND_META, KIND_MP) and source_etag is not None:
        payload.append(source_etag)
    return msgspec.json.encode(payload).decode("utf-8")


def _entry_from_leaf_payload(payload: list[Any]) -> Entry:
    """Build a leaf entry from an already-parsed positional payload."""
    kind = str(payload[0])
    bounds = _ARITY_BY_KIND.get(kind)
    if bounds is None or not (bounds[0] <= len(payload) <= bounds[1]):
        raise ValueError(f"Leaf entry payload has an unsupported shape: {kind}")
    source_uri = str(payload[5]) if kind in (KIND_META, KIND_MP) else None
    if kind == KIND_MP:
        footer = str(payload[6])
    elif kind == KIND_BP:
        footer = str(payload[5])
    else:
        footer = None
    source_etag: str | None = None
    if kind == KIND_META and len(payload) >= 7:
        source_etag = None if payload[6] is None else str(payload[6])
    elif kind == KIND_MP and len(payload) >= 8:
        source_etag = None if payload[7] is None else str(payload[7])
    return Entry(
        path=str(payload[1]),
        kind=kind,
        hash=str(payload[2]),
        size=int(payload[3]),
        mtime_ns=int(payload[4]),
        source_uri=source_uri,
        footer=footer,
        source_etag=source_etag,
    )


def decode_leaf(line: str | bytes) -> Entry:
    """Parse a leaf line, rejecting malformed input with a clear error."""
    entry = Entry.parse(line)
    if entry.is_subtree:
        raise ValueError("Expected a leaf entry payload, found a subtree pointer")
    return entry
