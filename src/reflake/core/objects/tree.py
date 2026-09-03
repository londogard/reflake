"""Tree objects: the Merkle-tree node format for Reflake v2.

A tree object is a sorted JSONL file of entries.  Each entry addresses either a
child tree (real directory ``t`` or name-range shard ``s``) or a leaf file
(``b``/``m``/``bp``/``mp``).  Trees are content-addressed by the blake3 of their
serialized bytes, so unchanged directories reuse the same hash and are never
rewritten.

Line shapes (name is a single path component):

    ["t", name, hash]                              # real subdirectory
    ["s", name, hash]                              # name-range shard of this directory
    ["b", name, hash, size, mtime_ns]              # blob-backed file
    ["m", name, hash, size, mtime_ns, source_uri]  # source pointer (unverifiable)
    ["bp", name, hash, size, mtime_ns, footer]     # parquet, blob-backed, footer stats
    ["mp", name, hash, size, mtime_ns, source_uri, footer]  # parquet, source, footer stats
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

from blake3 import blake3
import msgspec

from ..entry_codec import (
    KIND_BLOB,
    KIND_BP,
    KIND_META,
    KIND_MP,
    LEAF_KINDS,
    LeafRecord,
    decode_leaf_parts,
    encode_leaf,
    leaf_kind_for,
    validate_leaf,
)
from ..manifest import ManifestEntry

KIND_TREE = "t"
KIND_SHARD = "s"

SUBTREE_KINDS = frozenset({KIND_TREE, KIND_SHARD})
SUPPORTED_KINDS = frozenset({KIND_TREE, KIND_SHARD, *LEAF_KINDS})

#: A tree object holds at most this many direct entries before it is split
#: into name-range shard subtrees (≈1 MB at ~100 bytes/line).
MAX_TREE_ENTRIES = 10_000

_HEX_DIGITS = frozenset("0123456789abcdef")
_TREE_HEX_LENGTH = 64


def _is_hex_digest(value: str) -> bool:
    return len(value) == _TREE_HEX_LENGTH and all(c in _HEX_DIGITS for c in value)


def _validate_entry_name(name: str) -> None:
    if not name:
        raise ValueError("Tree entry name cannot be empty")
    if "/" in name or "\\" in name:
        raise ValueError("Tree entry name must be a single path component")
    if name in {".", ".."}:
        raise ValueError("Tree entry name cannot be '.' or '..'")


@dataclass(frozen=True)
class TreeEntry:
    """One line of a tree object."""

    name: str
    kind: str
    hash: str
    size: int = 0
    mtime_ns: int = 0
    source_uri: str | None = None
    footer: str | None = None

    def __post_init__(self) -> None:
        _validate_entry_name(self.name)
        if self.kind not in SUPPORTED_KINDS:
            raise ValueError(f"Unsupported tree entry kind: {self.kind}")
        if not _is_hex_digest(self.hash):
            raise ValueError(f"Tree entry hash must be a 64-character hex digest")
        if self.size < 0:
            raise ValueError("Tree entry size cannot be negative")
        if self.mtime_ns < 0:
            raise ValueError("Tree entry mtime_ns cannot be negative")
        if self.kind in SUBTREE_KINDS:
            if self.size or self.mtime_ns or self.source_uri or self.footer:
                raise ValueError("Subtree entries only carry a name and hash")
            return
        validate_leaf(self.kind, source_uri=self.source_uri, footer=self.footer)
        if self.footer is not None and not _is_hex_digest(self.footer):
            raise ValueError(
                "Parquet entries must carry a footer hash (64 hex chars)"
            )

    @property
    def is_subtree(self) -> bool:
        return self.kind in SUBTREE_KINDS

    @property
    def is_leaf(self) -> bool:
        return not self.is_subtree

    def serialize(self) -> str:
        if self.kind in SUBTREE_KINDS:
            return msgspec.json.encode([self.kind, self.name, self.hash]).decode("utf-8")
        return encode_leaf(self._leaf_record())

    def _leaf_record(self) -> LeafRecord:
        return LeafRecord(
            kind=self.kind,
            name=self.name,
            hash=self.hash,
            size=self.size,
            mtime_ns=self.mtime_ns,
            source_uri=self.source_uri,
            footer=self.footer,
        )

    @staticmethod
    def parse(line: str | bytes) -> "TreeEntry":
        try:
            payload = msgspec.json.decode(line)
        except (msgspec.DecodeError, ValueError) as error:
            raise ValueError("Corrupt tree entry payload") from error
        if not isinstance(payload, list) or not payload:
            raise ValueError("Tree entry payload must be a JSON array")
        kind = str(payload[0])
        if kind not in SUPPORTED_KINDS:
            raise ValueError(f"Tree entry payload has an unsupported shape: {kind}")
        if kind in SUBTREE_KINDS:
            try:
                name, hash_value = str(payload[1]), str(payload[2])
            except IndexError as error:
                raise ValueError(
                    "Tree entry payload is missing required fields"
                ) from error
            return TreeEntry(name=name, kind=kind, hash=hash_value)

        record = decode_leaf_parts(payload)
        return TreeEntry(
            name=record.name,
            kind=record.kind,
            hash=record.hash,
            size=record.size,
            mtime_ns=record.mtime_ns,
            source_uri=record.source_uri,
            footer=record.footer,
        )

    @staticmethod
    def name_from_payload(line: str | bytes) -> str:
        """Extract the entry name without a full JSON parse (for sort checks)."""
        try:
            payload = msgspec.json.decode(line)
        except (msgspec.DecodeError, ValueError) as error:
            raise ValueError("Corrupt tree entry payload") from error
        if not isinstance(payload, list) or len(payload) < 2:
            raise ValueError("Tree entry payload must be a JSON array")
        return str(payload[1])


def serialize_tree_object(entries: Iterable[TreeEntry]) -> bytes:
    """Serialize entries (sorted by name) into tree object bytes."""
    lines = [entry.serialize() for entry in entries]
    return ("\n".join(lines) + "\n").encode("utf-8")


def parse_tree_object(payload: bytes) -> list[TreeEntry]:
    entries: list[TreeEntry] = []
    previous_name: str | None = None
    for raw_line in payload.decode("utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        entry = TreeEntry.parse(line)
        if previous_name is not None and entry.name <= previous_name:
            raise ValueError(
                f"Tree entries must be sorted by name; {entry.name!r} after "
                f"{previous_name!r}"
            )
        previous_name = entry.name
        entries.append(entry)
    return entries


def leaf_to_tree_entry(entry: ManifestEntry) -> TreeEntry:
    """Convert a leaf manifest entry (full path) into a tree entry (name only)."""
    return TreeEntry(
        name=entry.path.rsplit("/", 1)[-1],
        kind=leaf_kind_for(entry.identity_mode, has_footer=entry.footer is not None),
        hash=entry.hash,
        size=entry.size,
        mtime_ns=entry.mtime_ns,
        source_uri=entry.source_uri if entry.identity_mode == "meta" else None,
        footer=entry.footer,
    )


def tree_object_hash(payload: bytes) -> str:
    return blake3(payload).hexdigest()
