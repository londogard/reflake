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
    ["mp", name, hash, size, mtime_ns, source_uri, footer]
    # parquet, source, footer stats

Entries are ``entry_codec.Entry`` (the single record type); this module only
adds the tree-object container operations.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace

from blake3 import blake3

from ..entry_codec import (
    KIND_BLOB as KIND_BLOB,
)
from ..entry_codec import (
    KIND_BP as KIND_BP,
)
from ..entry_codec import (
    KIND_META as KIND_META,
)
from ..entry_codec import (
    KIND_MP as KIND_MP,
)
from ..entry_codec import KIND_SHARD as KIND_SHARD
from ..entry_codec import KIND_TREE as KIND_TREE
from ..entry_codec import (
    LEAF_KINDS as LEAF_KINDS,
)
from ..entry_codec import (
    SUBTREE_KINDS as SUBTREE_KINDS,
)
from ..entry_codec import (
    SUPPORTED_KINDS as SUPPORTED_KINDS,
)
from ..entry_codec import Entry

#: A tree object holds at most this many direct entries before it is split
#: into name-range shard subtrees (≈1 MB at ~100 bytes/line).
MAX_TREE_ENTRIES = 10_000


def _validate_component_name(name: str) -> None:
    if not name:
        raise ValueError("Tree entry name cannot be empty")
    if "/" in name or "\\" in name:
        raise ValueError("Tree entry name must be a single path component")
    if name in {".", ".."}:
        raise ValueError("Tree entry name cannot be '.' or '..'")


def serialize_tree_object(entries: Iterable[Entry]) -> bytes:
    """Serialize entries (sorted by name) into tree object bytes."""
    lines = [entry.serialize() for entry in entries]
    return ("\n".join(lines) + "\n").encode("utf-8")


def parse_tree_object(payload: bytes) -> list[Entry]:
    entries: list[Entry] = []
    previous_name: str | None = None
    for raw_line in payload.decode("utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        entry = Entry.parse(line)
        _validate_component_name(entry.path)
        if previous_name is not None and entry.path <= previous_name:
            raise ValueError(
                f"Tree entries must be sorted by name; {entry.path!r} after "
                f"{previous_name!r}"
            )
        previous_name = entry.path
        entries.append(entry)
    return entries


def leaf_to_tree_entry(entry: Entry) -> Entry:
    """Convert a full-path leaf entry into a tree entry (name only)."""
    return replace(entry, path=entry.path.rsplit("/", 1)[-1])


def name_from_payload(line: str | bytes) -> str:
    """Extract the entry name without a full parse (for sort checks)."""
    return Entry.path_from_payload(line)


def tree_object_hash(payload: bytes) -> str:
    return blake3(payload).hexdigest()
