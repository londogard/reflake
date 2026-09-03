"""The single leaf-entry codec for manifests and trees.

Every content leaf in a manifest line or tree line is a positional JSON array
tagged by kind:

    ["b",  name, hash, size, mtime_ns]
    ["m",  name, hash, size, mtime_ns, source_uri]
    ["bp", name, hash, size, mtime_ns, footer]
    ["mp", name, hash, size, mtime_ns, source_uri, footer]

``name`` is the full logical path in manifests and a single path component in
tree nodes — the codec is agnostic; each record type validates its own name
rules.  Kind derivation, shape dispatch, field rules, encoding, and decoding
live here exactly once, so adding a leaf attribute touches this module plus
the record types, not every serializer in the codebase.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import msgspec

KIND_BLOB = "b"
KIND_META = "m"
KIND_BP = "bp"
KIND_MP = "mp"

LEAF_KINDS = frozenset({KIND_BLOB, KIND_META, KIND_BP, KIND_MP})

_ARITY_BY_KIND = {
    KIND_BLOB: 5,
    KIND_META: 6,
    KIND_BP: 6,
    KIND_MP: 7,
}


@dataclass(frozen=True)
class LeafRecord:
    """One decoded leaf line; ``name`` semantics are the caller's concern."""

    kind: str
    name: str
    hash: str
    size: int
    mtime_ns: int
    source_uri: str | None = None
    footer: str | None = None


def leaf_kind_for(identity_mode: str, *, has_footer: bool) -> str:
    """Map ``(identity_mode, footer presence)`` to a leaf kind."""
    if identity_mode == "blake3":
        return KIND_BP if has_footer else KIND_BLOB
    if identity_mode == "meta":
        return KIND_MP if has_footer else KIND_META
    raise ValueError(f"identity_mode must be one of: blake3, meta")


def validate_leaf(kind: str, *, source_uri: str | None, footer: str | None) -> None:
    """Enforce the per-kind field rules shared by both record types."""
    if kind not in LEAF_KINDS:
        raise ValueError(f"Unsupported leaf kind: {kind}")
    if kind in (KIND_BLOB, KIND_BP):
        if source_uri is not None:
            raise ValueError("Blob-backed entries cannot carry source_uri")
    else:
        if not source_uri:
            raise ValueError("Source-pointer entries must carry a non-empty source_uri")
    if kind in (KIND_BP, KIND_MP):
        if not footer:
            raise ValueError("Parquet entries must carry a footer")
    elif footer is not None:
        raise ValueError("Non-parquet entries cannot carry a footer")


def encode_leaf(record: LeafRecord) -> str:
    """Serialize a validated leaf record into its positional JSON line."""
    payload: list[object] = [record.kind, record.name, record.hash, record.size, record.mtime_ns]
    if record.kind in (KIND_META, KIND_MP):
        payload.append(record.source_uri)
    if record.kind in (KIND_BP, KIND_MP):
        payload.append(record.footer)
    return msgspec.json.encode(payload).decode("utf-8")


def decode_leaf_parts(payload: list[Any]) -> LeafRecord:
    """Build a record from an already-parsed positional payload."""
    if not payload:
        raise ValueError("Leaf entry payload must be a JSON array")
    kind = str(payload[0])
    expected = _ARITY_BY_KIND.get(kind)
    if expected is None or len(payload) != expected:
        raise ValueError(f"Leaf entry payload has an unsupported shape: {kind}")
    source_uri = str(payload[5]) if kind in (KIND_META, KIND_MP) else None
    footer = (
        str(payload[6])
        if kind == KIND_MP
        else (str(payload[5]) if kind == KIND_BP else None)
    )
    return LeafRecord(
        kind=kind,
        name=str(payload[1]),
        hash=str(payload[2]),
        size=int(payload[3]),
        mtime_ns=int(payload[4]),
        source_uri=source_uri,
        footer=footer,
    )


def decode_leaf(line: str | bytes) -> LeafRecord:
    """Parse a leaf line, rejecting malformed input with a clear error."""
    try:
        payload = msgspec.json.decode(line)
    except (msgspec.DecodeError, ValueError) as error:
        raise ValueError("Corrupt leaf entry payload") from error
    if not isinstance(payload, list):
        raise ValueError("Leaf entry payload must be a JSON array")
    return decode_leaf_parts(payload)
