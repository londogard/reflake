"""Pruned scan execution: read only the row groups stats cannot prune (§4).

``execute_scan`` turns the metadata-only plan from ``pruning.plan_pruned_file``
into rows:

- footer-backed entries read only the kept row groups through a ranged,
  accounting file object (``objects/rangefile.py``) — local file handles or
  S3 ``Range`` GETs;
- entries without footer stats are read fully and counted as full scans;
- non-parquet entries are skipped (counted);
- pointer entries (``m``/``mp``) are read through their ``source_uri``;
  missing or unreachable sources are skipped and counted (or fail under
  ``strict``).

Rows stream lazily (row group by row group) as dicts, and ``ScanStats``
reports files, row groups kept/total, bytes fetched vs bytes total, requests,
and inapplicable columns.  The exact ``pyarrow.compute`` filter mirrors the
null contract documented in ``pruning.py``: comparisons involving NULL never
match.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from blake3 import blake3

from ..domain import BlobIntegrityError, SourceNotFoundError, StorageUnavailableError
from ..entry_codec import KIND_BLOB, KIND_BP, KIND_META, KIND_MP, Entry
from ..objects import RangeFile, ScanStore, open_source_range_file, source_client
from ..objects.footer import FooterCache
from .pruning import Predicate, plan_pruned_file

#: Entry kinds backed by a repository blob vs an external source URI.
_CONTENT_KINDS = frozenset({KIND_BLOB, KIND_BP})
_POINTER_KINDS = frozenset({KIND_META, KIND_MP})

#: Raised while *opening* a pointer source: skipped unless ``strict``.
_SOURCE_ERRORS = (SourceNotFoundError, StorageUnavailableError, OSError)


# pyarrow.compute populates its functions dynamically at import time, which
# static type checkers cannot see — resolve the handful we use once, here.
def _compute(name: str) -> Any:
    """Look up a ``pyarrow.compute`` function by name (dynamic module surface)."""
    return getattr(pc, name)


_COMPARISON_FUNCS: dict[str, Any] = {
    "=": _compute("equal"),
    "!=": _compute("not_equal"),
    "<": _compute("less"),
    "<=": _compute("less_equal"),
    ">": _compute("greater"),
    ">=": _compute("greater_equal"),
}
_NULL_FUNCS: dict[str, Any] = {
    "is_null": _compute("is_null"),
    "is_not_null": _compute("is_valid"),
}
_FILL_NULL: Any = _compute("fill_null")
_AND: Any = _compute("and_")
_STRUCT_FIELD: Any = _compute("struct_field")


@dataclass
class ScanStats:
    """Cost/coverage accounting for a scan, filled in as rows are pulled."""

    #: Parquet files covered (read or fully pruned by stats).
    files: int = 0
    #: Entries skipped because they are not readable parquet.
    files_skipped: int = 0
    #: Pointer entries skipped because their source was missing/unreachable.
    files_unavailable: int = 0
    row_groups_kept: int = 0
    row_groups_total: int = 0
    #: Bytes fetched from blob/source storage while scanning.
    bytes_fetched: int = 0
    #: Logical size of every covered file (the full-scan cost).
    bytes_total: int = 0
    #: Store/S3 requests issued while scanning.
    requests: int = 0
    #: Paths of pointer entries whose sources could not be read.
    unavailable_paths: list[str] = field(default_factory=list)
    #: Predicate columns the exact filter could not apply (type mismatches).
    inapplicable: list[str] = field(default_factory=list)

    def _add_inapplicable(self, column: str) -> None:
        if column not in self.inapplicable:
            self.inapplicable.append(column)


@dataclass(frozen=True)
class Scan:
    """A streaming scan: consume ``rows``; ``stats`` fills as it runs."""

    rows: Iterator[dict[str, Any]]
    stats: ScanStats


def execute_scan(
    store: ScanStore,
    tree_hash: str,
    prefix: str,
    predicates: Sequence[Predicate],
    *,
    footer_cache: FooterCache | None = None,
    verify: bool = False,
    strict: bool = False,
) -> Scan:
    """Execute a pruned scan over *prefix* of *tree_hash*, streaming rows.

    ``verify`` streams each content blob through a fresh hash check before
    scanning it (pointer sources are not hash-verifiable and are skipped by
    verification). ``strict`` turns unavailable pointer sources into errors
    instead of counting them as skipped.
    """
    stats = ScanStats()
    rows = _iter_rows(
        store, tree_hash, prefix, predicates, stats, footer_cache, verify, strict
    )
    return Scan(rows=rows, stats=stats)


def _iter_rows(
    store: ScanStore,
    tree_hash: str,
    prefix: str,
    predicates: Sequence[Predicate],
    stats: ScanStats,
    footer_cache: FooterCache | None,
    verify: bool,
    strict: bool,
) -> Iterator[dict[str, Any]]:
    s3_client = source_client(store)
    for entry in store.iter_entries_for_prefix(tree_hash, prefix):
        if not entry.is_leaf:  # pragma: no cover - walkers only yield leaves
            continue
        is_pointer = entry.kind in _POINTER_KINDS
        plan = plan_pruned_file(store, entry, predicates, footer_cache=footer_cache)
        if plan is not None and not plan.kept_row_groups:
            # Stats prove this file cannot match: zero data bytes needed.
            stats.files += 1
            stats.bytes_total += entry.size
            stats.row_groups_total += plan.total_row_groups
            continue

        ranged: RangeFile | None = None
        try:
            ranged = _open_entry(store, entry, s3_client=s3_client)
            parquet_file = pq.ParquetFile(ranged)
        except pa.ArrowInvalid:
            # Not a parquet file: skip (counted); the probe bytes did happen.
            if ranged is not None:
                stats.bytes_fetched += ranged.bytes_fetched
                stats.requests += ranged.requests
                ranged.close()
            stats.files_skipped += 1
            continue
        except _SOURCE_ERRORS:
            if ranged is not None:
                ranged.close()
            if not is_pointer or strict:
                raise
            stats.files_unavailable += 1
            stats.unavailable_paths.append(entry.path)
            continue

        # [(bytes, requests)] synced from the ranged file into ``stats`` as
        # work happens, so an abandoned generator still reports honest costs.
        synced = [0, 0]
        with ranged:
            stats.files += 1
            stats.bytes_total += entry.size
            if verify and not is_pointer:
                _verify_blob(store, entry.hash, stats)
            _require_columns(entry.path, parquet_file, predicates)

            if plan is not None:
                kept = plan.kept_row_groups
                stats.row_groups_total += plan.total_row_groups
            else:
                # No footer stats: full scan, counted as such.
                kept = tuple(range(parquet_file.num_row_groups))
                stats.row_groups_total += parquet_file.num_row_groups
            stats.row_groups_kept += len(kept)

            for group_index in kept:
                table = parquet_file.read_row_groups([group_index], use_threads=False)
                _settle(stats, ranged, synced)
                filtered = _filter_table(table, predicates, entry.path, stats)
                yield from filtered.to_pylist()


def _settle(stats: ScanStats, ranged: RangeFile, synced: list[int]) -> None:
    """Fold the ranged file's counters into the scan totals (no double count)."""
    stats.bytes_fetched += ranged.bytes_fetched - synced[0]
    stats.requests += ranged.requests - synced[1]
    synced[0] = ranged.bytes_fetched
    synced[1] = ranged.requests


def _open_entry(store: ScanStore, entry: Entry, *, s3_client: Any | None) -> RangeFile:
    if entry.kind in _CONTENT_KINDS:
        return RangeFile(
            lambda offset, length: store.read_blob_range(entry.hash, offset, length),
            entry.size,
        )
    if entry.source_uri is None:  # pragma: no cover - entry validation forbids it
        raise SourceNotFoundError(f"Pointer entry has no source: {entry.path}")
    return open_source_range_file(entry.source_uri, entry.size, client=s3_client)


def _verify_blob(store: ScanStore, blob_hash: str, stats: ScanStats) -> None:
    """Full-hash-verify a content blob before scanning it (``--verify``)."""
    hasher = blake3()
    fetched = 0
    with store.open_blob(blob_hash) as handle:
        while chunk := handle.read(1024 * 1024):
            hasher.update(chunk)
            fetched += len(chunk)
    stats.bytes_fetched += fetched
    stats.requests += 1
    actual = hasher.hexdigest()
    if actual != blob_hash:
        raise BlobIntegrityError(
            expected=blob_hash, actual=actual, context="query scan --verify"
        )


def _require_columns(
    path: str, parquet_file: pq.ParquetFile, predicates: Sequence[Predicate]
) -> None:
    """Typo protection: every predicate column must exist in the file.

    Only top-level names are checked here; dotted paths are resolved against
    nested fields while filtering and raise the same error if unresolvable.
    """
    top_level = set(parquet_file.schema_arrow.names)
    missing = sorted(
        {p.column for p in predicates if p.column.split(".")[0] not in top_level}
    )
    if missing:
        raise ValueError(
            f"Predicate column(s) {', '.join(missing)} not present in "
            f"scanned file(s): {path}"
        )


def _resolve_column(table: pa.Table, name: str) -> Any | None:
    """Resolve a (possibly dotted, nested) column name to a chunked array."""
    if name in table.column_names:
        return table.column(name)
    parts = name.split(".")
    if len(parts) < 2 or parts[0] not in table.column_names:
        return None
    current: Any = table.column(parts[0])
    for part in parts[1:]:
        try:
            current = _STRUCT_FIELD(current, part)
        except (KeyError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
            return None
    return current


def _clause_mask(column: Any, predicate: Predicate, stats: ScanStats) -> Any | None:
    """Boolean mask for one clause; ``None`` when it cannot be applied.

    Type-incompatible clauses apply no filtering (mirroring pruning's
    conservative keep) and are reported as inapplicable.
    """
    if predicate.value is None and predicate.op not in _NULL_FUNCS:
        stats._add_inapplicable(predicate.column)
        return None
    func = _NULL_FUNCS.get(predicate.op) or _COMPARISON_FUNCS.get(predicate.op)
    assert func is not None, f"Unsupported predicate op: {predicate.op}"
    try:
        if predicate.op in _NULL_FUNCS:
            clause = func(column)
        else:
            clause = func(column, predicate.value)
    except (
        pa.ArrowInvalid,
        pa.ArrowNotImplementedError,
        pa.ArrowTypeError,
        TypeError,
    ):
        stats._add_inapplicable(predicate.column)
        return None
    # NULL comparisons never match (SQL three-valued logic, pruning contract).
    return _FILL_NULL(clause, False)


def _filter_table(
    table: pa.Table,
    predicates: Sequence[Predicate],
    path: str,
    stats: ScanStats,
) -> pa.Table:
    mask: Any | None = None
    for predicate in predicates:
        column = _resolve_column(table, predicate.column)
        if column is None:
            raise ValueError(
                f"Predicate column '{predicate.column}' not present in "
                f"scanned file(s): {path}"
            )
        clause = _clause_mask(column, predicate, stats)
        if clause is None:
            continue
        mask = clause if mask is None else _AND(mask, clause)
    if mask is None:
        return table
    return table.filter(mask)
