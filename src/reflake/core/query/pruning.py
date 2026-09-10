"""Row-group pruning over parquet footer stats (docs/architecture.md §4).

Given a WHERE-style predicate and the compact footer-stats object captured
at ingest (``footers/<hash>`` — schema hash + per-row-group column
min/max/nulls), decide which row groups *cannot* contain matching rows —
without ever reading data bytes.

Pruning is conservative: a row group is kept unless the stats prove it
cannot match.  Predicates over unknown columns or incomparable types apply
no pruning (every group is kept) and are reported as inapplicable.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from ..objects.base import ContentQueryStore
from ..objects.footer import FooterCache, FooterStats, RowGroupStats, parse_footer_stats

Op = Literal["=", "!=", "<", "<=", ">", ">=", "is_null", "is_not_null"]

_COMPARISON_OPS = frozenset({"=", "!=", "<", "<=", ">", ">="})
_NULL_OPS = frozenset({"is_null", "is_not_null"})

#: Clause separator: `AND` at the top level. Quoted literals are matched as
#: single tokens so an `AND` inside a string value is never a separator.
_CLAUSE_SPLIT = re.compile(
    r"'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\"|\s+AND\s+", re.IGNORECASE
)
_NULL_CLAUSE = re.compile(
    r"^\s*(?P<col>[A-Za-z_][A-Za-z0-9_.]*)\s+"
    r"(?P<op>IS\s+NOT\s+NULL|IS\s+NULL)\s*$",
    re.IGNORECASE,
)
_COMPARISON_CLAUSE = re.compile(
    r"^\s*(?P<col>[A-Za-z_][A-Za-z0-9_.]*)\s*"
    r"(?P<op>=|!=|<>|<=|>=|<|>)\s*(?P<val>.+?)\s*$"
)


def _split_and_clauses(text: str) -> list[str]:
    """Split *text* on unquoted ``AND`` keywords."""
    clauses: list[str] = []
    last = 0
    for match in _CLAUSE_SPLIT.finditer(text):
        token = match.group(0)
        if token.strip().upper() == "AND":
            clauses.append(text[last : match.start()])
            last = match.end()
    clauses.append(text[last:])
    return clauses


@dataclass(frozen=True)
class Predicate:
    """One WHERE clause: ``column op value`` (or ``column IS [NOT] NULL``)."""

    column: str
    op: Op
    value: Any = None


@dataclass(frozen=True)
class PruneResult:
    total_row_groups: int
    kept_row_groups: tuple[int, ...]
    pruned_row_groups: tuple[int, ...]
    applicable: tuple[str, ...]
    inapplicable: tuple[str, ...]


@dataclass(frozen=True)
class PrunedFileScan:
    """Per-file scan plan: the row groups that may match the predicates."""

    path: str
    footer_hash: str | None
    total_row_groups: int
    kept_row_groups: tuple[int, ...]
    pruned_row_groups: tuple[int, ...]


def _parse_value(raw: str) -> Any:
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value  # bare identifier: treat as a string literal


def parse_where_clause(text: str) -> list[Predicate]:
    """Parse a WHERE-style predicate list, e.g. ``id >= 100 AND active = true``.

    Clauses are ANDed together; ``=``, ``!=``, ``<>``, ``<``, ``<=``, ``>``,
    ``>=``, ``IS NULL``, and ``IS NOT NULL`` are supported.  Unknown columns
    or type mismatches never raise — they simply apply no pruning.
    """
    predicates: list[Predicate] = []
    for clause in _split_and_clauses(text):
        if re.search(r"\s+OR\s+", clause, re.IGNORECASE) is not None:
            raise ValueError(f"OR is not supported; AND predicates only: {clause!r}")
        null_match = _NULL_CLAUSE.match(clause)
        if null_match is not None:
            op: Op = (
                "is_null"
                if null_match.group("op").upper() == "IS NULL"
                else "is_not_null"
            )
            predicates.append(Predicate(column=null_match.group("col"), op=op))
            continue
        comparison_match = _COMPARISON_CLAUSE.match(clause)
        if comparison_match is None:
            raise ValueError(f"Unsupported WHERE clause: {clause!r}")
        raw_op = comparison_match.group("op")
        if raw_op == "<>":
            raw_op = "!="
        predicates.append(
            Predicate(
                column=comparison_match.group("col"),
                op=raw_op,  # type: ignore[arg-type]
                value=_parse_value(comparison_match.group("val")),
            )
        )
    if not predicates:
        raise ValueError("Empty WHERE clause")
    return predicates


def _compare(left: Any, right: Any) -> int | None:
    """Compare two values; ``None`` when the types are incomparable."""
    if left is None or right is None:
        return None
    if isinstance(left, bool) != isinstance(right, bool):
        return None  # never mix booleans with numbers or strings
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return (left > right) - (left < right)
    if isinstance(left, str) and isinstance(right, str):
        return (left > right) - (left < right)
    return None


def _possible(group: RowGroupStats, predicate: Predicate) -> bool | None:
    """Can *group* contain a row matching *predicate*?

    ``True`` = possibly (keep), ``False`` = provably empty (prune),
    ``None`` = undecidable (keep, conservatively).
    """
    column = next(
        (
            candidate
            for candidate in group.columns
            if candidate.path == predicate.column
        ),
        None,
    )
    if column is None:
        return None
    if predicate.op == "is_null":
        return column.nulls > 0
    if predicate.op == "is_not_null":
        return column.nulls < group.rows
    if predicate.op == "!=":
        if column.nulls > 0:
            # NULL rows satisfy neither `= value` nor `!= value` (SQL three-
            # valued logic), so the group may still contain matching rows.
            # Keeping it is the conservative, always-correct choice.
            return True
        min_cmp = _compare(column.min, predicate.value)
        max_cmp = _compare(column.max, predicate.value)
        if min_cmp is None or max_cmp is None:
            return None
        return not (min_cmp == 0 and max_cmp == 0)
    if predicate.op in ("<", "<="):
        cmp = _compare(column.min, predicate.value)
        if cmp is None:
            return None
        return cmp < 0 if predicate.op == "<" else cmp <= 0
    if predicate.op in (">", ">="):
        cmp = _compare(column.max, predicate.value)
        if cmp is None:
            return None
        return cmp > 0 if predicate.op == ">" else cmp >= 0
    # `=`: keep iff min <= value <= max (nulls never equal a value).
    min_cmp = _compare(column.min, predicate.value)
    max_cmp = _compare(column.max, predicate.value)
    if min_cmp is None or max_cmp is None:
        return None
    return min_cmp <= 0 and max_cmp >= 0


def prune_row_groups(
    stats: FooterStats,
    predicates: Sequence[Predicate],
) -> PruneResult:
    """Split a file's row groups into kept/pruned by *predicates* (AND)."""
    kept: list[int] = []
    pruned: list[int] = []
    inapplicable: set[str] = set()
    for index, group in enumerate(stats.row_groups):
        keep = True
        for predicate in predicates:
            decision = _possible(group, predicate)
            if decision is None:
                inapplicable.add(predicate.column)
            elif not decision:
                keep = False
                break
        if keep:
            kept.append(index)
        else:
            pruned.append(index)
    applicable = [p.column for p in predicates if p.column not in inapplicable]
    return PruneResult(
        total_row_groups=len(stats.row_groups),
        kept_row_groups=tuple(kept),
        pruned_row_groups=tuple(pruned),
        applicable=tuple(applicable),
        inapplicable=tuple(sorted(inapplicable)),
    )


def plan_pruned_scan(
    store: ContentQueryStore,
    tree_hash: str,
    prefix: str,
    predicates: Sequence[Predicate],
    *,
    footer_cache: FooterCache | None = None,
) -> Iterator[PrunedFileScan]:
    """Plan a pruned scan of *prefix* in *tree_hash*.

    For every parquet entry with captured footer stats, read the stats object
    (metadata-only — never data bytes) and report which row groups may match
    *predicates*. Entries without footer stats cannot be pruned and are
    omitted from the plan.

    Pass a ``footer_cache`` to skip store reads (and parsing) for footers
    already seen: content-addressed, so cached entries can never be stale.
    """
    for entry in store.iter_entries_for_prefix(tree_hash, prefix):
        if entry.footer is None:
            continue
        if footer_cache is not None:
            stats = footer_cache.get(store, entry.footer)
        else:
            payload = store.read_footer_bytes(entry.footer)
            if payload is None:
                continue
            try:
                stats = parse_footer_stats(payload)
            except (ValueError, KeyError, TypeError):
                continue
        if stats is None:
            continue
        result = prune_row_groups(stats, predicates)
        yield PrunedFileScan(
            path=entry.path,
            footer_hash=entry.footer,
            total_row_groups=result.total_row_groups,
            kept_row_groups=result.kept_row_groups,
            pruned_row_groups=result.pruned_row_groups,
        )
