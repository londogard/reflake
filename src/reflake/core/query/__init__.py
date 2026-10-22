"""Analytical query interop: DuckDB catalogs, footer-stats pruning, scans.

Absorbs the former ``core/index.py``: a commit's tree is flattened into a
DuckDB table (path, hash, size, identity_mode, footer, commit_id, branch) with an
optional parquet export.  The catalog is derivable from tree + footer objects
without re-reading object bytes, ``plan_pruned_scan`` prunes parquet row
groups from the footer stats alone, and ``execute_scan`` reads only the kept
row groups and returns the matching rows (docs/architecture.md §4).
"""

from __future__ import annotations

from ..domain import ReflakeError
from ..extras import EXTRAS
from .pruning import (
    Predicate,
    PrunedFileScan,
    PruneResult,
    parse_where_clause,
    plan_pruned_file,
    plan_pruned_scan,
    prune_row_groups,
)

try:
    from .catalog import (
        AnalyticalIndexPaths,
        build_analytical_index,
        drop_analytical_index,
        query_analytical_index,
    )
    from .execute import Scan, ScanStats, execute_scan
except ModuleNotFoundError as error:  # pragma: no cover – minimal installs
    raise ReflakeError(
        f"{EXTRAS['query']} needs the optional dependency "
        f"'{(error.name or '').split('.', 1)[0]}'. "
        "Install it with: pip install 'reflake[query]'"
    ) from error

__all__ = [
    "AnalyticalIndexPaths",
    "Predicate",
    "PrunedFileScan",
    "PruneResult",
    "Scan",
    "ScanStats",
    "build_analytical_index",
    "drop_analytical_index",
    "execute_scan",
    "parse_where_clause",
    "plan_pruned_file",
    "plan_pruned_scan",
    "prune_row_groups",
    "query_analytical_index",
]
