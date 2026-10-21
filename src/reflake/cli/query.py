"""Query commands: DuckDB catalog build, and pruned parquet scan execution.

``query scan --explain`` is the metadata-only plan view (the former
``query prune``): it reports which row groups would be read without fetching
any data bytes.
"""

from __future__ import annotations

import itertools
import json
import sys
from typing import Any

from ..core import (
    PrunedFileScan,
    ScanStats,
    build_analytical_index,
    execute_scan,
    open_repository,
    parse_where_clause,
    plan_pruned_scan,
)
from ..core.objects.footer import FooterCache
from .args import QueryBuildArgs, QueryScanArgs


def handle_query_build(command: QueryBuildArgs, repo_root: str, as_json: bool) -> int:
    paths = build_analytical_index(
        root=repo_root,
        output_dir=command.output_dir,
        export_parquet=command.parquet,
    )
    if as_json:
        print(
            json.dumps(
                {
                    "database_path": str(paths.database_path),
                    "parquet_path": (
                        str(paths.parquet_path)
                        if paths.parquet_path is not None
                        else None
                    ),
                },
                indent=2,
            )
        )
    else:
        print(f"Database: {paths.database_path}")
        if paths.parquet_path:
            print(f"Parquet:  {paths.parquet_path}")
    return 0


def handle_query_scan(command: QueryScanArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    commit_id = repo.resolve_ref(command.ref or repo.current_branch())
    commit_obj = repo.read_commit(commit_id)
    predicates = parse_where_clause(command.where)
    footer_cache = FooterCache(repo.client_state.cache_dir / "footers")

    if command.explain:
        scans = list(
            plan_pruned_scan(
                repo.store,
                commit_obj.tree,
                command.path,
                predicates,
                footer_cache=footer_cache,
            )
        )
        plan_stats = ScanStats(
            files=len(scans),
            row_groups_kept=sum(len(s.kept_row_groups) for s in scans),
            row_groups_total=sum(s.total_row_groups for s in scans),
        )
        if as_json:
            print(
                json.dumps(
                    {
                        "stats": _scan_stats_payload(plan_stats),
                        "rows": [],
                        "plan": _plan_payload(scans),
                    },
                    indent=2,
                )
            )
        else:
            _print_plan(scans)
        return 0

    scan = execute_scan(
        repo.store,
        commit_obj.tree,
        command.path,
        predicates,
        footer_cache=footer_cache,
        verify=command.verify,
        strict=command.strict,
    )
    rows = scan.rows
    if command.limit is not None:
        rows = itertools.islice(rows, command.limit)
    if as_json:
        payload_rows = list(rows)
        stats_payload = _scan_stats_payload(scan.stats)
        print(
            json.dumps(
                {"stats": stats_payload, "rows": payload_rows},
                indent=2,
                default=str,
            )
        )
    else:
        for row in rows:
            print(json.dumps(row, default=str))
        _print_scan_summary(scan.stats, limit=command.limit)
    return 0


def _scan_stats_payload(stats: ScanStats) -> dict[str, Any]:
    return {
        "files": stats.files,
        "files_skipped": stats.files_skipped,
        "files_unavailable": stats.files_unavailable,
        "row_groups_kept": stats.row_groups_kept,
        "row_groups_total": stats.row_groups_total,
        "bytes_fetched": stats.bytes_fetched,
        "bytes_total": stats.bytes_total,
        "requests": stats.requests,
        "inapplicable": list(stats.inapplicable),
    }


def _plan_payload(scans: list[PrunedFileScan]) -> list[dict[str, Any]]:
    return [
        {
            "path": scan.path,
            "footer_hash": scan.footer_hash,
            "total_row_groups": scan.total_row_groups,
            "kept_row_groups": list(scan.kept_row_groups),
            "pruned_row_groups": list(scan.pruned_row_groups),
        }
        for scan in scans
    ]


def _print_plan(scans: list[PrunedFileScan]) -> None:
    for scan in scans:
        print(
            f"{scan.path}: kept {len(scan.kept_row_groups)}/"
            f"{scan.total_row_groups} row groups "
            f"({len(scan.pruned_row_groups)} pruned)"
        )
    if not scans:
        print("No parquet files with captured footer stats under this path.")
    else:
        total_kept = sum(len(s.kept_row_groups) for s in scans)
        total_pruned = sum(len(s.pruned_row_groups) for s in scans)
        print(
            f"Summary: {len(scans)} files, {total_kept} kept / "
            f"{total_pruned} pruned row groups (metadata-only)."
        )


def _format_bytes(count: int) -> str:
    match count:
        case _ if count >= 1024 * 1024:
            return f"{count / (1024 * 1024):.1f} MiB"
        case _ if count >= 1024:
            return f"{count / 1024:.1f} KiB"
        case _:
            return f"{count} B"


def _print_scan_summary(stats: ScanStats, *, limit: int | None = None) -> None:
    suffix = f" (stopped at --limit {limit})" if limit is not None else ""
    print(
        f"Scanned {stats.files} file(s), kept "
        f"{stats.row_groups_kept}/{stats.row_groups_total} row groups; fetched "
        f"{_format_bytes(stats.bytes_fetched)} of {_format_bytes(stats.bytes_total)} "
        f"in {stats.requests} request(s){suffix}.",
        file=sys.stderr,
    )
    if stats.bytes_total > 0:
        saved = 1 - stats.bytes_fetched / stats.bytes_total
        print(f"Bytes saved: {saved:.1%}", file=sys.stderr)
    if stats.files_skipped:
        print(
            f"⚠  {stats.files_skipped} non-parquet file(s) skipped.",
            file=sys.stderr,
        )
    if stats.inapplicable:
        print(
            "⚠  Predicate column(s) not applicable to some files "
            f"(no filtering applied there): {', '.join(stats.inapplicable)}",
            file=sys.stderr,
        )
    if stats.files_unavailable:
        preview = ", ".join(stats.unavailable_paths[:5])
        more = "" if len(stats.unavailable_paths) <= 5 else ", …"
        print(
            f"⚠  {stats.files_unavailable} pointer file(s) skipped — source "
            f"missing or unreachable: {preview}{more}. Run `reflake identity "
            "promote` to make them scannable, or pass --strict to fail.",
            file=sys.stderr,
        )
