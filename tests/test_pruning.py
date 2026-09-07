"""Row-group pruning: predicate parsing, stats evaluation, and scan planning."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from reflake.cli import run_cli
from reflake.core.config import LocalConfig
from reflake.core.objects.footer import (
    ColumnStats,
    FooterStats,
    RowGroupStats,
    parse_footer_stats,
    serialize_footer_stats,
)
from reflake.core.query.pruning import (
    Predicate,
    parse_where_clause,
    plan_pruned_scan,
    prune_row_groups,
)
from reflake.core.repository import ReflakeRepository


def _make_stats(row_groups: tuple[RowGroupStats, ...]) -> FooterStats:
    return FooterStats(schema_hash="0" * 64, row_groups=row_groups)


def _id_group(
    rows: int, min_value: int, max_value: int, nulls: int = 0
) -> RowGroupStats:
    return RowGroupStats(
        rows=rows,
        columns=(ColumnStats("id", min=min_value, max=max_value, nulls=nulls),),
    )


# ── WHERE parsing ────────────────────────────────────────────────────────


def test_parse_where_clause_comparisons_and_types() -> None:
    predicates = parse_where_clause("id >= 100 AND active = true AND name = 'foo bar'")
    assert predicates == [
        Predicate(column="id", op=">=", value=100),
        Predicate(column="active", op="=", value=True),
        Predicate(column="name", op="=", value="foo bar"),
    ]


def test_parse_where_clause_null_ops() -> None:
    assert parse_where_clause("a IS NULL") == [Predicate(column="a", op="is_null")]
    assert parse_where_clause("a IS NOT NULL") == [
        Predicate(column="a", op="is_not_null")
    ]


def test_parse_where_clause_floats_and_neq() -> None:
    assert parse_where_clause("x <> 1.5") == [Predicate(column="x", op="!=", value=1.5)]


def test_parse_where_clause_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        parse_where_clause("id =")
    with pytest.raises(ValueError):
        parse_where_clause("id = 1 OR active = true")
    with pytest.raises(ValueError):
        parse_where_clause("   ")


# ── Row-group pruning ────────────────────────────────────────────────────


def test_prune_row_groups_range_scan() -> None:
    stats = _make_stats(
        (
            _id_group(100, 0, 99),
            _id_group(100, 100, 199),
            _id_group(100, 200, 299),
        )
    )
    result = prune_row_groups(stats, [Predicate("id", ">=", 150)])
    assert result.kept_row_groups == (1, 2)
    assert result.pruned_row_groups == (0,)
    assert result.total_row_groups == 3
    assert result.applicable == ("id",)
    assert result.inapplicable == ()


def test_prune_row_groups_exact_value_spans_group() -> None:
    stats = _make_stats(
        (
            _id_group(100, 0, 99),
            _id_group(100, 100, 199),
            _id_group(100, 200, 299),
        )
    )
    # id = 100 is possible only in the group whose [min, max] contains it.
    result = prune_row_groups(stats, [Predicate("id", "=", 100)])
    assert result.kept_row_groups == (1,)
    assert result.pruned_row_groups == (0, 2)


def test_prune_row_groups_not_equal_constant_group() -> None:
    stats = _make_stats(
        (
            _id_group(100, 0, 99),
            _id_group(100, 100, 100),
            _id_group(100, 200, 299),
        )
    )
    # The group where every value is 100 cannot satisfy id != 100.
    result = prune_row_groups(stats, [Predicate("id", "!=", 100)])
    assert result.pruned_row_groups == (1,)
    # Null rows satisfy `!=`, so an all-null group must be kept.
    stats_with_nulls = _make_stats((_id_group(100, 100, 100, nulls=100),))
    result_nulls = prune_row_groups(stats_with_nulls, [Predicate("id", "!=", 100)])
    assert result_nulls.kept_row_groups == (0,)


def test_prune_row_groups_null_predicates() -> None:
    stats = _make_stats(
        (
            _id_group(100, 0, 99, nulls=10),
            _id_group(100, 0, 99, nulls=0),
            _id_group(100, 0, 99, nulls=100),
        )
    )
    result = prune_row_groups(stats, [Predicate("id", "is_null")])
    assert result.kept_row_groups == (0, 2)

    result = prune_row_groups(stats, [Predicate("id", "is_not_null")])
    assert result.kept_row_groups == (0, 1)


def test_prune_row_groups_unknown_column_is_conservative() -> None:
    stats = _make_stats((_id_group(100, 0, 99),))
    result = prune_row_groups(stats, [Predicate("nope", ">=", 50)])
    assert result.kept_row_groups == (0,)
    assert result.inapplicable == ("nope",)


def test_prune_row_groups_type_mismatch_is_conservative() -> None:
    stats = _make_stats((_id_group(100, 0, 99),))
    result = prune_row_groups(stats, [Predicate("id", ">=", "abc")])
    assert result.kept_row_groups == (0,)
    assert result.inapplicable == ("id",)


def test_prune_row_groups_and_semantics() -> None:
    stats = _make_stats(
        (
            _id_group(100, 0, 99),
            _id_group(100, 100, 199),
        )
    )
    # id >= 100 AND id < 200: only the second group's range satisfies both.
    result = prune_row_groups(
        stats,
        [Predicate("id", ">=", 100), Predicate("id", "<", 200)],
    )
    assert result.kept_row_groups == (1,)
    assert result.pruned_row_groups == (0,)


def test_footer_stats_round_trip_through_store_format() -> None:
    stats = _make_stats(
        (
            _id_group(100, 0, 99),
            _id_group(100, 100, 199, nulls=5),
        )
    )
    assert parse_footer_stats(serialize_footer_stats(stats)) == stats


# ── Scan planning over a committed repo ──────────────────────────────────


# DuckDB's parquet writer honors ROW_GROUP_SIZE but clamps it to a 2048-row
# minimum, so multi-row-group files are built from 2048-row groups.
_ROW_GROUP_ROWS = 2048


def _write_parquet_groups(path: Path, groups: int) -> None:
    duckdb.sql(
        "COPY (SELECT range AS id, 'v' || range AS lbl FROM range(0, ?)) "
        f"TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE {_ROW_GROUP_ROWS})",
        params=[groups * _ROW_GROUP_ROWS],
    )


def test_plan_pruned_scan_on_repo(tmp_path: Path) -> None:
    _write_parquet_groups(tmp_path / "data.parquet", groups=5)
    LocalConfig(identity="content", parquet_footer=True).save(tmp_path)
    repo = ReflakeRepository(tmp_path)
    repo.commit("captured")

    commit_id = repo.resolve_ref("main")
    commit = repo.read_commit(commit_id)
    scans = list(
        plan_pruned_scan(repo.store, commit.tree, "", [Predicate("id", ">=", 6144)])
    )
    assert len(scans) == 1
    scan = scans[0]
    assert scan.path == "data.parquet"
    assert scan.footer_hash is not None
    assert scan.total_row_groups == 5
    assert scan.kept_row_groups == (3, 4)
    assert scan.pruned_row_groups == (0, 1, 2)


def test_plan_pruned_scan_prefix_filters_files(tmp_path: Path) -> None:
    _write_parquet_groups(tmp_path / "a.parquet", groups=3)
    _write_parquet_groups(tmp_path / "b.parquet", groups=3)
    (tmp_path / "note.txt").write_text("not parquet")
    LocalConfig(identity="content", parquet_footer=True).save(tmp_path)
    repo = ReflakeRepository(tmp_path)
    repo.commit("captured")

    commit = repo.read_commit(repo.resolve_ref("main"))
    scans = list(
        plan_pruned_scan(repo.store, commit.tree, "", [Predicate("id", "<", 2048)])
    )
    assert [scan.path for scan in scans] == ["a.parquet", "b.parquet"]
    for scan in scans:
        assert scan.kept_row_groups == (0,)
        assert scan.pruned_row_groups == (1, 2)


def test_cli_query_prune(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_parquet_groups(tmp_path / "data.parquet", groups=3)
    LocalConfig(identity="content", parquet_footer=True).save(tmp_path)
    ReflakeRepository(tmp_path).commit("captured")

    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    exit_code = run_cli(
        [
            "--repo",
            str(tmp_path),
            "query",
            "prune",
            "main",
            "data.parquet",
            "--where",
            "id >= 4096",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "kept 1/3 row groups (2 pruned)" in captured.out
    assert (
        "Summary: 1 files, 1 kept / 2 pruned row groups (metadata-only)."
        in captured.out
    )


def test_cli_query_prune_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_parquet_groups(tmp_path / "data.parquet", groups=3)
    LocalConfig(identity="content", parquet_footer=True).save(tmp_path)
    ReflakeRepository(tmp_path).commit("captured")

    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    exit_code = run_cli(
        [
            "--repo",
            str(tmp_path),
            "--json",
            "query",
            "prune",
            "main",
            "data.parquet",
            "--where",
            "id < 0",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload == [
        {
            "path": "data.parquet",
            "footer_hash": payload[0]["footer_hash"],
            "total_row_groups": 3,
            "kept_row_groups": [],
            "pruned_row_groups": [0, 1, 2],
        }
    ]
