"""``query scan`` execution: pruned reads, byte accounting, sources, CLI.

Covers the v0.4 definition of done: correctness against DuckDB full scans,
bytes fetched bounded by kept row groups, ``--explain`` reading zero data
bytes, missing-column errors, no-footer fallback, fake-S3 range requests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from reflake.cli import run_cli
from reflake.core import (
    ReflakeRepository,
    SourceNotFoundError,
    execute_scan,
    open_repository,
    parse_where_clause,
)
from reflake.core.config import LocalConfig


def _config(root: Path, *, identity: str = "content", footer: bool = True) -> None:
    LocalConfig(identity=identity, parquet_footer=footer).save(root)


def _write_parquet(path: Path, rows: int, row_group_size: int) -> None:
    table = pa.table(
        {
            "id": pa.array(range(rows), pa.int64()),
            "lbl": [f"v{i}" for i in range(rows)],
        }
    )
    pq.write_table(table, path, row_group_size=row_group_size)


def _committed_repo(tmp_path: Path) -> tuple[ReflakeRepository, str]:
    repo = ReflakeRepository(tmp_path)
    repo.commit("seed")
    commit = repo.read_commit(repo.resolve_ref("main"))
    return repo, commit.tree


def _row_group_span(row_group: Any) -> tuple[int, int]:
    """Conservative byte interval covering one row group's column chunks."""
    starts: list[int] = []
    ends: list[int] = []
    for index in range(row_group.num_columns):
        column = row_group.column(index)
        offsets = [column.data_page_offset]
        if column.dictionary_page_offset:
            offsets.append(column.dictionary_page_offset)
        start = min(offsets)
        starts.append(start)
        ends.append(start + column.total_compressed_size)
    return min(starts), max(ends)


def test_scan_matches_duckdb_and_prunes(tmp_path: Path) -> None:
    """Exact rows vs DuckDB ``WHERE`` — including NULL semantics."""
    path = tmp_path / "data.parquet"
    duckdb.sql(
        "COPY (SELECT i::BIGINT AS id, "
        "CASE WHEN i % 7 = 0 THEN NULL ELSE 'v' || i END AS lbl "
        f"FROM range(0, 20000) t(i)) TO '{path}' "
        "(FORMAT PARQUET, ROW_GROUP_SIZE 2048)"
    )
    _config(tmp_path)
    repo, tree = _committed_repo(tmp_path)
    entry = repo.resolve_entry("main", "data.parquet")
    assert entry is not None
    assert entry.kind == "bp" and entry.footer is not None

    clauses = [
        "id >= 19000",
        "id >= 1000 AND id < 3000",
        "lbl != 'v5'",
        "lbl IS NULL",
        "lbl IS NOT NULL AND id >= 15000",
        "id = 12345",
    ]
    for where in clauses:
        scan = execute_scan(repo.store, tree, "", parse_where_clause(where))
        rows = sorted((row["id"], row["lbl"]) for row in scan.rows)
        expected = duckdb.sql(
            f"SELECT id, lbl FROM read_parquet('{path}') WHERE {where} ORDER BY id"
        ).fetchall()
        assert rows == expected, where
        assert scan.stats.row_groups_total >= 8, where

    selective = execute_scan(repo.store, tree, "", parse_where_clause("id >= 19000"))
    rows = list(selective.rows)
    assert [row["id"] for row in rows] == list(range(19000, 20000))
    stats = selective.stats
    assert stats.files == 1
    assert stats.row_groups_kept < stats.row_groups_total
    assert stats.bytes_fetched < stats.bytes_total * 0.5
    assert stats.requests >= 1


def test_scan_bytes_bounded_by_kept_row_groups(tmp_path: Path) -> None:
    path = tmp_path / "big.parquet"
    _write_parquet(path, rows=40_000, row_group_size=5_000)
    size = path.stat().st_size
    assert size > 64 * 1024  # larger than the fixed footer-tail read
    _config(tmp_path)
    repo, tree = _committed_repo(tmp_path)

    scan = execute_scan(repo.store, tree, "", parse_where_clause("id >= 35000"))
    rows = list(scan.rows)
    assert [row["id"] for row in rows] == list(range(35000, 40000))

    stats = scan.stats
    assert stats.row_groups_kept == 1
    assert stats.row_groups_total == 8
    metadata = pq.ParquetFile(path).metadata.row_group(7)
    kept_compressed = sum(
        metadata.column(i).total_compressed_size for i in range(metadata.num_columns)
    )
    # Fetched = kept group's column chunks + the fixed 64 KiB metadata tail.
    # Slack is explicit: chunk padding/coalescing may add a little.
    assert stats.bytes_fetched >= kept_compressed
    assert stats.bytes_fetched <= min(size, 64 * 1024) + kept_compressed + 16_384
    assert stats.bytes_fetched < size * 0.5
    assert stats.requests >= 2

    verified = execute_scan(
        repo.store, tree, "", parse_where_clause("id >= 35000"), verify=True
    )
    assert [row["id"] for row in verified.rows] == list(range(35000, 40000))
    # --verify streams the whole blob (plus the pruned read), by design.
    assert verified.stats.bytes_fetched >= verified.stats.bytes_total


def test_scan_without_footer_reads_fully(tmp_path: Path) -> None:
    """No captured stats: the file is read fully and counted as a full scan."""
    path = tmp_path / "plain.parquet"
    _write_parquet(path, rows=20_000, row_group_size=2_500)
    _config(tmp_path, footer=False)
    repo, tree = _committed_repo(tmp_path)
    entry = repo.resolve_entry("main", "plain.parquet")
    assert entry is not None
    assert entry.kind == "b" and entry.footer is None

    scan = execute_scan(repo.store, tree, "", parse_where_clause("id >= 19990"))
    rows = list(scan.rows)
    assert [row["id"] for row in rows] == list(range(19990, 20000))

    stats = scan.stats
    assert stats.row_groups_kept == stats.row_groups_total == 8
    assert stats.bytes_total == entry.size
    assert stats.bytes_fetched >= entry.size * 0.9


def test_scan_reads_pointer_source_without_promote(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    _write_parquet(path, rows=20_000, row_group_size=2_500)
    _config(tmp_path, identity="pointer", footer=True)
    repo, tree = _committed_repo(tmp_path)
    entry = repo.resolve_entry("main", "data.parquet")
    assert entry is not None
    assert entry.kind == "mp" and entry.source_uri is not None

    scan = execute_scan(repo.store, tree, "", parse_where_clause("id >= 19990"))
    rows = list(scan.rows)
    assert [row["id"] for row in rows] == list(range(19990, 20000))
    stats = scan.stats
    assert stats.files == 1
    assert stats.files_unavailable == 0
    assert stats.row_groups_kept < stats.row_groups_total
    assert stats.bytes_fetched < stats.bytes_total


def test_scan_missing_pointer_source_skipped_or_strict(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    _write_parquet(path, rows=5_000, row_group_size=1_000)
    _config(tmp_path, identity="pointer", footer=True)
    repo, tree = _committed_repo(tmp_path)
    path.unlink()  # the source is a recipe, not a snapshot: it can vanish

    scan = execute_scan(repo.store, tree, "", parse_where_clause("id >= 4990"))
    assert list(scan.rows) == []
    assert scan.stats.files_unavailable == 1
    assert scan.stats.unavailable_paths == ["data.parquet"]

    strict = execute_scan(
        repo.store, tree, "", parse_where_clause("id >= 4990"), strict=True
    )
    with pytest.raises(SourceNotFoundError):
        list(strict.rows)


def test_scan_skips_non_parquet_entries(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("not a parquet file")
    _write_parquet(tmp_path / "data.parquet", rows=2_000, row_group_size=1_000)
    _config(tmp_path)
    repo, tree = _committed_repo(tmp_path)

    scan = execute_scan(repo.store, tree, "", parse_where_clause("id >= 1990"))
    rows = list(scan.rows)
    assert [row["id"] for row in rows] == list(range(1990, 2000))
    assert scan.stats.files == 1
    assert scan.stats.files_skipped == 1


def test_scan_type_mismatch_is_inapplicable(tmp_path: Path) -> None:
    """A clause with no kernel applies no filtering and is reported."""
    _write_parquet(tmp_path / "data.parquet", rows=2_000, row_group_size=1_000)
    _config(tmp_path)
    repo, tree = _committed_repo(tmp_path)

    scan = execute_scan(repo.store, tree, "", parse_where_clause("lbl = 5"))
    rows = list(scan.rows)
    assert len(rows) == 2_000  # pruning keeps all groups; filter is skipped
    assert scan.stats.inapplicable == ["lbl"]


def test_scan_missing_column_raises_listing_files(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data.parquet", rows=1_000, row_group_size=500)
    _config(tmp_path)
    repo, tree = _committed_repo(tmp_path)

    scan = execute_scan(repo.store, tree, "", parse_where_clause("nope = 1"))
    with pytest.raises(ValueError, match=r"nope.*data\.parquet"):
        list(scan.rows)


def test_scan_explain_reads_no_data_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_parquet(tmp_path / "data.parquet", rows=2_000, row_group_size=1_000)
    _config(tmp_path)
    _committed_repo(tmp_path)
    monkeypatch.setattr(
        "reflake.core.objects.local.LocalObjectStore.read_blob_range",
        lambda *args, **kwargs: pytest.fail("--explain must not read data bytes"),
    )

    exit_code = run_cli(
        ["--repo", str(tmp_path), "query", "scan", "id >= 1500", "--explain"]
    )
    assert exit_code == 0


def test_scan_cli_output_modes(tmp_path: Path, capsys) -> None:
    _write_parquet(tmp_path / "data.parquet", rows=20_000, row_group_size=2_500)
    _config(tmp_path)
    _committed_repo(tmp_path)

    assert run_cli(["--repo", str(tmp_path), "query", "scan", "id >= 19990"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line)["id"] for line in lines] == list(range(19990, 20000))

    assert (
        run_cli(
            [
                "--json",
                "--repo",
                str(tmp_path),
                "query",
                "scan",
                "id >= 19990",
                "--limit",
                "2",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"stats", "rows"}
    assert [row["id"] for row in payload["rows"]] == [19990, 19991]
    assert payload["stats"]["files"] == 1


def test_staged_local_add_captures_footer(tmp_path: Path) -> None:
    """`add` of a local parquet captures footers on the staged path too."""
    _write_parquet(tmp_path / "data.parquet", rows=2_000, row_group_size=500)
    _config(tmp_path, identity="pointer", footer=True)
    repo = ReflakeRepository(tmp_path)
    repo.add(["data.parquet"], identity_mode="pointer")
    repo.commit("staged add", staged_only=True)

    entry = repo.resolve_entry("main", "data.parquet")
    assert entry is not None
    assert entry.footer is not None


def test_s3_source_add_captures_footer_and_scans(
    tmp_path: Path, fake_s3_installer
) -> None:
    """`add s3://…` captures footers through ranged reads, then scans them."""
    seed = tmp_path / "seed.parquet"
    _write_parquet(seed, rows=20_000, row_group_size=2_500)
    payload = seed.read_bytes()
    seed.unlink()  # the source lives only in the (fake) bucket
    client = fake_s3_installer({"sources/data.parquet": payload})
    _config(tmp_path, identity="pointer", footer=True)
    repo = open_repository(
        "s3://demo-bucket/repos/scan-source", s3_client=client, worktree=tmp_path
    )
    repo.add(
        ["s3://demo-bucket/sources/data.parquet"],
        identity_mode="pointer",
        destination_path="data.parquet",
    )
    repo.commit("import from s3")

    entry = repo.resolve_entry("main", "data.parquet")
    assert entry is not None
    assert entry.kind == "mp" and entry.footer is not None

    commit = repo.read_commit(repo.resolve_ref("main"))
    scan = execute_scan(repo.store, commit.tree, "", parse_where_clause("id >= 19990"))
    rows = list(scan.rows)
    assert [row["id"] for row in rows] == list(range(19990, 20000))
    assert scan.stats.row_groups_kept < scan.stats.row_groups_total
    assert scan.stats.bytes_fetched < scan.stats.bytes_total


def test_s3_import_captures_footers_for_pointer_entries(
    tmp_path: Path, fake_s3_installer
) -> None:
    """`import_s3` must not silently yield statless parquet entries."""
    seed = tmp_path / "seed.parquet"
    _write_parquet(seed, rows=4_000, row_group_size=1_000)
    client = fake_s3_installer({"sources/data.parquet": seed.read_bytes()})
    seed.unlink()
    _config(tmp_path, identity="pointer", footer=True)
    repo = open_repository(
        "s3://demo-bucket/repos/scan-import", s3_client=client, worktree=tmp_path
    )
    repo.import_s3("s3://demo-bucket/sources/", "import", identity_mode="pointer")

    entry = repo.resolve_entry("main", "data.parquet")
    assert entry is not None
    assert entry.kind == "mp"
    assert entry.footer is not None


def test_scan_fake_s3_fetches_only_kept_ranges(
    tmp_path: Path, fake_s3_installer
) -> None:
    path = tmp_path / "data.parquet"
    _write_parquet(path, rows=40_000, row_group_size=5_000)
    size = path.stat().st_size
    _config(tmp_path)
    client = fake_s3_installer({})
    repo = open_repository(
        "s3://demo-bucket/repos/scan-bytes", s3_client=client, worktree=tmp_path
    )
    repo.commit("seed")
    commit = repo.read_commit(repo.resolve_ref("main"))

    scan = execute_scan(repo.store, commit.tree, "", parse_where_clause("id >= 35000"))
    rows = list(scan.rows)
    assert [row["id"] for row in rows] == list(range(35000, 40000))

    assert client.range_requests, "S3 scan must issue ranged GETs"
    metadata = pq.ParquetFile(path).metadata
    pruned_spans = [_row_group_span(metadata.row_group(i)) for i in range(7)]
    tail_start = size - min(size, 64 * 1024)
    for key, start, end in client.range_requests:
        assert "/blobs/" in key
        if start >= tail_start:
            continue  # the fixed footer-tail read is metadata, not data pages
        for low, high in pruned_spans:
            assert end < low or start > high, (start, end, (low, high))
    assert scan.stats.row_groups_kept == 1
    assert scan.stats.bytes_fetched < size * 0.5


@pytest.mark.benchmark
def test_scan_scales_bytes_saved(tmp_path: Path) -> None:
    """Cost, not wall-clock: 1/20 of the row groups must stay near 1/20 bytes."""
    rows = 200_000
    _write_parquet(tmp_path / "data.parquet", rows=rows, row_group_size=rows // 20)
    _config(tmp_path)
    repo, tree = _committed_repo(tmp_path)

    scan = execute_scan(repo.store, tree, "", parse_where_clause("id >= 195000"))
    returned = list(scan.rows)
    assert len(returned) == 5_000
    stats = scan.stats
    assert stats.row_groups_kept == 1
    assert stats.row_groups_total == 20
    assert stats.bytes_fetched / stats.bytes_total < 0.15
    assert stats.requests < 10  # no per-page read amplification
