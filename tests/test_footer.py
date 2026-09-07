"""Parquet footer capture: parser correctness and ingest wiring (bp/mp entries)."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from reflake.core.config import LocalConfig
from reflake.core.entry_codec import Entry
from reflake.core.objects.footer import (
    parse_parquet_footer,
    serialize_footer_stats,
)
from reflake.core.repository import ReflakeRepository


def _write_parquet(path: Path, rows: int = 1000) -> None:
    duckdb.sql(
        "COPY (SELECT range AS id, 'v' || range AS lbl FROM range(0, ?)) "
        f"TO '{path}' (FORMAT PARQUET)",
        params=[rows],
    )


def test_parse_parquet_footer_extracts_schema_and_stats(tmp_path: Path) -> None:
    pfile = tmp_path / "data.parquet"
    _write_parquet(pfile, rows=1000)

    with pfile.open("rb") as handle:
        stats = parse_parquet_footer(handle)

    assert stats.schema == (
        {"name": "id", "type": "INT64"},
        {"name": "lbl", "type": "BYTE_ARRAY"},
    )
    assert len(stats.row_groups) == 1
    group = stats.row_groups[0]
    assert group.rows == 1000
    by_path = {column.path: column for column in group.columns}
    assert by_path["id"].min == 0
    assert by_path["id"].max == 999
    assert by_path["id"].nulls == 0
    assert by_path["lbl"].min == "v0"
    assert by_path["lbl"].max == "v999"
    assert len(stats.schema_hash) == 64


def test_parse_rejects_non_parquet(tmp_path: Path) -> None:
    pfile = tmp_path / "plain.txt"
    pfile.write_text("not parquet at all" * 10)
    with pfile.open("rb") as handle:
        with pytest.raises(ValueError):
            parse_parquet_footer(handle)


def test_footer_stats_round_trip_serialization(tmp_path: Path) -> None:
    pfile = tmp_path / "data.parquet"
    _write_parquet(pfile)
    with pfile.open("rb") as handle:
        stats = parse_parquet_footer(handle)
    payload = serialize_footer_stats(stats)
    parsed = json.loads(payload)
    assert parsed["row_groups"][0]["rows"] == 1000
    assert parsed["row_groups"][0]["columns"][0]["path"] == "id"


def test_commit_captures_footer_only_when_enabled(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data.parquet")
    (tmp_path / "plain.txt").write_text("hello")

    # Default: no footer capture.
    repo = ReflakeRepository(tmp_path)
    repo.commit("default")
    entry = repo.resolve_entry("main", "data.parquet")
    assert entry.footer is None

    # Enable capture; an unchanged file gets its footer backfilled.
    LocalConfig(identity="content", parquet_footer=True).save(tmp_path)
    repo2 = ReflakeRepository(tmp_path)
    repo2.commit("captured")
    entry = repo2.resolve_entry("main", "data.parquet")
    assert entry.footer is not None
    assert len(entry.footer) == 64
    assert entry.blob_hash is not None

    footer_bytes = repo2.store.read_footer_bytes(entry.footer)
    assert footer_bytes is not None
    stats = json.loads(footer_bytes)
    assert stats["row_groups"][0]["rows"] == 1000

    # Non-parquet files are never captured.
    assert repo2.resolve_entry("main", "plain.txt").footer is None


def test_commit_meta_mode_captures_mp_entry(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data.parquet")
    LocalConfig(identity="pointer", parquet_footer=True).save(tmp_path)
    repo = ReflakeRepository(tmp_path)
    repo.commit("meta footer")
    entry = repo.resolve_entry("main", "data.parquet")
    assert entry.identity_mode == "pointer"
    assert entry.footer is not None
    assert entry.source_uri is not None
    assert entry.blob_hash is None


def test_staged_add_captures_footer(tmp_path: Path) -> None:
    source_dir = tmp_path / "external"
    source_dir.mkdir()
    _write_parquet(source_dir / "remote.parquet")

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    LocalConfig(identity="content", parquet_footer=True).save(repo_root)
    repo = ReflakeRepository(repo_root)
    repo.add([str(source_dir / "remote.parquet")], ref="main")
    repo.commit("staged footer", staged_only=True)
    entry = repo.resolve_entry("main", "remote.parquet")
    assert entry.footer is not None
    assert entry.blob_hash is not None


def test_tree_entry_round_trips_footer(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data.parquet")
    LocalConfig(identity="content", parquet_footer=True).save(tmp_path)
    repo = ReflakeRepository(tmp_path)
    repo.commit("captured")
    entry = repo.resolve_entry("main", "data.parquet")

    # The derived manifest keeps the footer reference.
    commit = repo.read_commit(repo.resolve_ref("main"))
    derived = repo.tree_writer.export_derived_manifest(commit.tree)
    assert derived.exists()
    found = None
    for line in derived.read_text().splitlines():
        candidate = Entry.parse(line)
        if candidate.path == "data.parquet":
            found = candidate
    assert found is not None
    assert found.footer == entry.footer
