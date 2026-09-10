"""Promote must keep captured footer stats when materializing blobs."""

from __future__ import annotations

from pathlib import Path

import duckdb

from reflake.core.config import LocalConfig
from reflake.core.repository import ReflakeRepository


def _write_parquet(path: Path, rows: int = 1000) -> None:
    duckdb.sql(
        "COPY (SELECT range AS id, 'v' || range AS lbl FROM range(0, ?)) "
        f"TO '{path}' (FORMAT PARQUET)",
        params=[rows],
    )


def test_promote_preserves_footer_stats(tmp_path: Path) -> None:
    """`promote` rewrites pointer entries as content blobs — footer included."""
    _write_parquet(tmp_path / "data.parquet", rows=1000)
    LocalConfig(identity="pointer", parquet_footer=True).save(tmp_path)
    repo = ReflakeRepository(tmp_path)
    repo.commit("meta footer")

    before = repo.resolve_entry("main", "data.parquet")
    assert before is not None
    assert before.identity_mode == "pointer"
    assert before.blob_hash is None
    assert before.footer is not None

    result = repo.promote()

    assert result.created_commit is True
    assert result.verified_entries == 1
    after = repo.resolve_entry("main", "data.parquet")
    assert after is not None
    assert after.identity_mode == "content"
    assert after.blob_hash is not None
    # Losing the footer here would silently disable pruning for every file
    # that went through promotion.
    assert after.footer == before.footer
    assert repo.store.read_blob_bytes(after.blob_hash) is not None
