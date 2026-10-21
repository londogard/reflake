"""Staging semantics: `add` snapshots content; `commit` never re-reads it."""

from __future__ import annotations

from pathlib import Path

from reflake.cli import run_cli
from reflake.core import create_repository


def test_staged_source_change_is_reported_and_the_snapshot_is_committed(
    tmp_path: Path, capsys
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    source = tmp_path / "incoming.txt"
    source.write_text("v1")

    assert run_cli(["--repo", str(repo_root), "init"]) == 0
    capsys.readouterr()
    assert (
        run_cli(
            [
                "--repo",
                str(repo_root),
                "add",
                "--as",
                "imports/incoming.txt",
                str(source),
            ]
        )
        == 0
    )
    capsys.readouterr()

    source.write_text("v2 with different size")
    assert (
        run_cli(["--repo", str(repo_root), "commit", "--staged", "-m", "snapshot"]) == 0
    )
    captured = capsys.readouterr()
    assert "changed since `add`" in captured.err
    assert "snapshot taken at" in captured.err
    assert "imports/incoming.txt" in captured.err

    # `add` froze the content (the blob was stored then), so the commit is
    # exactly what the user staged — not whatever the source says later.
    repo = create_repository(str(repo_root))
    assert repo.cat("main", "imports/incoming.txt") == b"v1"

    # Re-adding picks up the new content.
    assert (
        run_cli(
            [
                "--repo",
                str(repo_root),
                "add",
                "--as",
                "imports/incoming.txt",
                str(source),
            ]
        )
        == 0
    )
    assert run_cli(["--repo", str(repo_root), "commit", "-m", "refresh"]) == 0
    assert repo.cat("main", "imports/incoming.txt") == b"v2 with different size"


def test_staged_metadata_records_size_and_mtime(tmp_path: Path) -> None:
    repo = create_repository(str(tmp_path))
    (tmp_path / "file.txt").write_text("payload")
    stage = repo.add(["file.txt"])
    assert stage.added == ["file.txt"]

    staging = repo.staging.load("main")
    change = staging["file.txt"]
    assert change.size == len(b"payload")
    assert change.mtime_ns is not None
    # Content-mode staging records the blob, so a commit needs no source read.
    assert change.blob_hash is not None
