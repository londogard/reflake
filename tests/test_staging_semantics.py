"""Staging semantics: a staged add is a recipe, and drift is reported loudly."""

from __future__ import annotations

from pathlib import Path

from reflake import run_cli
from reflake.core import create_repository


def test_staged_source_change_is_reported_and_commits_current_content(
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
        run_cli(
            ["--repo", str(repo_root), "commit", "--staged", "-m", "snapshot"]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "Staged source(s) changed since `add`" in captured.err
    assert "imports/incoming.txt" in captured.err

    # The commit contains the content at commit time (documented semantics).
    repo = create_repository(str(repo_root))
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

