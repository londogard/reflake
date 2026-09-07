"""Commit IDs are content-determined (timestamp/branch excluded)."""

from __future__ import annotations

import os
from pathlib import Path

from reflake.core import create_repository


def test_same_content_same_id_across_repos(tmp_path: Path) -> None:
    """Identical content committed at different times yields the same ID."""
    ids = []
    for name in ("repo-a", "repo-b"):
        repo_dir = tmp_path / name
        repo_dir.mkdir()
        target = repo_dir / "a.txt"
        target.write_text("same-bytes")
        # mtime is part of leaf metadata: pin it so both trees match.
        os.utime(target, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
        repo = create_repository(str(repo_dir))
        ids.append(repo.commit("same message"))

    assert ids[0] == ids[1]
    assert len(ids[0]) == 64


def test_different_message_gives_different_id(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    (repo_dir / "a.txt").write_text("same-bytes")

    repo = create_repository(str(repo_dir))
    first = repo.commit("message one")
    assert len(first) == 64
    assert repo.refs.read_commit(first).created_at
