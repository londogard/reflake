"""Semantics and safety of individual operations.

These tests pin behaviors that were previously either wrong or undocumented:
``verify`` must be strictly read-only, ``log`` must expose merged lineages,
``restore`` must handle directory prefixes and reject unknown paths, blob
reads must be hash-verified, and CLI usage errors must exit 1 (not the
conflict code 2).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from reflake import run_cli
from reflake.core import create_repository, open_repository
from reflake.core.domain import BlobIntegrityError, StageChange
from reflake.core.objects import LocalObjectStore


def _seed_repo(tmp_path: Path):
    repo = create_repository(tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "a.txt").write_text("alpha")
    (tmp_path / "data" / "b.txt").write_text("beta")
    repo.commit("seed")
    return repo


def test_verify_dry_run_writes_no_objects(tmp_path: Path, monkeypatch) -> None:
    repo = _seed_repo(tmp_path)
    (tmp_path / "data" / "pointer.txt").write_text("pointer")
    repo.staging.save(
        "main",
        {
            "data/pointer.txt": StageChange(
                path="data/pointer.txt",
                action="add",
                identity_mode="pointer",
                source_uri=(tmp_path / "data" / "pointer.txt").as_uri(),
            )
        },
    )
    repo.commit("pointer entry", staged_only=True)

    writes: list[tuple[str, str]] = []
    original_tree_write = LocalObjectStore.write_tree_bytes
    original_commit_write = LocalObjectStore.write_commit_bytes

    def track(kind: str, original):
        def wrapper(self, object_id, payload, **kwargs):
            writes.append((kind, object_id))
            return original(self, object_id, payload, **kwargs)

        return wrapper

    monkeypatch.setattr(
        LocalObjectStore, "write_tree_bytes", track("tree", original_tree_write)
    )
    monkeypatch.setattr(
        LocalObjectStore,
        "write_commit_bytes",
        track("commit", original_commit_write),
    )

    result = repo.verify()

    assert result.candidate_entries == 1
    assert result.verified_entries == 0
    assert result.created_commit is False
    assert writes == []


def test_log_includes_merged_branch_commits(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    (tmp_path / "base.txt").write_text("base")
    repo.commit("base")
    repo.branch("feature")
    repo.set_current_branch("feature")
    (tmp_path / "feat.txt").write_text("feat")
    feature_commit = repo.commit("feature work")
    repo.set_current_branch("main")
    (tmp_path / "main.txt").write_text("main")
    repo.commit("main work")
    repo.merge("feature", "main")

    messages = [commit.message for commit in repo.log("main")]

    assert "merge feature into main" in messages
    assert "feature work" in messages, "first-parent-only log hides merged history"
    assert "main work" in messages
    assert "base" in messages
    assert repo.read_commit(feature_commit) is not None


def test_restore_accepts_directory_prefixes(tmp_path: Path) -> None:
    repo = _seed_repo(tmp_path)
    (tmp_path / "data" / "a.txt").unlink()
    (tmp_path / "data" / "b.txt").unlink()

    restored = repo.restore_files("main", paths=["data"])

    assert sorted(restored) == ["data/a.txt", "data/b.txt"]
    assert (tmp_path / "data" / "a.txt").read_text() == "alpha"


def test_restore_rejects_unknown_paths(tmp_path: Path) -> None:
    repo = _seed_repo(tmp_path)

    with pytest.raises(FileNotFoundError, match="unknown.txt"):
        repo.restore_files("main", paths=["unknown.txt"])


def test_restore_cli_reports_missing_path_as_exit_3(tmp_path: Path) -> None:
    _seed_repo(tmp_path)

    assert (
        run_cli(["--repo", str(tmp_path), "restore", "main", "--path", "nope.txt"])
        == 3
    )


def test_unknown_command_is_a_usage_error(tmp_path: Path) -> None:
    # 2 is reserved for retryable conflicts; usage errors are 1.
    assert run_cli(["--repo", str(tmp_path), "frobnicate"]) == 1


def test_read_blob_detects_corruption_on_disk(tmp_path: Path) -> None:
    repo = _seed_repo(tmp_path)
    entry = repo.resolve_entry("main", "data/a.txt")
    assert entry is not None and entry.blob_hash is not None

    blob_path = repo.store.blob_path(entry.blob_hash)
    blob_path.write_bytes(b"corrupted-on-disk")

    with pytest.raises(BlobIntegrityError, match="hash mismatch"):
        repo.read_blob(entry.blob_hash)
    with pytest.raises(BlobIntegrityError, match="hash mismatch"):
        repo.cat("main", "data/a.txt")


def test_read_blob_verifies_blobs_of_all_kinds(tmp_path: Path) -> None:
    repo = _seed_repo(tmp_path)
    payload = repo.read_blob(
        repo.resolve_entry("main", "data/a.txt").blob_hash  # type: ignore[union-attr]
    )
    assert payload == b"alpha"


def test_reopening_repository_does_not_change_state(tmp_path: Path) -> None:
    repo = _seed_repo(tmp_path)
    before = repo.head_commit()

    reopened = open_repository(tmp_path)

    assert reopened.head_commit() == before
