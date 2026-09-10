"""Repository lifecycle: opening must never mutate, first commit must create.

These invariants are easy to regress: the ref manager used to write an empty
branch ref for the default branch while opening a repository, which both
violated "open never mutates" and silently "created" nonexistent S3 prefixes.
"""

from __future__ import annotations

from pathlib import Path

from reflake.core import create_repository, open_repository
from reflake.core.objects import LocalObjectStore
from reflake.core.repository import ReflakeRepository


def test_open_does_not_recreate_missing_local_branch_ref(tmp_path: Path) -> None:
    create_repository(tmp_path)
    store = LocalObjectStore(tmp_path)
    (store.layout.heads_dir / "main").unlink()
    assert list(store.iter_branches()) == []

    open_repository(tmp_path)

    assert list(store.iter_branches()) == []


def test_open_does_not_create_objects_in_remote_store(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})

    open_repository("s3://demo-bucket/some/prefix", worktree=tmp_path)

    assert client._objects == {}


def test_first_commit_creates_unborn_branch_ref(tmp_path: Path) -> None:
    repo = ReflakeRepository(tmp_path)
    (tmp_path / "a.txt").write_text("payload")

    commit_id = repo.commit("first commit")

    assert repo.head_commit() == commit_id
    assert repo.resolve_ref("main") == commit_id
    assert repo.resolve_entry("main", "a.txt") is not None


def test_staging_works_before_the_first_commit(tmp_path: Path) -> None:
    repo = ReflakeRepository(tmp_path)
    (tmp_path / "a.txt").write_text("payload")

    stage = repo.add(["a.txt"])
    assert stage.added == ["a.txt"]

    status = repo.status()
    assert status.added == ["a.txt"]


def test_remote_first_commit_creates_only_expected_objects(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    repo = open_repository("s3://demo-bucket/some/prefix", worktree=worktree)

    (worktree / "a.txt").write_text("payload")
    repo.commit("first commit", staged_only=False)

    keys = set(client._objects)
    assert "some/prefix/refs/heads/main" in keys
    assert any(key.startswith("some/prefix/blobs/") for key in keys)
    assert any(key.startswith("some/prefix/trees/") for key in keys)
    assert any(key.startswith("some/prefix/commits/") for key in keys)
