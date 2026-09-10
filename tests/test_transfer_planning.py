"""Adaptive transfer planning: probes for small plans, inventory for large."""

from __future__ import annotations

from pathlib import Path

import pytest

from reflake.core import create_repository
from reflake.core.objects import S3ObjectStore
from reflake.core.repository_sync import push


def _repo_with_commits(tmp_path: Path, commit_count: int):
    repo = create_repository(tmp_path)
    for index in range(commit_count):
        (tmp_path / f"file_{index}.txt").write_text(f"content-{index}")
        repo.commit(f"commit {index}")
    return repo


def test_small_plan_uses_per_object_probes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_s3_installer
) -> None:
    """A handful of objects is cheaper to probe than to enumerate."""
    client = fake_s3_installer({})
    repo = _repo_with_commits(tmp_path, 1)

    def fail_inventory(self):
        raise AssertionError("inventory listing used for a tiny plan")

    monkeypatch.setattr(S3ObjectStore, "iter_object_ids", fail_inventory)

    result = push(repo, "s3://demo-bucket/repos/test")

    assert result.updated is True
    assert any(key.startswith("repos/test/blobs/") for key in client._objects)


def test_large_plan_uses_inventory_listing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_s3_installer
) -> None:
    """Many objects: one listing per kind instead of N HEAD requests."""
    client = fake_s3_installer({})
    repo = _repo_with_commits(tmp_path, 6)

    monkeypatch.setattr("reflake.core.repository_sync._INVENTORY_THRESHOLD", 1)

    def fail_probe(self, kind, object_id):
        raise AssertionError(f"per-object probe used for {kind}/{object_id}")

    monkeypatch.setattr(S3ObjectStore, "object_exists", fail_probe)

    result = push(repo, "s3://demo-bucket/repos/test")

    assert result.updated is True
    assert result.pushed_commits == 6
    assert any(key.startswith("repos/test/commits/") for key in client._objects)


def test_second_push_transfers_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_s3_installer
) -> None:
    """The inventory path must report already-present objects as present."""
    fake_s3_installer({})
    repo = _repo_with_commits(tmp_path, 6)
    monkeypatch.setattr("reflake.core.repository_sync._INVENTORY_THRESHOLD", 1)

    first = push(repo, "s3://demo-bucket/repos/test")
    second = push(repo, "s3://demo-bucket/repos/test")

    assert first.pushed_commits == 6
    assert second.pushed_commits == 0
    assert second.pushed_blobs == 0
    assert second.updated is False
