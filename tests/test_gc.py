"""GC reachability: merged (second-parent) lineage must survive prune."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from reflake.core import create_repository
from reflake.core.objects.s3 import S3ObjectStore


def _commit_file(repo_dir: Path, name: str, content: str, message: str) -> str:
    (repo_dir / name).write_text(content)
    repo = create_repository(str(repo_dir))
    return repo.commit(message)


def test_gc_prune_keeps_merged_second_parent_blobs(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    _commit_file(repo_dir, "base.txt", "base", "base commit")

    repo = create_repository(str(repo_dir))
    repo.refs.branch("feature")
    repo.refs.set_current_branch("feature")
    _commit_file(repo_dir, "feature.txt", "feature-data", "feature commit")

    repo.refs.set_current_branch("main")
    _commit_file(repo_dir, "main.txt", "main-data", "main commit")

    # Diverged 3-way merge: merge commit has parents [main, feature].
    result = repo.merge("feature", "main")
    assert result.updated is True
    merge_commit = repo.refs.read_commit(result.commit_id)
    assert len(merge_commit.parents) == 2

    # Sanity: blobs exist in the store before gc.
    assert list(repo.store.iter_object_ids("blob")), "expected blobs in store"

    audit = repo.gc(dry_run=True)
    assert audit.orphan_blobs == 0, f"merged lineage reported as orphan: {audit}"
    # base + 2 sides + merge = 4 reachable commits.
    assert audit.reachable_commits == 4, audit

    pruned = repo.gc(dry_run=False)
    assert pruned.pruned is False
    assert pruned.orphan_blobs == 0

    # Feature content still readable after prune.
    assert repo.cat("main", "feature.txt") == b"feature-data"


def test_gc_prune_skips_young_orphans(tmp_path: Path) -> None:
    """A freshly published orphan may belong to a writer mid-commit: never pruned."""
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    (repo_dir / "a.txt").write_text("a")
    repo = create_repository(str(repo_dir))
    repo.commit("c1")

    young = repo.tree_writer._write_tree_bytes(  # noqa: SLF001
        ('["b","young","' + "a" * 64 + '",1,1]\n').encode()
    )
    old = repo.tree_writer._write_tree_bytes(  # noqa: SLF001
        ('["b","old","' + "b" * 64 + '",1,1]\n').encode()
    )
    old_path = repo.store.tree_path(old)  # type: ignore[attr-defined]
    past = time.time() - 3 * 86400
    os.utime(old_path, (past, past))

    result = repo.gc(dry_run=False, grace_seconds=86400)

    remaining = set(repo.store.iter_object_ids("tree"))
    assert old not in remaining
    assert young in remaining
    assert result.skipped_young == 1


def test_gc_prune_skips_young_s3_orphans(
    tmp_path: Path,
    fake_s3_installer: pytest.MonkeyPatch,
) -> None:
    """S3 orphans use object LastModified for the grace window."""
    del tmp_path
    from reflake.core.repository import open_repository

    client = fake_s3_installer({})
    repo = open_repository(
        "s3://demo-bucket/repos/gc-grace", s3_client=client
    )
    old_id, young_id = "c" * 64, "d" * 64
    for index, object_id in enumerate((old_id, young_id)):
        repo.store.write_tree_bytes(
            object_id,
            (f'["b","f{index}","' + "a" * 64 + '",1,1]\n').encode(),
        )
    client._objects[repo.store._key("tree", old_id)]["LastModified"] = datetime(  # noqa: SLF001
        2020, 1, 1, tzinfo=UTC
    )

    result = repo.gc(dry_run=False, grace_seconds=86400)

    remaining = set(repo.store.iter_object_ids("tree"))
    assert old_id not in remaining
    assert young_id in remaining
    assert result.skipped_young == 1


def test_s3_deletes_are_batched(
    tmp_path: Path,
    fake_s3_installer: pytest.MonkeyPatch,
) -> None:
    """GC on S3 issues one DeleteObjects call per ≤1000 keys."""
    del tmp_path
    client = fake_s3_installer({})
    store = S3ObjectStore(bucket="demo-bucket", prefix="repo", client=client)
    object_ids = [f"{index:064x}" for index in range(2500)]
    for object_id in object_ids:
        client.put_object(
            Bucket="demo-bucket", Key=store._key("blob", object_id), Body=b"x"  # noqa: SLF001
        )

    assert store.delete_objects("blob", object_ids) == 2500
    assert client.delete_calls == [1000, 1000, 500]
    assert list(store.iter_object_ids("blob")) == []
