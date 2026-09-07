"""GC reachability: merged (second-parent) lineage must survive prune."""

from __future__ import annotations

from pathlib import Path

from reflake.core import create_repository


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
