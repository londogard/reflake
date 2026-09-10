"""Tree-level three-way merge: correctness edges and cost.

The merge walks *directory nodes* and uses content-addressed fast paths, so
two branches that each changed one file must not touch the rest of the tree.

Note on branch isolation: `checkout` never rewrites the worktree, so a *full*
commit on one branch captures whatever the other branch left on disk. These
tests therefore build branch-specific trees through staging (`add` from a file
outside the repository, or a staged `rm`), which is the only way to get
genuinely diverged trees in one working directory.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from reflake.core import create_repository
from reflake.core.config import LocalConfig
from reflake.core.domain import MergeConflictError
from reflake.core.objects import LocalObjectStore
from reflake.core.repository import ReflakeRepository

DIRECTORY_COUNT = 120
FILES_PER_DIRECTORY = 5


def _staging_dir(tmp_path: Path) -> Path:
    """A staging source directory that never lives inside the repository."""
    outside = tmp_path.parent / f"{tmp_path.name}-staging"
    outside.mkdir(exist_ok=True)
    return outside


def _stage_text(
    repo: ReflakeRepository, tmp_path: Path, logical_path: str, text: str
) -> None:
    """Stage *logical_path* from outside the worktree (branch-isolated)."""
    name = logical_path.rsplit("/", 1)[-1]
    source = _staging_dir(tmp_path) / name
    source.write_text(text)
    repo.add([str(source)], destination_path=logical_path)


def _seed(tmp_path: Path) -> ReflakeRepository:
    """A pointer-mode repository with many small directories (fast commits)."""
    repo = create_repository(tmp_path)
    for index in range(DIRECTORY_COUNT):
        directory = tmp_path / f"dir_{index:03d}"
        directory.mkdir()
        for file_index in range(FILES_PER_DIRECTORY):
            (directory / f"file_{file_index}.txt").write_text(
                f"content-{index}-{file_index}"
            )
    repo.commit("base")
    return repo


def _merged_entries(repo: ReflakeRepository, commit_id: str) -> dict[str, object]:
    tree = repo.read_commit(commit_id).tree
    return {entry.path: entry for entry in repo.store.iter_all_entries(tree)}


def test_merge_touches_only_changed_directories(tmp_path: Path, monkeypatch) -> None:
    LocalConfig(dataset_root=str(tmp_path), identity="pointer").save(tmp_path)
    repo = _seed(tmp_path)
    repo.branch("feature")

    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "dir_007/file_0.txt", "feature change")
    repo.commit("feature change", staged_only=True)

    repo.set_current_branch("main")
    _stage_text(repo, tmp_path, "dir_111/file_1.txt", "main change")
    repo.commit("main change", staged_only=True)

    reads: list[str] = []
    writes: list[str] = []
    real_read = repo.tree_writer._walker._read_tree  # noqa: SLF001
    real_write = LocalObjectStore.write_tree_bytes

    def counting_read(tree_hash: str) -> bytes | None:
        reads.append(tree_hash)
        return real_read(tree_hash)

    def counting_write(self, tree_hash, payload, **kwargs):
        writes.append(tree_hash)
        return real_write(self, tree_hash, payload, **kwargs)

    monkeypatch.setattr(repo.tree_writer._walker, "_read_tree", counting_read)  # noqa: SLF001
    monkeypatch.setattr(LocalObjectStore, "write_tree_bytes", counting_write)

    result = repo.merge("feature", "main")

    assert result.updated is True
    merged = _merged_entries(repo, result.commit_id)
    assert merged["dir_007/file_0.txt"].size == len("feature change")  # type: ignore[union-attr]
    assert merged["dir_111/file_1.txt"].size == len("main change")  # type: ignore[union-attr]
    assert len(merged) == DIRECTORY_COUNT * FILES_PER_DIRECTORY
    # A flattening merge reads and rebuilds every directory (120+): the whole
    # point of the tree-level merge is that it does not.
    assert len(reads) <= 12, f"merge read {len(reads)} tree nodes"
    assert len(writes) <= 6, f"merge wrote {len(writes)} tree nodes"


def test_merge_reuses_untouched_subtree_hashes(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    base_tree = repo.read_commit(repo.head_commit()).tree
    repo.branch("feature")

    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "dir_000/file_0.txt", "feature change")
    repo.commit("feature change", staged_only=True)

    repo.set_current_branch("main")
    _stage_text(repo, tmp_path, "dir_119/file_4.txt", "main change")
    repo.commit("main change", staged_only=True)

    result = repo.merge("feature", "main")

    base_children = {
        entry.path: entry for entry in repo.store.iter_all_entries(base_tree)
    }
    merged_children = _merged_entries(repo, result.commit_id)
    untouched = "dir_050/file_0.txt"
    assert merged_children[untouched].hash == base_children[untouched].hash  # type: ignore[union-attr]


def test_merge_same_identical_change_on_both_sides(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    repo.branch("feature")

    for branch in ("feature", "main"):
        repo.set_current_branch(branch)
        _stage_text(repo, tmp_path, "dir_003/file_2.txt", "identical change")
        repo.commit(f"{branch}: identical", staged_only=True)

    result = repo.merge("feature", "main")

    assert result.updated
    merged = _merged_entries(repo, result.commit_id)
    assert merged["dir_003/file_2.txt"].size == len("identical change")  # type: ignore[union-attr]


def test_merge_conflicts_in_nested_directory(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    repo.branch("feature")

    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "dir_009/file_3.txt", "feature version")
    repo.commit("feature change", staged_only=True)

    repo.set_current_branch("main")
    _stage_text(repo, tmp_path, "dir_009/file_3.txt", "main version")
    repo.commit("main change", staged_only=True)

    try:
        repo.merge("feature", "main")
    except MergeConflictError as error:
        assert "dir_009/file_3.txt" in str(error)
    else:  # pragma: no cover - the merge must not silently pick a side
        raise AssertionError("expected MergeConflictError")


def test_merge_delete_versus_modify_conflicts(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    repo.branch("feature")

    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "dir_004/file_4.txt", "feature modified")
    repo.commit("feature change", staged_only=True)

    repo.set_current_branch("main")
    repo.rm(["dir_004"])
    repo.commit("main removed dir", staged_only=True)

    try:
        repo.merge("feature", "main")
    except MergeConflictError as error:
        assert "dir_004" in str(error)
    else:  # pragma: no cover
        raise AssertionError("expected MergeConflictError")


def test_merge_uncontested_delete_wins(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    repo.branch("feature")

    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "unrelated.txt", "feature")
    repo.commit("feature change", staged_only=True)

    repo.set_current_branch("main")
    repo.rm(["dir_004"])
    repo.commit("main removed dir", staged_only=True)

    result = repo.merge("feature", "main")

    assert result.updated
    assert repo.resolve_entry("main", "dir_004/file_4.txt") is None
    assert repo.resolve_entry("main", "dir_004/file_0.txt") is None
    assert repo.resolve_entry("main", "unrelated.txt") is not None


def test_merge_file_replaced_by_directory_when_uncontested(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    (tmp_path / "p").write_text("a file")
    (tmp_path / "anchor.txt").write_text("anchor")
    repo.commit("base")
    repo.branch("feature")

    # feature replaces file `p` with a directory `p/` (staged; worktree stays
    # on the base layout so main sees `p` as a file).
    replacement = _staging_dir(tmp_path) / "replacement"
    replacement.mkdir(exist_ok=True)
    (replacement / "leaf.txt").write_text("now a directory")
    repo.set_current_branch("feature")
    repo.rm(["p"])
    repo.add([str(replacement)], destination_path="p")
    repo.commit("file to directory", staged_only=True)

    repo.set_current_branch("main")
    _stage_text(repo, tmp_path, "anchor.txt", "anchor changed")
    repo.commit("main change", staged_only=True)

    result = repo.merge("feature", "main")

    assert result.updated
    assert repo.resolve_entry("main", "p") is None
    assert repo.resolve_entry("main", "p/leaf.txt") is not None
    assert repo.resolve_entry("main", "anchor.txt") is not None


def test_merge_add_add_different_content_conflicts(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    (tmp_path / "anchor.txt").write_text("anchor")
    repo.commit("base")
    repo.branch("feature")

    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "added.txt", "from feature")
    repo.commit("feature adds", staged_only=True)
    repo.set_current_branch("main")
    _stage_text(repo, tmp_path, "added.txt", "from main, differently")
    repo.commit("main adds", staged_only=True)

    try:
        repo.merge("feature", "main")
    except MergeConflictError as error:
        assert "added.txt" in str(error)
    else:  # pragma: no cover
        raise AssertionError("expected MergeConflictError")


def test_merge_of_sharded_directory_compares_shard_ranges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both sides changed different shard ranges: no shard body is read."""
    monkeypatch.setattr("reflake.core.services.tree.MAX_TREE_ENTRIES", 8)
    LocalConfig(dataset_root=str(tmp_path), identity="pointer").save(tmp_path)
    repo = create_repository(tmp_path)
    directory = tmp_path / "d"
    directory.mkdir()
    names = [f"f{index:03d}.txt" for index in range(40)]
    for name in names:
        (directory / name).write_text(f"base-{name}")
    repo.commit("base")
    repo.branch("feature")

    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "d/f004.txt", "feature change")
    repo.commit("feature change", staged_only=True)
    repo.set_current_branch("main")
    _stage_text(repo, tmp_path, "d/f036.txt", "main change")
    repo.commit("main change", staged_only=True)

    reads: list[str] = []
    walker = repo.tree_writer._walker  # noqa: SLF001
    real_read = walker._read_tree  # noqa: SLF001

    def counting_read(tree_hash: str) -> bytes | None:
        reads.append(tree_hash)
        return real_read(tree_hash)

    monkeypatch.setattr(walker, "_read_tree", counting_read)

    result = repo.merge("feature", "main")

    assert result.updated
    merged = _merged_entries(repo, result.commit_id)
    assert len(merged) == len(names)
    assert merged["d/f004.txt"].size == len("feature change")  # type: ignore[union-attr]
    assert merged["d/f036.txt"].size == len("main change")  # type: ignore[union-attr]
    # Two sides + base for the root and the sharded directory: four nodes.
    # Expanding the shards instead would have read every shard of all three.
    assert len(reads) <= 8, f"merge read {len(reads)} tree nodes"


def test_merge_add_add_identical_content_succeeds(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    (tmp_path / "anchor.txt").write_text("anchor")
    repo.commit("base")
    repo.branch("feature")

    for branch in ("feature", "main"):
        repo.set_current_branch(branch)
        _stage_text(repo, tmp_path, "added.txt", "same bytes")
        repo.commit(f"{branch} adds", staged_only=True)

    result = repo.merge("feature", "main")

    assert result.updated
    assert repo.resolve_entry("main", "added.txt") is not None
