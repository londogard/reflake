"""Cost guarantees for commit, diff, merge-base and footer pruning.

These tests assert *algorithmic* behaviour — how many objects an operation
touches — not just its result.  The bounds are loose enough to survive
implementation tweaks but far below the O(tree) cost of a flattening
implementation, which is what they exist to prevent.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import reflake.core.services.tree as tree_module
from reflake.core import create_repository
from reflake.core.config import LocalConfig
from reflake.core.objects import LocalObjectStore
from reflake.core.objects.footer import FooterCache
from reflake.core.query.pruning import Predicate, plan_pruned_scan
from reflake.core.repository import ReflakeRepository
from reflake.core.repository_support import merge_base_commit

DIRECTORY_COUNT = 40
FILES_PER_DIRECTORY = 3


def _seed(
    tmp_path: Path, *, identity: str = "pointer", **config_overrides: object
) -> ReflakeRepository:
    """A repository with many small directories (fast commits)."""
    LocalConfig(
        dataset_root=str(tmp_path),
        identity=identity,
        **config_overrides,  # type: ignore[arg-type]
    ).save(tmp_path)
    repo = create_repository(tmp_path)
    for index in range(DIRECTORY_COUNT):
        directory = tmp_path / f"dir_{index:03d}"
        directory.mkdir()
        for file_index in range(FILES_PER_DIRECTORY):
            (directory / f"file_{file_index}.txt").write_text(
                f"content-{index}-{file_index}"
            )
    repo.commit("seed")
    return repo


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


def _count_tree_io(
    repo: ReflakeRepository, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[str], list[str]]:
    """Count tree-node reads and writes for *repo* from here on."""
    reads: list[str] = []
    writes: list[str] = []
    walker = repo.tree_writer._walker  # noqa: SLF001
    real_read = walker._read_tree  # noqa: SLF001
    real_write = LocalObjectStore.write_tree_bytes

    def counting_read(tree_hash: str) -> bytes | None:
        reads.append(tree_hash)
        return real_read(tree_hash)

    def counting_write(self, tree_hash, payload, **kwargs):
        writes.append(tree_hash)
        return real_write(self, tree_hash, payload, **kwargs)

    monkeypatch.setattr(walker, "_read_tree", counting_read)
    monkeypatch.setattr(LocalObjectStore, "write_tree_bytes", counting_write)
    return reads, writes


def test_full_commit_rewrites_only_changed_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A one-file change must not rewrite the other directories' nodes."""
    repo = _seed(tmp_path)
    before_commit = repo.head_commit()
    (tmp_path / "dir_007" / "file_1.txt").write_text("changed content")

    reads, writes = _count_tree_io(repo, monkeypatch)
    repo.commit("one file changed")

    assert len(writes) <= 4, f"commit wrote {len(writes)} tree nodes"
    before = {
        entry.path: entry
        for entry in repo.store.iter_all_entries(repo.read_commit(before_commit).tree)
    }
    after = {
        entry.path: entry
        for entry in repo.store.iter_all_entries(_head_tree(repo))
    }
    # The 39 untouched directories keep their exact subtree hash.
    assert after["dir_000/file_0.txt"].hash == before["dir_000/file_0.txt"].hash
    assert after["dir_007/file_1.txt"].hash != before["dir_007/file_1.txt"].hash


def _head_tree(repo: ReflakeRepository) -> str:
    return repo.read_commit(repo.resolve_ref("main")).tree


def test_trust_mtime_skips_hashing_unchanged_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ``trust_mtime`` nothing is re-hashed unless size/mtime moved."""
    repo = _seed(tmp_path, identity="content", trust_mtime=True)
    hashes: list[Path] = []
    real_digest = tree_module.blake3_digest_file

    def counting_digest(path: Path) -> str:
        hashes.append(path)
        return real_digest(path)

    monkeypatch.setattr(tree_module, "blake3_digest_file", counting_digest)

    repo.commit("no changes at all")
    assert hashes == [], f"re-hashed {len(hashes)} unchanged files"

    # Same size, new mtime: the file must be re-hashed (and only it).
    changed = tmp_path / "dir_003" / "file_0.txt"
    original_length = len(changed.read_text())
    changed.write_text("X" * original_length)
    repo.commit("same size, different bytes")
    assert [path.name for path in hashes] == ["file_0.txt"]
    entry = repo.resolve_entry("main", "dir_003/file_0.txt")
    assert entry is not None and entry.size == original_length


def test_default_mode_still_rehashes_same_size_files(tmp_path: Path) -> None:
    """``trust_mtime`` off: a same-size file is verified by hashing (v1 rule)."""
    repo = _seed(tmp_path, identity="content")
    before = repo.resolve_entry("main", "dir_003/file_0.txt")
    changed = tmp_path / "dir_003" / "file_0.txt"
    changed.write_text("Y" * len(changed.read_text()))
    repo.commit("same size, different bytes")
    after = repo.resolve_entry("main", "dir_003/file_0.txt")
    assert before is not None and after is not None
    assert after.hash != before.hash


def test_diff_reads_only_changed_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Structural diff touches O(changed directories), not the whole tree."""
    repo = _seed(tmp_path)
    repo.branch("feature")
    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "dir_002/file_0.txt", "feature change")
    _stage_text(repo, tmp_path, "dir_002/new.txt", "new file")
    repo.commit("feature change", staged_only=True)

    reads, _ = _count_tree_io(repo, monkeypatch)
    changes = repo.diff("main", "feature")

    assert [(change.path, change.change) for change in changes] == [
        ("dir_002/file_0.txt", "modified"),
        ("dir_002/new.txt", "added"),
    ]
    assert len(reads) <= 8, f"diff read {len(reads)} tree nodes"


def test_merge_base_walks_to_the_nearest_common_ancestor(tmp_path: Path) -> None:
    """Using the *nearest* base avoids false conflicts after a shared change."""
    LocalConfig(dataset_root=str(tmp_path), identity="pointer").save(tmp_path)
    repo = create_repository(tmp_path)
    (tmp_path / "shared.txt").write_text("v0")
    repo.commit("A")

    _stage_text(repo, tmp_path, "shared.txt", "v1")
    repo.commit("B: shared.txt -> v1", staged_only=True)
    shared_commit = repo.head_commit()

    repo.branch("feature")  # feature starts at B
    repo.set_current_branch("feature")
    _stage_text(repo, tmp_path, "feature-only.txt", "feature")
    repo.commit("D: feature only", staged_only=True)

    repo.set_current_branch("main")
    _stage_text(repo, tmp_path, "shared.txt", "v2")
    repo.commit("C: shared.txt -> v2", staged_only=True)

    base = merge_base_commit(
        repo.resolve_ref("feature"),
        repo.resolve_ref("main"),
        read_commit=repo.refs.read_commit,
    )
    assert base is not None and base.id == shared_commit

    # A merge that used "A" as base would see v1 vs v2 as a conflict; with
    # the real base (B) main simply changed the file after the divergence.
    result = repo.merge("feature", "main")
    assert result.updated
    entry = repo.resolve_entry("main", "shared.txt")
    assert entry is not None and entry.size == len("v2")
    assert repo.resolve_entry("main", "feature-only.txt") is not None


def test_footer_cache_makes_repeat_scans_read_free(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Footer stats are content-addressed: a warm cache reads nothing."""
    LocalConfig(
        dataset_root=str(tmp_path),
        identity="content",
        parquet_footer=True,
    ).save(tmp_path)
    repo = create_repository(tmp_path)
    for index in range(3):
        duckdb.sql(
            "COPY (SELECT range AS id FROM range(0, ?)) "
            f"TO '{tmp_path / f'part_{index}.parquet'}' (FORMAT PARQUET)",
            params=[500 * (index + 1)],
        )
    repo.commit("parquet")

    tree = _head_tree(repo)
    cache = FooterCache(tmp_path / "cache")
    reads: list[str] = []
    real_read = LocalObjectStore.read_footer_bytes

    def counting_read(self, footer_hash: str) -> bytes | None:
        reads.append(footer_hash)
        return real_read(self, footer_hash)

    monkeypatch.setattr(LocalObjectStore, "read_footer_bytes", counting_read)
    predicates = [Predicate(column="id", op=">=", value=100)]

    first = list(plan_pruned_scan(repo.store, tree, "", predicates, footer_cache=cache))
    assert len(first) == 3
    assert len(reads) == 3, "cold cache must read each distinct footer once"

    reads.clear()
    second = list(
        plan_pruned_scan(repo.store, tree, "", predicates, footer_cache=cache)
    )
    assert [scan.kept_row_groups for scan in second] == [
        scan.kept_row_groups for scan in first
    ]
    assert reads == [], f"warm cache still read {len(reads)} footers"
