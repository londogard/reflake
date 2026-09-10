"""Regression tests for sharded directories.

A directory with more than ``MAX_TREE_ENTRIES`` entries is stored as
name-range shard subtrees.  Every operation that merges a parent directory
into a new tree (full worktree commits, staged removals applied during a full
commit) must expand those shards to the real children first — otherwise shard
pointers are mistaken for named children, which either silently drops
parent-only entries or produces duplicate paths and unsorted tree streams.

The shard threshold is monkeypatched small so these tests stay fast; the code
path is identical for the real 10k bound.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from reflake.core import create_repository
from reflake.core.domain import StageChange
from reflake.core.objects import LocalObjectStore
from reflake.core.repository import ReflakeRepository

#: Small shard bound used by these tests (real bound: 10_000).
TEST_SHARD_LIMIT = 16


@pytest.fixture
def sharded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ReflakeRepository:
    """A repository with one sharded directory of pointer entries."""
    monkeypatch.setattr(
        "reflake.core.services.tree.MAX_TREE_ENTRIES", TEST_SHARD_LIMIT
    )
    repo = create_repository(tmp_path)
    directory = tmp_path / "d"
    directory.mkdir()
    names = [f"f{i:03d}.txt" for i in range(TEST_SHARD_LIMIT * 3 + 2)]
    for name in names:
        (directory / name).touch()
    _stage_pointer_directory(repo, directory, names)
    repo.commit("seed", staged_only=True)
    return repo


def _stage_pointer_directory(
    repo: ReflakeRepository, directory: Path, names: list[str]
) -> None:
    prefix = directory.relative_to(repo.root).as_posix()
    _stage_pointer_directory_named(repo, prefix, directory, names)


def _stage_pointer_directory_named(
    repo: ReflakeRepository,
    prefix: str,
    directory: Path,
    names: list[str],
) -> None:
    repo.staging.save(
        "main",
        {
            f"{prefix}/{name}": StageChange(
                path=f"{prefix}/{name}",
                action="add",
                identity_mode="pointer",
                source_uri=(directory / name).as_uri(),
            )
            for name in names
        },
    )


def _flattened(repo: ReflakeRepository) -> list[str]:
    commit_id = repo.resolve_ref("main")
    commit = repo.read_commit(commit_id)
    return [entry.path for entry in repo.store.iter_all_entries(commit.tree)]


def _shard_pointers(repo: ReflakeRepository) -> list[tuple[str, int]]:
    """``(pointer name, entries in shard)`` for the sharded directory ``d``."""
    commit = repo.read_commit(repo.resolve_ref("main"))
    payload = repo.store.read_tree_bytes(commit.tree)
    assert payload is not None
    d_hash = json.loads(payload.decode().splitlines()[0])[2]
    d_payload = repo.store.read_tree_bytes(d_hash)
    assert d_payload is not None
    pointers: list[tuple[str, int]] = []
    for line in d_payload.decode().splitlines():
        entry = json.loads(line)
        assert entry[0] == "s", f"expected shard pointer, got {entry[0]}"
        body = repo.store.read_tree_bytes(entry[2])
        assert body is not None
        pointers.append((entry[1], len(body.decode().splitlines())))
    return pointers


def _count_tree_io(
    repo: ReflakeRepository, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[str], list[str]]:
    """Count tree-node reads/writes from here on: ``(reads, writes)``."""
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


def test_directory_is_actually_sharded(sharded: ReflakeRepository) -> None:
    commit = sharded.read_commit(sharded.resolve_ref("main"))
    node = sharded.store.read_tree_bytes(commit.tree)
    assert node is not None
    # Root holds one directory; that directory must exceed the shard bound.
    assert len(list(sharded.store.iter_all_entries(commit.tree))) > TEST_SHARD_LIMIT


@pytest.mark.parametrize("deleted_index", [0, 1, TEST_SHARD_LIMIT + 1])
def test_full_commit_preserves_parent_entries_in_sharded_directory(
    sharded: ReflakeRepository, deleted_index: int
) -> None:
    """A file removed from the worktree alone survives a full commit.

    Deletions are explicit (`reflake rm`); a plain commit keeps the committed
    entry even when the file is missing locally.
    """
    all_paths = _flattened(sharded)
    victim = all_paths[deleted_index]
    (sharded.root / victim).unlink()

    sharded.commit("worktree lost one file")

    paths = _flattened(sharded)
    assert len(paths) == len(set(paths)), "duplicate paths in tree walk"
    assert paths == sorted(paths), "tree walk is not sorted"
    assert victim in paths
    for path in paths:
        assert sharded.resolve_entry("main", path) is not None


def test_staged_removal_in_sharded_directory_is_applied(
    sharded: ReflakeRepository,
) -> None:
    paths_before = _flattened(sharded)
    victim = paths_before[0]

    sharded.rm([victim])
    sharded.commit("remove one entry")

    paths = _flattened(sharded)
    assert victim not in paths
    assert len(paths) == len(paths_before) - 1
    assert len(paths) == len(set(paths))
    assert paths == sorted(paths)
    assert sharded.resolve_entry("main", victim) is None


def test_staged_removal_of_directory_in_sharded_directory_is_applied(
    sharded: ReflakeRepository,
) -> None:
    """Removing a subdirectory prefix removes its entries, no others."""
    nested = sharded.root / "d" / "nested"
    nested.mkdir()
    (nested / "leaf.txt").touch()
    _stage_pointer_directory_named(sharded, "d/nested", nested, ["leaf.txt"])
    sharded.commit("add nested", staged_only=True)

    paths_before = _flattened(sharded)
    sharded.rm(["d/nested"])
    sharded.commit("remove nested")

    paths = _flattened(sharded)
    removed = [path for path in paths_before if path.startswith("d/nested")]
    assert removed
    assert not [path for path in paths if path.startswith("d/nested")]
    assert len(paths) == len(paths_before) - len(removed)
    assert len(paths) == len(set(paths))


def test_promote_rebuilds_sharded_tree(
    sharded: ReflakeRepository,
) -> None:
    """`promote` rebuilds the tree from a flattened walk; it must stay valid."""
    all_paths = _flattened(sharded)
    extra = sharded.root / "d" / "extra.txt"
    extra.write_text("extra")
    _stage_pointer_directory_named(
        sharded, "d", sharded.root / "d", ["extra.txt"]
    )
    sharded.commit("add pointer entry", staged_only=True)

    result = sharded.promote()

    assert result.verified_entries == len(all_paths) + 1
    paths = _flattened(sharded)
    assert len(paths) == len(set(paths))
    assert paths == sorted(paths)
    assert "d/extra.txt" in paths
    for path in paths:
        assert sharded.resolve_entry("main", path) is not None


def test_prefix_matching_is_component_aware(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    for path in ("foobar/2.txt", "foo/1.txt"):
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(path)
    repo.commit("seed")

    assert sorted(repo.resolve_entries_for_prefix("main", "foo")) == ["foo/1.txt"]
    # `foobar` must not be treated as a child of `foo`.
    assert "foobar/2.txt" not in repo.resolve_entries_for_prefix("main", "foo")


def test_rm_reports_exactly_what_it_removes(tmp_path: Path) -> None:
    """`rm` path matching and tree splicing must agree on path boundaries."""
    repo = create_repository(tmp_path)
    for path in ("data/raw/a.txt", "data/rawish/b.txt"):
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(path)
    repo.commit("seed")

    repo.rm(["data/raw"])
    repo.commit("remove data/raw")

    remaining = sorted(repo.resolve_entries("main"))
    assert remaining == ["data/rawish/b.txt"]


def test_prefix_listing_covers_every_shard(sharded: ReflakeRepository) -> None:
    paths = _flattened(sharded)
    listed = sorted(sharded.resolve_entries_for_prefix("main", "d"))
    assert listed == sorted(paths)
    # Exact-file prefixes and directory prefixes both resolve through shards.
    one = paths[len(paths) // 2]
    assert list(sharded.resolve_entries_for_prefix("main", one)) == [one]
    assert sorted(sharded.resolve_entries_for_prefix("main", "d/")) == sorted(paths)


def test_staged_add_touches_one_shard(
    sharded: ReflakeRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Adding a file rewrites only the shard range that contains it."""
    directory = sharded.root / "d"
    # Sorts inside the second shard range (f016..f031).
    name = f"f{TEST_SHARD_LIMIT + 4:03d}x.txt"
    (directory / name).touch()
    _stage_pointer_directory_named(sharded, "d", directory, [name])

    reads, writes = _count_tree_io(sharded, monkeypatch)
    sharded.commit("add one file", staged_only=True)

    assert len(writes) <= 4, f"commit wrote {len(writes)} tree nodes"
    assert len(reads) <= 6, f"commit read {len(reads)} tree nodes"
    assert sharded.resolve_entry("main", f"d/{name}") is not None
    assert len(_flattened(sharded)) == TEST_SHARD_LIMIT * 3 + 3


def test_staged_removal_touches_one_shard(
    sharded: ReflakeRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    victim = f"d/f{TEST_SHARD_LIMIT + 4:03d}.txt"
    sharded.rm([victim])

    reads, writes = _count_tree_io(sharded, monkeypatch)
    sharded.commit("remove one file", staged_only=True)

    assert len(writes) <= 4, f"commit wrote {len(writes)} tree nodes"
    assert len(reads) <= 6, f"commit read {len(reads)} tree nodes"
    assert sharded.resolve_entry("main", victim) is None
    assert len(_flattened(sharded)) == TEST_SHARD_LIMIT * 3 + 1


def test_full_commit_rewrites_nothing_when_unchanged(
    sharded: ReflakeRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A full commit that changes nothing must not PUT a single tree node."""
    # The first full commit materializes the staged pointers as content
    # entries; from there the tree is stable.
    sharded.commit("materialize")

    _, writes = _count_tree_io(sharded, monkeypatch)
    sharded.commit("nothing changed")

    assert writes == [], f"unchanged commit wrote {len(writes)} tree nodes"


def test_full_commit_removal_rewrites_one_shard(
    sharded: ReflakeRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Staged removals are pruned shard by shard, not by expanding all."""
    sharded.commit("materialize")
    victim = f"d/f{TEST_SHARD_LIMIT + 4:03d}.txt"
    sharded.rm([victim])

    reads, writes = _count_tree_io(sharded, monkeypatch)
    sharded.commit("remove one file")

    assert len(writes) <= 5, f"commit wrote {len(writes)} tree nodes"
    assert len(reads) <= 20, f"commit read {len(reads)} tree nodes"
    assert sharded.resolve_entry("main", victim) is None
    assert sharded.resolve_entry("main", "d/f000.txt") is not None


def test_shard_overflow_splits_and_stays_addressable(
    sharded: ReflakeRepository,
) -> None:
    """A shard that outgrows its range splits instead of nesting shards."""
    before = _shard_pointers(sharded)
    assert [size for _, size in before] == [
        TEST_SHARD_LIMIT,
        TEST_SHARD_LIMIT,
        TEST_SHARD_LIMIT,
        2,
    ]

    directory = sharded.root / "d"
    name = "a-first.txt"  # sorts before every f* name, so it joins shard 0
    (directory / name).touch()
    _stage_pointer_directory_named(sharded, "d", directory, [name])
    sharded.commit("insert before the first range", staged_only=True)

    after = _shard_pointers(sharded)
    assert len(after) == len(before) + 1, after
    assert [name for name, _ in after] == sorted(name for name, _ in after)
    # The overflowing range split into 9 + 8; the rest are untouched.
    assert [size for _, size in after[:2]] == [9, 8]
    assert [size for _, size in after[2:]] == [size for _, size in before[1:]]
    assert sharded.resolve_entry("main", f"d/{name}") is not None
    last_in_first_range = f"d/f{TEST_SHARD_LIMIT - 1:03d}.txt"
    assert sharded.resolve_entry("main", last_in_first_range) is not None
    assert len(_flattened(sharded)) == TEST_SHARD_LIMIT * 3 + 3
    # And the rebuilt tree still promotes cleanly.
    assert sharded.promote().verified_entries == TEST_SHARD_LIMIT * 3 + 3
