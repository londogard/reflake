from __future__ import annotations

from pathlib import Path

import pytest

from reflake.core import (
    ReflakeRepository,
)
from reflake.core.entry_codec import Entry
from reflake.core.objects.tree import parse_tree_object


def test_splice_tree_reuses_untouched_subtrees(tmp_path: Path) -> None:
    """Verifies that splice_tree only visits modified directories and reuses
    untouched subtrees in O(1) without re-encoding them."""
    repo = ReflakeRepository(tmp_path)

    # Create two directories: dir_a and dir_b
    (tmp_path / "dir_a").mkdir()
    (tmp_path / "dir_b").mkdir()
    (tmp_path / "dir_a" / "file1.txt").write_text("hello a")
    (tmp_path / "dir_b" / "file2.txt").write_text("hello b")

    commit_1 = repo.commit("initial commit")
    root_1 = repo.read_commit(commit_1).tree

    # Find the hash of dir_a and dir_b subtrees in root_1
    root_entries_1 = {
        e.path: e for e in parse_tree_object(repo.store.read_tree_bytes(root_1) or b"")
    }
    assert "dir_a" in root_entries_1
    assert "dir_b" in root_entries_1
    dir_a_hash_before = root_entries_1["dir_a"].hash
    dir_b_hash_before = root_entries_1["dir_b"].hash

    # Now modify ONLY a file in dir_b via staged addition
    (tmp_path / "dir_b" / "file2.txt").write_text("hello b updated")
    repo.add(["dir_b/file2.txt"])

    # Track tree reads
    read_tree_hashes: list[str] = []
    orig_read_tree = repo.store.read_tree_bytes

    def tracking_read_tree(tree_hash: str) -> bytes | None:
        read_tree_hashes.append(tree_hash)
        return orig_read_tree(tree_hash)

    repo.store._walker._read_tree = tracking_read_tree  # type: ignore[attr-defined]

    commit_2 = repo.commit("update dir_b", staged_only=True)
    root_2 = repo.read_commit(commit_2).tree

    root_entries_2 = {
        e.path: e for e in parse_tree_object(repo.store.read_tree_bytes(root_2) or b"")
    }

    # dir_a must retain its exact same hash!
    assert root_entries_2["dir_a"].hash == dir_a_hash_before

    # dir_b must have a new hash!
    assert root_entries_2["dir_b"].hash != dir_b_hash_before

    # dir_a tree was NEVER loaded or read during the staged commit!
    assert dir_a_hash_before not in read_tree_hashes


def test_splice_tree_removal_and_move_do_not_flatten_entire_tree(
    tmp_path: Path,
) -> None:
    """Verifies that repo.remove_paths and repo.move use path splicing
    and do not disturb sibling subtrees."""
    repo = ReflakeRepository(tmp_path)

    (tmp_path / "data").mkdir()
    (tmp_path / "keep").mkdir()
    (tmp_path / "data" / "file1.txt").write_text("data 1")
    (tmp_path / "keep" / "file2.txt").write_text("keep 2")

    commit_1 = repo.commit("init")
    root_1 = repo.read_commit(commit_1).tree
    keep_hash_before = {
        e.path: e for e in parse_tree_object(repo.store.read_tree_bytes(root_1) or b"")
    }["keep"].hash

    # Move data to archive
    repo.move("data", "archive", "move data")
    commit_2 = repo.head_commit()
    assert commit_2 is not None
    root_2 = repo.read_commit(commit_2).tree
    entries_2 = {
        e.path: e for e in parse_tree_object(repo.store.read_tree_bytes(root_2) or b"")
    }

    assert "data" not in entries_2
    assert "archive" in entries_2
    assert entries_2["keep"].hash == keep_hash_before

    # Remove archive
    repo.remove_paths(["archive"], "remove archive")
    commit_3 = repo.head_commit()
    assert commit_3 is not None
    root_3 = repo.read_commit(commit_3).tree
    entries_3 = {
        e.path: e for e in parse_tree_object(repo.store.read_tree_bytes(root_3) or b"")
    }

    assert "archive" not in entries_3
    assert entries_3["keep"].hash == keep_hash_before


def test_staged_directory_replaces_committed_leaf(tmp_path: Path) -> None:
    """A staged directory may replace a committed leaf without corruption.

    Regression: the splice used to append the new subtree while keeping the
    leaf, publishing a node with two entries named ``data`` (unparseable).
    """
    repo = ReflakeRepository(tmp_path)
    (tmp_path / "data").write_text("plain file")
    repo.commit("initial")

    outside = tmp_path.parent / f"{tmp_path.name}-incoming"
    (outside / "pkg").mkdir(parents=True, exist_ok=True)
    (outside / "pkg" / "x.txt").write_text("inside directory")

    repo.add([str(outside / "pkg")], identity_mode="content", destination_path="data")
    commit_id = repo.commit("directory replaces file", staged_only=True)
    tree = repo.read_commit(commit_id).tree

    paths = [entry.path for entry in repo.store.iter_all_entries(tree)]
    assert paths == ["data/x.txt"]
    root_entry = {
        entry.path: entry
        for entry in parse_tree_object(repo.store.read_tree_bytes(tree) or b"")
    }["data"]
    assert root_entry.is_subtree


def test_move_under_a_file_parent_is_rejected(tmp_path: Path) -> None:
    """``mv a b/moved`` must fail when ``b`` is a file, not silently replace it."""
    repo = ReflakeRepository(tmp_path)
    (tmp_path / "a.txt").write_text("A")
    (tmp_path / "b").write_text("B")
    repo.commit("initial")

    with pytest.raises(ValueError, match="Destination parent is a file"):
        repo.move("a.txt", "b/moved.txt", "move under a file")

    # Nothing was committed and the branch still reads cleanly.
    head = repo.head_commit()
    assert head is not None
    tree = repo.read_commit(head).tree
    assert sorted(entry.path for entry in repo.store.iter_all_entries(tree)) == [
        "a.txt",
        "b",
    ]


def test_move_staged_into_itself_is_rejected(tmp_path: Path) -> None:
    repo = ReflakeRepository(tmp_path)
    (tmp_path / "a.txt").write_text("A")
    repo.commit("initial")

    with pytest.raises(ValueError, match="Cannot move a path into itself"):
        repo.move_staged("a.txt", "a.txt/nested.txt")


def test_conflicting_staged_additions_are_rejected(tmp_path: Path) -> None:
    """Staging ``a`` and ``a/b`` would build a duplicate-name node: reject it."""
    repo = ReflakeRepository(tmp_path)
    (tmp_path / "seed.txt").write_text("seed")
    repo.commit("initial")

    first = tmp_path.parent / f"{tmp_path.name}-one.txt"
    second = tmp_path.parent / f"{tmp_path.name}-two.txt"
    first.write_text("one")
    second.write_text("two")

    repo.add([str(first)], destination_path="a")
    with pytest.raises(ValueError, match="Conflicting staged additions"):
        repo.add([str(second)], destination_path="a/b")

    # The failed add left staging untouched and the repo readable.
    status = repo.status()
    assert status.added == ["a"]


def test_tree_writer_rejects_duplicate_names(tmp_path: Path) -> None:
    """The write path must never publish a node that fails to re-parse."""
    repo = ReflakeRepository(tmp_path)
    (tmp_path / "seed.txt").write_text("seed")
    repo.commit("initial")

    line = '["b","dup","' + "a" * 64 + '",1,1]'
    payload = f"{line}\n{line}\n".encode()
    with pytest.raises(ValueError, match="sorted by name"):
        repo.tree_writer._write_tree_bytes(payload)  # noqa: SLF001


def test_tree_entry_msgspec_serialization_roundtrip() -> None:
    """Verifies that msgspec serialization preserves exact tree entry fields."""
    entries = [
        Entry(path="dir1", kind="t", hash="a" * 64),
        Entry(path="file1.txt", kind="b", hash="b" * 64, size=1024, mtime_ns=5000),
        Entry(
            path="file2.parquet",
            kind="bp",
            hash="c" * 64,
            size=2048,
            mtime_ns=6000,
            footer="d" * 64,
        ),
        Entry(
            path="file3.meta",
            kind="m",
            hash="e" * 64,
            size=512,
            mtime_ns=7000,
            source_uri="s3://bucket/key",
        ),
    ]
    payload = ("\n".join(entry.serialize() for entry in entries) + "\n").encode("utf-8")
    parsed = parse_tree_object(payload)
    assert parsed == entries
