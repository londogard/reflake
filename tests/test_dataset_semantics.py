"""Tests for the v0.5 model changes: canonical entries, worktree safety,
shared repository format, and shard-aware prefix reads.

Each test pins a behaviour that used to be wrong or slow, so a regression is a
test failure rather than a benchmark surprise.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from reflake import init_repository, open_repository
from reflake.core.entry_codec import Entry, decode_leaf
from reflake.core.objects.tree import MAX_TREE_ENTRIES
from reflake.core.repo_format import RepoFormat
from reflake.core.repository_support import pointer_identity


def _seed(tmp_path: Path) -> object:
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "keep.txt").write_text("keep")
    (tmp_path / "data" / "drop.txt").write_text("drop")
    (tmp_path / "data" / "other.txt").write_text("other")
    init_repository(tmp_path)
    repo = open_repository(tmp_path)
    repo.commit("base")
    return repo


# ── Canonical entries ─────────────────────────────────────────────────────


def test_entries_carry_no_filesystem_metadata() -> None:
    """A leaf line is content identity + source identity, nothing else."""
    line = Entry(kind="b", path="f.bin", hash="a" * 64, size=3).serialize()
    assert line == '["b","f.bin","' + "a" * 64 + '",3]'
    assert tuple(decode_leaf(line).__dict__.keys()) == (
        "path",
        "kind",
        "hash",
        "size",
        "source_uri",
        "footer",
        "source_etag",
    )


def test_identical_content_commits_to_identical_trees(tmp_path: Path) -> None:
    """Two independent worktrees with the same bytes agree on the tree hash.

    This is the property that makes commit ids comparable across machines and
    is why mtime cannot live in an entry.
    """
    trees: list[str] = []
    for name in ("a", "b"):
        root = tmp_path / name
        root.mkdir()
        (root / "f.txt").write_text("same bytes")
        init_repository(root)
        repo = open_repository(root)
        commit_id = repo.commit("seed")
        trees.append(str(commit_id.stats.tree))
    assert trees[0] == trees[1]


def test_pointer_identity_tracks_the_source_version() -> None:
    first = pointer_identity(size=10, marker="mtime:1")
    assert first == pointer_identity(size=10, marker="mtime:1")
    assert first != pointer_identity(size=10, marker="mtime:2")
    assert first != pointer_identity(size=11, marker="mtime:1")


# ── Merge ────────────────────────────────────────────────────────────────


def test_merge_resolves_identical_deletions(tmp_path: Path) -> None:
    """Both sides deleting the same file is not a conflict (and must not crash)."""
    repo = _seed(tmp_path)
    repo.branch("side")

    (tmp_path / "data" / "keep.txt").write_text("keep-main")
    repo.rm(["data/drop.txt"])
    repo.commit("main: delete drop, edit keep")

    repo.set_current_branch("side")
    (tmp_path / "data" / "other.txt").write_text("other-side")
    repo.rm(["data/drop.txt"])
    repo.commit("side: delete drop, edit other")

    repo.set_current_branch("main")
    result = repo.merge("side", "main")
    assert result.updated
    paths = sorted(repo.resolve_entries("main"))
    assert paths == ["data/keep.txt", "data/other.txt"]


# ── Worktree safety ──────────────────────────────────────────────────────


def test_rm_removes_the_worktree_copy(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    repo.rm(["data/drop.txt"])
    assert not (tmp_path / "data" / "drop.txt").exists()

    repo.commit("rm")
    repo.commit("unrelated")
    assert "data/drop.txt" not in repo.resolve_entries("main")


def test_rm_keep_worktree_is_metadata_only(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    repo.rm(["data/drop.txt"], worktree=False)
    assert (tmp_path / "data" / "drop.txt").exists()
    repo.commit("rm")
    assert "data/drop.txt" not in repo.resolve_entries("main")
    # ... and a later full commit re-adds it, which is why the default deletes.
    repo.commit("unrelated")
    assert "data/drop.txt" in repo.resolve_entries("main")


def test_mv_renames_the_worktree_copy(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    repo.move_staged("data/keep.txt", "data/kept.txt")
    assert not (tmp_path / "data" / "keep.txt").exists()
    assert (tmp_path / "data" / "kept.txt").read_text() == "keep"

    repo.commit("mv")
    repo.commit("unrelated")
    assert sorted(repo.resolve_entries("main")) == [
        "data/drop.txt",
        "data/kept.txt",
        "data/other.txt",
    ]


def test_rm_of_a_missing_path_is_an_error(tmp_path: Path) -> None:
    repo = _seed(tmp_path)
    with pytest.raises(FileNotFoundError):
        repo.rm(["data/nope.txt"])


def test_restored_files_are_immediately_clean(tmp_path: Path) -> None:
    """Restoring must refresh the stat cache, or every file re-hashes forever."""
    repo = _seed(tmp_path)
    (tmp_path / "data" / "keep.txt").unlink()

    assert repo.restore_files("main") == ["data/keep.txt"]
    status = repo.status(working_tree=True)
    assert status.working_tree_modified == []
    assert status.working_tree_added == []
    assert status.working_tree_removed == []


# ── Shared repository format ─────────────────────────────────────────────


def test_shared_format_is_written_on_init(tmp_path: Path) -> None:
    init_repository(tmp_path, default_branch="develop")
    payload = (tmp_path / ".reflake" / "repo.json").read_bytes()
    assert RepoFormat.parse(payload).default_branch == "develop"


def test_shared_format_wins_over_client_config(tmp_path: Path) -> None:
    """A client cannot silently change the ingest policy of a repository."""
    init_repository(tmp_path)
    (tmp_path / ".reflake" / "repo.json").write_bytes(
        RepoFormat(identity="pointer").to_json()
    )

    from reflake.core.config import LocalConfig

    LocalConfig(dataset_root=str(tmp_path), identity="content").save(tmp_path)
    repo = open_repository(tmp_path)
    assert repo.identity_mode == "pointer"


def test_default_branch_from_init_is_the_branch_that_gets_commits(
    tmp_path: Path,
) -> None:
    (tmp_path / "a.txt").write_text("a")
    init_repository(tmp_path, default_branch="develop")
    repo = open_repository(tmp_path)
    assert repo.current_branch() == "develop"
    repo.commit("first")
    assert [branch["branch"] for branch in repo.branches()] == ["develop"]


# ── Sharded directories ──────────────────────────────────────────────────


def test_node_size_is_small_enough_for_cheap_lookups() -> None:
    assert MAX_TREE_ENTRIES <= 4096


def test_prefix_listing_reads_only_the_touched_shard(tmp_path: Path) -> None:
    """A prefix query must not fetch every shard of a wide directory."""
    flat = tmp_path / "flat"
    flat.mkdir()
    count = MAX_TREE_ENTRIES * 3
    for index in range(count):
        (flat / f"f{index:06d}.txt").write_text("x")
    init_repository(tmp_path)
    repo = open_repository(tmp_path)
    commit = repo.commit("wide")
    tree = str(commit.stats.tree)

    reads: list[str] = []
    real = repo.store.read_tree_bytes

    def counting(tree_hash: str) -> bytes | None:
        reads.append(tree_hash)
        return real(tree_hash)

    repo.store.tree_cache._entries.clear()
    repo.store._walker._read_tree = counting  # type: ignore[attr-defined]

    target = f"flat/f{(count - 1):06d}.txt"
    found = repo.store.lookup_entry(tree, target)
    assert found is not None and found.path == target
    shard_reads = len(reads)
    assert shard_reads <= 3, f"lookup touched {shard_reads} nodes"

    reads.clear()
    repo.store.tree_cache._entries.clear()
    matches = list(repo.store.iter_entries_for_prefix(tree, target))
    assert all(entry.path == target for entry in matches)
    assert len(reads) <= 3, f"prefix listing touched {len(reads)} nodes"


# ── Sync ergonomics ──────────────────────────────────────────────────────


def test_clone_is_metadata_only_by_default(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """A clone makes the dataset readable without copying a single blob."""
    from reflake.cli import run_cli

    client = fake_s3_installer({})
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.txt").write_text("payload")
    remote = "s3://demo-bucket/datasets/demo"
    monkeypatch.chdir(source)
    assert run_cli(["--repo", remote, "init"]) == 0
    assert run_cli(["--repo", remote, "commit", "-m", "seed"]) == 0
    capsys.readouterr()

    target = tmp_path / "clone"
    assert run_cli(["clone", remote, str(target)]) == 0
    capsys.readouterr()

    # The clone is a *virtual* worktree: config points at the bucket and no
    # object was copied locally.
    blob_keys = [key for key in client._objects if "/blobs/" in key]
    assert blob_keys
    assert not (target / ".reflake" / "blobs").exists()

    assert run_cli(["--repo", str(target), "cat", "data.txt"]) == 0
    assert capsys.readouterr().out == "payload"


def test_pull_metadata_only_skips_blobs(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    from reflake.cli import run_cli

    client = fake_s3_installer({})
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.txt").write_text("payload")
    remote = "s3://demo-bucket/datasets/demo2"
    monkeypatch.chdir(source)
    assert run_cli(["--repo", remote, "init"]) == 0
    assert run_cli(["--repo", remote, "commit", "-m", "seed"]) == 0
    capsys.readouterr()

    target = tmp_path / "target"
    target.mkdir()
    monkeypatch.chdir(target)
    assert run_cli(["--repo", str(target), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(target), "pull", remote, "--metadata-only"]) == 0
    output = capsys.readouterr().out
    assert "0 blob(s)" in output
    assert not any((target / ".reflake" / "blobs").iterdir())
    assert client._objects  # the remote still has them
