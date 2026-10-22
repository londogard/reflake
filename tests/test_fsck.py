"""`reflake fsck`: the audit that tells you *which* object is broken."""

from __future__ import annotations

from pathlib import Path

from reflake import init_repository, open_repository
from reflake.cli import run_cli
from reflake.core import create_repository


def _repo(tmp_path: Path):
    (tmp_path / "d").mkdir()
    (tmp_path / "d" / "a.txt").write_text("alpha")
    (tmp_path / "d" / "b.txt").write_text("beta")
    init_repository(tmp_path)
    repo = open_repository(tmp_path)
    repo.commit("seed")
    return repo


def _blob_path(repo, blob_hash: str) -> Path:
    return repo.store.blob_path(blob_hash)


def test_clean_repository_passes(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    result = repo.fsck(verify_blobs=True)
    assert result.ok
    assert result.commits == 1
    assert result.leaves == 2
    assert result.blobs == 2
    assert result.verified_blobs == 2
    assert result.pointer_entries == 0


def test_empty_repository_passes(tmp_path: Path) -> None:
    create_repository(tmp_path)
    result = open_repository(tmp_path).fsck()
    assert result.ok
    assert result.refs == 0
    assert result.commits == 0


def test_missing_blob_is_reported(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    entry = repo.resolve_entry("main", "d/a.txt")
    assert entry is not None and entry.blob_hash is not None
    _blob_path(repo, entry.blob_hash).unlink()

    result = repo.fsck()
    assert not result.ok
    assert result.missing_objects == [f"blob:{entry.blob_hash}"]


def test_cli_exit_code_reflects_problems(tmp_path: Path, capsys) -> None:
    repo = _repo(tmp_path)
    entry = repo.resolve_entry("main", "d/a.txt")
    assert entry is not None and entry.blob_hash is not None

    assert run_cli(["--repo", str(tmp_path), "--json", "fsck"]) == 0
    capsys.readouterr()

    _blob_path(repo, entry.blob_hash).unlink()
    assert run_cli(["--repo", str(tmp_path), "fsck"]) == 1
    captured = capsys.readouterr()
    assert "Missing objects" in captured.err
    assert entry.blob_hash in captured.err


def test_corrupt_tree_object_is_reported(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    commit = repo.read_commit(repo.head_commit())
    tree_path = repo.store.tree_path(commit.tree)
    tree_path.write_bytes(b'{"not": "a tree"}\n')

    result = repo.fsck()
    assert not result.ok
    assert f"tree:{commit.tree}" in result.corrupt_objects


def test_blob_bytes_are_verified_only_on_request(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    entry = repo.resolve_entry("main", "d/a.txt")
    assert entry is not None and entry.blob_hash is not None
    # Same name, different bytes: metadata still looks perfect.
    _blob_path(repo, entry.blob_hash).write_bytes(b"tampered")

    assert repo.fsck().ok
    verified = repo.fsck(verify_blobs=True)
    assert not verified.ok
    assert verified.corrupt_blobs == [entry.blob_hash]


def test_pointer_entries_are_counted_not_flagged(tmp_path: Path) -> None:
    (tmp_path / "source.txt").write_text("payload")
    init_repository(tmp_path)
    repo = open_repository(tmp_path)
    repo.add(["source.txt"], identity_mode="pointer")
    repo.commit("pointer", staged_only=True)

    result = repo.fsck()
    assert result.ok, "pointer entries are unverifiable by design, not broken"
    assert result.pointer_entries == 1
    assert result.blobs == 0
