"""Regression tests for the 0.6 hardening pass (P0/P1 review findings)."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from reflake.core import (
    BlobIntegrityError,
    CorruptCommitError,
    NotARepositoryError,
    WorktreeStaleError,
    create_repository,
    init_repository,
    open_repository,
    pull,
    push,
)
from reflake.core.config import LocalConfig
from reflake.core.objects import S3ObjectStore
from reflake.core.query import parse_where_clause
from reflake.core.repository_sync import fetch
from reflake.core.vfs import ReflakeFileSystem


def test_stat_cache_persists_between_repository_instances(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("a")
    repo = create_repository(str(tmp_path))
    repo.commit("base")
    repo.close()

    assert (tmp_path / ".reflake/cache/stat-cache.json").exists()
    reopened = open_repository(str(tmp_path))
    assert len(reopened.stat_cache) == 1


def test_gc_keeps_staged_blobs_and_commit_repairs_a_pruned_one(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("staged")
    repo = create_repository(str(tmp_path))
    repo.add(["a.txt"])
    staged = repo.staging.load("main")["a.txt"]
    assert staged.blob_hash
    assert repo.store.object_exists("blob", staged.blob_hash)

    repo.gc(dry_run=False, grace_seconds=0)
    assert repo.store.object_exists("blob", staged.blob_hash)

    repo.commit("staged", staged_only=True)
    assert repo.fsck().ok


def test_gc_does_not_prune_reflog_only_commits(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("a")
    repo = create_repository(str(tmp_path))
    first = repo.commit("first")
    (tmp_path / "b.txt").write_text("b")
    second = repo.commit("second")
    repo.reset(first)

    repo.gc(dry_run=False, grace_seconds=0)
    assert repo.store.object_exists("commit", second)


def test_partial_push_is_repaired_before_the_ref_moves(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    root = tmp_path / "repo"
    root.mkdir()
    (root / "a.txt").write_text("payload")
    repo = create_repository(str(root))
    repo.commit("c1")
    push(repo, "s3://demo-bucket/repo")

    # Simulate a crash after commits/trees were written but before blobs and
    # the ref CAS. A fresh client has no cached snapshot.
    for key in list(client._objects):
        if "/blobs/" in key or key.endswith("/refs/heads/main"):
            del client._objects[key]
    shutil.rmtree(root / ".reflake/clients", ignore_errors=True)

    retry_repo = open_repository(str(root))
    result = push(retry_repo, "s3://demo-bucket/repo")
    assert result.updated is True
    remote = open_repository(
        "s3://demo-bucket/repo",
        worktree=tmp_path / "remote",
        client_root=tmp_path / "cs",
    )
    assert remote.fsck().ok


def test_metadata_only_pull_hydrates_on_the_next_pull(
    tmp_path: Path, fake_s3_installer
) -> None:
    fake_s3_installer({})
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("payload")
    repo_a = create_repository(str(source))
    repo_a.commit("seed")
    push(repo_a, "s3://demo-bucket/demo")

    target = tmp_path / "target"
    target.mkdir()
    repo_b = create_repository(str(target))
    meta = pull(repo_b, "s3://demo-bucket/demo", metadata_only=True)
    assert meta.pulled_blobs == 0
    assert repo_b.client_state.is_partial("main")
    assert not repo_b.store.object_exists(
        "blob", repo_b.resolve_entry("main", "a.txt").hash
    )

    hydrated = pull(repo_b, "s3://demo-bucket/demo")
    assert hydrated.updated is False
    assert hydrated.pulled_blobs == 1
    assert repo_b.cat("main", "a.txt") == b"payload"
    assert not repo_b.client_state.is_partial("main")


def test_fetch_repairs_missing_blobs(tmp_path: Path, fake_s3_installer) -> None:
    fake_s3_installer({})
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("payload")
    repo_a = create_repository(str(source))
    repo_a.commit("seed")
    push(repo_a, "s3://demo-bucket/demo")

    target = tmp_path / "target"
    target.mkdir()
    repo_b = create_repository(str(target))
    pull(repo_b, "s3://demo-bucket/demo", metadata_only=True)
    result = fetch(repo_b, "s3://demo-bucket/demo")
    assert result.fetched_blobs == 1
    assert repo_b.cat("main", "a.txt") == b"payload"


def test_tampered_tree_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "b.txt").write_text("beta")
    repo = create_repository(str(tmp_path))
    commit = repo.commit("base")
    tree = repo.read_commit(commit).tree
    repo.store.tree_path(tree).write_bytes(b'["b","a.txt","' + b"0" * 64 + b'",1]\n')

    with pytest.raises(BlobIntegrityError):
        list(open_repository(str(tmp_path)).store.iter_all_entries(tree))


def test_tampered_commit_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    repo = create_repository(str(tmp_path))
    commit = repo.commit("base")
    payload = json.loads(repo.store.commit_path(commit).read_text())
    payload["message"] = "tampered"
    repo.store.commit_path(commit).write_text(json.dumps(payload))

    with pytest.raises(CorruptCommitError):
        open_repository(str(tmp_path)).read_commit(commit)


def test_pointer_identity_is_stable_across_move(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    LocalConfig(dataset_root=str(tmp_path), identity="pointer").save(tmp_path)
    repo = create_repository(str(tmp_path))
    repo.commit("base")
    before = repo.resolve_entry("main", "a.txt")
    assert before is not None
    repo.move("a.txt", "moved.txt", "move")
    after = repo.resolve_entry("main", "moved.txt")
    assert after is not None
    assert after.hash == before.hash
    assert repo.verify(drift_check=True).drifted_paths == []


def test_default_branch_cannot_traverse(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    with pytest.raises(ValueError, match="invalid path segment"):
        init_repository(root, default_branch="../../pwned")


def test_repository_is_relocatable(tmp_path: Path) -> None:
    original = tmp_path / "original"
    original.mkdir()
    (original / "a.txt").write_text("a")
    repo = create_repository(str(original))
    repo.commit("base")
    moved = tmp_path / "moved"
    shutil.move(str(original), str(moved))
    assert open_repository(str(moved)).head_commit() is not None


def test_null_comparison_is_rejected() -> None:
    with pytest.raises(ValueError, match="IS NULL"):
        parse_where_clause("col = null")


def test_status_does_not_report_ignored_committed_files_as_removed(
    tmp_path: Path,
) -> None:
    (tmp_path / "a.tmp").write_text("a")
    repo = create_repository(str(tmp_path))
    repo.commit("base")
    (tmp_path / ".reflakeignore").write_text("*.tmp\n")
    status = open_repository(str(tmp_path)).status(working_tree=True)
    assert status.working_tree_removed == []
    repo.commit("ignore added")
    assert repo.resolve_entry("main", "a.tmp") is not None


def test_vfs_directories_glob_and_hierarchical_refs(tmp_path: Path) -> None:
    (tmp_path / "d").mkdir()
    (tmp_path / "d" / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")
    repo = create_repository(str(tmp_path))
    repo.commit("base")
    repo.branch("feature/x")
    repo.set_current_branch("feature/x")
    (tmp_path / "x.txt").write_text("x")
    repo.commit("feature")

    fs = ReflakeFileSystem(dataset_roots={"ds": tmp_path})
    root = fs.ls("reflake://ds@main", detail=False)
    assert "reflake://ds@main/d" in root
    assert fs.isdir("reflake://ds@main/d")
    assert not fs.isdir("reflake://ds@main/b.txt")
    assert fs.glob("reflake://ds@main/*.txt") == ["reflake://ds@main/b.txt"]
    with fs.open("reflake://ds@feature/x/x.txt", "rb") as handle:
        assert handle.read() == b"x"


def test_merge_then_full_commit_refuses_to_revert(tmp_path: Path) -> None:
    (tmp_path / "f.txt").write_text("base")
    repo = create_repository(str(tmp_path))
    repo.commit("base")
    repo.branch("feature")
    repo.set_current_branch("feature")
    (tmp_path / "f.txt").write_text("feature")
    repo.commit("feature change")
    repo.set_current_branch("main")
    repo.merge("feature", "main")
    # The worktree still says "base" (e.g. it was never materialized); a full
    # commit would silently revert the merge.
    (tmp_path / "f.txt").write_text("base")
    with pytest.raises(WorktreeStaleError):
        repo.commit("revert?")


def test_remote_must_exist_is_opt_in(tmp_path: Path, fake_s3_installer) -> None:
    client = fake_s3_installer({})
    # Lazy default: a handle can be opened for bootstrap/push.
    open_repository("s3://demo-bucket/missing", s3_client=client)
    with pytest.raises(NotARepositoryError):
        open_repository(
            "s3://demo-bucket/missing", s3_client=client, must_exist=True
        )


def test_s3_delete_reports_per_key_errors() -> None:
    class _ErrorClient:
        def delete_objects(self, **_kwargs):
            return {"Errors": [{"Key": "k", "Message": "denied"}]}

    store = S3ObjectStore("demo-bucket", client=_ErrorClient())
    with pytest.raises(Exception, match="failed"):
        store.delete_objects("tree", ["a" * 64])


def test_key_value_core_supports_a_new_backend(tmp_path: Path) -> None:
    """The object semantics live above the six KV primitives."""
    from reflake.core.objects.kv import KeyValueObjectStore

    class MemoryKV(KeyValueObjectStore):
        def __init__(self) -> None:
            self.data: dict[str, tuple[bytes, int]] = {}
            self._init_tree_walker()

        def _kv_path(self, key: str) -> Path | None:
            return None

        def _kv_uri(self, key: str) -> str | None:
            return None

        def _kv_get(self, key: str) -> bytes | None:
            item = self.data.get(key)
            return item[0] if item else None

        def _kv_put(self, key, payload, *, if_absent=False):
            if if_absent and key in self.data:
                return False
            self.data[key] = (payload, 1)
            return True

        def _kv_put_file(self, key, source, *, if_absent=False):
            return self._kv_put(key, source.read_bytes(), if_absent=if_absent)

        def _kv_delete(self, keys):
            count = 0
            for key in keys:
                count += 1 if self.data.pop(key, None) else 0
            return count

        def _kv_list(self, prefix):
            for key, (_, mtime) in sorted(self.data.items()):
                if key.startswith(prefix):
                    yield key, mtime

        def _kv_exists(self, key):
            return key in self.data

        def _kv_read_range(self, key, offset, length):
            return self.data[key][0][offset : offset + length]

    store = MemoryKV()
    tree_payload = b'["b","a.txt","' + b"a" * 64 + b'",1]\n'
    tree_hash = __import__("blake3").blake3(tree_payload).hexdigest()
    store.write_tree_bytes(tree_hash, tree_payload)
    assert store.read_tree_bytes(tree_hash) == tree_payload
    assert [entry.path for entry in store.iter_all_entries(tree_hash)] == ["a.txt"]
    store.write_branch_ref("main", "c" * 64)
    assert store.read_branch_ref("main").commit_id == "c" * 64
    assert list(store.iter_branches()) == ["main"]


def test_stat_cache_persists_and_skips_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    repo = create_repository(str(tmp_path))
    repo.commit("base")
    repo.close()

    reopened = open_repository(str(tmp_path))
    from reflake.core.objects.local import LocalObjectStore

    reads: list[str] = []
    real = LocalObjectStore.materialize_blob_file

    def counting(self, source_path, *, known_digest=None):
        if known_digest is None:
            reads.append(str(source_path))
        return real(self, source_path, known_digest=known_digest)

    monkeypatch.setattr(LocalObjectStore, "materialize_blob_file", counting)
    reopened.commit("no changes")
    assert reads == [], "an unchanged file was re-read despite the persisted cache"


def test_reset_then_full_commit_refuses_to_resurrect(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("a")
    repo = create_repository(str(tmp_path))
    first = repo.commit("first")
    (tmp_path / "b.txt").write_text("b")
    repo.commit("second")
    repo.reset(first)
    # b.txt is no longer in the reset tree, but the stale worktree still has
    # it; a full commit would resurrect it.
    with pytest.raises(WorktreeStaleError):
        repo.commit("resurrect?")
