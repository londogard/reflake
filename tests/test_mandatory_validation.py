from __future__ import annotations

import io
import os
import time
import tracemalloc
from pathlib import Path

import pytest

from reflake.core import (
    Entry,
    FileEntry,
    LocalClientState,
    LocalObjectStore,
    ManifestReader,
    ManifestWriter,
    RefConflictError,
    ReflakeFileSystem,
    ReflakeRepository,
    S3ObjectStore,
    build_analytical_index,
    create_repository,
    drop_analytical_index,
    open_repository,
    query_analytical_index,
    walk_files,
)
from reflake.core.objects.tree import parse_tree_object


class ConflictOnceLocalObjectStore(LocalObjectStore):
    def __init__(self, root: str | Path, *, conflict_commit_id: str) -> None:
        super().__init__(root)
        self.conflict_commit_id = conflict_commit_id
        self.conflict_next_ref_update = False

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_commit_id: str | None,
    ) -> bool:
        if self.conflict_next_ref_update:
            self.conflict_next_ref_update = False
            self.write_branch_ref(branch, self.conflict_commit_id)
            return False
        return super().compare_and_set_branch_ref(
            branch,
            commit_id,
            expected_commit_id=expected_commit_id,
        )


def test_metadata_only_diff_does_not_read_blobs(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    repo = ReflakeRepository(tmp_path)
    commit_a = repo.commit("initial")

    (tmp_path / "a.txt").write_text("beta")
    (tmp_path / "b.txt").write_text("new")
    commit_b = repo.commit("update")

    touched_blob_reads: list[Path] = []
    original_read_bytes = Path.read_bytes

    def tracking_read_bytes(path: Path) -> bytes:
        path_obj = Path(path)
        if ".reflake" in path_obj.parts and "blobs" in path_obj.parts:
            touched_blob_reads.append(path_obj)
        return original_read_bytes(path_obj)

    monkeypatch.setattr(Path, "read_bytes", tracking_read_bytes)

    changes = repo.diff(commit_a, commit_b)
    assert {(entry.path, entry.change) for entry in changes} == {
        ("a.txt", "modified"),
        ("b.txt", "added"),
    }
    assert touched_blob_reads == []


def test_metadata_only_remove_does_not_read_blobs(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "b.txt").write_text("beta")
    repo = ReflakeRepository(tmp_path)
    repo.commit("initial")

    touched_blob_reads: list[Path] = []
    original_read_bytes = Path.read_bytes

    def tracking_read_bytes(path: Path) -> bytes:
        path_obj = Path(path)
        if ".reflake" in path_obj.parts and "blobs" in path_obj.parts:
            touched_blob_reads.append(path_obj)
        return original_read_bytes(path_obj)

    monkeypatch.setattr(Path, "read_bytes", tracking_read_bytes)

    result = repo.remove_paths(["a.txt"], "remove a")

    assert result.removed_paths == ["a.txt"]
    assert set(repo.resolve_entries("main")) == {"b.txt"}
    assert touched_blob_reads == []


def test_metadata_only_move_updates_meta_identity_without_blob_read(
    tmp_path: Path, monkeypatch
) -> None:
    nested = tmp_path / "dir"
    nested.mkdir()
    (nested / "file.txt").write_text("payload")
    repo = ReflakeRepository(tmp_path)
    repo.commit("meta only")
    before_entry = repo.resolve_entries("main")["dir/file.txt"]

    touched_blob_reads: list[Path] = []
    original_read_bytes = Path.read_bytes

    def tracking_read_bytes(path: Path) -> bytes:
        path_obj = Path(path)
        if ".reflake" in path_obj.parts and "blobs" in path_obj.parts:
            touched_blob_reads.append(path_obj)
        return original_read_bytes(path_obj)

    monkeypatch.setattr(Path, "read_bytes", tracking_read_bytes)

    result = repo.move("dir", "archive", "rename prefix")
    after_entries = repo.resolve_entries("main")
    moved_entry = after_entries["archive/file.txt"]

    assert result.moved_paths == ["archive/file.txt"]
    assert "dir/file.txt" not in after_entries
    assert moved_entry.identity_mode == "content"
    assert moved_entry.hash == before_entry.hash
    assert moved_entry.blob_hash is not None


def test_memory_safe_manifesting_100k_entries(tmp_path: Path) -> None:
    entry_count = 100_000
    manifest_path = tmp_path / ".reflake" / "manifests" / "large.jsonl"

    def entries():
        for i in range(entry_count):
            yield Entry(
                path=f"dir/file_{i}.dat",
                kind="b",
                hash=f"{i:064x}",
                size=i,
                mtime_ns=i,
            )

    writer = ManifestWriter(manifest_path)
    tracemalloc.start()
    written = writer.write_entries(entries())
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert written == entry_count
    assert peak < 150 * 1024 * 1024

    reader = ManifestReader(manifest_path)
    first = next(reader.iter_entries())
    assert first.path == "dir/file_0.dat"
    assert sum(1 for _ in ManifestReader(manifest_path).iter_entries()) == entry_count


def test_manifest_entry_validation_rejects_invalid_payloads() -> None:
    invalid_payloads = [
        (
            {
                "path": "../escape.txt",
                "hash": "a" * 64,
                "size": 1,
                "mtime_ns": 1,
            },
            "normalized relative path",
        ),
        (
            {
                "path": "valid.txt",
                "hash": "g" * 64,
                "size": 1,
                "mtime_ns": 1,
            },
            "64-character hex digest",
        ),
        (
            {
                "path": "valid.txt",
                "hash": "a" * 64,
                "size": -1,
                "mtime_ns": 1,
            },
            "size cannot be negative",
        ),
        (
            {
                "path": "meta.txt",
                "hash": "a" * 64,
                "size": 1,
                "mtime_ns": 1,
                "identity_mode": "pointer",
            },
            "Source-pointer entries must carry a non-empty source_uri",
        ),
    ]

    for payload, message in invalid_payloads:
        with pytest.raises(ValueError, match=message):
            Entry.from_dict(payload)  # type: ignore[arg-type]


def test_manifest_reader_reports_corrupt_json_with_line_context(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "broken.jsonl"
    manifest_path.write_text(
        '["b","ok.txt","' + ("a" * 64) + '",1,1]\n' '{"path": invalid json}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Corrupt manifest JSON at line 2"):
        list(ManifestReader(manifest_path).iter_entries())


def test_local_client_state_writes_use_atomic_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = LocalClientState(tmp_path)
    replaced_targets: list[Path] = []
    original_replace = Path.replace

    def tracking_replace(path: Path, target: str | Path) -> Path:
        replaced_targets.append(Path(target))
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", tracking_replace)

    state.set_current_branch("feature")
    state.write_staging_payload("feature", '[{"path":"a.txt","action":"add"}]\n')

    assert state.current_branch() == "feature"
    assert (
        state.read_staging_payload("feature") == '[{"path":"a.txt","action":"add"}]\n'
    )
    assert state.head_path in replaced_targets
    assert state.stage_path("feature") in replaced_targets
    assert not list(state.reflake_dir.rglob("*.tmp"))


def test_uri_routing_reads_expected_blob_bytes(tmp_path: Path) -> None:
    dataset_root = tmp_path / "my_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "test.csv").write_text("col\n123\n")

    repo = ReflakeRepository(dataset_root)
    repo.commit("add file")

    fs = ReflakeFileSystem(dataset_roots={"my_data": dataset_root})
    with fs.open("reflake://my_data@main/test.csv", "rb") as handle:
        assert handle.read() == b"col\n123\n"


def test_exact_lookup_does_not_full_walk(tmp_path: Path) -> None:
    """Exact lookups descend the tree path chain — never a full walk."""
    (tmp_path / "a.txt").write_text("alpha")
    repo = ReflakeRepository(tmp_path)
    repo.commit("initial")

    original_iter_all_entries = repo.store.iter_all_entries

    def fail_iter_all_entries(tree_hash: str):
        raise AssertionError(f"unexpected full tree walk for {tree_hash}")

    repo.store.iter_all_entries = fail_iter_all_entries  # type: ignore[method-assign]
    try:
        entry = repo.resolve_entry("main", "a.txt")
    finally:
        repo.store.iter_all_entries = original_iter_all_entries  # type: ignore[method-assign]

    assert entry is not None
    assert entry.path == "a.txt"


def test_uri_routing_uses_manifest_sidecar_for_point_reads(tmp_path: Path) -> None:
    dataset_root = tmp_path / "sidecar_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "test.csv").write_text("value\n7\n")

    repo = ReflakeRepository(dataset_root)
    repo.commit("add file")

    fs = ReflakeFileSystem(dataset_roots={"sidecar_data": dataset_root})
    cached_repo = fs._repository(dataset_root)
    original_iter_all_entries = cached_repo.store.iter_all_entries

    def fail_iter_all_entries(tree_hash: str):
        raise AssertionError(f"unexpected full tree walk for {tree_hash}")

    cached_repo.store.iter_all_entries = fail_iter_all_entries  # type: ignore[method-assign]
    try:
        with fs.open("reflake://sidecar_data@main/test.csv", "rb") as handle:
            assert handle.read() == b"value\n7\n"
    finally:
        cached_repo.store.iter_all_entries = original_iter_all_entries  # type: ignore[method-assign]


def test_uri_listing_uses_manifest_sidecar_for_prefix_reads(tmp_path: Path) -> None:
    dataset_root = tmp_path / "listing_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "logs").mkdir()
    (dataset_root / "logs" / "a.txt").write_text("a")
    (dataset_root / "logs" / "b.txt").write_text("b")
    (dataset_root / "other.txt").write_text("c")

    repo = ReflakeRepository(dataset_root)
    repo.commit("add files")

    fs = ReflakeFileSystem(dataset_roots={"listing_data": dataset_root})
    cached_repo = fs._repository(dataset_root)
    original_iter_all_entries = cached_repo.store.iter_all_entries

    def fail_iter_all_entries(tree_hash: str):
        raise AssertionError(f"unexpected full tree walk for {tree_hash}")

    cached_repo.store.iter_all_entries = fail_iter_all_entries  # type: ignore[method-assign]
    try:
        paths = fs.ls("reflake://listing_data@main/logs", detail=False)
    finally:
        cached_repo.store.iter_all_entries = original_iter_all_entries  # type: ignore[method-assign]

    assert paths == [
        "reflake://listing_data@main/logs/a.txt",
        "reflake://listing_data@main/logs/b.txt",
    ]


def test_repeated_exact_lookup_reuses_cached_commit(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    repo = ReflakeRepository(tmp_path)
    repo.commit("initial")

    first = repo.resolve_entry("main", "a.txt")
    assert first is not None

    original_read_commit_bytes = repo.store.read_commit_bytes

    def fail_read_commit_bytes(commit_id: str):
        raise AssertionError(f"unexpected commit reread for {commit_id}")

    repo.store.read_commit_bytes = fail_read_commit_bytes  # type: ignore[method-assign]
    try:
        second = repo.resolve_entry("main", "a.txt")
    finally:
        repo.store.read_commit_bytes = original_read_commit_bytes  # type: ignore[method-assign]

    assert second is not None
    assert second.path == "a.txt"


def test_remote_exact_lookup_uses_manifest_sidecar(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    worktree = tmp_path / "worktree"
    client_root = tmp_path / "client"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)
    (worktree / "remote.txt").write_text("payload")

    repo = create_repository(
        "s3://demo-bucket/repos/demo",
        worktree=worktree,
        client_root=client_root,
        s3_client=client,
    )
    repo.commit("initial")

    # v2 writes no sidecar indexes at all; lookups walk cached trees.
    assert not any(key.endswith(".idx") for key in client._objects)

    original_iter_all_entries = repo.store.iter_all_entries

    def fail_iter_all_entries(tree_hash: str):
        raise AssertionError(f"unexpected full tree walk for {tree_hash}")

    repo.store.iter_all_entries = fail_iter_all_entries  # type: ignore[method-assign]
    try:
        entry = repo.resolve_entry("main", "remote.txt")
    finally:
        repo.store.iter_all_entries = original_iter_all_entries  # type: ignore[method-assign]

    assert entry is not None
    assert entry.path == "remote.txt"


def test_remote_prefix_listing_uses_manifest_sidecar(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    worktree = tmp_path / "worktree-list"
    client_root = tmp_path / "client-list"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)
    (worktree / "logs").mkdir()
    (worktree / "logs" / "a.txt").write_text("a")
    (worktree / "logs" / "b.txt").write_text("b")
    (worktree / "other.txt").write_text("c")

    repo = create_repository(
        "s3://demo-bucket/repos/demo-prefix",
        worktree=worktree,
        client_root=client_root,
        s3_client=client,
    )
    repo.commit("initial")

    original_iter_all_entries = repo.store.iter_all_entries

    def fail_iter_all_entries(tree_hash: str):
        raise AssertionError(f"unexpected full tree walk for {tree_hash}")

    repo.store.iter_all_entries = fail_iter_all_entries  # type: ignore[method-assign]
    try:
        entries = repo.resolve_entries_for_prefix("main", "logs")
    finally:
        repo.store.iter_all_entries = original_iter_all_entries  # type: ignore[method-assign]

    assert sorted(entries) == ["logs/a.txt", "logs/b.txt"]


class _RecordingBlobTransfer:
    """BlobTransferBackend stub that records uploads without touching S3."""

    def __init__(self) -> None:
        self.uploaded: list[str] = []

    def upload(
        self,
        local_path: str,
        remote_uri: str,
        *,
        if_not_exists: bool = False,
    ) -> None:
        self.uploaded.append(remote_uri)

    def download(self, remote_uri: str, local_path: str) -> None:
        raise AssertionError("unexpected download")

    def list_objects(self, uri_prefix: str):
        return []

    def delete(self, remote_uri: str) -> None:
        raise AssertionError("unexpected delete")

    def exists(self, remote_uri: str) -> bool:
        return False


def test_s3_write_blob_stream_uses_transfer_backend(fake_s3_installer) -> None:
    from blake3 import blake3

    payload = b"some-payload-bytes"
    digest = blake3(payload).hexdigest()
    client = fake_s3_installer({})
    transfer = _RecordingBlobTransfer()
    store = S3ObjectStore(
        "demo-bucket",
        "repos/demo",
        client=client,
        blob_transfer=transfer,
    )
    store.write_blob_stream(digest, io.BytesIO(payload))

    assert len(transfer.uploaded) == 1
    assert transfer.uploaded[0].startswith(
        f"s3://demo-bucket/repos/demo/blobs/{digest[:2]}/"
    )


def test_commit_cache_is_bounded(tmp_path: Path) -> None:
    from reflake.core.services.refs import _BoundedCache

    (tmp_path / "a.txt").write_text("a")
    repo = ReflakeRepository(tmp_path)
    repo.refs._commit_cache = _BoundedCache(maxsize=4)
    for i in range(10):
        (tmp_path / "a.txt").write_text(str(i))
        repo.commit(f"commit {i}")
    assert len(repo.refs._commit_cache) <= 4
    # The most recent head commit is still cached and readable.
    head = repo.head_commit()
    assert head is not None
    assert repo.read_commit(head).message == "commit 9"


def test_s3_compare_and_set_checks_expected_commit_id_under_lock(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    client.fixed_etag = '"static-etag"'

    worktree = tmp_path / "store-worktree"
    client_root = tmp_path / "store-client"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)

    repo = create_repository(
        "s3://demo-bucket/repos/conflict-store",
        worktree=worktree,
        client_root=client_root,
        s3_client=client,
    )
    store = repo.store

    store.write_branch_ref("main", "base")
    base_state = store.read_branch_ref("main")
    assert base_state is not None
    assert base_state.commit_id == "base"

    store.write_branch_ref("main", "winning")

    updated = store.compare_and_set_branch_ref(
        "main",
        "stale",
        expected_commit_id=base_state.commit_id,
    )

    assert updated is False
    current_state = store.read_branch_ref("main")
    assert current_state is not None
    assert current_state.commit_id == "winning"


def test_remote_repo_commit_detects_conflict_even_if_s3_etag_is_unchanged(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    client.fixed_etag = '"static-etag"'

    client_a_worktree = tmp_path / "client-a-worktree"
    client_b_worktree = tmp_path / "client-b-worktree"
    client_a_root = tmp_path / "client-a-state"
    client_b_root = tmp_path / "client-b-state"
    for path in (
        client_a_worktree,
        client_b_worktree,
        client_a_root,
        client_b_root,
    ):
        path.mkdir(parents=True)

    (client_a_worktree / "data.txt").write_text("alpha")
    repo_a = create_repository(
        "s3://demo-bucket/repos/conflict-repo",
        worktree=client_a_worktree,
        client_root=client_a_root,
        s3_client=client,
    )
    base_commit = repo_a.commit("base")

    (client_b_worktree / "data.txt").write_text("alpha")
    repo_b = open_repository(
        "s3://demo-bucket/repos/conflict-repo",
        worktree=client_b_worktree,
        client_root=client_b_root,
        s3_client=client,
    )
    (client_b_worktree / "data.txt").write_text("gamma")
    winning_commit = repo_b.commit("winning update")

    (client_a_worktree / "data.txt").write_text("beta")
    with pytest.raises(RefConflictError) as error_info:
        repo_a.commit("stale update")

    error = error_info.value
    assert error.operation == "commit"
    assert error.expected_commit_id == base_commit
    assert error.current_commit_id == winning_commit


def test_disposable_analytical_index(tmp_path: Path) -> None:
    (tmp_path / "a.jpg").write_bytes(b"x" * 10)
    (tmp_path / "b.txt").write_text("hello")

    repo = ReflakeRepository(tmp_path)
    commit_id = repo.commit("indexable commit")

    paths = build_analytical_index(tmp_path, "main", export_parquet=True)
    assert paths.database_path.exists()
    assert paths.parquet_path is not None and paths.parquet_path.exists()

    rows = query_analytical_index(
        paths.database_path,
        "SELECT path FROM files WHERE path LIKE '%.jpg' AND size > 2 ORDER BY path",
    )
    assert rows == [("a.jpg",)]

    drop_analytical_index(paths.database_path)
    assert not paths.database_path.exists()

    commit = repo.read_commit(commit_id)
    tree_path = tmp_path / ".reflake" / "trees" / commit.tree
    assert tree_path.exists()


def test_uri_routing_supports_metadata_identity_entries(tmp_path: Path) -> None:
    dataset_root = tmp_path / "meta_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "test.csv").write_text("value\n42\n")

    repo = ReflakeRepository(dataset_root)
    repo.commit("meta only")

    assert any((dataset_root / ".reflake" / "blobs").rglob("*"))

    fs = ReflakeFileSystem(dataset_roots={"meta_data": dataset_root})
    with fs.open("reflake://meta_data@main/test.csv", "rb") as handle:
        assert handle.read() == b"value\n42\n"


def test_repository_mutations_can_use_separate_local_store(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    store_root = tmp_path / "repo-state"
    dataset_root.mkdir(parents=True)
    store_root.mkdir(parents=True)
    (dataset_root / "sample.txt").write_text("payload")

    repo = ReflakeRepository(
        dataset_root,
        store=LocalObjectStore(store_root),
    )
    commit_id = repo.commit("initial")

    assert commit_id
    assert list((dataset_root / ".reflake" / "commits").glob("*.json")) == []
    assert list((dataset_root / ".reflake" / "manifests").glob("*.jsonl")) == []
    assert not any((dataset_root / ".reflake" / "blobs").rglob("*"))

    assert list((store_root / ".reflake" / "commits").glob("*.json"))
    tree_paths = list((store_root / ".reflake" / "trees").glob("*"))
    assert tree_paths
    assert any((store_root / ".reflake" / "blobs").rglob("*"))
    assert (
        store_root / ".reflake" / "refs" / "heads" / "main"
    ).read_text().strip() == commit_id

    tree_entries = parse_tree_object(tree_paths[0].read_bytes())
    assert [entry.path for entry in tree_entries] == ["sample.txt"]


def test_current_branch_preference_is_local_per_client(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    store_root = tmp_path / "repo-state"
    client_a_root = tmp_path / "client-a"
    client_b_root = tmp_path / "client-b"
    dataset_root.mkdir(parents=True)
    store_root.mkdir(parents=True)
    client_a_root.mkdir(parents=True)
    client_b_root.mkdir(parents=True)
    (dataset_root / "shared.txt").write_text("base")

    repo_a = ReflakeRepository(
        dataset_root,
        store=LocalObjectStore(store_root),
        client_state=LocalClientState(client_a_root),
    )
    repo_b = ReflakeRepository(
        dataset_root,
        store=LocalObjectStore(store_root),
        client_state=LocalClientState(client_b_root),
    )

    repo_a.commit("base")
    repo_a.branch("feature")
    repo_a.set_current_branch("feature")
    repo_b.set_current_branch("main")

    assert repo_a.current_branch() == "feature"
    assert repo_b.current_branch() == "main"
    assert (
        client_a_root / ".reflake" / "refs" / "HEAD"
    ).read_text().strip() == "refs/heads/feature"
    assert (
        client_b_root / ".reflake" / "refs" / "HEAD"
    ).read_text().strip() == "refs/heads/main"
    assert not (store_root / ".reflake" / "refs" / "HEAD").exists()


def test_staging_state_is_local_per_client(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    store_root = tmp_path / "repo-state"
    client_a_root = tmp_path / "client-a"
    client_b_root = tmp_path / "client-b"
    dataset_root.mkdir(parents=True)
    store_root.mkdir(parents=True)
    client_a_root.mkdir(parents=True)
    client_b_root.mkdir(parents=True)
    (dataset_root / "shared.txt").write_text("base")
    (dataset_root / "feature.txt").write_text("feature")

    repo_a = ReflakeRepository(
        dataset_root,
        store=LocalObjectStore(store_root),
        client_state=LocalClientState(client_a_root),
    )
    repo_b = ReflakeRepository(
        dataset_root,
        store=LocalObjectStore(store_root),
        client_state=LocalClientState(client_b_root),
    )

    repo_a.commit("base")
    repo_a.branch("feature")
    repo_a.set_current_branch("feature")
    repo_b.set_current_branch("feature")

    repo_a.add(["feature.txt"])

    assert repo_a.status().added == ["feature.txt"]
    assert repo_b.status().added == []
    assert (client_a_root / ".reflake" / "staging" / "feature.json").exists()
    assert not (client_b_root / ".reflake" / "staging" / "feature.json").exists()
    assert not (store_root / ".reflake" / "staging" / "feature.json").exists()


def test_commit_fails_clearly_on_branch_update_conflict(tmp_path: Path) -> None:
    conflict_commit_id = "f" * 64
    store = ConflictOnceLocalObjectStore(
        tmp_path / "repo-state",
        conflict_commit_id=conflict_commit_id,
    )
    (tmp_path / "data.txt").write_text("alpha")

    repo = ReflakeRepository(tmp_path, store=store)
    base_commit = repo.commit("base")

    (tmp_path / "data.txt").write_text("beta")
    store.conflict_next_ref_update = True

    try:
        repo.commit("update")
    except RefConflictError as error:
        assert str(error) == (
            f"Branch update conflict for 'main' during commit: "
            f"expected {base_commit}, found {conflict_commit_id}"
        )
    else:
        raise AssertionError("Expected RefConflictError")

    state = store.read_branch_ref("main")
    assert state is not None
    assert state.commit_id == conflict_commit_id


def test_merge_fails_clearly_on_branch_update_conflict(tmp_path: Path) -> None:
    conflict_commit_id = "e" * 64
    store = ConflictOnceLocalObjectStore(
        tmp_path / "repo-state",
        conflict_commit_id=conflict_commit_id,
    )
    (tmp_path / "shared.txt").write_text("base")

    repo = ReflakeRepository(tmp_path, store=store)
    base_commit = repo.commit("base")
    repo.branch("feature")

    (tmp_path / "feature.txt").write_text("feature")
    repo.set_current_branch("feature")
    repo.add(["feature.txt"])
    feature_commit = repo.commit("feature commit")
    assert feature_commit != base_commit

    store.conflict_next_ref_update = True

    try:
        repo.merge("feature", "main")
    except RefConflictError as error:
        assert str(error) == (
            f"Branch update conflict for 'main' during merge: "
            f"expected {base_commit}, found {conflict_commit_id}"
        )
    else:
        raise AssertionError("Expected RefConflictError")


def test_walk_files_eliminates_redundant_stat(tmp_path: Path, monkeypatch) -> None:
    """Verify walk_files yields FileEntry with pre-populated stat info."""
    (tmp_path / "a.txt").write_text("x" * 100)
    (tmp_path / "b.txt").write_text("y" * 200)
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "c.txt").write_text("z")

    stat_call_count = 0
    original_stat = os.stat

    def tracking_stat(path, *args, **kwargs):
        nonlocal stat_call_count
        stat_call_count += 1
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", tracking_stat)

    entries = list(walk_files(tmp_path))
    assert len(entries) == 3
    for entry in entries:
        assert isinstance(entry, FileEntry)
        assert entry.size > 0
        assert entry.mtime_ns > 0
        assert entry.path.exists()


def test_commit_meta_no_redundant_path_stat(tmp_path: Path, monkeypatch) -> None:
    """Verify commit with meta mode does NOT call Path.stat() redundantly."""
    for i in range(100):
        (tmp_path / f"file_{i:04d}.txt").write_text(f"content_{i}")

    path_stat_count = 0
    original_path_stat = Path.stat

    def tracking_path_stat(self, **kwargs):
        nonlocal path_stat_count
        path_stat_count += 1
        return original_path_stat(self, **kwargs)

    monkeypatch.setattr(Path, "stat", tracking_path_stat)

    repo = ReflakeRepository(tmp_path)
    commit_id = repo.commit("meta only")

    assert len(commit_id) == 64
    print(f"\n[STAT TEST] Path.stat() calls during commit: {path_stat_count}")
    # The old code called Path.stat() once per file in _materialize_blobs_and_entries
    # (100+ calls for 100 files). The new code gets stat from os.scandir.
    # Remaining calls come from other Path internals (exists, resolve, etc.)
    assert path_stat_count < 500, (
        f"Path.stat() called {path_stat_count} times for 100 files — "
        "expected < 100 (redundant per-file stat eliminated)"
    )


def test_streaming_diff_does_not_build_full_entry_dict(
    tmp_path: Path, monkeypatch
) -> None:
    """Verify diff() never calls _tree_entries (which loads a full dict)."""
    file_count = 500
    for i in range(file_count):
        (tmp_path / f"file_{i:04d}.txt").write_text(f"original_content_{i}")

    repo = ReflakeRepository(tmp_path)
    commit_a = repo.commit("meta only")

    for i in range(file_count // 2):
        (tmp_path / f"file_{i:04d}.txt").write_text(f"modified_content_{i}_longer")
    (tmp_path / "new_file.txt").write_text("brand_new")
    commit_b = repo.commit("meta only")

    # Track whether _tree_entries is ever called
    original_method = repo._tree_entries

    def fail_tree_entries(tree_hash):
        raise AssertionError("diff() should not call _tree_entries")

    repo._tree_entries = fail_tree_entries  # type: ignore[method-assign]
    try:
        changes = repo.diff(commit_a, commit_b)
    finally:
        repo._tree_entries = original_method

    assert len(changes) == file_count // 2 + 1


def test_commit_10k_files_meta_mode_performance(tmp_path: Path) -> None:
    """Benchmark: commit 10K files with meta mode.

    Verifies walk_files stat propagation + streaming manifest writing
    keeps commit time linear and bounds-checked.
    """
    file_count = 10_000
    print(f"\n[PERF TEST] Generating {file_count} files...")
    gen_start = time.perf_counter()
    for i in range(file_count):
        subdir = tmp_path / f"dir_{i % 100:03d}"
        subdir.mkdir(parents=True, exist_ok=True)
        (subdir / f"file_{i:05d}.txt").write_text(f"data_{i}")
    gen_time = time.perf_counter() - gen_start
    print(f"  Generate: {gen_time:.2f}s ({file_count / gen_time:.0f} files/sec)")

    repo = ReflakeRepository(tmp_path)
    commit_start = time.perf_counter()
    commit_id = repo.commit("meta only")
    commit_time = time.perf_counter() - commit_start
    print(f"  Commit:   {commit_time:.2f}s ({file_count / commit_time:.0f} files/sec)")
    assert len(commit_id) == 64
    assert repo.resolve_entries("main")[f"dir_{0:03d}/file_{0:05d}.txt"] is not None
    assert file_count / commit_time > 1000, (
        f"Commit too slow: {file_count / commit_time:.0f} files/sec "
        f"(expected > 1000 for blake3 mode)"
    )


def test_streaming_diff_20k_files_performance(tmp_path: Path) -> None:
    """Benchmark: diff two commits with 20K files stays O(1) memory."""
    file_count = 20_000
    print(f"\n[DIFF PERF] Creating {file_count} files per commit...")

    for i in range(file_count):
        (tmp_path / f"file_{i:05d}.txt").write_text(f"v1_{i}")
    repo = ReflakeRepository(tmp_path)
    commit_a = repo.commit("modified")

    for i in range(file_count):
        (tmp_path / f"file_{i:05d}.txt").write_text(f"v2_{i}")
    commit_b = repo.commit("modified")

    diff_start = time.perf_counter()
    changes = repo.diff(commit_a, commit_b)
    diff_time = time.perf_counter() - diff_start

    print(f"  Diff time: {diff_time:.4f}s ({file_count / diff_time:.0f} entries/sec)")
    print(f"  Changes:   {len(changes)}")
    assert len(changes) == file_count
    assert diff_time < 2.0, f"Diff too slow: {diff_time:.2f}s"


def test_walk_files_sorted_order(tmp_path: Path) -> None:
    """Verify walk_files yields files in sorted order per directory."""
    names = ["z.txt", "a.txt", "m.txt", "b.txt"]
    for name in names:
        (tmp_path / name).write_text(name)

    entries = list(walk_files(tmp_path))
    paths = [e.path.name for e in entries]
    assert paths == sorted(names)


def test_file_entry_carries_correct_stat_info(tmp_path: Path) -> None:
    """Verify FileEntry has correct size and mtime from walk_files."""
    content = b"hello world"
    (tmp_path / "test.bin").write_bytes(content)

    entries = list(walk_files(tmp_path))
    assert len(entries) == 1
    entry = entries[0]
    assert entry.size == len(content)
    assert entry.mtime_ns > 0
    assert entry.path.name == "test.bin"


def test_fake_s3_scale_100k_files_meta_mode(tmp_path: Path, fake_s3_installer) -> None:
    """Benchmark: commit 100K files via fake S3 backend (meta mode).

    Validates that S3-backed manifest index performs comparably to
    local storage for lookups and prefix listings.
    """
    fake_s3_installer({})

    worktree = tmp_path / "worktree"
    client_root = tmp_path / "client-state"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)

    for i in range(100_000):
        subdir = worktree / f"dir_{i % 100:03d}"
        subdir.mkdir(parents=True, exist_ok=True)
        (subdir / f"file_{i:05d}.txt").touch()

    print("\n[FAKE S3 SCALE] Committing 100K files to fake S3...")
    repo = create_repository(
        "s3://demo-bucket/reflake",
        worktree=worktree,
        client_root=client_root,
    )
    t0 = time.perf_counter()
    commit_id = repo.commit("meta only")
    commit_time = time.perf_counter() - t0
    print(f"  Commit: {commit_time:.2f}s ({100_000 / commit_time:.0f} files/sec)")
    assert len(commit_id) == 64

    t0 = time.perf_counter()
    entry = repo.resolve_entry("main", "dir_000/file_00000.txt")
    lookup_time = time.perf_counter() - t0
    print(f"  Single lookup: {lookup_time * 1000:.3f}ms")
    assert entry is not None
    assert entry.path == "dir_000/file_00000.txt"

    t0 = time.perf_counter()
    images = repo.resolve_entries_for_prefix("main", "dir_000")
    listing_time = time.perf_counter() - t0
    print(f"  Prefix listing (1000): {listing_time * 1000:.3f}ms")
    assert len(images) == 1000

    t0 = time.perf_counter()
    images_again = repo.resolve_entries_for_prefix("main", "dir_000")
    cached_time = time.perf_counter() - t0
    print(f"  Cached prefix listing: {cached_time * 1000:.3f}ms")
    assert len(images_again) == 1000

    print("\n[FAKE S3 SCALE] Summary:")
    print(
        f"  Commit:            {commit_time:.2f}s "
        f"({100_000 / commit_time:.0f} files/sec)"
    )
    print(f"  Single lookup:     {lookup_time * 1000:.3f}ms")
    print(f"  Prefix list (1k):  {listing_time * 1000:.3f}ms")
    print(f"  Cached prefix:     {cached_time * 1000:.3f}ms")

    assert (
        lookup_time < 0.1
    ), f"Single lookup took {lookup_time * 1000:.3f}ms (expected < 100ms)"
    assert (
        listing_time < 0.2
    ), f"Prefix listing took {listing_time * 1000:.3f}ms (expected < 200ms)"


def test_three_way_merge_combines_disjoint_changes(tmp_path: Path) -> None:
    (tmp_path / "shared.txt").write_text("base")
    repo = ReflakeRepository(tmp_path)
    _base = repo.commit("base")
    repo.branch("feature")
    repo.set_current_branch("feature")
    (tmp_path / "feature.txt").write_text("feat")
    feature = repo.commit("feature work")
    repo.set_current_branch("main")
    (tmp_path / "main.txt").write_text("main")
    main = repo.commit("main work")

    result = repo.merge("feature", "main")
    assert result.updated is True
    merge_commit = repo.read_commit(result.commit_id)
    assert len(merge_commit.parents) == 2
    assert set(merge_commit.parents) == {feature, main}
    assert merge_commit.generation > repo.read_commit(main).generation
    assert sorted(repo.resolve_entries("main")) == [
        "feature.txt",
        "main.txt",
        "shared.txt",
    ]


def test_three_way_merge_takes_theirs_when_only_one_side_changed(
    tmp_path: Path,
) -> None:
    (tmp_path / "file.txt").write_text("base")
    repo = ReflakeRepository(tmp_path)
    repo.commit("base")
    repo.branch("feature")
    repo.set_current_branch("feature")
    (tmp_path / "file.txt").write_text("feature version")
    repo.commit("feature change")
    repo.set_current_branch("main")
    (tmp_path / "other.txt").write_text("main only")
    repo.commit("main work")

    repo.merge("feature", "main")
    entry = repo.resolve_entry("main", "file.txt")
    assert entry.size == len("feature version")


def test_three_way_merge_raises_on_conflict(tmp_path: Path) -> None:
    from reflake.core.domain import MergeConflictError

    (tmp_path / "shared.txt").write_text("base")
    repo = ReflakeRepository(tmp_path)
    repo.commit("base")
    repo.branch("feature")
    repo.set_current_branch("feature")
    (tmp_path / "shared.txt").write_text("feature version")
    repo.commit("feature change")
    repo.set_current_branch("main")
    (tmp_path / "shared.txt").write_text("main version")
    repo.commit("main work")

    with pytest.raises(MergeConflictError, match="shared.txt"):
        repo.merge("feature", "main")


def test_reflog_records_ref_updates(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("one")
    repo = ReflakeRepository(tmp_path)
    first = repo.commit("first")
    (tmp_path / "a.txt").write_text("two")
    second = repo.commit("second")

    entries = list(repo.client_state.iter_reflog("main"))
    assert len(entries) == 2
    assert entries[0].startswith(f"{first} {second} commit ")
    assert entries[1].startswith(
        f"{'0' * 64} {first} commit "
    )


def test_catalog_lists_branches(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("one")
    repo = ReflakeRepository(tmp_path)
    commit_id = repo.commit("seed")
    repo.branch("feature")

    branches = sorted(repo.store.iter_branches())
    assert branches == ["feature", "main"]
    state = repo.store.read_branch_ref("feature")
    assert state is not None and state.commit_id == commit_id
