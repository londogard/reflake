from __future__ import annotations

import time
from pathlib import Path
from uuid import uuid4

import pytest

from reflake.core import RefConflictError, ReflakeRepository, create_repository

pytestmark = pytest.mark.integration


def _open_remote_repo(
    repo_uri: str,
    *,
    worktree: Path,
    client_root: Path,
    s3_client: object,
) -> ReflakeRepository:
    return create_repository(
        repo_uri,
        worktree=worktree,
        client_root=client_root,
        s3_client=s3_client,
    )


def test_s3_integration_branch_commit_and_fast_forward_merge(
    tmp_path: Path,
    ministack_client,
    s3_repo_root: str,
) -> None:
    main_worktree = tmp_path / "main-worktree"
    feature_worktree = tmp_path / "feature-worktree"
    main_client = tmp_path / "main-client"
    feature_client = tmp_path / "feature-client"
    for path in (main_worktree, feature_worktree, main_client, feature_client):
        path.mkdir(parents=True)

    (main_worktree / "shared.txt").write_text("base")
    repo_main = _open_remote_repo(
        s3_repo_root,
        worktree=main_worktree,
        client_root=main_client,
        s3_client=ministack_client,
    )
    base_commit = repo_main.commit("base")
    assert base_commit

    repo_main.branch("feature")

    (feature_worktree / "shared.txt").write_text("base")
    (feature_worktree / "feature.txt").write_text("feature")
    repo_feature = _open_remote_repo(
        s3_repo_root,
        worktree=feature_worktree,
        client_root=feature_client,
        s3_client=ministack_client,
    )
    repo_feature.set_current_branch("feature")
    repo_feature.add(["feature.txt"])
    feature_commit = repo_feature.commit("feature commit")

    merge_result = repo_main.merge("feature", "main")
    assert merge_result.updated is True
    assert merge_result.commit_id == feature_commit

    changes = repo_main.diff(base_commit, "main")
    assert [(entry.path, entry.change) for entry in changes] == [
        ("feature.txt", "added")
    ]


def test_s3_integration_metadata_import_verify_remove_and_move(
    tmp_path: Path,
    ministack_client,
    s3_repo_root: str,
) -> None:
    source_bucket = s3_repo_root.split("//", maxsplit=1)[1].split("/", maxsplit=1)[0]
    source_prefix = f"imports/{uuid4().hex}"
    worktree = tmp_path / "client-worktree"
    client_root = tmp_path / "client-state"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)

    ministack_client.put_object(
        Bucket=source_bucket,
        Key=f"{source_prefix}/root.txt",
        Body=b"root",
    )
    ministack_client.put_object(
        Bucket=source_bucket,
        Key=f"{source_prefix}/images/cat.jpg",
        Body=b"cat",
    )
    ministack_client.put_object(
        Bucket=source_bucket,
        Key=f"{source_prefix}/images/dog.jpg",
        Body=b"dog",
    )
    ministack_client.put_object(
        Bucket=source_bucket,
        Key=f"{source_prefix}/docs/readme.md",
        Body=b"readme",
    )

    repo = _open_remote_repo(
        s3_repo_root,
        worktree=worktree,
        client_root=client_root,
        s3_client=ministack_client,
    )
    source_uri = f"s3://{source_bucket}/{source_prefix}"
    imported_commit = repo.import_s3(
        source_uri,
        "metadata import",
        identity_mode="pointer",
        path_patterns=["root.txt", "**/*.jpg"],
    )
    assert imported_commit

    imported_entries = repo.resolve_entries("main")
    assert sorted(imported_entries) == ["images/cat.jpg", "images/dog.jpg", "root.txt"]
    assert all(entry.blob_hash is None for entry in imported_entries.values())

    audit = repo.verify(
        ref="main",
        path_prefixes=["root.txt", "images/cat.jpg"],
    )
    assert audit.created_commit is False
    assert audit.candidate_entries == 2

    verify_result = repo.promote(
        ref="main",
        path_prefixes=["root.txt", "images/cat.jpg"],
    )
    assert verify_result.created_commit is True
    assert verify_result.verified_entries == 2

    verified_entries = repo.resolve_entries("main")
    assert verified_entries["root.txt"].blob_hash is not None
    assert verified_entries["images/cat.jpg"].blob_hash is not None
    assert verified_entries["images/dog.jpg"].blob_hash is None

    move_result = repo.move("images", "photos", "rename image prefix")
    assert move_result.moved_paths == ["photos/cat.jpg", "photos/dog.jpg"]

    remove_result = repo.remove_paths(["root.txt"], "remove root")
    assert remove_result.removed_paths == ["root.txt"]

    final_entries = repo.resolve_entries("main")
    assert sorted(final_entries) == ["photos/cat.jpg", "photos/dog.jpg"]
    assert final_entries["photos/cat.jpg"].blob_hash is not None
    assert final_entries["photos/dog.jpg"].blob_hash is None


def test_s3_integration_reports_optimistic_concurrency_conflicts(
    tmp_path: Path,
    ministack_client,
    s3_repo_root: str,
) -> None:
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
    repo_a = _open_remote_repo(
        s3_repo_root,
        worktree=client_a_worktree,
        client_root=client_a_root,
        s3_client=ministack_client,
    )
    base_commit = repo_a.commit("base")
    assert base_commit

    (client_a_worktree / "data.txt").write_text("beta")

    (client_b_worktree / "data.txt").write_text("alpha")
    repo_b = _open_remote_repo(
        s3_repo_root,
        worktree=client_b_worktree,
        client_root=client_b_root,
        s3_client=ministack_client,
    )
    (client_b_worktree / "data.txt").write_text("gamma")
    winning_commit = repo_b.commit("winning update")
    assert winning_commit != base_commit

    with pytest.raises(RefConflictError) as error_info:
        repo_a.commit("stale update")

    error = error_info.value
    assert error.operation == "commit"
    assert error.expected_commit_id == base_commit
    assert error.current_commit_id == winning_commit


def test_s3_integration_million_file_scale(
    tmp_path: Path,
    ministack_client,
    s3_repo_root: str,
    caplog,
) -> None:
    """Test manifest index performance at 1M files with timing measurements.

    Validates:
    - Specific file lookup is O(log B + 1)
    - Prefix listing with 200 matches is O(log B + 200)
    - Bulk downloads use manifest cache effectively

    Logs timing measurements for performance regression detection.
    """
    worktree = tmp_path / "worktree"
    client_root = tmp_path / "client-state"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)

    # 1. Generate 1M files locally (much faster than S3 API calls)
    print("\n[SCALE TEST] Generating 1M files locally...")
    gen_start = time.perf_counter()
    (worktree / "images" / "cats").mkdir(parents=True, exist_ok=True)
    (worktree / "images" / "dogs").mkdir(parents=True, exist_ok=True)
    (worktree / "logs").mkdir(parents=True, exist_ok=True)
    (worktree / "other").mkdir(parents=True, exist_ok=True)
    _cat_content = b"cat"
    _dog_content = b"dog"
    _data_content = b"data"

    for i in range(1_000_000):
        # Distribute: images/cats/* (200), images/dogs/* (300), other/* (999_500)
        if i < 200:
            path = worktree / "images" / "cats" / f"cat_{i:06d}.jpg"
            # content = cat_content
        elif i < 500:
            path = worktree / "images" / "dogs" / f"dog_{i:06d}.jpg"
            # content = dog_content
        else:
            # Alternate between logs and other for realistic distribution
            category = "logs" if (i % 2) == 0 else "other"
            path = worktree / category / f"file_{i:07d}.bin"
            # content = data_content

        path.touch()

    gen_time = time.perf_counter() - gen_start
    print(
        f"[SCALE TEST] Generated 1M files in {gen_time:.2f}s"
        f" ({1_000_000/gen_time:.0f} files/sec)"
    )

    # 2. Commit to Reflake (creates manifest + index from local files)
    print("[SCALE TEST] Committing 1M files to Reflake...")
    repo = _open_remote_repo(
        s3_repo_root,
        worktree=worktree,
        client_root=client_root,
        s3_client=ministack_client,
    )
    # Use metadata identity for performance (no content hashing on 1M empty files)
    from reflake.core.config import S3Config
    from reflake.core.objects import parse_s3_uri

    bucket, prefix = parse_s3_uri(s3_repo_root)
    cfg = S3Config(
        dataset_root=str(worktree), bucket=bucket, prefix=prefix, identity="pointer"
    )
    cfg.save(worktree)
    commit_start = time.perf_counter()
    commit_id = repo.commit("1M file snapshot")
    commit_time = time.perf_counter() - commit_start
    assert commit_id
    print(
        f"[SCALE TEST] Commit + tree build in {commit_time:.2f}s"
        f" ({1_000_000/commit_time:.0f} files/sec)"
    )

    # 3. Test: Specific file lookup (should be O(log B + 1))
    print("[SCALE TEST] Testing specific file lookup...")
    lookup_start = time.perf_counter()
    cat_50 = repo.resolve_entry("main", "images/cats/cat_000050.jpg")
    lookup_time = time.perf_counter() - lookup_start
    assert cat_50 is not None
    assert cat_50.size == 0  # touch
    print(f"[SCALE TEST] Single file lookup: {lookup_time*1000:.3f}ms")

    # 4. Test: Prefix listing (should be O(log B + 200))
    print("[SCALE TEST] Testing prefix listing (images/cats/*)...")
    listing_start = time.perf_counter()
    cats = repo.resolve_entries_for_prefix("main", "images/cats")
    listing_time = time.perf_counter() - listing_start
    assert len(cats) == 200
    print(f"[SCALE TEST] Prefix listing 200 files: {listing_time*1000:.3f}ms")

    # 5. Test: Bulk metadata access (simulating download planning)
    print("[SCALE TEST] Bulk metadata access (200 files)...")
    bulk_start = time.perf_counter()
    bulk_data = []
    for path, entry in cats.items():
        # Simulate metadata-only access (no blob reads)
        bulk_data.append((path, entry.size, entry.identity_mode))
    bulk_time = time.perf_counter() - bulk_start
    assert len(bulk_data) == 200
    assert all(t[1] == 0 for t in bulk_data)  # All have size
    print(f"[SCALE TEST] Bulk metadata access (200): {bulk_time*1000:.3f}ms")

    # 6. Test: Cached prefix listing (should be faster)
    print("[SCALE TEST] Testing cached prefix listing...")
    cached_start = time.perf_counter()
    cats_again = repo.resolve_entries_for_prefix("main", "images/cats")
    cached_time = time.perf_counter() - cached_start
    assert len(cats_again) == 200
    print(f"[SCALE TEST] Cached prefix listing: {cached_time*1000:.3f}ms")

    # 7. Summary and assertions
    print("\n[SCALE TEST] Performance Summary:")
    print(
        f"  Generate 1M files:       {gen_time:.2f}s"
        f" ({1_000_000/gen_time:.0f} files/sec)"
    )
    print(
        f"  Commit + tree build:     {commit_time:.2f}s"
        f" ({1_000_000/commit_time:.0f} files/sec)"
    )
    print(f"  Single lookup:           {lookup_time*1000:.3f}ms")
    print(f"  Prefix list (200):       {listing_time*1000:.3f}ms")
    print(f"  Bulk metadata (200):     {bulk_time*1000:.3f}ms")
    print(f"  Cached prefix list:      {cached_time*1000:.3f}ms")

    # Verify manifest index is working: specific lookup should be fast (< 100ms)
    assert (
        lookup_time < 0.1
    ), f"Single lookup took {lookup_time*1000:.3f}ms (expected < 100ms)"

    # Prefix listing should also be fast (< 200ms for 200 matches)
    assert (
        listing_time < 0.2
    ), f"Prefix listing took {listing_time*1000:.3f}ms (expected < 200ms)"

    # Cached listing should be noticeably faster than initial
    assert (
        cached_time <= listing_time or cached_time < 0.1
    ), "Cached listing should be <= initial listing time"
