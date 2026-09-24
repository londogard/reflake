from __future__ import annotations

import json
from pathlib import Path

import pytest

from reflake import (
    NotARepositoryError,
    ReflakeFileSystem,
    create_repository,
    open_repository,
    run_cli,
)
from reflake.core.objects import S3ObjectStore


def test_top_level_exports_are_the_stable_surface() -> None:
    import reflake

    # Documented stable surface.
    for name in (
        "init_repository",
        "create_repository",
        "open_repository",
        "ReflakeRepository",
        "ReflakeFileSystem",
        "ReflakeURI",
        "push",
        "pull",
        "fetch",
        "ReflakeError",
        "NotARepositoryError",
    ):
        assert hasattr(reflake, name), name

    # Internals stay in reflake.core and are deliberately not re-exported.
    for internal in (
        "S3ObjectStore",
        "LocalObjectStore",
        "ObjectStore",
        "Entry",
        "parse_where_clause",
        "plan_pruned_scan",
        "prune_row_groups",
        "blake3_digest_file",
        "init_config",
    ):
        assert not hasattr(reflake, internal), internal


def test_s3_atomic_cas_conditional_write(fake_s3_installer) -> None:
    client = fake_s3_installer({})
    store = S3ObjectStore("demo-bucket", "repos/test", client=client)

    # Initial CAS: expected None -> creates ref with IfNoneMatch='*'
    assert store.compare_and_set_branch_ref(
        "main", "1" * 64, expected_commit_id=None
    ) is True
    ref_state = store.read_branch_ref("main")
    assert ref_state is not None
    assert ref_state.commit_id == "1" * 64

    # Create-only CAS on an existing branch fails
    assert store.compare_and_set_branch_ref(
        "main", "2" * 64, expected_commit_id=None
    ) is False

    # CAS with wrong expected commit fails
    assert store.compare_and_set_branch_ref(
        "main", "2" * 64, expected_commit_id="0" * 64
    ) is False

    # CAS with correct expected commit succeeds
    assert store.compare_and_set_branch_ref(
        "main", "2" * 64, expected_commit_id="1" * 64
    ) is True
    updated_state = store.read_branch_ref("main")
    assert updated_state is not None
    assert updated_state.commit_id == "2" * 64


def test_vfs_s3_dataset_roots(
    fake_s3_installer, tmp_path: Path, monkeypatch
) -> None:
    fake_s3_installer({})
    repo_uri = "s3://demo-bucket/repos/remote_demo"
    monkeypatch.chdir(tmp_path)

    (tmp_path / "data.txt").write_text("s3 dataset content")
    assert run_cli(["--repo", repo_uri, "commit", "-m", "remote seed"]) == 0

    fs = ReflakeFileSystem(dataset_roots={"remote": repo_uri})
    # Check that dataset_roots preserves the s3:// URI
    assert fs.dataset_roots["remote"] == repo_uri

    # Read file via fsspec
    with fs.open("reflake://remote@main/data.txt", "rb") as handle:
        assert handle.read() == b"s3 dataset content"

    # List files via fsspec
    entries = fs.ls("reflake://remote@main/")
    assert len(entries) == 1
    assert entries[0]["name"] == "reflake://remote@main/data.txt"


def test_repository_methods_cover_convenience_operations(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("content a")
    repo = create_repository(tmp_path)
    commit_id = repo.commit("commit 1")
    assert len(commit_id) == 64

    # Test cat
    data = repo.cat("main", "a.txt")
    assert data == b"content a"

    # Test reflog
    logs = list(repo.reflog("main"))
    assert len(logs) >= 1
    assert "commit" in logs[0]

    # Test catalog
    cat_entries = repo.branches()
    assert len(cat_entries) == 1
    assert cat_entries[0]["branch"] == "main"
    assert cat_entries[0]["commit_id"] == commit_id

    # Test gc
    gc_res = repo.gc(dry_run=True)
    assert gc_res.reachable_commits == 1
    assert gc_res.orphan_commits == 0


def test_not_a_repository_validation(tmp_path: Path, capsys) -> None:
    empty_dir = tmp_path / "uninitialized"
    empty_dir.mkdir()

    assert run_cli(["--repo", str(empty_dir), "status"]) == 1
    err = capsys.readouterr().err
    assert "not a reflake repository" in err

    with pytest.raises(NotARepositoryError):
        open_repository(empty_dir, must_exist=True)


def test_commit_json_flag(tmp_path: Path, capsys) -> None:
    (tmp_path / "file.txt").write_text("hello json")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "--json", "commit", "-m", "init"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert "commit_id" in out
    assert len(out["commit_id"]) == 64
