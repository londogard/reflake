from __future__ import annotations

import json
from pathlib import Path
import pytest

from reflake import (
    ReflakeFileSystem,
    NotARepositoryError,
    S3ObjectStore,
    open_repository,
    parse_where_clause,
    plan_pruned_scan,
    prune_row_groups,
    run_cli,
)


def test_top_level_exports_and_star_import() -> None:
    import reflake

    assert hasattr(reflake, "parse_where_clause")
    assert hasattr(reflake, "plan_pruned_scan")
    assert hasattr(reflake, "prune_row_groups")
    assert hasattr(reflake, "open_repository")
    assert hasattr(reflake, "ReflakeRepository")
    assert hasattr(reflake, "NotARepositoryError")


def test_s3_atomic_cas_conditional_write(fake_s3_installer) -> None:
    client = fake_s3_installer({})
    store = S3ObjectStore("demo-bucket", "repos/test", client=client)

    # Initial CAS: expected_version_token is None -> creates ref with IfNoneMatch='*'
    assert store.compare_and_set_branch_ref(
        "main", "1" * 64, expected_version_token=None
    ) is True
    ref_state = store.read_branch_ref("main")
    assert ref_state is not None
    assert ref_state.commit_id == "1" * 64
    initial_version = ref_state.version_token
    assert initial_version is not None

    # CAS with wrong version token fails
    assert store.compare_and_set_branch_ref(
        "main", "2" * 64, expected_version_token="stale_token"
    ) is False

    # CAS with correct version token succeeds
    assert store.compare_and_set_branch_ref(
        "main", "2" * 64, expected_version_token=initial_version
    ) is True
    updated_state = store.read_branch_ref("main")
    assert updated_state is not None
    assert updated_state.commit_id == "2" * 64


def test_vfs_s3_dataset_roots(
    fake_s3_installer, tmp_path: Path, monkeypatch
) -> None:
    client = fake_s3_installer({})
    repo_uri = "s3://demo-bucket/repos/remote_demo"
    monkeypatch.chdir(tmp_path)

    (tmp_path / "data.txt").write_text("s3 dataset content")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "remote seed"]) == 0

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
    repo = open_repository(tmp_path)
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
    cat_entries = repo.catalog()
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

    assert run_cli(["status", "--repo", str(empty_dir)]) == 1
    err = capsys.readouterr().err
    assert "not a reflake repository" in err

    with pytest.raises(NotARepositoryError):
        open_repository(empty_dir, must_exist=True)


def test_commit_json_flag(tmp_path: Path, capsys) -> None:
    (tmp_path / "file.txt").write_text("hello json")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert "commit_id" in out
    assert len(out["commit_id"]) == 64
