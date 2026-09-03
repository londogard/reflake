from __future__ import annotations

from typing import Any

import io
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from reflake.core import ReflakeFileSystem, open_repository
from reflake import run_cli
from reflake.core.objects.tree import parse_tree_object


def test_cli_supports_command_local_repo_flag_for_s3_repositories(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"

    (tmp_path / "a.txt").write_text("one")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "initial"]) == 0
    commit_a = capsys.readouterr().out.strip()
    assert len(commit_a) == 64

    assert run_cli(["branch", "--repo", repo_uri, "feature"]) == 0
    branch_out = capsys.readouterr().out.strip()
    assert branch_out == "Created branch 'feature'"

    (tmp_path / "a.txt").write_text("two")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "update"]) == 0
    commit_b = capsys.readouterr().out.strip()
    assert len(commit_b) == 64
    assert commit_a != commit_b

    assert run_cli(["diff", "--repo", repo_uri, commit_a, commit_b, "--json"]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert [entry["path"] for entry in diff_payload] == ["a.txt"]
    assert diff_payload[0]["change"] == "modified"

    assert run_cli(["query", "build", "--repo", repo_uri, "--json"]) == 0
    build_payload = json.loads(capsys.readouterr().out)
    db_path = Path(build_payload["database_path"])
    assert db_path.exists()
    assert "clients" in db_path.as_posix()

    assert not list((tmp_path / ".reflake" / "commits").glob("*.json"))
    assert f"repos/demo/commits/{commit_b}.json" in client._objects
    assert any(key.startswith("repos/demo/trees/") for key in client._objects)
    assert "repos/demo/refs/heads/main" in client._objects


def test_cli_metadata_only_rm_and_mv_work_for_s3_repositories(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"

    (tmp_path / "a.txt").write_text("one")
    (tmp_path / "dir").mkdir()
    (tmp_path / "dir" / "b.txt").write_text("two")

    assert run_cli(["commit", "--repo", repo_uri, "-m", "initial"]) == 0
    initial_commit = capsys.readouterr().out.strip()
    assert initial_commit

    (tmp_path / "a.txt").unlink()
    shutil.rmtree(tmp_path / "dir")

    assert (
        run_cli(
            [
                "mv",
                "--repo",
                repo_uri,
                "a.txt",
                "renamed.txt",
                "--json",
            ]
        )
        == 0
    )
    mv_payload = json.loads(capsys.readouterr().out)
    assert mv_payload["added"] == ["renamed.txt"]
    assert mv_payload["removed"] == ["a.txt"]

    assert run_cli(["rm", "--repo", repo_uri, "dir", "--json"]) == 0
    rm_payload = json.loads(capsys.readouterr().out)
    assert rm_payload["removed"] == ["a.txt", "dir"]

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                repo_uri,
                "-m",
                "rename and remove",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    assert (
        run_cli(["diff", "--repo", repo_uri, initial_commit, commit_id, "--json"]) == 0
    )
    diff_payload = json.loads(capsys.readouterr().out)
    assert [(entry["path"], entry["change"]) for entry in diff_payload] == [
        ("a.txt", "removed"),
        ("dir/b.txt", "removed"),
        ("renamed.txt", "added"),
    ]

    assert not list((tmp_path / ".reflake" / "commits").glob("*.json"))
    assert f"repos/demo/commits/{commit_id}.json" in client._objects


def test_cli_commit_branch_and_diff(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("one")

    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "initial"]) == 0
    commit_a = capsys.readouterr().out.strip()
    assert len(commit_a) == 64

    (tmp_path / "a.txt").write_text("two")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "update"]) == 0
    commit_b = capsys.readouterr().out.strip()
    assert len(commit_b) == 64

    assert run_cli(["branch", "--repo", str(tmp_path), "exp"]) == 0
    branch_out = capsys.readouterr().out.strip()
    assert branch_out == "Created branch 'exp'"

    assert run_cli(["diff", "--repo", str(tmp_path), commit_a, commit_b, "--json"]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert diff_payload == [
        {
            "path": "a.txt",
            "change": "modified",
            "before_hash": diff_payload[0]["before_hash"],
            "after_hash": diff_payload[0]["after_hash"],
            "before_size": 3,
            "after_size": 3,
        }
    ]
    assert diff_payload[0]["before_hash"] != diff_payload[0]["after_hash"]


def test_cli_cat_reads_file_bytes(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("hello cat")

    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "seed"]) == 0
    capsys.readouterr()

    assert run_cli(["cat", "--repo", str(tmp_path), "main", "a.txt"]) == 0
    assert capsys.readouterr().out == "hello cat"

    assert run_cli(["cat", "--repo", str(tmp_path), "main", "missing.txt"]) == 1
    assert "cat error: Path not found" in capsys.readouterr().err


def test_cli_virtual_ops_work_without_local_repo(tmp_path: Path, capsys, monkeypatch, fake_s3_installer) -> None:
    """list/cat/diff/log are virtual: they read committed trees, not the worktree."""
    fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"

    (tmp_path / "a.txt").write_text("alpha")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "seed"]) == 0
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    assert run_cli(["cat", "--repo", repo_uri, "main", "a.txt"]) == 0
    assert capsys.readouterr().out == "alpha"

    assert run_cli(["list", "--repo", repo_uri, "", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [entry["path"] for entry in payload] == ["a.txt"]

    assert run_cli(["diff", "--repo", repo_uri, commit_id, commit_id, "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == []

    assert run_cli(["log", "--repo", repo_uri, "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1


def test_cli_index_build_query_drop(tmp_path: Path, capsys) -> None:
    (tmp_path / "x.jpg").write_bytes(b"img")
    (tmp_path / "y.txt").write_text("text")

    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "seed"]) == 0
    capsys.readouterr()

    assert (
        run_cli(["query", "build", "--repo", str(tmp_path), "--parquet", "--json"]) == 0
    )
    build_payload = json.loads(capsys.readouterr().out)
    db_path = Path(build_payload["database_path"])
    parquet_path = Path(build_payload["parquet_path"])
    assert db_path.exists()
    assert parquet_path.exists()


def test_cli_commit_with_metadata_identity_mode(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")

    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "meta"]) == 0
    capsys.readouterr()

    commit_payload = json.loads(
        next((tmp_path / ".reflake" / "commits").glob("*.json")).read_text()
    )
    tree_path = tmp_path / ".reflake" / "trees" / commit_payload["tree"]
    entries = parse_tree_object(tree_path.read_bytes())
    assert len(entries) == 1
    assert entries[0].kind == "b"
    assert entries[0].hash


def test_cli_add_reports_missing_source_cleanly(tmp_path: Path, capsys) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    assert run_cli(["add", "--repo", str(repo_root), "missing.txt"]) == 1
    stderr = capsys.readouterr().err
    assert "add error: Cannot stage missing path: missing.txt" in stderr


def test_cli_verify_on_blake3_commit_does_nothing(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "meta"]) == 0
    capsys.readouterr()

    assert run_cli(["verify", "--repo", str(tmp_path), "--json"]) == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["created_commit"] is False
    assert verify_payload["verified_entries"] == 0
    assert verify_payload["dry_run"] is False


def test_cli_verify_dry_run_reports_no_candidates(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")

    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "meta"]) == 0
    first_commit = capsys.readouterr().out.strip()

    assert run_cli(["verify", "--repo", str(tmp_path), "--dry-run", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["created_commit"] is False
    assert payload["verified_entries"] == 0
    assert payload["candidate_entries"] == 0
    assert payload["commit_id"] == first_commit


def test_cli_verify_on_meta_entry_promotes_it(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer({"data/file.txt": b"remote payload"})
    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(tmp_path),
                "--identity",
                "meta",
                "s3://demo-bucket/data/file.txt",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "import"]) == 0
    first_commit = capsys.readouterr().out.strip()

    assert run_cli(["verify", "--repo", str(tmp_path), "--json"]) == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["created_commit"] is True
    assert verify_payload["verified_entries"] == 1
    assert verify_payload["commit_id"] != first_commit

    commit_payload = json.loads(
        (
            tmp_path / ".reflake" / "commits" / f"{verify_payload['commit_id']}.json"
        ).read_text()
    )
    tree_path = tmp_path / ".reflake" / "trees" / commit_payload["tree"]
    latest_entries = parse_tree_object(tree_path.read_bytes())
    assert latest_entries[0].kind == "b"

    assert run_cli(["verify", "--repo", str(tmp_path), "--json"]) == 0
    verify_again_payload = json.loads(capsys.readouterr().out)
    assert verify_again_payload["created_commit"] is False
    assert verify_again_payload["verified_entries"] == 0


def test_cli_staging_commit_is_branch_scoped(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "base"]) == 0
    base_commit = capsys.readouterr().out.strip()
    assert base_commit

    assert run_cli(["branch", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "feature.txt").write_text("feature")
    assert run_cli(["checkout", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(tmp_path),
                "feature.txt",
                "--json",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["feature.txt"]

    fs = ReflakeFileSystem(dataset_roots={"demo": tmp_path})
    staged_root_listing = fs.ls("reflake://demo@feature+staged/", detail=False)
    assert "reflake://demo@feature/shared.txt" in staged_root_listing
    assert "reflake://demo@feature/feature.txt" in staged_root_listing

    staged_wildcard_listing = fs.ls("reflake://demo@feature+staged/*", detail=False)
    assert "reflake://demo@feature/shared.txt" in staged_wildcard_listing
    assert "reflake://demo@feature/feature.txt" in staged_wildcard_listing

    with fs.open("reflake://demo@feature+staged/feature.txt", "rb") as handle:
        assert handle.read() == b"feature"

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(tmp_path),
                "-m",
                "feature only",
            ]
        )
        == 0
    )
    feature_commit = capsys.readouterr().out.strip()
    assert feature_commit and feature_commit != base_commit

    assert run_cli(["status", "--repo", str(tmp_path), "--json"]) == 0
    status_payload = json.loads(capsys.readouterr().out)
    assert status_payload["added"] == []
    assert status_payload["removed"] == []

    assert run_cli(["diff", "--repo", str(tmp_path), "main", "feature", "--json"]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert diff_payload == [
        {
            "path": "feature.txt",
            "change": "added",
            "before_hash": None,
            "after_hash": diff_payload[0]["after_hash"],
            "before_size": None,
            "after_size": 7,
        }
    ]


def test_cli_add_supports_arbitrary_local_source_with_logical_destination(
    tmp_path: Path, capsys
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    external_file = tmp_path / "external.txt"
    external_file.write_text("external payload")

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(repo_root),
                "--as",
                "imports/external.txt",
                str(external_file),
                "--json",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["imports/external.txt"]

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(repo_root),
                "-m",
                "ingest external",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    repo = open_repository(repo_root)
    entries = repo.resolve_entries("main")
    assert list(entries) == ["imports/external.txt"]
    assert entries["imports/external.txt"].source_uri is None


def test_cli_add_supports_s3_source_with_staged_read_and_logical_destination(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer({"incoming/source.txt": b"remote payload"})
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    (repo_root / "base.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(repo_root), "-m", "base"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(repo_root),
                "--identity",
                "meta",
                "--as",
                "imports/source.txt",
                "s3://demo-bucket/incoming/source.txt",
                "--json",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["imports/source.txt"]

    fs = ReflakeFileSystem(dataset_roots={"demo": repo_root})
    with fs.open("reflake://demo@main+staged/imports/source.txt", "rb") as handle:
        assert handle.read() == b"remote payload"

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(repo_root),
                "-m",
                "ingest remote",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    repo = open_repository(repo_root)
    entries = repo.resolve_entries("main")
    assert set(entries) == {"base.txt", "imports/source.txt"}
    imported_entry = entries["imports/source.txt"]
    assert imported_entry.identity_mode == "meta"
    assert imported_entry.blob_hash is None
    assert imported_entry.source_uri == "s3://demo-bucket/incoming/source.txt"


def test_cli_add_supports_local_directory_source_with_destination_prefix(
    tmp_path: Path, capsys
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    external_dir = tmp_path / "bundle"
    (external_dir / "nested").mkdir(parents=True)
    (external_dir / "a.txt").write_text("alpha")
    (external_dir / "nested" / "b.txt").write_text("beta")

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(repo_root),
                "--as",
                "imports/bundle",
                str(external_dir),
                "--json",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == [
        "imports/bundle/a.txt",
        "imports/bundle/nested/b.txt",
    ]

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(repo_root),
                "-m",
                "ingest bundle",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    repo = open_repository(repo_root)
    assert list(repo.resolve_entries("main")) == [
        "imports/bundle/a.txt",
        "imports/bundle/nested/b.txt",
    ]


def test_cli_add_supports_s3_prefix_with_destination_prefix(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
        {
            "incoming/batch/a.txt": b"alpha",
            "incoming/batch/nested/b.txt": b"beta",
        }
    )
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "base.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(repo_root), "-m", "base"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(repo_root),
                "--identity",
                "meta",
                "--as",
                "imports/batch",
                "s3://demo-bucket/incoming/batch",
                "--json",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == [
        "imports/batch/a.txt",
        "imports/batch/nested/b.txt",
    ]

    fs = ReflakeFileSystem(dataset_roots={"demo": repo_root})
    with fs.open(
        "reflake://demo@main+staged/imports/batch/nested/b.txt", "rb"
    ) as handle:
        assert handle.read() == b"beta"

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(repo_root),
                "-m",
                "ingest batch",
            ]
        )
        == 0
    )
    capsys.readouterr()


def test_cli_remote_staged_add_preserves_existing_entries_and_uploads_one_blob(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"

    (tmp_path / "a.txt").write_text("alpha")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "initial"]) == 0
    first_commit = capsys.readouterr().out.strip()
    assert len(first_commit) == 64

    initial_blob_keys = {
        key for key in client._objects if key.startswith("repos/demo/blobs/")
    }
    assert len(initial_blob_keys) == 1

    (tmp_path / "a.txt").unlink()
    (tmp_path / "b.txt").write_text("beta")

    assert run_cli(["add", "--repo", repo_uri, "b.txt", "--json"]) == 0
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["b.txt"]

    assert run_cli(["commit", "--repo", repo_uri, "-m", "add b"]) == 0
    second_commit = capsys.readouterr().out.strip()
    assert len(second_commit) == 64
    assert second_commit != first_commit

    new_blob_keys = {
        key for key in client._objects if key.startswith("repos/demo/blobs/")
    }
    assert len(new_blob_keys - initial_blob_keys) == 1

    assert (
        run_cli(["diff", "--repo", repo_uri, first_commit, second_commit, "--json"])
        == 0
    )
    diff_payload = json.loads(capsys.readouterr().out)
    assert diff_payload == [
        {
            "path": "b.txt",
            "change": "added",
            "before_hash": None,
            "after_hash": diff_payload[0]["after_hash"],
            "before_size": None,
            "after_size": 4,
        }
    ]


def test_cli_remote_commit_ignores_stale_lock_objects(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """Stale lock objects are inert in v2 — the lock machinery is gone (CAS only)."""
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"
    lock_key = "repos/demo/locks/refs/heads/main.lock"
    client._objects[lock_key] = {
        "Body": b"legacy-stale-lock",
        "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "ETag": '"11-11"',
    }

    (tmp_path / "a.txt").write_text("alpha")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "initial"]) == 0
    commit_id = capsys.readouterr().out.strip()

    assert len(commit_id) == 64
    # Locks are never consulted or created; the pre-seeded object is inert.
    assert client._objects[lock_key]["Body"] == b"legacy-stale-lock"
    assert "repos/demo/refs/heads/main" in client._objects


def test_cli_merge_fast_forwards_target_branch(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "base"]) == 0
    base_commit = capsys.readouterr().out.strip()
    assert base_commit

    assert run_cli(["branch", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "feature.txt").write_text("feature")
    assert run_cli(["checkout", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()
    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(tmp_path),
                "feature.txt",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(tmp_path),
                "-m",
                "feature commit",
            ]
        )
        == 0
    )
    feature_commit = capsys.readouterr().out.strip()
    assert feature_commit and feature_commit != base_commit

    assert run_cli(["checkout", "--repo", str(tmp_path), "main"]) == 0
    capsys.readouterr()

    assert run_cli(["merge", "--repo", str(tmp_path), "feature", "main", "--json"]) == 0
    merge_payload = json.loads(capsys.readouterr().out)
    assert merge_payload == {
        "source_ref": "feature",
        "target_ref": "main",
        "commit_id": feature_commit,
        "updated": True,
    }

    assert run_cli(["diff", "--repo", str(tmp_path), "main", "feature", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == []


def test_cli_merge_three_way_succeeds(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "base"]) == 0
    capsys.readouterr()

    assert run_cli(["branch", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "main.txt").write_text("main")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "main commit"]) == 0
    main_commit = capsys.readouterr().out.strip()
    assert main_commit

    (tmp_path / "feature.txt").write_text("feature")
    assert run_cli(["checkout", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()
    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(tmp_path),
                "feature.txt",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(tmp_path),
                "-m",
                "feature commit",
            ]
        )
        == 0
    )
    feature_commit = capsys.readouterr().out.strip()
    assert feature_commit and feature_commit != main_commit

    # Diverged branches with disjoint files: metadata-only 3-way merge succeeds.
    assert (
        run_cli(["merge", "--repo", str(tmp_path), "feature", "main", "--json"]) == 0
    )
    merge_payload = json.loads(capsys.readouterr().out)
    assert merge_payload["updated"] is True
    assert merge_payload["commit_id"] != main_commit

    merged = sorted(open_repository(str(tmp_path)).resolve_entries("main"))
    assert merged == ["feature.txt", "main.txt", "shared.txt"]


def test_cli_log_command(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    # 1. Run log on empty main branch (no commits yet)
    assert run_cli(["log", "--repo", str(tmp_path)]) == 0
    empty_out = capsys.readouterr().out.strip()
    assert empty_out == ""

    # 2. Make first commit
    (tmp_path / "a.txt").write_text("one")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "initial seed"]) == 0
    commit_a = capsys.readouterr().out.strip()

    # 3. Make second commit
    (tmp_path / "a.txt").write_text("two")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "update progress"]) == 0
    commit_b = capsys.readouterr().out.strip()

    # 4. Run log with default format
    assert run_cli(["log", "--repo", str(tmp_path)]) == 0
    log_out = capsys.readouterr().out.strip()

    assert f"commit {commit_b}" in log_out
    assert f"commit {commit_a}" in log_out
    assert "Parent: " + commit_a in log_out
    assert "initial seed" in log_out
    assert "update progress" in log_out
    assert log_out.index(commit_b) < log_out.index(commit_a)  # Descendant-first order

    # 5. Run log with --json
    assert run_cli(["log", "--repo", str(tmp_path), "--json"]) == 0
    log_json = json.loads(capsys.readouterr().out)

    assert len(log_json) == 2
    assert log_json[0]["id"] == commit_b
    assert log_json[0]["parents"] == [commit_a]
    assert log_json[0]["message"] == "update progress"
    assert log_json[0]["branch"] == "main"
    assert log_json[1]["id"] == commit_a
    assert log_json[1]["parents"] == []
    assert log_json[1]["message"] == "initial seed"

    # 6. Query log starting at a specific commit ref
    assert run_cli(["log", "--repo", str(tmp_path), commit_a, "--json"]) == 0
    log_single = json.loads(capsys.readouterr().out)
    assert len(log_single) == 1
    assert log_single[0]["id"] == commit_a


def test_cli_status_reports_working_tree_changes(tmp_path: Path, capsys) -> None:
    (tmp_path / "base.txt").write_text("hello")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "initial"]) == 0
    capsys.readouterr()

    (tmp_path / "new.txt").write_text("new file")
    (tmp_path / "base.txt").write_text("modified content")
    (tmp_path / "removed.txt").write_text("to be deleted")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "second"]) == 0
    capsys.readouterr()

    (tmp_path / "removed.txt").unlink()
    (tmp_path / "new.txt").write_text("updated new file")
    (tmp_path / "another.txt").write_text("added file")

    assert run_cli(["status", "--repo", str(tmp_path), "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert "new.txt" in status["working_tree_modified"]
    assert "removed.txt" in status["working_tree_removed"]
    assert "another.txt" in status["working_tree_added"]
    assert status["added"] == []
    assert status["removed"] == []


def test_cli_status_reports_clean_tree(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("content")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    assert run_cli(["status", "--repo", str(tmp_path)]) == 0
    out = capsys.readouterr().out.strip()
    assert "Nothing to commit, working tree clean" in out


def test_cli_status_reports_staged_and_working_tree_changes(
    tmp_path: Path, capsys
) -> None:
    (tmp_path / "base.txt").write_text("hello")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    (tmp_path / "staged.txt").write_text("staged")
    assert run_cli(["add", "--repo", str(tmp_path), "staged.txt", "--json"]) == 0
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["staged.txt"]

    (tmp_path / "unstaged.txt").write_text("unstaged")
    (tmp_path / "base.txt").write_text("modified")

    assert run_cli(["status", "--repo", str(tmp_path), "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert "staged.txt" in status["added"]
    assert "unstaged.txt" in status["working_tree_added"]
    assert "base.txt" in status["working_tree_modified"]


def test_cli_status_on_empty_repo(tmp_path: Path, capsys) -> None:
    (tmp_path / "f.txt").write_text("x")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    (tmp_path / "f.txt").unlink()

    assert run_cli(["status", "--repo", str(tmp_path), "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert "f.txt" in status["working_tree_removed"]


def test_cli_status_human_readable_format(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("hello")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    (tmp_path / "b.txt").write_text("new")
    (tmp_path / "a.txt").write_text("world")

    assert run_cli(["status", "--repo", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "modified: a.txt" in out
    assert "added:    b.txt" in out


def test_cli_list_command(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "a.txt").write_text("hello")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "b.txt").write_text("world")
    assert run_cli(["commit", "-m", "init"]) == 0
    capsys.readouterr()

    assert run_cli(["list"]) == 0
    out = capsys.readouterr().out.strip().splitlines()
    assert "a.txt" in out
    assert "nested/b.txt" in out

    assert run_cli(["list", "nested"]) == 0
    out = capsys.readouterr().out.strip()
    assert out == "nested/b.txt"


def test_cli_list_command_json(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "a.txt").write_text("hello")
    assert run_cli(["commit", "-m", "init"]) == 0
    capsys.readouterr()

    assert run_cli(["list", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload) == 1
    assert payload[0]["path"] == "a.txt"
    assert payload[0]["size"] == 5
    assert payload[0]["hash"] is not None


def test_cli_list_with_ref(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "a.txt").write_text("hello")
    assert run_cli(["commit", "-m", "init"]) == 0
    capsys.readouterr()

    assert run_cli(["list"]) == 0
    out = capsys.readouterr().out.strip()
    assert out == "a.txt"


def test_cli_checkout_command(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    # Make an initial commit on main
    (tmp_path / "a.txt").write_text("hello")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "first"]) == 0
    capsys.readouterr()

    # Create branch feature
    assert run_cli(["branch", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    # Checkout feature branch
    assert run_cli(["checkout", "--repo", str(tmp_path), "feature"]) == 0
    checkout_out = capsys.readouterr().out.strip()
    assert checkout_out == "Switched to branch 'feature'"

    # Verify current branch is indeed feature
    assert run_cli(["status", "--repo", str(tmp_path), "--json"]) == 0
    status_payload = json.loads(capsys.readouterr().out)
    assert status_payload["ref"] == "feature"

    # Attempt checking out non-existent branch should fail
    assert run_cli(["checkout", "--repo", str(tmp_path), "non-existent"]) == 1
    err_out = capsys.readouterr().err.strip()
    assert "Unknown branch or commit: non-existent" in err_out


def test_cli_checkout_restore_all_files(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_text("beta")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "first"]) == 0
    capsys.readouterr()

    (tmp_path / "a.txt").unlink()
    (tmp_path / "sub" / "b.txt").unlink()
    (tmp_path / "sub").rmdir()
    assert not (tmp_path / "a.txt").exists()
    assert not (tmp_path / "sub" / "b.txt").exists()

    assert run_cli(["restore", "--repo", str(tmp_path), "main"]) == 0
    out = capsys.readouterr().out
    assert "Restored 2 file(s) from 'main'" in out
    assert (tmp_path / "a.txt").read_text() == "alpha"
    assert (tmp_path / "sub" / "b.txt").read_text() == "beta"


def test_cli_checkout_restore_specific_paths(tmp_path: Path, capsys) -> None:
    (tmp_path / "x.txt").write_text("x")
    (tmp_path / "y.txt").write_text("y")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    (tmp_path / "x.txt").unlink()
    (tmp_path / "y.txt").unlink()

    assert run_cli(["restore", "--repo", str(tmp_path), "main", "--path", "x.txt"]) == 0
    out = capsys.readouterr().out
    assert "Restored 1 file(s) from 'main'" in out
    assert (tmp_path / "x.txt").read_text() == "x"
    assert not (tmp_path / "y.txt").exists()


def test_cli_checkout_restore_force_overwrite(tmp_path: Path, capsys) -> None:
    (tmp_path / "f.txt").write_text("original")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    (tmp_path / "f.txt").write_text("modified")

    assert run_cli(["restore", "--repo", str(tmp_path), "main", "--force"]) == 0
    out = capsys.readouterr().out
    assert "Restored 1 file(s) from 'main'" in out
    assert (tmp_path / "f.txt").read_text() == "original"


def test_cli_checkout_restore_safe_skips_existing(tmp_path: Path, capsys) -> None:
    (tmp_path / "f.txt").write_text("original")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    (tmp_path / "f.txt").write_text("modified")

    assert run_cli(["restore", "--repo", str(tmp_path), "main"]) == 0
    out = capsys.readouterr().out
    assert "Nothing to restore" in out
    assert (tmp_path / "f.txt").read_text() == "modified"


def test_cli_restore_requires_ref(tmp_path: Path, capsys) -> None:
    assert run_cli(["restore", "--repo", str(tmp_path)]) == 2
    err_out = capsys.readouterr().err.strip()
    assert "the following arguments are required" in err_out


def test_cli_restore_unknown_ref(tmp_path: Path, capsys) -> None:
    assert run_cli(["restore", "--repo", str(tmp_path), "nonexistent"]) == 1
    err_out = capsys.readouterr().err.strip()
    assert "Unknown" in err_out


def test_blob_stream_read_via_filesystem(tmp_path: Path) -> None:
    from reflake.core.vfs import _BlobReadFile

    (tmp_path / "data.bin").write_bytes(b"streaming test content")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0

    fs = ReflakeFileSystem(dataset_roots={"ds": tmp_path})
    handle = fs._open(f"reflake://ds@main/data.bin", "rb")
    assert isinstance(handle, _BlobReadFile)
    assert handle.readable()
    assert handle.read() == b"streaming test content"
    handle.close()


def test_blob_stream_chunked_read(tmp_path: Path) -> None:
    content = b"chunked-read-test-data"
    (tmp_path / "chunked.bin").write_bytes(content)
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0

    fs = ReflakeFileSystem(dataset_roots={"ds": tmp_path})
    with fs.open("reflake://ds@main/chunked.bin", "rb") as handle:
        first = handle.read(7)
        assert first == b"chunked"
        rest = handle.read()
        assert rest == b"-read-test-data"


def test_blob_stream_seek_and_read(tmp_path: Path) -> None:
    content = b"0123456789abcdef"
    (tmp_path / "seekable.bin").write_bytes(content)
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0

    fs = ReflakeFileSystem(dataset_roots={"ds": tmp_path})
    with fs.open("reflake://ds@main/seekable.bin", "rb") as handle:
        assert handle.seekable()
        handle.seek(5)
        assert handle.read(3) == b"567"
        assert handle.tell() == 8
        handle.seek(0)
        assert handle.read(4) == b"0123"


def test_blob_stream_large_file_via_filesystem(tmp_path: Path) -> None:
    blob_size = 10 * 1024 * 1024  # 10 MB
    data = b"X" * blob_size
    (tmp_path / "large.bin").write_bytes(data)
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "large"]) == 0

    fs = ReflakeFileSystem(dataset_roots={"ds": tmp_path})
    position = [0]

    class TrackingReadFile:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def read(self, size: int = -1) -> bytes:
            chunk = self._inner.read(size)
            position[0] += len(chunk)
            return chunk

    repo = fs._repository(tmp_path)
    commit_id = repo.resolve_ref("main")
    commit = repo.read_commit(commit_id)
    entry = repo.resolve_entry("main", "large.bin")
    assert entry is not None and entry.blob_hash is not None

    raw = repo.open_blob_stream(entry.blob_hash)
    assert raw is not None
    chunk = raw.read(8192)
    assert len(chunk) == 8192
    raw.close()


def test_blob_stream_returns_type_from_open_blob_stream(tmp_path: Path) -> None:
    from reflake.core.vfs import _BlobReadFile

    (tmp_path / "f.txt").write_text("hello")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0

    fs = ReflakeFileSystem(dataset_roots={"ds": tmp_path})
    handle = fs._open("reflake://ds@main/f.txt", "rb")
    assert type(handle).__name__ == "_BlobReadFile"
    assert not isinstance(handle, io.BytesIO)
    handle.close()
