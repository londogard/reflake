"""Pointer drift detection (`identity verify --drift`) and source client threading."""

from __future__ import annotations

from pathlib import Path

import pytest

from reflake import run_cli
from reflake.core import create_repository, open_repository


def test_verify_drift_detects_changed_local_source(tmp_path: Path, capsys) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    source = tmp_path / "incoming.txt"
    source.write_text("v1")

    assert run_cli(["--repo", str(repo_root), "init"]) == 0
    capsys.readouterr()
    assert (
        run_cli(
            [
                "--repo",
                str(repo_root),
                "add",
                "--identity",
                "pointer",
                "--as",
                "imports/incoming.txt",
                str(source),
            ]
        )
        == 0
    )
    assert (
        run_cli(
            ["--repo", str(repo_root), "commit", "--staged", "-m", "pointer"]
        )
        == 0
    )
    capsys.readouterr()

    repo = create_repository(str(repo_root))
    clean = repo.verify(drift_check=True)
    assert clean.drifted_paths == []

    source.write_text("v2-changed-size")
    drifted = repo.verify(drift_check=True)
    assert drifted.drifted_paths == ["imports/incoming.txt"]

    # Read-only audit reports drift and exits 1.
    exit_code = run_cli(
        ["--repo", str(repo_root), "identity", "verify", "--drift"]
    )
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "drifted" in captured.err
    # Nothing was written by the audit.
    assert repo.head_commit() == drifted.commit_id


def test_s3_source_access_uses_repository_client(
    tmp_path: Path,
    fake_s3_installer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An S3-backed repo must reuse its configured client for source access."""
    client = fake_s3_installer({"source.txt": b"payload"})
    monkeypatch.setattr(
        "reflake.core.objects.source.build_s3_client",
        lambda *args, **kwargs: pytest.fail(
            "source access created a default boto3 client instead of "
            "reusing the repository client"
        ),
    )

    repo = open_repository(
        "s3://demo-bucket/repos/source-threading",
        s3_client=client,
        worktree=tmp_path,
    )
    repo.add(["s3://demo-bucket/source.txt"], identity_mode="pointer")
    commit_id = repo.commit("pointer import")

    entry = repo.store.lookup_entry(repo.read_commit(commit_id).tree, "source.txt")
    assert entry is not None
    assert entry.identity_mode == "pointer"
    assert entry.source_etag, "pointer entries from S3 must record the source ETag"

    # Overwrite the source: metadata-only drift check catches it.
    client.put_object(
        Bucket="demo-bucket", Key="source.txt", Body=b"a much longer payload"
    )
    report = repo.verify(drift_check=True)
    assert report.drifted_paths == ["source.txt"]
