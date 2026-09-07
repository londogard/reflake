"""Regression tests for the 2026-08 review findings.

Covers the staged-overlay sort-order bug, merge-aware ancestry (is_ancestor,
fast-forward, push after a 3-way merge), quote-safe manifest index parsing,
streaming three-way merge output, S3 endpoint wiring, and the batch transfer
backend API.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from reflake import run_cli
from reflake.core import create_repository, open_repository
from reflake.core.domain import StageChange
from reflake.core.entry_codec import Entry
from reflake.core.manifest import ManifestWriter
from reflake.core.objects.transfer import (
    S3BlobTransferBackend,
    S5CmdBlobTransferBackend,
)
from reflake.core.repository_sync import push


def _make_commit(repo, name: str, message: str) -> str:
    path = repo.root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"content-{name}")
    return repo.commit(message)


def _stage_add(repo, path: str, *, source_uri: str | None = None) -> None:
    repo.staging.save(
        "main",
        {
            **repo.staging.load("main"),
            path: StageChange(
                path=path,
                action="add",
                identity_mode="content",
                source_uri=source_uri or (repo.root / path).as_uri(),
            ),
        },
    )


# ── B1: commit --staged with an alphabetically-earlier new path ──────────────


def test_staged_commit_with_earlier_sorting_path(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    _make_commit(repo, "z.txt", "base")

    (tmp_path / "a.txt").write_text("a-content")
    _stage_add(repo, "a.txt")

    commit_id = repo.commit("staged add of a.txt", staged_only=True)
    assert commit_id
    assert sorted(repo.resolve_entries("main")) == ["a.txt", "z.txt"]


def test_staged_commit_with_nested_earlier_sorting_paths(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    _make_commit(repo, "t/top.txt", "base")

    (tmp_path / "a" / "deep.txt").parent.mkdir(parents=True)
    (tmp_path / "a" / "deep.txt").write_text("deep")
    (tmp_path / "b_first.txt").write_text("b")
    _stage_add(repo, "a/deep.txt")
    _stage_add(repo, "b_first.txt")

    repo.commit("nested staged adds", staged_only=True)
    assert sorted(repo.resolve_entries("main")) == [
        "a/deep.txt",
        "b_first.txt",
        "t/top.txt",
    ]


def test_cli_staged_commit_with_earlier_sorting_path(
    tmp_path: Path, capsys
) -> None:
    (tmp_path / "zeta.txt").write_text("zeta")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "base"]) == 0
    capsys.readouterr()

    (tmp_path / "alpha.txt").write_text("alpha")
    assert run_cli(["--repo", str(tmp_path), "add", "alpha.txt"]) == 0
    capsys.readouterr()

    assert (
        run_cli(["--repo", str(tmp_path), "commit", "--staged", "-m", "alpha"]) == 0
    )
    commit_id = capsys.readouterr().out.strip()
    assert commit_id

    assert run_cli(["--repo", str(tmp_path), "--json", "list"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert sorted(item["path"] for item in payload) == ["alpha.txt", "zeta.txt"]


# ── B2: merge-aware ancestry ─────────────────────────────────────────────────


def _build_diverged_and_merged(repo):
    base = _make_commit(repo, "base.txt", "base")
    repo.store.write_branch_ref("feature", base)
    repo.set_current_branch("feature")
    feature_head = _make_commit(repo, "feat/feature.txt", "feature work")

    repo.set_current_branch("main")
    main_head = _make_commit(repo, "main/main.txt", "main work")
    assert main_head != feature_head

    result = repo.merge("feature", "main")
    return base, feature_head, main_head, result.commit_id


def test_is_ancestor_traverses_merge_second_parent(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    _, feature_head, _, merge_commit = _build_diverged_and_merged(repo)

    assert repo.refs.is_ancestor(
        ancestor_commit=feature_head, descendant_commit=merge_commit
    )
    assert not repo.refs.is_ancestor(
        ancestor_commit=merge_commit, descendant_commit=feature_head
    )


def test_fast_forward_onto_merge_commit_after_merge(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    _, feature_head, _, merge_commit = _build_diverged_and_merged(repo)

    assert repo.fast_forward_branch("feature", merge_commit, operation="test")


def test_push_after_merge_transfers_merged_lineage(
    tmp_path: Path, fake_s3_installer
) -> None:
    from reflake.core.repository_support import collect_ancestors

    fake_s3_installer({})
    remote = "s3://demo-bucket/repos/merge-push"

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    repo = create_repository(repo_root)
    _, _, _, merge_commit = _build_diverged_and_merged(repo)

    result = push(repo, remote)
    assert result.updated

    remote_repo = open_repository(remote, worktree=repo.root)
    for commit_id in collect_ancestors(merge_commit, read_commit=repo.read_commit):
        assert remote_repo.store.object_exists("commit", commit_id), commit_id
    remote_state = remote_repo.store.read_branch_ref("main")
    assert remote_state is not None and remote_state.commit_id == merge_commit


# ── B4: quote-safe manifest index extraction ────────────────────────────────


def test_manifest_index_extraction_handles_quoted_paths(tmp_path: Path) -> None:
    weird = 'weird"name.txt'
    writer = ManifestWriter(tmp_path / "derived.jsonl", block_entry_count=1)
    written = writer.write_entries(
        [
            Entry(path=weird, kind="b", hash="0" * 64, size=1, mtime_ns=2),
        ]
    )
    assert written == 1
    index = writer.build_index()
    assert index is not None
    assert index.blocks[0].first_path == weird


def test_staged_flow_with_quoted_filename(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    weird = 'weird"name.txt'
    _make_commit(repo, "z.txt", "base")

    (tmp_path / weird).write_text("quoted")
    _stage_add(repo, weird)
    repo.commit("add quoted", staged_only=True)

    tree_hash = repo.refs.read_commit(repo.head_commit()).tree
    exported = repo.tree_writer.export_derived_manifest(tree_hash)
    assert exported.exists()
    entry = repo.tree_writer.lookup_derived_entry(tree_hash, weird)
    assert entry is not None and entry.path == weird


# ── Streaming three-way merge keeps global sort order ───────────────────────


def test_three_way_merge_output_stays_sorted_at_both_ends(tmp_path: Path) -> None:
    repo = create_repository(tmp_path)
    base = _make_commit(repo, "m/mid.txt", "base")
    repo.store.write_branch_ref("theirs", base)
    repo.set_current_branch("theirs")
    _make_commit(repo, "a/a_first.txt", "theirs adds low path")

    repo.set_current_branch("main")
    _make_commit(repo, "z/z_last.txt", "ours adds high path")

    result = repo.merge("theirs", "main")
    entries = sorted(repo.resolve_entries("main"))
    assert entries == [
        "a/a_first.txt",
        "m/mid.txt",
        "z/z_last.txt",
    ]
    assert result.commit_id


# ── B6: S3 endpoint_url wiring ───────────────────────────────────────────────


def test_endpoint_url_from_config_reaches_s3_client(
    tmp_path: Path, monkeypatch, fake_s3_installer
) -> None:
    captured: list[dict[str, Any]] = []
    real_client = fake_s3_installer({})

    def recording_client(service_name: str, **kwargs: Any):
        captured.append(kwargs)
        return real_client

    monkeypatch.setattr("reflake.core.objects.boto3.client", recording_client)

    from reflake.core.config import init_config

    config = init_config(
        tmp_path,
        backend="s3",
        s3_bucket="demo-bucket",
        s3_endpoint_url="http://127.0.0.1:4566",
    )
    config.save(tmp_path)

    create_repository(tmp_path)
    assert captured, "expected a boto3 client to be constructed"
    assert captured[-1].get("endpoint_url") == "http://127.0.0.1:4566"


# ── Batch transfer backend API ───────────────────────────────────────────────


def test_batch_transfer_backend_capabilities() -> None:
    assert S3BlobTransferBackend().supports_batch() is False
    assert S5CmdBlobTransferBackend().supports_batch() is True


def test_transfer_on_boto3_backend_uploads_each_item(tmp_path: Path) -> None:
    from reflake.core.objects.backends import TransferItem, TransferPlan

    class RecordingBackend(S3BlobTransferBackend):
        def __init__(self) -> None:
            super().__init__(client=object())
            self.uploads: list[tuple[str, str]] = []

        def upload(self, local_path: str, remote_uri: str, *, if_not_exists=False):
            self.uploads.append((local_path, remote_uri))

    backend = RecordingBackend()
    plan = TransferPlan(
        direction="upload",
        items=(
            TransferItem(kind="blob", object_id="k1", local_path="a",
                         remote_uri="s3://b/k1"),
            TransferItem(kind="blob", object_id="k2", local_path="c",
                         remote_uri="s3://b/k2"),
        ),
    )
    transferred = backend.transfer(plan)
    assert transferred == 2
    assert backend.uploads == [("a", "s3://b/k1"), ("c", "s3://b/k2")]
