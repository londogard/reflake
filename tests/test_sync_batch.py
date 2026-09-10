"""Batch transfer direction: uploads, downloads, and error translation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from blake3 import blake3

from reflake.core.domain import BlobIntegrityError, StorageUnavailableError
from reflake.core.objects.backends import TransferItem, TransferPlan
from reflake.core.objects.transfer import S5CmdBlobTransferBackend
from reflake.core.repository_sync import execute_transfer_plan


class _FakeLocalStore:
    pass


class _FakeRemoteStore:
    pass


class RecordingBatchBackend(S5CmdBlobTransferBackend):
    def __init__(self) -> None:
        super().__init__()
        self.uploads: list[tuple[str, str]] = []
        self.downloads: list[tuple[str, str]] = []
        self.runs: list[list[str]] = []

    def _run(self, args: list[str], input_data: str | None = None):  # type: ignore[override]
        self.runs.append((list(args), input_data or ""))
        lines = (input_data or "").strip().splitlines()
        for line in lines:
            parts = line.split()
            # lines look like: cp [--if-not-exists] <src> <dst>
            src, dst = parts[-2], parts[-1]
            if src.startswith("s3://"):
                self.downloads.append((src, dst))
            else:
                self.uploads.append((src, dst))

        class _Result:
            stdout = ""
            stderr = ""
            returncode = 0

        return _Result()


def _plan(direction: str, tmp_path: Path) -> TransferPlan:
    """A two-item plan; the blob local file exists with matching content."""
    blob_payload = b"blob-payload"
    blob_path = tmp_path / "blob"
    blob_path.write_bytes(blob_payload)
    return TransferPlan(
        direction=direction,  # type: ignore[arg-type]
        items=(
            TransferItem(
                kind="blob",
                object_id=blake3(blob_payload).hexdigest(),
                local_path=str(blob_path),
                remote_uri="s3://b/blobs/a",
            ),
            TransferItem(
                kind="tree",
                object_id="t" * 64,
                local_path=str(tmp_path / "tree"),
                remote_uri="s3://b/trees/t",
            ),
        ),
    )


def test_batch_upload_uses_upload_order(tmp_path: Path) -> None:
    backend = RecordingBatchBackend()
    plan = _plan("upload", tmp_path)
    n = execute_transfer_plan(
        plan,
        local_store=_FakeLocalStore(),  # type: ignore[arg-type]
        remote_store=_FakeRemoteStore(),  # type: ignore[arg-type]
        batch_backend=backend,  # type: ignore[arg-type]
    )
    assert n == 2
    a_path = plan.items[0].local_path
    t_path = plan.items[1].local_path
    assert backend.uploads == [
        (a_path, "s3://b/blobs/a"),
        (t_path, "s3://b/trees/t"),
    ]
    assert backend.downloads == []


def test_batch_download_uses_download_order(tmp_path: Path) -> None:
    backend = RecordingBatchBackend()
    plan = _plan("download", tmp_path)
    n = execute_transfer_plan(
        plan,
        local_store=_FakeLocalStore(),  # type: ignore[arg-type]
        remote_store=_FakeRemoteStore(),  # type: ignore[arg-type]
        batch_backend=backend,  # type: ignore[arg-type]
    )
    assert n == 2
    a_path = plan.items[0].local_path
    t_path = plan.items[1].local_path
    assert backend.downloads == [
        ("s3://b/blobs/a", a_path),
        ("s3://b/trees/t", t_path),
    ]
    assert backend.uploads == []


def test_batch_download_verifies_blob_content(tmp_path: Path) -> None:
    backend = RecordingBatchBackend()
    plan = _plan("download", tmp_path)
    # Corrupt the "downloaded" blob: verification must reject it.
    Path(plan.items[0].local_path).write_bytes(b"tampered")

    with pytest.raises(BlobIntegrityError, match="hash mismatch"):
        execute_transfer_plan(
            plan,
            local_store=_FakeLocalStore(),  # type: ignore[arg-type]
            remote_store=_FakeRemoteStore(),  # type: ignore[arg-type]
            batch_backend=backend,  # type: ignore[arg-type]
        )


def test_batch_rejects_newlines_in_paths() -> None:
    backend = S5CmdBlobTransferBackend()
    plan = TransferPlan(
        direction="upload",
        items=(
            TransferItem(
                kind="blob",
                object_id="a" * 64,
                local_path="/tmp/a\ncp /etc/passwd s3://evil/x",
                remote_uri="s3://b/blobs/a",
            ),
        ),
    )
    with pytest.raises(ValueError, match="newline"):
        backend.transfer(plan)


def test_plan_rejects_unknown_direction() -> None:
    with pytest.raises(ValueError, match="direction"):
        TransferPlan(direction="sideways", items=())  # type: ignore[arg-type]


def test_missing_s5cmd_binary_is_a_domain_error(tmp_path: Path) -> None:
    backend = S5CmdBlobTransferBackend(s5cmd_path="definitely-not-installed")
    source = tmp_path / "blob"
    source.write_bytes(b"payload")

    with pytest.raises(StorageUnavailableError, match="not found"):
        backend.upload(str(source), "s3://b/blobs/x")


def test_failing_s5cmd_command_is_translated(tmp_path: Path) -> None:
    # `python ls <uri>` exits non-zero: the failure must surface as a domain
    # error (never a raw CalledProcessError).
    backend = S5CmdBlobTransferBackend(s5cmd_path=sys.executable)

    with pytest.raises(StorageUnavailableError, match="failed"):
        backend.delete("s3://b/blobs/x")

    assert backend.exists("s3://b/blobs/x") is False
