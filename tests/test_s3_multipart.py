"""Large-blob transfer: multipart uploads, streaming, and hash verification."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from blake3 import blake3

import reflake.core.objects.multipart as multipart
from reflake.core import BlobIntegrityError
from reflake.core.objects.s3 import S3ObjectStore
from reflake.core.objects.transfer import S3BlobTransferBackend

PAYLOAD = b"0123456789abcdefghij"  # 20 bytes


@pytest.fixture
def small_parts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shrink multipart thresholds so 20-byte payloads exercise the path."""
    monkeypatch.setattr(multipart, "MULTIPART_THRESHOLD", 8)
    monkeypatch.setattr(multipart, "MULTIPART_PART_SIZE", 8)
    monkeypatch.setattr(multipart, "MIN_PART_SIZE", 4)


def _store(client) -> S3ObjectStore:
    return S3ObjectStore(bucket="demo-bucket", prefix="repo", client=client)


def test_large_blob_file_uses_multipart(
    tmp_path: Path, fake_s3_installer, small_parts
) -> None:
    client = fake_s3_installer({})
    store = _store(client)
    digest = blake3(PAYLOAD).hexdigest()
    path = tmp_path / "big.bin"
    path.write_bytes(PAYLOAD)

    store.write_blob_file(digest, path, if_missing=True)

    operations = [operation for operation, _ in client.multipart_calls]
    assert operations[0] == "create"
    assert operations[-1] == "complete"
    assert operations.count("part") >= 2
    assert store.read_blob_bytes(digest) == PAYLOAD

    # Deduplicated writes skip the upload entirely.
    client.multipart_calls.clear()
    store.write_blob_file(digest, path, if_missing=True)
    assert client.multipart_calls == []


def test_small_blob_uses_single_put(tmp_path: Path, fake_s3_installer) -> None:
    client = fake_s3_installer({})
    store = _store(client)
    digest = blake3(PAYLOAD).hexdigest()
    path = tmp_path / "small.bin"
    path.write_bytes(PAYLOAD)

    store.write_blob_file(digest, path, if_missing=True)

    assert client.multipart_calls == []
    assert store.read_blob_bytes(digest) == PAYLOAD


def test_stream_upload_verifies_hash_before_complete(
    fake_s3_installer, small_parts
) -> None:
    client = fake_s3_installer({})
    store = _store(client)
    digest = blake3(PAYLOAD).hexdigest()

    store.write_blob_stream(digest, io.BytesIO(PAYLOAD), if_missing=True)
    assert client.multipart_calls[-1][0] == "complete"
    assert store.read_blob_bytes(digest) == PAYLOAD

    # A mismatched stream must abort the multipart upload, not complete it.
    client.multipart_calls.clear()
    with pytest.raises(BlobIntegrityError):
        store.write_blob_stream("f" * 64, io.BytesIO(PAYLOAD), if_missing=True)
    assert ("abort", "repo/blobs/ff/" + "f" * 62) in client.multipart_calls
    assert not store.object_exists("blob", "f" * 64)


def test_transfer_backend_streams_both_directions(
    tmp_path: Path, fake_s3_installer, small_parts
) -> None:
    client = fake_s3_installer({})
    backend = S3BlobTransferBackend(client=client)
    source = tmp_path / "src.bin"
    source.write_bytes(PAYLOAD)
    uri = "s3://demo-bucket/blobs/ab/cd"

    backend.upload(str(source), uri, if_not_exists=True)
    assert any(operation == "complete" for operation, _ in client.multipart_calls)

    target = tmp_path / "out.bin"
    backend.download(uri, str(target))
    assert target.read_bytes() == PAYLOAD
