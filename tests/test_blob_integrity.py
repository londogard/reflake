"""Local blob writes are atomic and hash-verified."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from blake3 import blake3

from reflake.core import create_repository


def test_write_blob_stream_verifies_hash(tmp_path: Path) -> None:
    repo = create_repository(str(tmp_path / "repo"))
    store = repo.store
    good = b"hello-world"
    digest = blake3(good).hexdigest()
    store.write_blob_stream(digest, io.BytesIO(good))
    assert store.read_blob_bytes(digest) == good

    # Same content again is idempotent (no re-verify, no error).
    store.write_blob_stream(digest, io.BytesIO(b"tampered-bytes"))
    assert store.read_blob_bytes(digest) == good

    # A *new* hash with non-matching bytes must fail without residue.
    other = blake3(b"expected-bytes").hexdigest()
    with pytest.raises(ValueError, match="hash mismatch"):
        store.write_blob_stream(other, io.BytesIO(b"tampered-bytes"))
    assert not store.object_exists("blob", other)


def test_write_blob_file_is_atomic_and_verified(tmp_path: Path) -> None:
    repo = create_repository(str(tmp_path / "repo"))
    store = repo.store
    src = tmp_path / "src.bin"
    src.write_bytes(b"file-payload")
    digest = blake3(b"file-payload").hexdigest()
    store.write_blob_file(digest, src)
    assert store.read_blob_bytes(digest) == b"file-payload"

    other = blake3(b"expected-bytes").hexdigest()
    src.write_bytes(b"other-bytes")
    with pytest.raises(ValueError, match="hash mismatch"):
        store.write_blob_file(other, src)
    assert not store.object_exists("blob", other)
    assert store.read_blob_bytes(digest) == b"file-payload"
