from __future__ import annotations

from pathlib import Path
from typing import BinaryIO

from blake3 import blake3

DEFAULT_CHUNK_SIZE = 1024 * 1024


def blake3_digest_stream(stream: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    hasher = blake3()
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.hexdigest()


def blake3_digest_file(
    file_path: str | Path, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> str:
    with Path(file_path).open("rb") as handle:
        return blake3_digest_stream(handle, chunk_size=chunk_size)
