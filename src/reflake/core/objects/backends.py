"""Low-level storage backends and transfer protocols.

``StorageBackend`` is the raw bytes backend over a repository-relative path;
``BlobTransferBackend`` is the pluggable batch-transfer interface used by
sync (§7 of docs/architecture.md).  Both object-store adapters
(``LocalObjectStore`` / ``S3ObjectStore`` in this package) and the services
layer depend on these names.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Protocol, Sequence

from ..domain import OptimisticLockError


class StorageBackend(Protocol):
    def read_bytes(self, relative_path: str) -> bytes: ...

    def write_bytes(
        self,
        relative_path: str,
        data: bytes,
        *,
        if_none_match: bool = False,
    ) -> None: ...

    def exists(self, relative_path: str) -> bool: ...

    def ensure_dir(self, relative_path: str) -> None: ...

    def iter_keys(self, prefix: str = "") -> Iterator[str]: ...

    def etag(self, relative_path: str) -> str | None: ...


@dataclass(frozen=True)
class S3ObjectMetadata:
    bucket: str
    key: str
    size: int
    mtime_ns: int

    @property
    def source_uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


@dataclass(frozen=True)
class SourceObjectMetadata:
    source_uri: str
    size: int
    mtime_ns: int


class BlobTransferBackend(Protocol):
    """Interface for blob storage transfer operations.

    Implementations can use any tool (boto3, s5cmd, awscli, etc.)
    Swappable to suit different environments and performance needs.
    """

    def upload(
        self,
        local_path: str,
        remote_uri: str,
        *,
        if_not_exists: bool = False,
    ) -> None: ...

    def download(self, remote_uri: str, local_path: str) -> None: ...

    def list_objects(self, uri_prefix: str) -> Iterator[S3ObjectMetadata]: ...

    def delete(self, remote_uri: str) -> None: ...

    def exists(self, remote_uri: str) -> bool: ...

    def supports_batch(self) -> bool:
        """True when the backend can execute a batch of copies at once."""
        ...

    def upload_batch(
        self,
        pairs: Sequence[tuple[str, str]],
        *,
        if_not_exists: bool = False,
    ) -> int:
        """Execute many ``(local_path, remote_uri)`` copies; returns count."""
        ...


class LocalStorageBackend:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()

    def _full_path(self, relative_path: str) -> Path:
        return self.root / relative_path

    def read_bytes(self, relative_path: str) -> bytes:
        return self._full_path(relative_path).read_bytes()

    def write_bytes(
        self,
        relative_path: str,
        data: bytes,
        *,
        if_none_match: bool = False,
    ) -> None:
        file_path = self._full_path(relative_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if if_none_match and file_path.exists():
            raise OptimisticLockError(f"Path already exists: {relative_path}")
        file_path.write_bytes(data)

    def exists(self, relative_path: str) -> bool:
        return self._full_path(relative_path).exists()

    def ensure_dir(self, relative_path: str) -> None:
        self._full_path(relative_path).mkdir(parents=True, exist_ok=True)

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        base = self._full_path(prefix)
        if not base.exists():
            return
        for file_path in base.rglob("*"):
            if file_path.is_file():
                yield str(file_path.relative_to(self.root))

    def etag(self, relative_path: str) -> str | None:
        if not self.exists(relative_path):
            return None
        stat = self._full_path(relative_path).stat()
        return f"{stat.st_mtime_ns}-{stat.st_size}"
