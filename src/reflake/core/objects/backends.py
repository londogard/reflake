"""Low-level storage backends and transfer protocols.

``S3ObjectMetadata`` / ``SourceObjectMetadata`` describe external objects, and
``BlobTransferBackend`` is the pluggable batch-transfer interface used by §7
of docs/architecture.md (plan-then-batch sync).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Literal, Protocol


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


TransferDirection = Literal["upload", "download"]


@dataclass(frozen=True)
class TransferItem:
    """One object copy in a transfer plan (docs/architecture.md §7)."""

    kind: str
    object_id: str
    local_path: str
    remote_uri: str


@dataclass(frozen=True)
class TransferPlan:
    """A batch of object copies in one direction."""

    direction: TransferDirection
    items: tuple[TransferItem, ...] = ()

    def __post_init__(self) -> None:
        if self.direction not in ("upload", "download"):
            raise ValueError(
                f"Transfer direction must be 'upload' or 'download', "
                f"got: {self.direction!r}"
            )


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
        """True when the backend can execute a whole plan at once."""
        ...

    def transfer(self, plan: TransferPlan) -> int:
        """Execute every item in *plan*; returns the transferred count."""
        ...
