"""Object store package: protocols, adapters, backends, and source access.

This package merges the former ``repository_store/`` and ``storage/``
packages (docs/architecture.md §12): one ``ObjectStore`` protocol, two
adapters (``LocalObjectStore`` / ``S3ObjectStore``), plus the transfer
backends and source-URI access the services layer needs.

``boto3``, ``fsspec`` and the S3 adapters are **optional**: they are installed
with ``reflake[s3]`` / ``reflake[vfs]``, so importing this package (and
therefore ``import reflake``) must not import them. S3-specific names are
resolved lazily by module ``__getattr__`` below; ``boto3`` is exposed the same
way so test fixtures can keep patching ``reflake.core.objects.boto3.client``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..domain import BranchRefState  # noqa: F401 – re-exported for callers
from .backends import (
    BlobTransferBackend,
    S3ObjectMetadata,
    SourceObjectMetadata,
    TransferDirection,
    TransferItem,
    TransferPlan,
)
from .base import (
    BlobRangeReader,
    ContentQueryStore,
    HasLocalPath,
    HasRemoteURI,
    ObjectInventory,
    ObjectIO,
    ObjectStore,
    QueryRefStore,
    RefCas,
    RefObjectStore,
    RepositoryStore,
    ScanStore,
    StoreInventory,
    TagRefs,
    TreeQuery,
)
from .local import LocalObjectStore
from .uris import is_s3_uri, parse_s3_uri

#: Names that pull ``boto3``/``botocore`` (the ``s3`` extra). Resolved on first
#: access so a local-only install never imports them.
_LAZY_S3_NAMES: dict[str, str] = {
    "S3ObjectStore": ".s3",
    "RangeFile": ".rangefile",
    "open_local_range_file": ".rangefile",
    "open_s3_range_file": ".rangefile",
    "open_source_range_file": ".rangefile",
    "_s3_is_404": ".source",
    "_s3_is_precondition_failed": ".source",
    "build_s3_client": ".source",
    "describe_source_uri": ".source",
    "iter_s3_objects": ".source",
    "open_source_uri": ".source",
    "source_client": ".source",
    "S3BlobTransferBackend": ".transfer",
    "S5CmdBlobTransferBackend": ".transfer",
    "build_blob_transfer_backend": ".transfer",
}


def __getattr__(name: str) -> Any:
    """Resolve optional-backend names on first use (PEP 562)."""
    if name == "boto3":
        import boto3

        return boto3
    module_name = _LAZY_S3_NAMES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


if TYPE_CHECKING:  # pragma: no cover - static surface only, never imported
    from .rangefile import (
        RangeFile,
        open_local_range_file,
        open_s3_range_file,
        open_source_range_file,
    )
    from .s3 import S3ObjectStore
    from .source import (
        _s3_is_404,
        _s3_is_precondition_failed,
        build_s3_client,
        describe_source_uri,
        iter_s3_objects,
        open_source_uri,
        source_client,
    )
    from .transfer import (
        S3BlobTransferBackend,
        S5CmdBlobTransferBackend,
        build_blob_transfer_backend,
    )

__all__ = [
    "BlobRangeReader",
    "BlobTransferBackend",
    "BranchRefState",
    "ContentQueryStore",
    "HasLocalPath",
    "HasRemoteURI",
    "LocalObjectStore",
    "ObjectIO",
    "ObjectInventory",
    "ObjectStore",
    "QueryRefStore",
    "RangeFile",
    "RefCas",
    "RefObjectStore",
    "RepositoryStore",
    "S3BlobTransferBackend",
    "S3ObjectMetadata",
    "S3ObjectStore",
    "S5CmdBlobTransferBackend",
    "ScanStore",
    "SourceObjectMetadata",
    "StoreInventory",
    "TagRefs",
    "TransferDirection",
    "TransferItem",
    "TransferPlan",
    "TreeQuery",
    "build_blob_transfer_backend",
    "build_s3_client",
    "describe_source_uri",
    "iter_s3_objects",
    "open_local_range_file",
    "open_s3_range_file",
    "open_source_range_file",
    "open_source_uri",
    "is_s3_uri",
    "parse_s3_uri",
    "source_client",
    # Private helpers re-exported for adapter compatibility
    "_s3_is_404",
    "_s3_is_precondition_failed",
]
