"""Object store package: protocols, adapters, backends, and source access.

This package merges the former ``repository_store/`` and ``storage/``
packages (docs/architecture.md §12): one ``ObjectStore`` protocol, two
adapters (``LocalObjectStore`` / ``S3ObjectStore``), plus the transfer
backends and source-URI access the services layer needs.

The ``boto3`` name is explicitly imported and re-exposed so that test
fixtures that monkeypatch ``reflake.core.objects.boto3.client`` continue to
work.
"""

from __future__ import annotations

import boto3  # noqa: F401  – kept for monkeypatching in tests

from ..domain import BranchRefState  # noqa: F401 – re-exported for callers

from .backends import (
    BlobTransferBackend,
    LocalStorageBackend,
    S3ObjectMetadata,
    SourceObjectMetadata,
    StorageBackend,
)
from .base import (
    ObjectIO,
    ObjectStore,
    RefCas,
    StoreInventory,
    TreeQuery,
)
from .local import LocalObjectStore
from .s3 import S3ObjectStore
from .source import (
    S3StorageBackend,
    _s3_is_404,
    _s3_is_precondition_failed,
    build_s3_client,
    describe_source_uri,
    iter_s3_objects,
    open_source_uri,
    parse_s3_uri,
)
from .transfer import (
    S3BlobTransferBackend,
    S5CmdBlobTransferBackend,
    build_blob_transfer_backend,
)

__all__ = [
    "BlobTransferBackend",
    "BranchRefState",
    "LocalObjectStore",
    "LocalStorageBackend",
    "ObjectIO",
    "ObjectStore",
    "RefCas",
    "S3BlobTransferBackend",
    "S3ObjectMetadata",
    "S3ObjectStore",
    "S3StorageBackend",
    "S5CmdBlobTransferBackend",
    "SourceObjectMetadata",
    "StorageBackend",
    "StoreInventory",
    "TreeQuery",
    "build_blob_transfer_backend",
    "build_s3_client",
    "describe_source_uri",
    "iter_s3_objects",
    "open_source_uri",
    "parse_s3_uri",
    # Private helpers re-exported for adapter compatibility
    "_s3_is_404",
    "_s3_is_precondition_failed",
]
