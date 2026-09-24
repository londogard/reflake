"""Reflake: serverless, object-storage-first data versioning.

The package root exports only the **stable public surface** documented in
the README:

- construction: ``init_repository`` / ``create_repository`` / ``open_repository``
- repository: ``ReflakeRepository``
- reads: ``ReflakeFileSystem`` / ``ReflakeURI``
- sync: ``push`` / ``pull`` / ``fetch``
- data types returned by those APIs
- errors: ``ReflakeError`` and subclasses

Everything else (stores, codecs, tree writers, pruning internals) lives in
``reflake.core`` and carries no stability guarantee — import it explicitly
when you need it, and expect it to move between minors.
"""

from .cli import build_parser, main, run_cli
from .core import (
    DEFAULT_CHUNK_SIZE,
    BaseConfig,
    BlobIntegrityError,
    CommitObject,
    CorruptCommitError,
    DiffEntry,
    EmptyBranchError,
    FetchResult,
    FileEntry,
    GcResult,
    LocalConfig,
    MergeConflictError,
    MergeResult,
    MoveResult,
    NonFastForwardError,
    NotARepositoryError,
    ObjectMissingError,
    OptimisticLockError,
    PreconditionFailedError,
    PullResult,
    PushResult,
    RefConflictError,
    ReflakeConfig,
    ReflakeError,
    ReflakeFileSystem,
    ReflakeRepository,
    ReflakeURI,
    RemoveResult,
    ResetResult,
    S3Config,
    SourceNotFoundError,
    StageChange,
    StageStatus,
    StorageUnavailableError,
    TransferEndpointError,
    UnknownCommitError,
    UnknownRefError,
    VerifyResult,
    create_repository,
    fetch,
    init_repository,
    open_repository,
    pull,
    push,
)

__all__ = [
    # construction / repository
    "init_repository",
    "create_repository",
    "open_repository",
    "ReflakeRepository",
    # reads
    "ReflakeFileSystem",
    "ReflakeURI",
    # sync
    "push",
    "pull",
    "fetch",
    # types returned by the API
    "CommitObject",
    "DiffEntry",
    "FileEntry",
    "StageChange",
    "StageStatus",
    "MergeResult",
    "ResetResult",
    "RemoveResult",
    "MoveResult",
    "GcResult",
    "VerifyResult",
    "FetchResult",
    "PullResult",
    "PushResult",
    # config
    "BaseConfig",
    "LocalConfig",
    "S3Config",
    "ReflakeConfig",
    # errors
    "ReflakeError",
    "RefConflictError",
    "NonFastForwardError",
    "UnknownRefError",
    "UnknownCommitError",
    "EmptyBranchError",
    "BlobIntegrityError",
    "ObjectMissingError",
    "StorageUnavailableError",
    "TransferEndpointError",
    "MergeConflictError",
    "NotARepositoryError",
    "OptimisticLockError",
    "PreconditionFailedError",
    "SourceNotFoundError",
    "CorruptCommitError",
    # CLI entry points
    "main",
    "run_cli",
    "build_parser",
    # constants
    "DEFAULT_CHUNK_SIZE",
]
