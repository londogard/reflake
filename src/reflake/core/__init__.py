from .client_state import LocalClientState
from .config import BaseConfig, LocalConfig, ReflakeConfig, S3Config, init_config
from .domain import (
    BlobIntegrityError,
    BranchRefState,
    CommitId,
    CommitObject,
    CommitStats,
    CorruptCommitError,
    DiffEntry,
    EmptyBranchError,
    FetchResult,
    FsckResult,
    GcResult,
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
    ReflakeError,
    RemoveResult,
    RepositoryObjectKind,
    ResetResult,
    SourceNotFoundError,
    StageChange,
    StageStatus,
    StorageUnavailableError,
    TransferEndpointError,
    UnknownCommitError,
    UnknownRefError,
    VerifyResult,
    WorktreeStaleError,
)
from .entry_codec import CorruptEntryError, Entry
from .hashing import DEFAULT_CHUNK_SIZE, blake3_digest_file, blake3_digest_stream
from .layout import ReflakeLayout, blob_relpath, initialize_reflake_layout
from .manifest import FileEntry, walk_files
from .objects import (
    BlobTransferBackend,
    LocalObjectStore,
    ObjectInventory,
    ObjectIO,
    ObjectStore,
    RefCas,
    S3ObjectMetadata,
    StoreInventory,
    TreeQuery,
    parse_s3_uri,
)
from .repository import (
    ReflakeRepository,
    create_repository,
    init_repository,
    open_repository,
)
from .repository_sync import fetch, pull, push

#: Optional extras resolved on first use:
#:   `reflake[query]` -> duckdb + pyarrow, `reflake[vfs]` -> fsspec.
_LAZY_OPTIONAL_NAMES: dict[str, str] = {
    "AnalyticalIndexPaths": ".query",
    "Predicate": ".query",
    "PrunedFileScan": ".query",
    "PruneResult": ".query",
    "Scan": ".query",
    "ScanStats": ".query",
    "build_analytical_index": ".query",
    "drop_analytical_index": ".query",
    "execute_scan": ".query",
    "parse_where_clause": ".query",
    "plan_pruned_scan": ".query",
    "prune_row_groups": ".query",
    "query_analytical_index": ".query",
    "ReflakeFileSystem": ".vfs",
    "ReflakeURI": ".vfs",
}


def __getattr__(name: str) -> object:
    """Import query/VFS helpers only when they are actually used.

    `import reflake` must not require pyarrow, duckdb, or fsspec: they are
    extras, and the CLI only needs them for `query`/VFS commands.
    """
    module_name = _LAZY_OPTIONAL_NAMES.get(name)
    if module_name is None:
        # S3 adapter/transfer/source names live in `.objects` (also lazy,
        # because they need the `s3` extra).
        from . import objects as _objects

        value = getattr(_objects, name)
    else:
        from importlib import import_module

        extra = "vfs" if module_name == ".vfs" else "query"
        try:
            module = import_module(module_name, __name__)
        except ModuleNotFoundError as error:
            from .extras import EXTRAS

            package = (error.name or "").split(".", 1)[0]
            raise ReflakeError(
                f"{EXTRAS[extra]} needs the optional dependency '{package}'. "
                f"Install it with: pip install 'reflake[{extra}]'"
            ) from error
        value = getattr(module, name)
    globals()[name] = value
    return value

__all__ = [
    "DEFAULT_CHUNK_SIZE",
    "BaseConfig",
    "ReflakeConfig",
    "LocalConfig",
    "S3Config",
    "init_config",
    "AnalyticalIndexPaths",
    "LocalClientState",
    "BlobTransferBackend",
    "CommitId",
    "CommitObject",
    "CommitStats",
    "DiffEntry",
    "ReflakeFileSystem",
    "ReflakeRepository",
    "ReflakeError",
    "MergeConflictError",
    "MergeResult",
    "NonFastForwardError",
    "NotARepositoryError",
    "ObjectMissingError",
    "create_repository",
    "init_repository",
    "open_repository",
    "RefConflictError",
    "S3BlobTransferBackend",
    "S5CmdBlobTransferBackend",
    "StageChange",
    "StageStatus",
    "ReflakeURI",
    "ReflakeLayout",
    "FileEntry",
    "BranchRefState",
    "LocalObjectStore",
    "BlobIntegrityError",
    "TransferEndpointError",
    "CorruptEntryError",
    "CorruptCommitError",
    "SourceNotFoundError",
    "Entry",
    "ObjectIO",
    "ObjectInventory",
    "OptimisticLockError",
    "PreconditionFailedError",
    "ObjectStore",
    "Predicate",
    "PrunedFileScan",
    "PruneResult",
    "RefCas",
    "S3ObjectStore",
    "Scan",
    "ScanStats",
    "StorageUnavailableError",
    "StoreInventory",
    "TreeQuery",
    "UnknownCommitError",
    "UnknownRefError",
    "WorktreeStaleError",
    "EmptyBranchError",
    "build_analytical_index",
    "execute_scan",
    "parse_where_clause",
    "plan_pruned_scan",
    "prune_row_groups",
    "blake3_digest_file",
    "blake3_digest_stream",
    "build_blob_transfer_backend",
    "blob_relpath",
    "initialize_reflake_layout",
    "FetchResult",
    "FsckResult",
    "GcResult",
    "MoveResult",
    "PullResult",
    "PushResult",
    "RemoveResult",
    "ResetResult",
    "RepositoryObjectKind",
    "VerifyResult",
    "build_s3_client",
    "drop_analytical_index",
    "fetch",
    "pull",
    "push",
    "query_analytical_index",
    "S3ObjectMetadata",
    "walk_files",
    "iter_s3_objects",
    "open_source_uri",
    "parse_s3_uri",
]
