from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal

import msgspec

#: One format constant for both client config and the shared ``repo.json``.
#: Version 2 is the canonical-entry model introduced in 0.5.0.
from .repo_format import CURRENT_FORMAT_VERSION

SUPPORTED_FORMAT_VERSIONS: frozenset[int] = frozenset({CURRENT_FORMAT_VERSION})

BackendType = Literal["local", "s3"]

CONFIG_FILENAME = "config.json"


class BaseConfig(msgspec.Struct, tag_field="backend"):
    format_version: int = CURRENT_FORMAT_VERSION
    dataset_root: str = "."
    default_branch: str = "main"
    identity: str = "content"
    transfer_backend: str | None = None
    parquet_footer: bool = False
    #: Use the client-local stat cache (path -> size, mtime, digest) to skip
    #: re-reading unchanged worktree files in ``status``/``commit``. This is
    #: what makes both O(changes) instead of O(bytes); ``false`` re-hashes
    #: every file on every command. A same-size, same-mtime edit is not
    #: detected while enabled (the same trade every stat-based VCS makes).
    stat_cache: bool = True
    #: Copy large local blobs with a copy-on-write clone when the filesystem
    #: supports it (APFS clonefile / btrfs/XFS FICLONE) instead of a full
    #: byte copy. Set false to force plain copies.
    reflink_blobs: bool = True
    #: ``gc --prune`` never deletes objects younger than this many seconds.
    #: Protects objects a concurrent writer published between its object
    #: writes and its ref CAS (mark-and-sweep races with no coordination).
    gc_grace_seconds: int = 86400
    #: Flush local object/ref writes to stable storage. Disable only for
    #: disposable repositories or tests; the default is crash-safe.
    fsync: bool = True

    def __post_init__(self) -> None:
        if self.format_version not in SUPPORTED_FORMAT_VERSIONS:
            if self.format_version > CURRENT_FORMAT_VERSION:
                raise ValueError(
                    f"Repository format version {self.format_version} is newer than "
                    f"this version of reflake (supports {CURRENT_FORMAT_VERSION}). "
                    f"Please upgrade reflake to access this repository."
                )
            raise ValueError(
                f"Repository format version {self.format_version} is no longer "
                "supported: this repository was written by Reflake 0.4.x or "
                "older, whose tree objects use a different entry layout. "
                "Recreate the repository with this version (0.5.0 is a format "
                "break; see CHANGELOG), or read it with Reflake 0.4.x to "
                "export the data."
            )
        if not self.dataset_root:
            raise ValueError("dataset_root must not be empty")
        if not self.default_branch:
            raise ValueError("default_branch must not be empty")
        from .repository_support import validate_branch_name

        validate_branch_name(self.default_branch)

    @staticmethod
    def load(root: str | Path) -> ReflakeConfig | None:
        path = Path(root).resolve() / ".reflake" / CONFIG_FILENAME
        if not path.exists():
            return None
        return msgspec.json.decode(path.read_text("utf-8"), type=ReflakeConfig)

    def save(self, root: str | Path) -> Path:
        path = Path(root).resolve() / ".reflake" / CONFIG_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = msgspec.json.encode(self) + b"\n"
        with NamedTemporaryFile(dir=path.parent, delete=False) as temp:
            temp_path = Path(temp.name)
            temp.write(payload)
        try:
            os.replace(temp_path, path)
        finally:
            temp_path.unlink(missing_ok=True)
        return path

    def validate(self) -> None:
        pass


class LocalConfig(BaseConfig, tag="local"):
    def validate(self) -> None:
        # ``dataset_root`` is informational. It must not make a repository
        # unopenable after the directory is moved or copied; the repository
        # root is the argument to init/open, not a stored absolute path.
        return None


class S3Config(BaseConfig, tag="s3"):
    bucket: str = ""
    prefix: str = ""
    endpoint_url: str | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.bucket:
            raise ValueError("S3 backend requires a bucket")


ReflakeConfig = LocalConfig | S3Config


def init_config(
    root: str | Path,
    *,
    backend: BackendType = "local",
    default_branch: str = "main",
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
    s3_endpoint_url: str | None = None,
) -> ReflakeConfig:
    dataset_root = str(Path(root).resolve())
    if backend == "s3":
        if not s3_bucket:
            raise ValueError("S3 backend requires a bucket")
        config: ReflakeConfig = S3Config(
            dataset_root=dataset_root,
            default_branch=default_branch,
            bucket=s3_bucket,
            prefix=s3_prefix or "",
            endpoint_url=s3_endpoint_url,
        )
    else:
        config = LocalConfig(
            dataset_root=dataset_root,
            default_branch=default_branch,
        )
    config.validate()
    return config
