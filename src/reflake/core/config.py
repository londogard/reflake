from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal

import msgspec

CURRENT_FORMAT_VERSION = 1
SUPPORTED_FORMAT_VERSIONS: frozenset[int] = frozenset({1})

BackendType = Literal["local", "s3"]

CONFIG_FILENAME = "config.json"


class BaseConfig(msgspec.Struct, tag_field="backend"):
    format_version: int = CURRENT_FORMAT_VERSION
    dataset_root: str = "."
    default_branch: str = "main"
    identity: str = "blake3"
    transfer_backend: str | None = None
    parquet_footer: bool = False

    def __post_init__(self) -> None:
        if self.format_version not in SUPPORTED_FORMAT_VERSIONS:
            if self.format_version > CURRENT_FORMAT_VERSION:
                raise ValueError(
                    f"Repository format version {self.format_version} is newer than "
                    f"this version of reflake (supports {CURRENT_FORMAT_VERSION}). "
                    f"Please upgrade reflake to access this repository."
                )
            raise ValueError(
                f"Repository format version {self.format_version} is no longer supported. "
                f"Run 'reflake migrate' to upgrade the repository."
            )
        if not self.dataset_root:
            raise ValueError("dataset_root must not be empty")
        if not self.default_branch:
            raise ValueError("default_branch must not be empty")

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
        root_path = Path(self.dataset_root)
        if not root_path.exists():
            raise ValueError(f"Local dataset root does not exist: {self.dataset_root}")


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
