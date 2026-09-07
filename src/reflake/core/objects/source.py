"""Source-URI access and the S3 storage backend.

``open_source_uri`` / ``describe_source_uri`` / ``iter_s3_objects`` open
external data sources (``file://``, local paths, ``s3://``) for staging,
restore, and the virtual filesystem.  ``S3StorageBackend`` is the raw
bytes backend over a bucket/prefix.

All botocore errors are translated at this boundary into domain errors
(``ObjectMissingError`` / ``PreconditionFailedError`` /
``StorageUnavailableError``), so no caller ever sees a ``ClientError``
(§8 of docs/architecture.md).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import unquote, urlparse

import boto3
import fsspec
from botocore.exceptions import ClientError

from ..domain import (
    ObjectMissingError,
    PreconditionFailedError,
    StorageUnavailableError,
)
from .backends import S3ObjectMetadata, SourceObjectMetadata


def _s3_is_404(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code", "") in {
        "404",
        "NoSuchKey",
        "NotFound",
    }


def build_s3_client(endpoint_url: str | None = None) -> Any:
    """Create a boto3 S3 client, honoring an explicit endpoint when given."""
    if endpoint_url:
        return boto3.client("s3", endpoint_url=endpoint_url)
    return boto3.client("s3")


def _s3_is_precondition_failed(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code", "") in {
        "PreconditionFailed",
        "412",
    }


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _translate_s3_error(error: ClientError, operation: str) -> Exception:
    if _s3_is_404(error):
        return ObjectMissingError(f"{operation}: object not found")
    if _s3_is_precondition_failed(error):
        return PreconditionFailedError(f"{operation}: precondition failed")
    return StorageUnavailableError(f"{operation}: {error}")


def _mtime_ns(value: object) -> int:
    if isinstance(value, datetime):
        timestamp = value
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        return int(timestamp.timestamp() * 1_000_000_000)
    return 0


def iter_s3_objects(
    source_uri: str,
    *,
    client: Any | None = None,
) -> Iterator[S3ObjectMetadata]:
    bucket, prefix = parse_s3_uri(source_uri)
    s3_client = client or build_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                key = str(obj["Key"])
                yield S3ObjectMetadata(
                    bucket=bucket,
                    key=key,
                    size=int(obj.get("Size", 0)),
                    mtime_ns=_mtime_ns(obj.get("LastModified")),
                )
    except ClientError as error:
        raise _translate_s3_error(error, "list_objects") from error


def describe_source_uri(
    source_uri: str,
    *,
    client: Any | None = None,
) -> SourceObjectMetadata:
    if source_uri.startswith("s3://"):
        bucket, key = parse_s3_uri(source_uri)
        if not key or key.endswith("/"):
            raise ValueError(f"S3 source must be an object, not a prefix: {source_uri}")
        s3_client = client or build_s3_client()
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if _s3_is_404(error):
                raise FileNotFoundError(
                    f"Cannot stage missing file: {source_uri}"
                ) from error
            raise _translate_s3_error(error, "head_object") from error
        return SourceObjectMetadata(
            source_uri=source_uri,
            size=int(response.get("ContentLength", 0)),
            mtime_ns=_mtime_ns(response.get("LastModified")),
        )

    parsed = urlparse(source_uri)
    if parsed.scheme == "file":
        path = Path(unquote(parsed.path)).resolve()
    else:
        path = Path(source_uri).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Cannot stage missing file: {path}")
    stat = path.stat()
    return SourceObjectMetadata(
        source_uri=path.as_uri(),
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
    )


@contextmanager
def open_source_uri(
    source_uri: str,
    *,
    client: Any | None = None,
) -> Iterator[BinaryIO]:
    if source_uri.startswith("s3://"):
        bucket, key = parse_s3_uri(source_uri)
        s3_client = client or build_s3_client()
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
        except ClientError as error:
            raise _translate_s3_error(error, "get_object") from error
        body = response["Body"]
        try:
            yield body
        finally:
            body.close()
        return
    parsed = urlparse(source_uri)
    if parsed.scheme == "file":
        path = Path(unquote(parsed.path))
        with path.open("rb") as handle:
            yield handle
        return
    with fsspec.open(source_uri, mode="rb") as handle:
        yield handle  # type: ignore[invalid-yield]


class S3StorageBackend:
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        client: Any | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client or build_s3_client()

    def _key(self, relative_path: str) -> str:
        if self.prefix:
            return f"{self.prefix}/{relative_path.lstrip('/')}"
        return relative_path.lstrip("/")

    def read_bytes(self, relative_path: str) -> bytes:
        try:
            response = self.client.get_object(
                Bucket=self.bucket, Key=self._key(relative_path)
            )
        except ClientError as error:
            raise _translate_s3_error(error, "read_bytes") from error
        return response["Body"].read()

    def write_bytes(
        self,
        relative_path: str,
        data: bytes,
        *,
        if_none_match: bool = False,
    ) -> None:
        kwargs = {
            "Bucket": self.bucket,
            "Key": self._key(relative_path),
            "Body": data,
        }
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
        except ClientError as error:
            if if_none_match and _s3_is_precondition_failed(error):
                raise PreconditionFailedError(
                    f"Path already exists: {relative_path}"
                ) from error
            raise _translate_s3_error(error, "write_bytes") from error

    def exists(self, relative_path: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(relative_path))
            return True
        except ClientError as error:
            if _s3_is_404(error):
                return False
            raise _translate_s3_error(error, "exists") from error

    def ensure_dir(self, _relative_path: str) -> None:
        return None

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        key_prefix = self._key(prefix)
        try:
            pages = paginator.paginate(Bucket=self.bucket, Prefix=key_prefix)
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if self.prefix and key.startswith(f"{self.prefix}/"):
                        key = key[len(self.prefix) + 1 :]
                    yield key
        except ClientError as error:
            raise _translate_s3_error(error, "list_objects") from error

    def delete(self, relative_path: str) -> None:
        try:
            self.client.delete_object(Bucket=self.bucket, Key=self._key(relative_path))
        except ClientError as error:
            raise _translate_s3_error(error, "delete") from error

    def etag(self, relative_path: str) -> str | None:
        try:
            response = self.client.head_object(
                Bucket=self.bucket, Key=self._key(relative_path)
            )
        except ClientError as error:
            if _s3_is_404(error):
                return None
            raise _translate_s3_error(error, "etag") from error
        return response.get("ETag", "").strip('"') or None
