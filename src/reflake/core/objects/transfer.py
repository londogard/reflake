from __future__ import annotations

import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ..domain import BlobIntegrityError, ObjectMissingError, StorageUnavailableError
from ..hashing import blake3_digest_file
from .backends import S3ObjectMetadata, TransferPlan
from .source import S3StorageBackend, _mtime_ns, build_s3_client, parse_s3_uri


def _require_batch_safe(*values: str) -> None:
    """Reject control characters that would break line-delimited batch manifests.

    ``s5cmd run`` reads one command per line, so a newline (or carriage
    return) inside a path would inject extra commands. Fail loudly instead.
    """
    for value in values:
        if "\n" in value or "\r" in value:
            raise ValueError(
                "Batch transfer paths must not contain newline characters: "
                f"{value!r}"
            )


class S3BlobTransferBackend:
    """Blob transfer backend using boto3 directly."""

    def __init__(
        self,
        client: Any | None = None,
        *,
        endpoint_url: str | None = None,
    ) -> None:
        self._client = client or build_s3_client(endpoint_url)
        self._backends: dict[str, S3StorageBackend] = {}

    def supports_batch(self) -> bool:
        return False

    def transfer(self, plan: TransferPlan) -> int:
        if plan.direction == "upload":
            for item in plan.items:
                self.upload(item.local_path, item.remote_uri, if_not_exists=True)
        else:
            for item in plan.items:
                self.download(item.remote_uri, item.local_path)
        return len(plan.items)

    def _backend(self, remote_uri: str) -> tuple[S3StorageBackend, str]:
        bucket, key = parse_s3_uri(remote_uri)
        if bucket not in self._backends:
            self._backends[bucket] = S3StorageBackend(
                bucket, prefix="", client=self._client
            )
        return self._backends[bucket], key

    def upload(
        self,
        local_path: str,
        remote_uri: str,
        *,
        if_not_exists: bool = False,
    ) -> None:
        backend, key = self._backend(remote_uri)
        backend.write_bytes(
            key,
            Path(local_path).read_bytes(),
            if_none_match=if_not_exists,
        )

    def download(self, remote_uri: str, local_path: str) -> None:
        backend, key = self._backend(remote_uri)
        Path(local_path).write_bytes(backend.read_bytes(key))

    def list_objects(self, uri_prefix: str) -> Iterator[S3ObjectMetadata]:
        bucket, prefix = parse_s3_uri(uri_prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield S3ObjectMetadata(
                    bucket=bucket,
                    key=str(obj["Key"]),
                    size=int(obj.get("Size", 0)),
                    mtime_ns=_mtime_ns(obj.get("LastModified")),
                )

    def delete(self, remote_uri: str) -> None:
        backend, key = self._backend(remote_uri)
        backend.delete(key)

    def exists(self, remote_uri: str) -> bool:
        backend, key = self._backend(remote_uri)
        return backend.exists(key)


class S5CmdBlobTransferBackend:
    """Blob transfer backend using s5cmd for high-throughput S3 operations.

    Uses subprocess to shell out to s5cmd. Best for bulk operations
    where s5cmd's parallel transfer engine provides significant speedups.

    Requires s5cmd to be installed and available on PATH.
    """

    def __init__(
        self,
        s5cmd_path: str = "s5cmd",
        endpoint_url: str | None = None,
    ) -> None:
        self._s5cmd_path = s5cmd_path
        self._endpoint = endpoint_url

    def supports_batch(self) -> bool:
        return True

    def transfer(self, plan: TransferPlan) -> int:
        lines: list[str] = []
        if plan.direction == "upload":
            for item in plan.items:
                _require_batch_safe(item.local_path, item.remote_uri)
                lines.append(
                    f"cp --if-not-exists {item.local_path} {item.remote_uri}"
                )
        else:
            for item in plan.items:
                _require_batch_safe(item.remote_uri, item.local_path)
                Path(item.local_path).parent.mkdir(parents=True, exist_ok=True)
                lines.append(f"cp {item.remote_uri} {item.local_path}")
        self._run(["run"], input_data="\n".join(lines) + "\n")
        if plan.direction == "download":
            self._verify_downloaded_blobs(plan)
        return len(plan.items)

    @staticmethod
    def _verify_downloaded_blobs(plan: TransferPlan) -> None:
        """Check downloaded blobs against their content-addressed ids.

        s5cmd transfers bytes without parsing them; a silent mismatch would
        poison the local object store, so every blob is re-hashed once.
        """
        for item in plan.items:
            if item.kind != "blob":
                continue
            local_path = Path(item.local_path)
            if not local_path.exists():
                raise ObjectMissingError(
                    f"Blob download missing after transfer: {item.remote_uri}"
                )
            digest = blake3_digest_file(local_path)
            if digest != item.object_id:
                raise BlobIntegrityError(
                    expected=item.object_id,
                    actual=digest,
                    context="s5cmd download",
                )

    def _run(
        self,
        args: list[str],
        input_data: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        cmd = [self._s5cmd_path]
        if self._endpoint:
            cmd.extend(["--endpoint-url", self._endpoint])
        cmd.extend(args)
        try:
            return subprocess.run(
                cmd,
                text=True,
                capture_output=True,
                input=input_data,
                check=True,
            )
        except FileNotFoundError as error:
            raise StorageUnavailableError(
                f"s5cmd executable not found: {self._s5cmd_path}"
            ) from error
        except subprocess.CalledProcessError as error:
            detail = (error.stderr or error.stdout or "").strip().splitlines()
            tail = detail[-1] if detail else f"exit code {error.returncode}"
            raise StorageUnavailableError(
                f"s5cmd {' '.join(args)} failed: {tail}"
            ) from error

    def upload(
        self,
        local_path: str,
        remote_uri: str,
        *,
        if_not_exists: bool = False,
    ) -> None:
        flag = "--if-not-exists" if if_not_exists else ""
        parts = ["cp", flag, local_path, remote_uri]
        self._run([p for p in parts if p])

    def download(self, remote_uri: str, local_path: str) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self._run(["cp", remote_uri, local_path])

    def list_objects(self, uri_prefix: str) -> Iterator[S3ObjectMetadata]:
        bucket, _ = parse_s3_uri(uri_prefix)
        result = self._run(["ls", uri_prefix])
        for line in result.stdout.strip().splitlines():
            if not line.strip():
                continue
            parts = line.split(None, 3)
            if len(parts) < 4:
                continue
            # s5cmd ls output: "2024-01-15 10:30:00         1234 s3://bucket/key"
            try:
                size = int(parts[2])
            except (ValueError, IndexError):
                try:
                    size = int(parts[1])
                except (ValueError, IndexError):
                    size = 0
            key = parts[-1]
            key_path = (
                key[len(f"s3://{bucket}/") :]
                if key.startswith(f"s3://{bucket}/")
                else key.lstrip("/")
            )
            yield S3ObjectMetadata(
                bucket=bucket,
                key=key_path,
                size=size,
                mtime_ns=0,
            )

    def delete(self, remote_uri: str) -> None:
        self._run(["rm", remote_uri])

    def exists(self, remote_uri: str) -> bool:
        """True when *remote_uri* is listable; command failures mean "no"."""
        try:
            result = self._run(["ls", remote_uri])
        except StorageUnavailableError:
            return False
        return bool(result.stdout.strip())


def build_blob_transfer_backend(
    backend_type: str = "boto3",
    **kwargs: Any,
) -> S3BlobTransferBackend | S5CmdBlobTransferBackend:
    """Factory to create a BlobTransferBackend by name.

    Args:
        backend_type: "boto3" (default) or "s5cmd"
        **kwargs: Passed to the backend constructor (e.g. endpoint_url).

    Returns:
        A BlobTransferBackend instance.
    """
    if backend_type == "boto3":
        return S3BlobTransferBackend(**kwargs)
    if backend_type == "s5cmd":
        return S5CmdBlobTransferBackend(**kwargs)
    raise ValueError(f"Unknown blob transfer backend: {backend_type}")
