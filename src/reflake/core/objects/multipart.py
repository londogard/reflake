"""Multipart S3 upload/download helpers for large blobs.

Blobs are content-addressed and bytes are hash-verified by callers, so the
multipart path can skip conditional writes: re-uploading the same key writes
identical bytes, and skipping a concurrent duplicate is harmless.  (S3 also
does not support ``IfNoneMatch`` on ``CreateMultipartUpload``.)  Callers do a
cheap ``object_exists`` probe first to avoid the upload in the common
deduplicated case.

Parts are sized so every part except the last is at least S3's 5 MiB minimum
and the part count never exceeds the 10 000 service limit. A file on disk is
uploaded with several parts in flight (a 10 GiB file is 1 280 sequential
round trips otherwise); a non-seekable stream stays sequential because its
bytes cannot be read out of order.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, BinaryIO, Protocol


class _Readable(Protocol):
    """Minimal stream surface: boto3 bodies, files, and wrappers all match."""

    def read(self, size: int | None = None, /) -> bytes: ...


#: Files at or below this size use a single ``PutObject``.
MULTIPART_THRESHOLD = 64 * 1024 * 1024
#: Default part size. Smaller parts than the old 64 MiB keep several uploads
#: in flight without unbounded memory (16 x 8 MiB = 128 MiB per file).
MULTIPART_PART_SIZE = 8 * 1024 * 1024
#: S3's minimum part size (except the final part).
MIN_PART_SIZE = 5 * 1024 * 1024
#: S3's maximum number of parts per upload.
MAX_PARTS = 10_000
#: Memory budget for in-flight parts of one file.
PART_MEMORY_BUDGET = 128 * 1024 * 1024


def part_size_for(total_size: int) -> int:
    """A part size that respects the minimum and the 10k-part limit."""
    return max(MULTIPART_PART_SIZE, -(-total_size // MAX_PARTS))


def _workers_for(part_size: int, count: int) -> int:
    """In-flight parts for one file, bounded by the memory budget."""
    by_memory = max(2, PART_MEMORY_BUDGET // max(part_size, 1))
    return max(1, min(count, by_memory, 16))


def _read_parts(source: BinaryIO, part_size: int) -> Iterator[bytes]:
    while True:
        chunk = source.read(part_size)
        if not chunk:
            return
        yield chunk


def _complete_upload(
    client: Any, bucket: str, key: str, upload_id: str, parts: list[dict[str, Any]]
) -> None:
    client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )


def upload_stream(
    client: Any,
    bucket: str,
    key: str,
    source: _Readable,
    *,
    part_size: int | None = None,
    threshold: int | None = None,
    pre_complete: Callable[[], None] | None = None,
    if_none_match: bool = False,
) -> None:
    """Upload *source* to (bucket, key), choosing a single PUT or multipart.

    The first chunk decides the mode: ``threshold + 1`` bytes are buffered,
    so a stream that ends within *threshold* is a single ``PutObject``
    (conditional when *if_none_match* is set); otherwise the buffered chunk
    becomes part 1 and the rest streams through multipart.  *pre_complete*
    runs after the last part but before ``CompleteMultipartUpload`` — the
    place to verify a content hash and still abort cleanly on mismatch.

    Multipart uploads are unconditional: S3 has no ``IfNoneMatch`` on
    ``CreateMultipartUpload``, and re-uploading a content-addressed key
    writes identical bytes, so callers that care probe first.
    """
    threshold = MULTIPART_THRESHOLD if threshold is None else threshold
    base_part = MULTIPART_PART_SIZE if part_size is None else part_size

    first = source.read(threshold + 1)
    if not first:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": b""}
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        client.put_object(**kwargs)
        return
    if len(first) <= threshold:
        if pre_complete is not None:
            pre_complete()
        kwargs = {"Bucket": bucket, "Key": key, "Body": first}
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        client.put_object(**kwargs)
        return

    upload = client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = upload["UploadId"]
    parts: list[dict[str, Any]] = []
    try:
        body = first
        number = 1
        while body:
            response = client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=number,
                Body=body,
            )
            parts.append({"ETag": response["ETag"], "PartNumber": number})
            number += 1
            body = source.read(base_part)
        if pre_complete is not None:
            pre_complete()
        _complete_upload(client, bucket, key, upload_id, parts)
    except BaseException:
        try:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:  # noqa: BLE001 – best-effort cleanup
            pass
        raise


def upload_file(
    client: Any,
    bucket: str,
    key: str,
    local_path: str | Path,
    *,
    if_none_match: bool = False,
) -> None:
    """Upload a local file: one PUT below the threshold, parallel parts above.

    ``os.pread`` lets worker threads read disjoint ranges without sharing a
    file cursor, so parts are uploaded concurrently and completed in order.
    """
    path = Path(local_path)
    size = path.stat().st_size
    if if_none_match and size <= MULTIPART_THRESHOLD:
        with path.open("rb") as handle:
            client.put_object(Bucket=bucket, Key=key, Body=handle, IfNoneMatch="*")
        return
    if size <= MULTIPART_THRESHOLD:
        with path.open("rb") as handle:
            client.put_object(Bucket=bucket, Key=key, Body=handle)
        return

    from ..concurrency import map_bounded

    part_size = part_size_for(size)
    count = max(1, -(-size // part_size))
    upload = client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = upload["UploadId"]
    file_descriptor = os.open(path, os.O_RDONLY)
    try:

        def put_part(number: int) -> dict[str, Any]:
            offset = (number - 1) * part_size
            body = os.pread(file_descriptor, min(part_size, size - offset), offset)
            response = client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=number,
                Body=body,
            )
            return {"ETag": response["ETag"], "PartNumber": number}

        parts = map_bounded(
            put_part,
            range(1, count + 1),
            workers=_workers_for(part_size, count),
        )
        _complete_upload(client, bucket, key, upload_id, parts)
    except BaseException:
        try:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:  # noqa: BLE001 – best-effort cleanup
            pass
        raise
    finally:
        os.close(file_descriptor)


def download_to_file(
    client: Any, bucket: str, key: str, local_path: str | Path
) -> None:
    """Stream an object to *local_path* atomically (temp file + replace)."""
    target = Path(local_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    with NamedTemporaryFile(dir=target.parent, delete=False) as temp:
        temp_path = Path(temp.name)
        try:
            while chunk := body.read(1024 * 1024):
                temp.write(chunk)
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise
        finally:
            body.close()
    try:
        os.replace(temp_path, target)
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise
