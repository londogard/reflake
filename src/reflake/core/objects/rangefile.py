"""Seekable ranged file objects over blob/source bytes, with accounting.

``query scan`` hands a :class:`RangeFile` to pyarrow (via ``pa.PythonFile``)
so parquet reads become ranged fetches: local file handles seek and read,
S3 blobs and pointer sources issue ``Range`` GETs.  Every fetch is counted
(``bytes_fetched`` / ``requests``), so a scan can report bytes fetched vs
bytes total — the whole point of executing a pruned scan
(docs/architecture.md §4).

The file is deliberately minimal (read/seek/tell only) and *not* thread-safe:
run parquet reads with ``use_threads=False`` so seek/read pairs cannot
interleave across columns.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from ..domain import SourceNotFoundError
from .source import _s3_is_404, _translate_s3_error, build_s3_client
from .uris import parse_s3_uri as parse_s3_uri  # noqa: F401 – re-exported


def _client_error() -> type[BaseException]:
    """botocore's exception class, imported only for S3 range reads."""
    from ..extras import optional_import

    return optional_import("botocore.exceptions", extra="s3").ClientError


class RangeFile:
    """A seekable, read-only file over a ranged byte source.

    ``fetch(offset, length)`` returns up to *length* bytes starting at
    *offset* (short reads at EOF are allowed and expected).  ``size`` is the
    logical object size used for seeks from the end and read-to-EOF.
    """

    __slots__ = (
        "_fetch",
        "_on_close",
        "_pos",
        "_size",
        "bytes_fetched",
        "closed",
        "requests",
    )

    def __init__(
        self,
        fetch: Callable[[int, int], bytes],
        size: int,
        *,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        if size < 0:
            raise ValueError("RangeFile size cannot be negative")
        self._fetch = fetch
        self._size = size
        self._on_close = on_close
        self._pos = 0
        self.closed = False
        #: Total bytes returned by ``fetch`` calls (store-level accounting).
        self.bytes_fetched = 0
        #: Number of ``fetch`` calls issued.
        self.requests = 0

    # ── Minimal file protocol (what pyarrow's PythonFile uses) ─────────

    @property
    def size(self) -> int:
        return self._size

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        self._check_open()
        return self._pos

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        self._check_open()
        match whence:
            case os.SEEK_SET:
                position = offset
            case os.SEEK_CUR:
                position = self._pos + offset
            case os.SEEK_END:
                position = self._size + offset
            case _:
                raise ValueError(f"Invalid whence: {whence}")
        if position < 0:
            raise ValueError("Negative seek position")
        self._pos = position
        return position

    def read(self, size: int | None = -1) -> bytes:
        self._check_open()
        if size is None or size < 0:
            length = self._size - self._pos
        else:
            length = min(size, self._size - self._pos)
        if length <= 0:
            return b""
        data = self._fetch(self._pos, length)
        self.bytes_fetched += len(data)
        self.requests += 1
        self._pos += len(data)
        return data

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self._on_close is not None:
            self._on_close()

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed RangeFile")

    def __enter__(self) -> RangeFile:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def open_local_range_file(path: str | Path, size: int | None = None) -> RangeFile:
    """Open a local file as a :class:`RangeFile` (one handle, seek + read)."""
    handle = Path(path).open("rb")
    actual_size = os.fstat(handle.fileno()).st_size if size is None else size

    def fetch(offset: int, length: int) -> bytes:
        handle.seek(offset)
        return handle.read(length)

    return RangeFile(fetch, actual_size, on_close=handle.close)


def open_s3_range_file(
    client: Any,
    bucket: str,
    key: str,
    size: int,
    *,
    context: str,
) -> RangeFile:
    """Open an S3 object as a :class:`RangeFile` (ranged GETs, closed ranges)."""

    def fetch(offset: int, length: int) -> bytes:
        try:
            response = client.get_object(
                Bucket=bucket,
                Key=key,
                Range=f"bytes={offset}-{offset + length - 1}",
            )
        except _client_error() as error:
            if _s3_is_404(error):
                raise SourceNotFoundError(
                    f"Cannot scan missing source: {context}"
                ) from error
            raise _translate_s3_error(error, "get_object") from error
        return response["Body"].read()

    return RangeFile(fetch, size)


def open_source_range_file(
    source_uri: str,
    size: int,
    *,
    client: Any | None = None,
) -> RangeFile:
    """Open a staged source URI (``file://`` path or ``s3://object``) ranged.

    Missing or unreadable sources surface uniformly as
    ``SourceNotFoundError`` so a scan can count them as skipped (or fail
    under ``--strict``) without special-casing each backend.
    """
    if source_uri.startswith("s3://"):
        bucket, key = parse_s3_uri(source_uri)
        if not key or key.endswith("/"):
            raise SourceNotFoundError(
                f"Cannot scan source prefix as a file: {source_uri}"
            )
        s3_client = client or build_s3_client()
        return open_s3_range_file(s3_client, bucket, key, size, context=source_uri)

    parsed = urlparse(source_uri)
    if parsed.scheme == "file":
        path = Path(unquote(parsed.path))
    else:
        path = Path(source_uri).expanduser()
    try:
        return open_local_range_file(path, size)
    except OSError as error:
        raise SourceNotFoundError(
            f"Cannot scan missing source: {source_uri}"
        ) from error
