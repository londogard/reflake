"""URI helpers with no third-party dependencies.

These used to live in :mod:`objects.source`, which imports ``boto3`` and
``fsspec``. Anything on the import path of ``import reflake`` (staging, the
repository facade, the CLI) needs URI parsing but must not require the S3 or
VFS extras, so it lives here instead.
"""

from __future__ import annotations

from urllib.parse import urlparse


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)``."""
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def is_s3_uri(uri: str) -> bool:
    return uri.startswith("s3://")
