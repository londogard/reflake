"""Optional-dependency helpers.

Reflake's heavy backends are extras:

    pip install 'reflake[s3]'     # boto3      — S3 adapter, push/pull/clone
    pip install 'reflake[query]'  # duckdb, pyarrow — `query build/scan`
    pip install 'reflake[vfs]'    # fsspec     — `reflake://` URIs

A missing extra must produce one actionable sentence, not a traceback from
five frames deep ("No module named 'botocore'"). Everything that imports an
optional dependency goes through :func:`optional_import`.
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

from .domain import ReflakeError

#: Extra -> human-readable capability, for the error message.
EXTRAS: dict[str, str] = {
    "s3": "S3 object storage",
    "query": "DuckDB catalogs and pruned parquet scans",
    "vfs": "fsspec / reflake:// URIs",
}


def optional_import(module: str, *, extra: str) -> ModuleType:
    """Import *module*, or explain which extra provides it."""
    try:
        return import_module(module)
    except ModuleNotFoundError as error:
        capability = EXTRAS.get(extra, extra)
        package = module.split(".", 1)[0]
        raise ReflakeError(
            f"{capability} needs the optional dependency '{package}'. "
            f"Install it with: pip install 'reflake[{extra}]'"
        ) from error
