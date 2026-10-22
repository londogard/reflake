"""Parquet footer capture: compact row-group statistics for pruning.

At ingest, the file footer is read through pyarrow's parquet reader — the same
reader ``query scan`` executes with — and mapped to the compact stats model:
schema plus per-row-group column min/max/nulls.  ``min_raw``/``max_raw`` keep
the **physical** values (timestamps stay integers, byte arrays decode as UTF-8
when possible), so the pruning domain is unchanged: types without a usable
bound (opaque binary, decimals, INT96) stay ``None`` and never prune.

The result is a small content-addressed JSON stats object stored under
``footers/<hash>`` and referenced from ``bp``/``mp`` tree entries.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Protocol

from blake3 import blake3

from .base import ObjectIO


class SeekableReader(Protocol):
    """Just enough file surface to read a parquet footer (see ``rangefile``)."""

    def read(self, size: int | None = -1, /) -> bytes: ...

    def seek(self, offset: int, whence: int = 0, /) -> int: ...

    def tell(self) -> int: ...


_NUMERIC_TYPES = frozenset({"INT32", "INT64", "FLOAT", "DOUBLE"})
_BYTE_TYPES = frozenset({"BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"})


def _decode_stat(value: Any, physical_type: str) -> Any:
    """Normalize a raw pyarrow statistic to the JSON-safe pruning domain.

    pyarrow's ``min``/``max`` convert by logical type (timestamps become
    datetimes, decimals become Decimals); the ``*_raw`` variants keep the
    physical value, which is what the stored stats and the pruning contract
    expect.
    """
    if value is None:
        return None
    if physical_type in _BYTE_TYPES:
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                # Opaque binary values have no usable bound in the string
                # domain; returning None keeps pruning conservative.
                return None
        return None
    if physical_type == "BOOLEAN":
        return bool(value)
    if physical_type in _NUMERIC_TYPES:
        return value
    # INT96 (deprecated timestamps) and unknown types: no bound.
    return None


@dataclass(frozen=True)
class ColumnStats:
    path: str
    min: Any = None
    max: Any = None
    nulls: int = 0


@dataclass(frozen=True)
class RowGroupStats:
    rows: int
    columns: tuple[ColumnStats, ...] = ()


@dataclass(frozen=True)
class FooterStats:
    schema_hash: str
    schema: tuple[dict[str, str], ...] = ()
    row_groups: tuple[RowGroupStats, ...] = ()
    created_by: str | None = None


def parse_parquet_footer(source: SeekableReader) -> FooterStats:
    """Parse the footer of a seekable parquet *source* into compact stats.

    Raises ``ValueError`` (pyarrow's ``ArrowInvalid``) when *source* is not a
    readable parquet file.
    """
    from ..extras import optional_import

    pq = optional_import("pyarrow.parquet", extra="query")

    parquet_file = pq.ParquetFile(source)
    metadata = parquet_file.metadata
    schema_columns = [
        parquet_file.schema.column(index) for index in range(len(parquet_file.schema))
    ]
    schema = tuple(
        {"name": column.path, "type": column.physical_type} for column in schema_columns
    )
    schema_hash = blake3(json.dumps(schema, sort_keys=True).encode("utf-8")).hexdigest()

    row_groups: list[RowGroupStats] = []
    for index in range(metadata.num_row_groups):
        row_group = metadata.row_group(index)
        columns: list[ColumnStats] = []
        for column_index in range(row_group.num_columns):
            chunk = row_group.column(column_index)
            statistics = chunk.statistics
            minimum: Any = None
            maximum: Any = None
            nulls = 0
            if statistics is not None:
                if statistics.has_min_max:
                    minimum = _decode_stat(statistics.min_raw, chunk.physical_type)
                    maximum = _decode_stat(statistics.max_raw, chunk.physical_type)
                if statistics.has_null_count and statistics.null_count is not None:
                    nulls = statistics.null_count
            columns.append(
                ColumnStats(
                    path=schema_columns[column_index].path,
                    min=minimum,
                    max=maximum,
                    nulls=nulls,
                )
            )
        row_groups.append(
            RowGroupStats(rows=row_group.num_rows, columns=tuple(columns))
        )

    return FooterStats(
        schema_hash=schema_hash,
        schema=schema,
        row_groups=tuple(row_groups),
        created_by=metadata.created_by,
    )


def serialize_footer_stats(stats: FooterStats) -> str:
    payload = {
        "schema_hash": stats.schema_hash,
        "schema": [dict(item) for item in stats.schema],
        "created_by": stats.created_by,
        "row_groups": [
            {
                "rows": group.rows,
                "columns": [
                    {
                        "path": column.path,
                        "min": column.min,
                        "max": column.max,
                        "nulls": column.nulls,
                    }
                    for column in group.columns
                ],
            }
            for group in stats.row_groups
        ],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def parse_footer_stats(payload: str | bytes) -> FooterStats:
    """Deserialize the compact stats object written by ``serialize_footer_stats``.

    The stored object is a small JSON document (``footers/<hash>``); loading
    it is metadata-only — no data bytes are ever read (docs/architecture.md §4).
    """
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    data = json.loads(payload)
    schema = tuple(
        {"name": str(item["name"]), "type": str(item.get("type") or "")}
        for item in data.get("schema", [])
    )
    row_groups = tuple(
        RowGroupStats(
            rows=int(group["rows"]),
            columns=tuple(
                ColumnStats(
                    path=str(column.get("path", "")),
                    min=column.get("min"),
                    max=column.get("max"),
                    nulls=int(column.get("nulls", 0)),
                )
                for column in group.get("columns", [])
            ),
        )
        for group in data.get("row_groups", [])
    )
    return FooterStats(
        schema_hash=str(data["schema_hash"]),
        schema=schema,
        row_groups=row_groups,
        created_by=data.get("created_by"),
    )


def capture_footer_stats(store: ObjectIO, source: SeekableReader) -> str | None:
    """Capture a parquet footer from a seekable *source* into the store.

    Returns the content hash of the stats object, or ``None`` when *source*
    is not a parquet file (or its footer is unreadable).
    """
    try:
        stats = parse_parquet_footer(source)
    except (ValueError, OSError):
        return None
    payload = serialize_footer_stats(stats).encode("utf-8")
    stats_hash = blake3(payload).hexdigest()
    if store.object_exists("footer", stats_hash):
        return stats_hash
    with NamedTemporaryFile(mode="wb", suffix=".footer", delete=False) as temp:
        temp_path = Path(temp.name)
        temp.write(payload)
    try:
        store.write_footer_file(stats_hash, temp_path, if_missing=True)
    finally:
        temp_path.unlink(missing_ok=True)
    return stats_hash


class FooterCache:
    """Client-local cache of footer stats, keyed by content hash.

    Footers are content-addressed and immutable, so a cache entry can never
    be stale — same argument as the tree cache. Repeated ``query scan`` runs
    over a large prefix then cost O(misses) store reads instead of one read
    per file, and never re-parse a footer they have already seen.
    """

    def __init__(self, root: str | Path, maxsize: int = 4096) -> None:
        self._dir = Path(root)
        self._memory: OrderedDict[str, FooterStats] = OrderedDict()
        self._maxsize = maxsize

    def get(self, store: ObjectIO, footer_hash: str) -> FooterStats | None:
        """Return parsed stats for *footer_hash*, reading the store on a miss."""
        cached = self._memory.get(footer_hash)
        if cached is not None:
            self._memory.move_to_end(footer_hash)
            return cached

        payload: bytes | None = None
        cache_path = self._dir / footer_hash
        if cache_path.exists():
            try:
                payload = cache_path.read_bytes()
            except OSError:
                payload = None
        if payload is None:
            payload = store.read_footer_bytes(footer_hash)
            if payload is None:
                return None
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with NamedTemporaryFile(
                    mode="wb", dir=cache_path.parent, delete=False
                ) as temp:
                    temp_path = Path(temp.name)
                    temp.write(payload)
                temp_path.replace(cache_path)
            except OSError:
                pass  # cache is an optimization; failures are not fatal

        try:
            stats = parse_footer_stats(payload)
        except (ValueError, KeyError, TypeError):
            return None
        self._memory[footer_hash] = stats
        if len(self._memory) > self._maxsize:
            self._memory.popitem(last=False)
        return stats
