"""Parquet footer capture: compact row-group statistics for pruning.

At ingest, only the file footer is read (``head_object``-style: the last 8
bytes give the footer length, then the footer itself).  The footer is a
thrift-compact-encoded ``FileMetaData``; this module decodes just enough of it
— schema, row groups, and per-column min/max/null statistics — to prune row
groups without ever reading data pages (docs/architecture.md §4).

The result is a small content-addressed JSON stats object stored under
``footers/<hash>`` and referenced from ``bp``/``mp`` tree entries.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, BinaryIO, Callable

from blake3 import blake3

from .base import ObjectIO

_PARQUET_MAGIC = b"PAR1"
_MAX_FOOTER_SIZE = 64 * 1024 * 1024

# thrift compact protocol field types (Apache Thrift numbering)
_BOOL_TRUE = 1
_BOOL_FALSE = 2
_BYTE = 3
_I16 = 4
_I32 = 5
_I64 = 6
_DOUBLE = 7
_BINARY = 8
_LIST = 9
_SET = 10
_MAP = 11
_STRUCT = 12
_UUID = 13

_PARQUET_TYPES = {
    0: "BOOLEAN",
    1: "INT32",
    2: "INT64",
    3: "INT96",
    4: "FLOAT",
    5: "DOUBLE",
    6: "BYTE_ARRAY",
    7: "FIXED_LEN_BYTE_ARRAY",
}


class _Reader:
    __slots__ = ("data", "pos")

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def read_byte(self) -> int:
        value = self.data[self.pos]
        self.pos += 1
        return value

    def read_varint(self) -> int:
        result = 0
        shift = 0
        while True:
            byte = self.read_byte()
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return result
            shift += 7

    def read_zigzag(self) -> int:
        raw = self.read_varint()
        return (raw >> 1) ^ -(raw & 1)

    def read_binary(self) -> bytes:
        length = self.read_varint()
        value = self.data[self.pos : self.pos + length]
        if len(value) != length:
            raise ValueError("truncated thrift binary")
        self.pos += length
        return value

    def read_double(self) -> float:
        value = struct.unpack_from("<d", self.data, self.pos)[0]
        self.pos += 8
        return value

    def skip(self, field_type: int) -> None:
        if field_type in (_BOOL_TRUE, _BOOL_FALSE):
            return
        if field_type == _BYTE:
            self.pos += 1
        elif field_type == _I16:
            self.read_zigzag()
        elif field_type == _I32:
            self.read_zigzag()
        elif field_type == _I64:
            self.read_zigzag()
        elif field_type == _DOUBLE:
            self.pos += 8
        elif field_type == _UUID:
            self.pos += 16
        elif field_type == _BINARY:
            self.read_binary()
        elif field_type in (_LIST, _SET):
            size_and_type = self.read_byte()
            size = size_and_type >> 4
            element_type = size_and_type & 0x0F
            if size == 15:
                size = self.read_varint()
            for _ in range(size):
                self.skip(element_type)
        elif field_type == _MAP:
            size_and_type = self.read_byte()
            size = size_and_type >> 4
            if size == 15:
                size = self.read_varint()
            for _ in range(size):
                self.skip(self.read_byte())
                self.skip(self.read_byte())
        elif field_type == _STRUCT:
            while True:
                header = self.read_byte()
                if header == 0:
                    return
                delta = header >> 4
                inner_type = header & 0x0F
                if delta == 15:
                    self.read_zigzag()
                self.skip(inner_type)
        else:
            raise ValueError(f"unsupported thrift field type {field_type}")


def _read_fields(
    reader: _Reader, handlers: dict[int, Callable[[int], None]]
) -> None:
    """Read a thrift-compact struct, dispatching fields by id.

    Field ids are delta-encoded relative to the previous field, so a single
    pass keeps the running field id.  ``handlers`` maps field id → callable
    taking the field type nibble; unknown fields are skipped by type.
    """
    last_field = 0
    while True:
        header = reader.read_byte()
        delta = header >> 4
        if delta == 0:
            return  # struct stop
        field_type = header & 0x0F
        if delta == 15:
            delta = reader.read_zigzag()
        field_id = last_field + delta
        last_field = field_id
        handler = handlers.get(field_id)
        if handler is not None:
            handler(field_type)
        else:
            reader.skip(field_type)


def _read_list_header(reader: _Reader) -> tuple[int, int]:
    """Read a thrift-compact list header; returns ``(size, element_type)``."""
    size_and_type = reader.read_byte()
    size = size_and_type >> 4
    element_type = size_and_type & 0x0F
    if size == 15:
        size = reader.read_varint()
    return size, element_type


def _decode_min_max(raw: bytes | None, column_type: int) -> Any:
    if raw is None:
        return None
    if column_type == 0:  # BOOLEAN
        return bool(raw[0]) if raw else None
    if column_type == 1:  # INT32
        return struct.unpack("<i", raw[:4])[0] if len(raw) >= 4 else None
    if column_type == 2:  # INT64
        return struct.unpack("<q", raw[:8])[0] if len(raw) >= 8 else None
    if column_type == 4:  # FLOAT
        return struct.unpack("<f", raw[:4])[0] if len(raw) >= 4 else None
    if column_type == 5:  # DOUBLE
        return struct.unpack("<d", raw[:8])[0] if len(raw) >= 8 else None
    if column_type in (6, 7):  # BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.hex()
    return raw.hex()  # INT96 and anything else: opaque


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


def parse_parquet_footer(source: BinaryIO) -> FooterStats:
    """Parse the FileMetaData footer of a seekable parquet *source*."""
    source.seek(0, 2)
    end = source.tell()
    if end < 12:
        raise ValueError("file too small to be parquet")
    source.seek(end - 8)
    tail = source.read(8)
    if tail[4:] != _PARQUET_MAGIC:
        raise ValueError("missing PAR1 magic — not a parquet file")
    footer_length = struct.unpack("<I", tail[:4])[0]
    if footer_length <= 0 or footer_length > _MAX_FOOTER_SIZE or footer_length > end - 8:
        raise ValueError(f"invalid parquet footer length {footer_length}")
    source.seek(end - 8 - footer_length)
    footer_bytes = source.read(footer_length)
    if len(footer_bytes) != footer_length:
        raise ValueError("truncated parquet footer")
    return _parse_file_metadata(footer_bytes)


def _parse_file_metadata(data: bytes) -> FooterStats:
    reader = _Reader(data)
    schema_elements: list[dict[str, Any]] = []
    row_groups: list[RowGroupStats] = []
    num_rows = 0
    created_by: str | None = None

    def handle_schema(field_type: int) -> None:
        if field_type != _LIST:
            reader.skip(field_type)
            return
        size, element_type = _read_list_header(reader)
        if element_type != _STRUCT:
            for _ in range(size):
                reader.skip(element_type)
            return
        for _ in range(size):
            schema_elements.append(_parse_schema_element(reader))

    def handle_rows(field_type: int) -> None:
        nonlocal num_rows
        if field_type == _I64:
            num_rows = reader.read_zigzag()
        else:
            reader.skip(field_type)

    def handle_row_groups(field_type: int) -> None:
        if field_type != _LIST:
            reader.skip(field_type)
            return
        size, element_type = _read_list_header(reader)
        if element_type != _STRUCT:
            for _ in range(size):
                reader.skip(element_type)
            return
        for _ in range(size):
            row_groups.append(_parse_row_group(reader))

    def handle_created_by(field_type: int) -> None:
        nonlocal created_by
        if field_type == _BINARY:
            created_by = reader.read_binary().decode("utf-8", errors="replace")
        else:
            reader.skip(field_type)

    _read_fields(reader, {2: handle_schema, 3: handle_rows, 4: handle_row_groups, 6: handle_created_by})

    schema = tuple(
        {"name": str(element["name"]), "type": str(element.get("type") or "")}
        for element in schema_elements
        if element.get("type") is not None
    )
    schema_hash = blake3(json.dumps(schema, sort_keys=True).encode("utf-8")).hexdigest()
    return FooterStats(
        schema_hash=schema_hash,
        schema=schema,
        row_groups=tuple(row_groups),
        created_by=created_by,
    )


def _parse_schema_element(reader: _Reader) -> dict[str, Any]:
    element: dict[str, Any] = {}

    def handle_type(field_type: int) -> None:
        if field_type in (_I32, _I16):
            element["type"] = _PARQUET_TYPES.get(reader.read_zigzag())
        else:
            reader.skip(field_type)

    def handle_name(field_type: int) -> None:
        if field_type == _BINARY:
            element["name"] = reader.read_binary().decode("utf-8", errors="replace")
        else:
            reader.skip(field_type)

    def handle_num_children(field_type: int) -> None:
        if field_type in (_I32, _I16):
            element["num_children"] = reader.read_zigzag()
        else:
            reader.skip(field_type)

    _read_fields(reader, {1: handle_type, 4: handle_name, 5: handle_num_children})
    return element


def _parse_row_group(reader: _Reader) -> RowGroupStats:
    columns: list[ColumnStats] = []
    num_rows = 0

    def handle_columns(field_type: int) -> None:
        if field_type != _LIST:
            reader.skip(field_type)
            return
        size, element_type = _read_list_header(reader)
        if element_type != _STRUCT:
            for _ in range(size):
                reader.skip(element_type)
            return
        for _ in range(size):
            columns.append(_parse_column_chunk(reader))

    def handle_rows(field_type: int) -> None:
        nonlocal num_rows
        if field_type == _I64:
            num_rows = reader.read_zigzag()
        else:
            reader.skip(field_type)

    _read_fields(reader, {1: handle_columns, 3: handle_rows})
    return RowGroupStats(rows=num_rows, columns=tuple(columns))


def _parse_column_chunk(reader: _Reader) -> ColumnStats:
    meta: ColumnStats | None = None

    def handle_meta(field_type: int) -> None:
        nonlocal meta
        if field_type == _STRUCT:
            meta = _parse_column_metadata(reader)
        else:
            reader.skip(field_type)

    _read_fields(reader, {3: handle_meta})
    if meta is None:
        return ColumnStats(path="")
    return meta


def _parse_column_metadata(reader: _Reader) -> ColumnStats:
    column_type = 0
    path_parts: list[str] = []
    min_value: bytes | None = None
    max_value: bytes | None = None
    nulls = 0

    def handle_type(field_type: int) -> None:
        nonlocal column_type
        if field_type in (_I32, _I16):
            column_type = reader.read_zigzag()
        else:
            reader.skip(field_type)

    def handle_path(field_type: int) -> None:
        if field_type != _LIST:
            reader.skip(field_type)
            return
        size, element_type = _read_list_header(reader)
        if element_type != _BINARY:
            for _ in range(size):
                reader.skip(element_type)
            return
        for _ in range(size):
            path_parts.append(reader.read_binary().decode("utf-8", errors="replace"))

    def handle_statistics(field_type: int) -> None:
        nonlocal min_value, max_value, nulls
        if field_type != _STRUCT:
            reader.skip(field_type)
            return

        def stats_field(field_id: int) -> Callable[[int], None]:
            # Parquet Statistics: field 1 = max, 2 = min (deprecated);
            # field 5 = max_value, 6 = min_value.
            def handler(field_type: int) -> None:
                nonlocal min_value, max_value, nulls
                if field_type != _BINARY:
                    reader.skip(field_type)
                    return
                raw = reader.read_binary()
                if field_id in (1, 5):
                    max_value = raw
                else:
                    min_value = raw

            return handler

        def stats_nulls(field_type: int) -> None:
            nonlocal nulls
            if field_type == _I64:
                nulls = reader.read_zigzag()
            else:
                reader.skip(field_type)

        _read_fields(
            reader,
            {1: stats_field(1), 2: stats_field(2), 3: stats_nulls, 5: stats_field(5), 6: stats_field(6)},
        )

    _read_fields(
        reader,
        {1: handle_type, 3: handle_path, 12: handle_statistics},
    )
    return ColumnStats(
        path=".".join(path_parts),
        min=_decode_min_max(min_value, column_type),
        max=_decode_min_max(max_value, column_type),
        nulls=nulls,
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


def capture_footer_stats(store: ObjectIO, source: BinaryIO) -> str | None:
    """Capture a parquet footer from a seekable *source* into the store.

    Returns the content hash of the stats object, or ``None`` when *source*
    is not a parquet file (or its footer is unreadable).
    """
    try:
        stats = parse_parquet_footer(source)
    except (ValueError, OSError, IndexError, struct.error):
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
