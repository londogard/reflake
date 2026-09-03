from __future__ import annotations

import json
import os
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from pathlib import PurePosixPath

import msgspec
from typing import Callable, Iterable, Iterator

from .entry_codec import (
    LeafRecord,
    decode_leaf_parts,
    encode_leaf,
    leaf_kind_for,
    validate_leaf,
)
from .hashing import blake3_digest_file

SUPPORTED_IDENTITY_MODES = frozenset({"blake3", "meta"})
_BLAKE3_HEX_LENGTH = 64
_HEX_DIGITS = frozenset("0123456789abcdef")


def _is_hex_digest(value: str) -> bool:
    normalized = value.lower()
    return len(normalized) == _BLAKE3_HEX_LENGTH and all(
        character in _HEX_DIGITS for character in normalized
    )


def _validate_manifest_path(path: str) -> None:
    if not path:
        raise ValueError("Manifest entry path cannot be empty")
    if path.startswith("/") or path.endswith("/"):
        raise ValueError("Manifest entry path must be a normalized relative path")
    if "\\" in path or "//" in path:
        raise ValueError("Manifest entry path must use normalized POSIX separators")

    parts = PurePosixPath(path).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("Manifest entry path must be a normalized relative path")


def _validate_hex_digest(value: str, *, field_name: str) -> None:
    if not _is_hex_digest(value):
        raise ValueError(
            f"Manifest entry {field_name} must be a 64-character hex digest"
        )


@dataclass(frozen=True)
class ManifestEntry:
    """A leaf entry addressed by its full logical path.

    ``identity_value`` and ``blob_hash`` are derived, not stored: the identity
    is always ``hash``, and the blob hash exists exactly when the entry is
    content-verified (``identity_mode == "blake3"``).
    """

    path: str
    hash: str
    size: int
    mtime_ns: int
    identity_mode: str = "blake3"
    source_uri: str | None = None
    footer: str | None = None

    def __post_init__(self) -> None:
        _validate_manifest_path(self.path)
        _validate_hex_digest(self.hash, field_name="hash")

        if self.identity_mode not in SUPPORTED_IDENTITY_MODES:
            supported_modes = ", ".join(sorted(SUPPORTED_IDENTITY_MODES))
            raise ValueError(
                f"Manifest entry identity_mode must be one of: {supported_modes}"
            )
        if self.size < 0:
            raise ValueError("Manifest entry size cannot be negative")
        if self.mtime_ns < 0:
            raise ValueError("Manifest entry mtime_ns cannot be negative")
        if self.source_uri is not None and not self.source_uri.strip():
            raise ValueError("Manifest entry source_uri cannot be empty")
        if self.footer is not None:
            _validate_hex_digest(self.footer, field_name="footer")

        kind = leaf_kind_for(self.identity_mode, has_footer=self.footer is not None)
        validate_leaf(kind, source_uri=self.source_uri, footer=self.footer)

    @property
    def identity_value(self) -> str:
        return self.hash

    @property
    def blob_hash(self) -> str | None:
        return self.hash if self.identity_mode == "blake3" else None

    @property
    def is_verified(self) -> bool:
        """True when the entry is backed by a canonical content blob.

        Metadata-only entries (``identity_mode == "meta"``) are *unverifiable*:
        their identity is derived from path and size, not from content bytes.
        Use ``reflake verify`` to read the source blob, compute a Blake3 hash,
        and promote the entry to ``blake3`` mode.
        """
        return self.identity_mode == "blake3"

    def serialize(self) -> str:
        return encode_leaf(
            LeafRecord(
                kind=leaf_kind_for(
                    self.identity_mode, has_footer=self.footer is not None
                ),
                name=self.path,
                hash=self.hash,
                size=self.size,
                mtime_ns=self.mtime_ns,
                source_uri=self.source_uri,
                footer=self.footer,
            )
        )

    @staticmethod
    def deserialize(payload_text: str) -> "ManifestEntry":
        try:
            payload = _load_manifest_payload(payload_text)
        except (JSONDecodeError, msgspec.DecodeError) as error:
            raise ValueError("Corrupt manifest entry payload") from error
        return _manifest_entry_from_payload(payload)

    @staticmethod
    def path_from_payload(payload_text: str) -> str:
        try:
            payload = _load_manifest_payload(payload_text)
        except (JSONDecodeError, msgspec.DecodeError) as error:
            raise ValueError("Corrupt manifest entry payload") from error
        if not isinstance(payload, list) or len(payload) < 2:
            raise ValueError("Manifest entry payload must be a JSON array")
        return str(payload[1])

    @staticmethod
    def from_dict(data: dict[str, object]) -> "ManifestEntry":
        if not isinstance(data, dict):
            raise ValueError("Manifest entry payload must be an object")
        hash_value = str(data.get("hash") or data.get("identity_value") or "")
        if not hash_value:
            raise ValueError("Manifest entry must include hash or identity_value")

        try:
            path = str(data["path"])
            size = int(data["size"])  # type: ignore[arg-type]
            mtime_ns = int(data["mtime_ns"])  # type: ignore[arg-type]
        except KeyError as error:
            raise ValueError(
                f"Manifest entry is missing required field: {error.args[0]}"
            ) from error
        except (TypeError, ValueError) as error:
            raise ValueError(
                "Manifest entry size and mtime_ns must be integers"
            ) from error

        source_uri = data.get("source_uri")
        footer = data.get("footer")
        return ManifestEntry(
            path=path,
            hash=hash_value,
            size=size,
            mtime_ns=mtime_ns,
            identity_mode=str(data.get("identity_mode") or "blake3"),
            source_uri=str(source_uri) if source_uri is not None else None,
            footer=str(footer) if footer is not None else None,
        )


def _load_manifest_payload(payload_text: str) -> object:
    return msgspec.json.decode(payload_text)


def _manifest_entry_from_payload(payload: object) -> ManifestEntry:
    if not isinstance(payload, list):
        raise ValueError("Manifest entry payload must be a JSON array")
    record = decode_leaf_parts(payload)
    return ManifestEntry(
        path=record.name,
        hash=record.hash,
        size=record.size,
        mtime_ns=record.mtime_ns,
        identity_mode="blake3" if record.kind in ("b", "bp") else "meta",
        source_uri=record.source_uri,
        footer=record.footer,
    )


def _manifest_entry_path_for_index(payload_text: str) -> str:
    """Extract the path from a serialized manifest entry.

    Parses the payload as JSON so paths containing quotes or escapes are
    extracted correctly (the previous quote-splitting shortcut mangled them,
    corrupting derived-manifest index keys).
    """
    return ManifestEntry.path_from_payload(payload_text)


class ManifestWriter:
    def __init__(
        self, manifest_path: str | Path, *, block_entry_count: int = 0
    ) -> None:
        self.manifest_path = Path(manifest_path)
        self.block_entry_count = block_entry_count
        self._blocks: list[tuple[str, int]] = []
        self._entry_count = 0
        self._manifest_size = 0

    def write_entries(
        self, entries: Iterable[ManifestEntry | str | tuple[str, str]]
    ) -> int:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        offset = 0
        previous_path: str | None = None
        with self.manifest_path.open("wb") as handle:
            for entry in entries:
                path = ""
                if isinstance(entry, tuple):
                    path, payload = entry
                    line = payload.encode("utf-8")
                elif isinstance(entry, str):
                    line = entry.encode("utf-8")
                    if self.block_entry_count > 0:
                        path = _manifest_entry_path_for_index(entry)
                else:
                    line = entry.serialize().encode("utf-8")
                    path = entry.path

                if self.block_entry_count > 0:
                    if previous_path is not None and path <= previous_path:
                        raise ValueError(
                            "Manifest entries must be sorted by path to build an index"
                        )
                    if written % self.block_entry_count == 0:
                        self._blocks.append((path, offset))
                    previous_path = path

                handle.write(line + b"\n")
                offset += len(line) + 1
                written += 1

        self._entry_count = written
        self._manifest_size = offset
        return written

    def build_index(self):
        if self.block_entry_count <= 0 or not self._blocks:
            return None
        from .objects.derived import DerivedIndex, DerivedIndexBlock

        return DerivedIndex(
            manifest_size=self._manifest_size,
            block_entry_count=self.block_entry_count,
            blocks=tuple(
                DerivedIndexBlock(first_path=p, offset=o) for p, o in self._blocks
            ),
        )

    def write_files(
        self,
        files: Iterable[str | Path],
        *,
        root: str | Path,
        hash_file: Callable[[str | Path], str] = blake3_digest_file,
    ) -> int:
        root_path = Path(root).resolve()
        return self.write_entries(
            build_manifest_entries(files=files, root=root_path, hash_file=hash_file)
        )


class ManifestReader:
    def __init__(self, manifest_path: str | Path) -> None:
        self.manifest_path = Path(manifest_path)

    def iter_entries(self) -> Iterator[ManifestEntry]:
        if not self.manifest_path.exists():
            return
        with self.manifest_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = _load_manifest_payload(line)
                except (JSONDecodeError, msgspec.DecodeError) as error:
                    raise ValueError(
                        f"Corrupt manifest JSON at line {line_number} in {self.manifest_path}"
                    ) from error
                try:
                    yield _manifest_entry_from_payload(payload)
                except ValueError as error:
                    raise ValueError(
                        f"Invalid manifest entry at line {line_number} in {self.manifest_path}: {error}"
                    ) from error


def build_manifest_entries(
    files: Iterable[str | Path],
    *,
    root: str | Path,
    hash_file: Callable[[str | Path], str] = blake3_digest_file,
) -> Iterator[ManifestEntry]:
    root_path = Path(root).resolve()
    for file_path_raw in files:
        file_path = Path(file_path_raw).resolve()
        stat = file_path.stat()
        relative_path = file_path.relative_to(root_path).as_posix()
        digest = hash_file(file_path)
        yield ManifestEntry(
            path=relative_path,
            hash=digest,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            identity_mode="blake3",
        )


@dataclass(frozen=True)
class FileEntry:
    path: Path
    relative_path: str
    size: int
    mtime_ns: int


def walk_files(root: str | Path) -> Iterator[FileEntry]:
    root_path = Path(root).resolve()

    def iter_dir(path: Path, rel_parts: tuple[str, ...]) -> Iterator[FileEntry]:
        try:
            entries = sorted(os.scandir(path), key=lambda entry: entry.name)
        except PermissionError:
            return
        for entry in entries:
            if entry.name == ".reflake":
                continue
            if entry.is_dir(follow_symlinks=False):
                yield from iter_dir(Path(entry.path), rel_parts + (entry.name,))
            elif entry.is_file(follow_symlinks=False):
                stat = entry.stat()
                rel_path = "/".join(rel_parts + (entry.name,))
                yield FileEntry(
                    path=Path(entry.path),
                    relative_path=rel_path,
                    size=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                )

    yield from iter_dir(root_path, ())
