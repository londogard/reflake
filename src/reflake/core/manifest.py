from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from .entry_codec import CorruptEntryError, Entry
from .hashing import blake3_digest_file


def _manifest_entry_path_for_index(payload_text: str) -> str:
    """Extract the path from a serialized manifest entry.

    Parses the payload as JSON so paths containing quotes or escapes are
    extracted correctly (the previous quote-splitting shortcut mangled them,
    corrupting derived-manifest index keys).
    """
    return Entry.path_from_payload(payload_text)


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
        self, entries: Iterable[Entry | str | tuple[str, str]]
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

    def iter_entries(self) -> Iterator[Entry]:
        if not self.manifest_path.exists():
            return
        with self.manifest_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield Entry.parse(line)
                except CorruptEntryError as error:
                    raise ValueError(
                        f"Corrupt manifest JSON at line {line_number} "
                        f"in {self.manifest_path}"
                    ) from error
                except ValueError as error:
                    raise ValueError(
                        f"Invalid manifest entry at line {line_number} "
                        f"in {self.manifest_path}: {error}"
                    ) from error


def build_manifest_entries(
    files: Iterable[str | Path],
    *,
    root: str | Path,
    hash_file: Callable[[str | Path], str] = blake3_digest_file,
) -> Iterator[Entry]:
    root_path = Path(root).resolve()
    for file_path_raw in files:
        file_path = Path(file_path_raw).resolve()
        stat = file_path.stat()
        relative_path = file_path.relative_to(root_path).as_posix()
        digest = hash_file(file_path)
        yield Entry.with_identity(
            relative_path,
            digest,
            stat.st_size,
            stat.st_mtime_ns,
            "content",
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
