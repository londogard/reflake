"""Worktree walking: the file stream a full commit's tree is built from.

v2 has no manifest objects (commits point at Merkle trees), so all that
remains here is the deterministic worktree walk used by full commits and by
working-tree status comparisons.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


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
