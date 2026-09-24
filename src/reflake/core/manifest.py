"""Worktree walking: the file stream a full commit's tree is built from.

v2 has no manifest objects (commits point at Merkle trees), so all that
remains here is the deterministic worktree walk used by full commits and by
working-tree status comparisons.  Walks honor ``.reflakeignore`` plus the
built-in defaults (``.reflake``, ``.git``); see :mod:`reflake.core.ignore`.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from .ignore import IgnoreRules


@dataclass(frozen=True)
class FileEntry:
    path: Path
    relative_path: str
    size: int
    mtime_ns: int


def walk_files(
    root: str | Path,
    *,
    ignore: IgnoreRules | None = None,
    rel_prefix: str = "",
) -> Iterator[FileEntry]:
    """Yield files under *root* in sorted order, skipping ignored paths.

    *ignore* defaults to the rules of *root* (built-ins + ``.reflakeignore``).
    *rel_prefix* is the position of *root* relative to the tree the rules
    were loaded from, so an ignored anchored pattern still applies when
    staging a subdirectory of the worktree.
    """
    root_path = Path(root).resolve()
    rules = ignore if ignore is not None else IgnoreRules.load(root_path)

    def relative(parts: tuple[str, ...]) -> str:
        joined = "/".join(parts)
        return f"{rel_prefix}/{joined}" if rel_prefix else joined

    def iter_dir(path: Path, rel_parts: tuple[str, ...]) -> Iterator[FileEntry]:
        try:
            entries = sorted(os.scandir(path), key=lambda entry: entry.name)
        except PermissionError:
            return
        for entry in entries:
            parts = rel_parts + (entry.name,)
            full = relative(parts)
            if entry.is_dir(follow_symlinks=False):
                if rules.ignores(full, is_dir=True):
                    continue
                yield from iter_dir(Path(entry.path), parts)
            elif entry.is_file(follow_symlinks=False):
                if rules.ignores(full, is_dir=False):
                    continue
                stat = entry.stat()
                yield FileEntry(
                    path=Path(entry.path),
                    relative_path=full,
                    size=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                )

    yield from iter_dir(root_path, ())
