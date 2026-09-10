from __future__ import annotations

import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO

from ..domain import BranchRefState, OptimisticLockError, RepositoryObjectKind
from ..entry_codec import Entry
from ..layout import initialize_reflake_layout, object_relative_key
from .query import TreeCache, TreeWalker


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as temp:
        temp_path = Path(temp.name)
        temp.write(payload)
    try:
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def _lock_exclusive(fd: int) -> None:
    """Hold an exclusive inter-process lock on an open lock file."""
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
        except OSError:
            # Fresh (empty) lock file: grow it by one byte, then lock.
            # Concurrent growers write the same byte; the lock serializes.
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, b"\0")
            msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
    else:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX)


def _unlock(fd: int) -> None:
    """Release an inter-process lock held by :func:`_lock_exclusive`."""
    if os.name == "nt":
        import msvcrt

        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
    else:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_UN)


class LocalObjectStore:
    def __init__(self, root: str | Path) -> None:
        self.layout = initialize_reflake_layout(root)
        self.tree_cache = TreeCache()
        self._walker = TreeWalker(
            read_tree=self.read_tree_bytes,
            cache=self.tree_cache,
        )

    # ── Commits ──────────────────────────────────────────────────────────

    def read_commit_bytes(self, commit_id: str) -> bytes | None:
        commit_path = self.commit_path(commit_id)
        if not commit_path.exists():
            return None
        return commit_path.read_bytes()

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None:
        commit_path = self.commit_path(commit_id)
        if if_missing and commit_path.exists():
            raise OptimisticLockError(f"Commit already exists: {commit_id}")
        _atomic_write_bytes(commit_path, payload)

    # ── Trees ────────────────────────────────────────────────────────────

    def read_tree_bytes(self, tree_hash: str) -> bytes | None:
        tree_path = self.tree_path(tree_hash)
        if not tree_path.exists():
            return None
        return tree_path.read_bytes()

    def write_tree_bytes(
        self,
        tree_hash: str,
        payload: bytes,
        *,
        if_missing: bool = True,
    ) -> None:
        tree_path = self.tree_path(tree_hash)
        tree_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and tree_path.exists():
            return
        _atomic_write_bytes(tree_path, payload)

    def write_tree_file(
        self,
        tree_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        tree_path = self.tree_path(tree_hash)
        source = Path(source_path)
        tree_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and tree_path.exists():
            return
        source.replace(tree_path)

    # ── Tree-walk queries ────────────────────────────────────────────────

    def iter_all_entries(self, tree_hash: str) -> Iterator[Entry]:
        yield from self._walker.iter_all_entries(tree_hash)

    def lookup_entry(
        self, tree_hash: str, logical_path: str
    ) -> Entry | None:
        return self._walker.lookup_entry(tree_hash, logical_path)

    def iter_entries_for_prefix(
        self, tree_hash: str, logical_prefix: str
    ) -> Iterator[Entry]:
        yield from self._walker.iter_entries_for_prefix(tree_hash, logical_prefix)

    # ── Footers ──────────────────────────────────────────────────────────

    def read_footer_bytes(self, footer_hash: str) -> bytes | None:
        footer_path = self.footer_path(footer_hash)
        if not footer_path.exists():
            return None
        return footer_path.read_bytes()

    def write_footer_file(
        self,
        footer_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        footer_path = self.footer_path(footer_hash)
        source = Path(source_path)
        footer_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and footer_path.exists():
            return
        source.replace(footer_path)

    # ── Refs ─────────────────────────────────────────────────────────────

    def read_branch_ref(self, branch: str) -> BranchRefState | None:
        branch_path = self._branch_path(branch)
        if not branch_path.exists():
            return None
        commit_id = branch_path.read_text(encoding="utf-8").strip() or None
        return BranchRefState(branch=branch, commit_id=commit_id)

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None:
        payload = f"{commit_id}\n".encode() if commit_id else b""
        _atomic_write_bytes(self._branch_path(branch), payload)

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_commit_id: str | None,
    ) -> bool:
        lock_file_path = self.layout.refs_dir / ".lock"
        lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_file_path, "a") as lock_file:
            _lock_exclusive(lock_file.fileno())
            try:
                current_state = self.read_branch_ref(branch)
                current_commit_id = (
                    current_state.commit_id if current_state else None
                )
                if current_commit_id != expected_commit_id:
                    return False
                self.write_branch_ref(branch, commit_id)
                return True
            finally:
                _unlock(lock_file.fileno())

    # ── Blobs ────────────────────────────────────────────────────────────

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        return self.blob_path(blob_hash).read_bytes()

    def open_blob(self, blob_hash: str) -> BinaryIO:
        return self.blob_path(blob_hash).open("rb")

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        blob_path = self.blob_path(blob_hash)
        if if_missing and blob_path.exists():
            return
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        with Path(source_path).open("rb") as src:
            with NamedTemporaryFile(dir=blob_path.parent, delete=False) as temp:
                temp_path = Path(temp.name)
                try:
                    from blake3 import blake3 as _blake3

                    hasher = _blake3()
                    while chunk := src.read(1024 * 1024):
                        hasher.update(chunk)
                        temp.write(chunk)
                    temp.flush()
                    actual = hasher.hexdigest()
                    if actual != blob_hash:
                        raise ValueError(
                            f"Blob hash mismatch: expected {blob_hash}, got {actual}"
                        )
                    if if_missing and blob_path.exists():
                        return
                    os.replace(temp_path, blob_path)
                finally:
                    temp_path.unlink(missing_ok=True)

    def write_blob_stream(
        self,
        blob_hash: str,
        source: BinaryIO,
        *,
        if_missing: bool = True,
    ) -> None:
        blob_path = self.blob_path(blob_hash)
        if if_missing and blob_path.exists():
            return
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=blob_path.parent, delete=False) as temp:
            temp_path = Path(temp.name)
            try:
                from blake3 import blake3 as _blake3

                hasher = _blake3()
                while chunk := source.read(1024 * 1024):
                    hasher.update(chunk)
                    temp.write(chunk)
                temp.flush()
                actual = hasher.hexdigest()
                if actual != blob_hash:
                    raise ValueError(
                        f"Blob hash mismatch: expected {blob_hash}, got {actual}"
                    )
                if if_missing and blob_path.exists():
                    return
                os.replace(temp_path, blob_path)
            finally:
                temp_path.unlink(missing_ok=True)

    # ── Enumeration (GC) ────────────────────────────────────────────────

    def iter_branches(self) -> Iterator[str]:
        for path in self.layout.heads_dir.iterdir():
            if path.is_file() and not path.name.startswith("."):
                yield path.name

    def iter_object_ids(self, kind: RepositoryObjectKind) -> Iterator[str]:
        if kind == "blob":
            for shard in self.layout.blobs_dir.iterdir():
                if shard.is_dir():
                    for path in shard.iterdir():
                        yield shard.name + path.name
        elif kind == "tree":
            yield from (p.name for p in self.layout.trees_dir.iterdir() if p.is_file())
        elif kind == "footer":
            yield from (
                p.name for p in self.layout.footers_dir.iterdir() if p.is_file()
            )
        elif kind == "commit":
            yield from (p.stem for p in self.layout.commits_dir.glob("*.json"))

    def delete_objects(
        self, kind: RepositoryObjectKind, object_ids: Iterable[str]
    ) -> int:
        deleted = 0
        for object_id in object_ids:
            self._path_for(kind, object_id).unlink(missing_ok=True)
            deleted += 1
        return deleted

    def object_path(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        return self._path_for(kind, object_id)

    # ── Paths ────────────────────────────────────────────────────────────

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool:
        return self._path_for(kind, object_id).exists()

    def blob_path(self, blob_hash: str) -> Path:
        return self._path_for("blob", blob_hash)

    def commit_path(self, commit_id: str) -> Path:
        return self._path_for("commit", commit_id)

    def tree_path(self, tree_hash: str) -> Path:
        return self._path_for("tree", tree_hash)

    def footer_path(self, footer_hash: str) -> Path:
        return self._path_for("footer", footer_hash)

    def _branch_path(self, branch: str) -> Path:
        return self._path_for("ref", branch)

    def _path_for(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        return self.layout.reflake_dir / object_relative_key(kind, object_id)
