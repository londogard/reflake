from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO

from ..domain import BranchRefState, OptimisticLockError, RepositoryObjectKind
from ..entry_codec import Entry
from ..layout import initialize_reflake_layout, object_relative_key, ref_relative_key
from .query import TreeCache, TreeWalker


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as temp:
        temp_path = Path(temp.name)
        temp.write(payload)
    try:
        try:
            os.replace(temp_path, path)
        except OSError:
            # A ref that used to be a namespace directory (``v1/`` after the
            # last tag below it was deleted) blocks the file path; remove the
            # now-empty directory and retry. Non-empty directories still fail.
            if not path.is_dir():
                raise
            path.rmdir()
            os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def _clone_file(source: Path, target: Path) -> bool:
    """Publish *target* as a copy-on-write clone of *source* when possible.

    APFS (``clonefile``) and btrfs/XFS (``FICLONE``) clone in O(1) without
    reading or writing a byte, which is safe here because blobs are immutable
    and never rewritten. Returns ``False`` when the platform/filesystem does
    not support it, so callers fall back to a byte copy.
    """
    try:
        if sys.platform == "darwin":
            import ctypes

            libc = ctypes.CDLL(None, use_errno=True)
            clonefile = libc.clonefile
            clonefile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
            clonefile.restype = ctypes.c_int
            return (
                clonefile(
                    str(source).encode(),
                    str(target).encode(),
                    0,
                )
                == 0
            )
        if sys.platform.startswith("linux"):
            import fcntl

            ficlone = 0x40049409
            with source.open("rb") as src, target.open("wb") as dst:
                fcntl.ioctl(dst.fileno(), ficlone, src.fileno())
            return True
    except (OSError, AttributeError):
        try:
            target.unlink(missing_ok=True)
        except OSError:
            pass
    return False


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

    def lookup_entry(self, tree_hash: str, logical_path: str) -> Entry | None:
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
        # A directory here is a *namespace* entry (some branch below it),
        # not a ref: report "no such ref" instead of crashing on read_text.
        if not branch_path.is_file():
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
                current_commit_id = current_state.commit_id if current_state else None
                if current_commit_id != expected_commit_id:
                    return False
                self.write_branch_ref(branch, commit_id)
                return True
            finally:
                _unlock(lock_file.fileno())

    # ── Tags ─────────────────────────────────────────────────────────────

    def read_tag_ref(self, tag: str) -> str | None:
        tag_path = self._tag_path(tag)
        # A directory here is a *namespace* entry (some tag below it), not a
        # ref: report "no such tag" instead of crashing on read_text.
        if not tag_path.is_file():
            return None
        return tag_path.read_text(encoding="utf-8").strip() or None

    def create_tag_ref(self, tag: str, commit_id: str) -> bool:
        # Create-only under the same refs lock as branch CAS: an existing tag
        # is never overwritten, so a concurrent create has exactly one winner.
        lock_file_path = self.layout.refs_dir / ".lock"
        lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_file_path, "a") as lock_file:
            _lock_exclusive(lock_file.fileno())
            try:
                tag_path = self._tag_path(tag)
                if tag_path.is_file():
                    return False
                _atomic_write_bytes(tag_path, f"{commit_id}\n".encode())
                return True
            finally:
                _unlock(lock_file.fileno())

    def delete_tag_ref(self, tag: str) -> None:
        self._tag_path(tag).unlink(missing_ok=True)

    def iter_tags(self) -> Iterator[str]:
        tags_dir = self.layout.refs_dir / "tags"
        if not tags_dir.is_dir():
            return
        for path in sorted(tags_dir.rglob("*")):
            if path.is_file() and not path.name.startswith("."):
                yield path.relative_to(tags_dir).as_posix()

    # ── Blobs ────────────────────────────────────────────────────────────

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        return self.blob_path(blob_hash).read_bytes()

    def open_blob(self, blob_hash: str) -> BinaryIO:
        return self.blob_path(blob_hash).open("rb")

    def read_blob_range(self, blob_hash: str, offset: int, length: int) -> bytes:
        if length <= 0:
            return b""
        with self.blob_path(blob_hash).open("rb") as handle:
            handle.seek(offset)
            return handle.read(length)

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        """Publish *source_path* as the blob for *blob_hash*.

        The caller already knows the digest, so this is a pure copy (or a
        copy-on-write clone). Re-hashing here would double every commit's
        read and CPU cost — the digest is verified by
        ``materialize_blob_file`` when it computes the hash itself.
        """
        blob_path = self.blob_path(blob_hash)
        if if_missing and blob_path.exists():
            return
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        source = Path(source_path)
        if _clone_file(source, blob_path):
            return
        with NamedTemporaryFile(dir=blob_path.parent, delete=False) as temp:
            temp_path = Path(temp.name)
            try:
                with source.open("rb") as src:
                    shutil.copyfileobj(src, temp, 1024 * 1024)
                temp.flush()
                if if_missing and blob_path.exists():
                    return
                os.replace(temp_path, blob_path)
            finally:
                temp_path.unlink(missing_ok=True)

    def materialize_blob_file(
        self, source_path: str | Path, *, known_digest: str | None = None
    ) -> str:
        """Store *source_path* and return its content digest in **one** pass.

        With ``known_digest`` (a stat-cache hit) the bytes are cloned/copied
        without re-hashing; without it the file is read once while hashing and
        copied, so a new blob costs one read + one write instead of the
        hash-then-copy double pass.
        """
        if known_digest is not None:
            self.write_blob_file(known_digest, source_path, if_missing=True)
            return known_digest

        from blake3 import blake3 as _blake3

        source = Path(source_path)
        shard_dir = self.layout.blobs_dir
        shard_dir.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=shard_dir, delete=False) as temp:
            temp_path = Path(temp.name)
            try:
                hasher = _blake3()
                with source.open("rb") as src:
                    while chunk := src.read(1024 * 1024):
                        hasher.update(chunk)
                        temp.write(chunk)
                temp.flush()
                digest = hasher.hexdigest()
                blob_path = self.blob_path(digest)
                if blob_path.exists():
                    return digest
                blob_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(temp_path, blob_path)
                return digest
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
        # Nested branch names (feature/x) are directories under heads/: walk
        # the whole tree and ignore atomic-write temp files (leading dot).
        for path in sorted(self.layout.heads_dir.rglob("*")):
            if path.is_file() and not path.name.startswith("."):
                yield path.relative_to(self.layout.heads_dir).as_posix()

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

    def iter_object_ids_with_mtimes(
        self, kind: RepositoryObjectKind
    ) -> Iterator[tuple[str, int]]:
        """Yield ``(object_id, mtime_ns)`` for stored objects of *kind*.

        Blob shards are snapshotted before their entries are yielded so a GC
        sweep may delete objects while iterating without skipping siblings on
        filesystems whose ``readdir`` is disturbed by concurrent unlinks.
        (Each shard holds a bounded number of files.)  The flat tree/footer/
        commit directories are streamed lazily: a skipped entry there is a
        leftover, never a correctness problem, and the next sweep catches it.
        """

        def _mtime_ns(path: Path) -> int:
            try:
                return path.stat().st_mtime_ns
            except OSError:
                return 0

        if kind == "blob":
            if not self.layout.blobs_dir.is_dir():
                return
            for shard in sorted(self.layout.blobs_dir.iterdir()):
                if not shard.is_dir():
                    continue
                for path in sorted(shard.iterdir()):
                    yield shard.name + path.name, _mtime_ns(path)
        elif kind == "tree":
            if self.layout.trees_dir.is_dir():
                for path in self.layout.trees_dir.iterdir():
                    yield path.name, _mtime_ns(path)
        elif kind == "footer":
            if self.layout.footers_dir.is_dir():
                for path in self.layout.footers_dir.iterdir():
                    yield path.name, _mtime_ns(path)
        elif kind == "commit":
            for path in self.layout.commits_dir.glob("*.json"):
                yield path.stem, _mtime_ns(path)

    def delete_objects(
        self, kind: RepositoryObjectKind, object_ids: Iterable[str]
    ) -> int:
        deleted = 0
        shard_dirs: set[Path] = set()
        for object_id in object_ids:
            path = self._path_for(kind, object_id)
            path.unlink(missing_ok=True)
            if kind == "blob":
                shard_dirs.add(path.parent)
            deleted += 1
        # Tidy up emptied 2-hex blob shards (best effort; never fatal).
        for shard_dir in shard_dirs:
            try:
                shard_dir.rmdir()
            except OSError:
                pass
        return deleted

    def read_repo_format_bytes(self) -> bytes | None:
        path = self.layout.reflake_dir / "repo.json"
        if not path.is_file():
            return None
        return path.read_bytes()

    def write_repo_format_bytes(self, payload: bytes) -> None:
        path = self.layout.reflake_dir / "repo.json"
        if path.exists():
            return
        _atomic_write_bytes(path, payload)

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
        return self.layout.reflake_dir / ref_relative_key("heads", branch)

    def _tag_path(self, tag: str) -> Path:
        return self.layout.reflake_dir / ref_relative_key("tags", tag)

    def _path_for(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        return self.layout.reflake_dir / object_relative_key(kind, object_id)
