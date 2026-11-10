from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO

from ..domain import (
    BlobIntegrityError,
    RepositoryObjectKind,
)
from ..entry_codec import Entry
from ..layout import initialize_reflake_layout, object_relative_key, ref_relative_key
from .kv import KeyValueObjectStore
from .query import TreeCache, TreeWalker


def _fsync_fd(fd: int) -> None:
    """Flush one descriptor to stable storage (best effort on odd filesystems)."""
    try:
        os.fsync(fd)
    except OSError:
        # Some network/overlay filesystems reject fsync on directories or
        # report EINVAL for descriptors they cannot flush. The rename is still
        # atomic; we do not turn a durability hint into a failed command.
        return


def _fsync_dir(path: Path) -> None:
    """Make a directory entry rename durable (POSIX)."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        _fsync_fd(fd)
    finally:
        os.close(fd)


def _ensure_parent_dir(path: Path) -> None:
    """Create a ref's parent, replacing ancestor tombstones.

    Deleting ``feature`` leaves an empty ref file; creating ``feature/x``
    must turn that tombstone into a directory. A non-empty ref is a real
    namespace conflict and is left untouched.
    """
    while True:
        try:
            path.mkdir(parents=True, exist_ok=True)
            return
        except (FileExistsError, NotADirectoryError):
            blocker = path
            while not blocker.exists() and blocker != blocker.parent:
                blocker = blocker.parent
            if blocker.is_file() and blocker.stat().st_size == 0:
                blocker.unlink()
                continue
            raise


def _atomic_write_bytes(path: Path, payload: bytes, *, fsync: bool = True) -> None:
    _ensure_parent_dir(path.parent)
    with NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as temp:
        temp_path = Path(temp.name)
        temp.write(payload)
        temp.flush()
        if fsync:
            _fsync_fd(temp.fileno())
    try:
        try:
            os.replace(temp_path, path)
        except OSError:
            # A ref that used to be a namespace directory (``v1/`` after the
            # last tag below it was deleted) blocks the file path. Remove the
            # directory if it contains only deleted-ref tombstones; a live ref
            # below it is a real namespace conflict and still fails.
            if not path.is_dir():
                raise
            _prune_tombstone_tree(path)
            path.rmdir()
            os.replace(temp_path, path)
        if fsync:
            _fsync_dir(path.parent)
    finally:
        temp_path.unlink(missing_ok=True)


def _prune_tombstone_tree(path: Path) -> None:
    """Remove empty ref files/dirs below a namespace path.

    Deleted branches/tags are CAS tombstones (empty files); they must not
    prevent a parent name from being reused. Any non-empty ref is live and
    aborts the operation.
    """
    for child in path.iterdir():
        if child.is_dir():
            _prune_tombstone_tree(child)
            child.rmdir()
        elif child.is_file() and child.stat().st_size == 0:
            child.unlink()
        else:
            raise OSError(f"ref namespace is not empty: {child}")


def _publish_file(
    source: Path,
    target: Path,
    *,
    if_missing: bool,
    fsync: bool,
) -> None:
    """Atomically publish *source* at *target* by copying into the target dir.

    The copy always happens in the destination directory, so a source on a
    different filesystem (notably ``/tmp`` on tmpfs) never triggers ``EXDEV``.
    Tree and footer objects are small, so the extra copy is cheaper than a
    cross-device failure. The source is left in place for the caller to
    clean up.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    if if_missing and target.exists():
        return
    with NamedTemporaryFile(dir=target.parent, delete=False) as temp:
        temp_path = Path(temp.name)
        try:
            with source.open("rb") as src:
                shutil.copyfileobj(src, temp, 1024 * 1024)
            temp.flush()
            if fsync:
                _fsync_fd(temp.fileno())
            os.replace(temp_path, target)
            if fsync:
                _fsync_dir(target.parent)
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
    """Hold an exclusive inter-process lock on an open lock file (POSIX)."""
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_EX)


def _unlock(fd: int) -> None:
    """Release an inter-process lock held by :func:`_lock_exclusive`."""
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_UN)


class LocalObjectStore(KeyValueObjectStore):
    """Local filesystem object store (POSIX; see README platform support)."""

    def __init__(
        self,
        root: str | Path,
        *,
        fsync: bool = True,
        use_reflink: bool = True,
    ) -> None:
        self.layout = initialize_reflake_layout(root)
        self.fsync = fsync
        self.use_reflink = use_reflink
        self.tree_cache = TreeCache()
        self._walker = TreeWalker(
            read_tree=self.read_tree_bytes,
            cache=self.tree_cache,
        )

    # ── KV primitives ──────────────────────────────────────────────────

    def _kv_path(self, key: str) -> Path | None:
        return self.layout.reflake_dir / key

    def _kv_uri(self, key: str) -> str | None:
        return None

    def _kv_get(self, key: str) -> bytes | None:
        path = self.layout.reflake_dir / key
        if not path.is_file():
            return None
        return path.read_bytes()

    def _kv_put(
        self, key: str, payload: bytes, *, if_absent: bool = False
    ) -> bool:
        path = self.layout.reflake_dir / key
        if not (if_absent and key.startswith("refs/")):
            if if_absent and path.exists():
                return False
            _atomic_write_bytes(path, payload, fsync=self.fsync)
            return True
        # Create-only ref writes (tags) are atomic across processes under the
        # same refs lock used by branch CAS.
        lock_file_path = self.layout.refs_dir / ".lock"
        lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_file_path, "a") as lock_file:
            _lock_exclusive(lock_file.fileno())
            try:
                if path.is_file():
                    return False
                if path.is_dir():
                    # A namespace containing only deleted-ref tombstones can
                    # be reused for the parent name.
                    try:
                        _prune_tombstone_tree(path)
                        path.rmdir()
                    except OSError:
                        return False
                _atomic_write_bytes(path, payload, fsync=self.fsync)
                return True
            finally:
                _unlock(lock_file.fileno())

    def _kv_put_file(
        self, key: str, source: Path, *, if_absent: bool = False
    ) -> bool:
        path = self.layout.reflake_dir / key
        if if_absent and path.exists():
            return False
        _publish_file(source, path, if_missing=False, fsync=self.fsync)
        return True

    def _kv_delete(self, keys: Iterable[str]) -> int:
        deleted = 0
        shard_dirs: set[Path] = set()
        for key in keys:
            path = self.layout.reflake_dir / key
            if path.is_file():
                path.unlink(missing_ok=True)
                deleted += 1
                if key.startswith("blobs/"):
                    shard_dirs.add(path.parent)
        for shard_dir in shard_dirs:
            try:
                shard_dir.rmdir()
            except OSError:
                pass
        return deleted

    def _kv_list(self, prefix: str) -> Iterator[tuple[str, int]]:
        base = self.layout.reflake_dir / prefix
        if not base.exists():
            return
        if base.is_file():
            yield prefix.rstrip("/"), base.stat().st_mtime_ns
            return
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.name.startswith("."):
                continue
            relative = path.relative_to(self.layout.reflake_dir).as_posix()
            try:
                mtime_ns = path.stat().st_mtime_ns
            except OSError:
                mtime_ns = 0
            yield relative, mtime_ns

    def _kv_exists(self, key: str) -> bool:
        return (self.layout.reflake_dir / key).exists()

    def _kv_read_range(self, key: str, offset: int, length: int) -> bytes:
        if length <= 0:
            return b""
        with (self.layout.reflake_dir / key).open("rb") as handle:
            handle.seek(offset)
            return handle.read(length)

    # ── Commits ──────────────────────────────────────────────────────────



    # ── Trees ────────────────────────────────────────────────────────────




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



    # ── Refs ─────────────────────────────────────────────────────────────



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
                _atomic_write_bytes(
                    tag_path, f"{commit_id}\n".encode(), fsync=self.fsync
                )
                return True
            finally:
                _unlock(lock_file.fileno())

    def delete_tag_ref(self, tag: str) -> None:
        self._tag_path(tag).unlink(missing_ok=True)


    # ── Blobs ────────────────────────────────────────────────────────────


    def open_blob(self, blob_hash: str) -> BinaryIO:
        return self.blob_path(blob_hash).open("rb")


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
        if self.use_reflink:
            # Clone into a temp name and rename into place: a crash during
            # clonefile/FICLONE must never leave a partial blob at the final
            # content-addressed path.
            with NamedTemporaryFile(
                dir=blob_path.parent, prefix=".clone.", delete=False
            ) as temp:
                clone_target = Path(temp.name)
            clone_target.unlink(missing_ok=True)
            try:
                if _clone_file(source, clone_target):
                    if self.fsync:
                        with clone_target.open("rb") as handle:
                            _fsync_fd(handle.fileno())
                    os.replace(clone_target, blob_path)
                    if self.fsync:
                        _fsync_dir(blob_path.parent)
                    return
            finally:
                clone_target.unlink(missing_ok=True)
        with NamedTemporaryFile(dir=blob_path.parent, delete=False) as temp:
            temp_path = Path(temp.name)
            try:
                with source.open("rb") as src:
                    shutil.copyfileobj(src, temp, 1024 * 1024)
                temp.flush()
                if self.fsync:
                    _fsync_fd(temp.fileno())
                if if_missing and blob_path.exists():
                    return
                os.replace(temp_path, blob_path)
                if self.fsync:
                    _fsync_dir(blob_path.parent)
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
                if self.fsync:
                    _fsync_fd(temp.fileno())
                digest = hasher.hexdigest()
                blob_path = self.blob_path(digest)
                if blob_path.exists():
                    return digest
                blob_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(temp_path, blob_path)
                if self.fsync:
                    _fsync_dir(blob_path.parent)
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
                if self.fsync:
                    _fsync_fd(temp.fileno())
                actual = hasher.hexdigest()
                if actual != blob_hash:
                    raise BlobIntegrityError(
                        expected=blob_hash,
                        actual=actual,
                        context="write_blob_stream",
                    )
                if if_missing and blob_path.exists():
                    return
                os.replace(temp_path, blob_path)
                if self.fsync:
                    _fsync_dir(blob_path.parent)
            finally:
                temp_path.unlink(missing_ok=True)

    # ── Enumeration (GC) ────────────────────────────────────────────────







    def object_path(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        return self._path_for(kind, object_id)

    # ── Paths ────────────────────────────────────────────────────────────


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
