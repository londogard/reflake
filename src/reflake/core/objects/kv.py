"""One object-store core over a tiny key/value surface.

The local filesystem and S3 adapters differ in *how* they store bytes
(atomic local renames, conditional S3 PUTs, multipart uploads). They should
not differ in *what* a repository object is. This module owns the object
semantics — commit/tree/footer bytes, refs, tags, key mapping, listings,
deletion, tree-walk queries, and content-hash verification — and asks the
adapter for six key/value primitives:

``_kv_get`` / ``_kv_put`` / ``_kv_put_file`` / ``_kv_delete`` /
``_kv_list`` / ``_kv_exists``, plus ranged reads for blobs.

Adding a backend (GCS, Azure, a test double) now means implementing those
primitives, not re-implementing refs, tags, GC enumeration, and CAS plumbing.
"""

from __future__ import annotations

import io
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import BinaryIO, Protocol

from blake3 import blake3

from ..domain import (
    BlobIntegrityError,
    BranchRefState,
    OptimisticLockError,
    RepositoryObjectKind,
)
from ..entry_codec import Entry
from ..layout import object_relative_key, ref_relative_key
from ..repo_format import REPO_FORMAT_KEY
from .query import TreeCache, TreeWalker


class KeyValueStore(Protocol):
    """The primitive surface a backend provides."""

    def _kv_get(self, key: str) -> bytes | None: ...

    def _kv_put(
        self, key: str, payload: bytes, *, if_absent: bool = False
    ) -> bool: ...

    def _kv_put_file(
        self, key: str, source: Path, *, if_absent: bool = False
    ) -> bool: ...

    def _kv_delete(self, keys: Iterable[str]) -> int: ...

    def _kv_list(self, prefix: str) -> Iterator[tuple[str, int]]: ...

    def _kv_exists(self, key: str) -> bool: ...

    def _kv_read_range(self, key: str, offset: int, length: int) -> bytes: ...

    def _kv_path(self, key: str) -> Path | None: ...

    def _kv_uri(self, key: str) -> str | None: ...


class KeyValueObjectStore:
    """Repository object semantics over a :class:`KeyValueStore` backend."""

    tree_cache: TreeCache

    # ── Backend primitives (overridden by adapters) ─────────────────────

    def _kv_get(self, key: str) -> bytes | None:
        raise NotImplementedError

    def _kv_put(
        self, key: str, payload: bytes, *, if_absent: bool = False
    ) -> bool:
        raise NotImplementedError

    def _kv_put_file(
        self, key: str, source: Path, *, if_absent: bool = False
    ) -> bool:
        raise NotImplementedError

    def _kv_delete(self, keys: Iterable[str]) -> int:
        raise NotImplementedError

    def _kv_list(self, prefix: str) -> Iterator[tuple[str, int]]:
        raise NotImplementedError

    def _kv_exists(self, key: str) -> bool:
        raise NotImplementedError

    def _kv_read_range(self, key: str, offset: int, length: int) -> bytes:
        raise NotImplementedError

    def _kv_path(self, key: str) -> Path | None:
        raise NotImplementedError

    def _kv_uri(self, key: str) -> str | None:
        raise NotImplementedError

    def _init_tree_walker(self) -> None:
        self.tree_cache = TreeCache()
        self._walker = TreeWalker(read_tree=self.read_tree_bytes, cache=self.tree_cache)

    # ── Key mapping ─────────────────────────────────────────────────────

    def _key(self, kind: RepositoryObjectKind, object_id: str) -> str:
        return object_relative_key(kind, object_id)

    def _ref_key(self, namespace: str, name: str = "") -> str:
        return ref_relative_key(namespace, name)

    def _repo_format_key(self) -> str:
        return REPO_FORMAT_KEY

    # ── Commits ────────────────────────────────────────────────────────

    def read_commit_bytes(self, commit_id: str) -> bytes | None:
        return self._kv_get(self._key("commit", commit_id))

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None:
        if not self._kv_put(
            self._key("commit", commit_id), payload, if_absent=if_missing
        ):
            raise OptimisticLockError(f"Commit already exists: {commit_id}")

    # ── Trees ──────────────────────────────────────────────────────────

    def read_tree_bytes(self, tree_hash: str) -> bytes | None:
        payload = self._kv_get(self._key("tree", tree_hash))
        if payload is None:
            return None
        actual = blake3(payload).hexdigest()
        if actual != tree_hash:
            raise BlobIntegrityError(
                expected=tree_hash,
                actual=actual,
                context="read_tree_bytes",
            )
        return payload

    def write_tree_bytes(
        self,
        tree_hash: str,
        payload: bytes,
        *,
        if_missing: bool = True,
    ) -> None:
        self._kv_put(self._key("tree", tree_hash), payload, if_absent=if_missing)

    def write_tree_file(
        self,
        tree_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        self._kv_put_file(
            self._key("tree", tree_hash),
            Path(source_path),
            if_absent=if_missing,
        )

    # ── Footers ────────────────────────────────────────────────────────

    def read_footer_bytes(self, footer_hash: str) -> bytes | None:
        payload = self._kv_get(self._key("footer", footer_hash))
        if payload is None:
            return None
        actual = blake3(payload).hexdigest()
        if actual != footer_hash:
            raise BlobIntegrityError(
                expected=footer_hash,
                actual=actual,
                context="read_footer_bytes",
            )
        return payload

    def write_footer_file(
        self,
        footer_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        self._kv_put_file(
            self._key("footer", footer_hash),
            Path(source_path),
            if_absent=if_missing,
        )

    # ── Refs ───────────────────────────────────────────────────────────

    def read_branch_ref(self, branch: str) -> BranchRefState | None:
        payload = self._kv_get(self._ref_key("heads", branch))
        if payload is None:
            return None
        return BranchRefState(
            branch=branch,
            commit_id=payload.decode("utf-8").strip() or None,
        )

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None:
        payload = f"{commit_id}\n".encode() if commit_id else b""
        self._kv_put(self._ref_key("heads", branch), payload)

    # ── Tags ───────────────────────────────────────────────────────────

    def read_tag_ref(self, tag: str) -> str | None:
        payload = self._kv_get(self._ref_key("tags", tag))
        if payload is None:
            return None
        return payload.decode("utf-8").strip() or None

    def create_tag_ref(self, tag: str, commit_id: str) -> bool:
        return self._kv_put(
            self._ref_key("tags", tag),
            f"{commit_id}\n".encode(),
            if_absent=True,
        )

    def delete_tag_ref(self, tag: str) -> None:
        self._kv_delete([self._ref_key("tags", tag)])

    def iter_tags(self) -> Iterator[str]:
        prefix = self._ref_key("tags")
        for key, _ in self._kv_list(prefix):
            name = key[len(prefix) :]
            if name:
                yield name

    # ── Enumeration ────────────────────────────────────────────────────

    def iter_branches(self) -> Iterator[str]:
        prefix = self._ref_key("heads")
        for key, _ in self._kv_list(prefix):
            name = key[len(prefix) :]
            if name:
                yield name

    def iter_object_ids(self, kind: RepositoryObjectKind) -> Iterator[str]:
        for object_id, _ in self.iter_object_ids_with_mtimes(kind):
            yield object_id

    def iter_object_ids_with_mtimes(
        self, kind: RepositoryObjectKind
    ) -> Iterator[tuple[str, int]]:
        prefix = self._key(kind, "")
        for key, mtime_ns in self._kv_list(prefix):
            relative = key[len(prefix) :]
            if not relative:
                continue
            if kind == "blob":
                yield relative.replace("/", ""), mtime_ns
            elif kind == "commit":
                name = relative.rsplit("/", 1)[-1]
                if name.endswith(".json"):
                    yield name[: -len(".json")], mtime_ns
            else:
                yield relative.rsplit("/", 1)[-1], mtime_ns

    def delete_objects(
        self, kind: RepositoryObjectKind, object_ids: Iterable[str]
    ) -> int:
        return self._kv_delete([self._key(kind, object_id) for object_id in object_ids])

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool:
        return self._kv_exists(self._key(kind, object_id))

    # ── Blob bytes ─────────────────────────────────────────────────────

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        payload = self._kv_get(self._key("blob", blob_hash))
        if payload is None:
            raise FileNotFoundError(blob_hash)
        return payload

    def open_blob(self, blob_hash: str) -> BinaryIO:
        return io.BytesIO(self.read_blob_bytes(blob_hash))

    def read_blob_range(self, blob_hash: str, offset: int, length: int) -> bytes:
        if length <= 0:
            return b""
        return self._kv_read_range(self._key("blob", blob_hash), offset, length)

    # ── Tree-walk queries ──────────────────────────────────────────────

    def iter_all_entries(self, tree_hash: str) -> Iterator[Entry]:
        yield from self._walker.iter_all_entries(tree_hash)

    def lookup_entry(self, tree_hash: str, logical_path: str) -> Entry | None:
        return self._walker.lookup_entry(tree_hash, logical_path)

    def iter_entries_for_prefix(
        self, tree_hash: str, logical_prefix: str
    ) -> Iterator[Entry]:
        yield from self._walker.iter_entries_for_prefix(tree_hash, logical_prefix)

    # ── Shared format ──────────────────────────────────────────────────

    def read_repo_format_bytes(self) -> bytes | None:
        return self._kv_get(self._repo_format_key())

    def write_repo_format_bytes(
        self, payload: bytes, *, if_missing: bool = True
    ) -> None:
        self._kv_put(self._repo_format_key(), payload, if_absent=if_missing)
