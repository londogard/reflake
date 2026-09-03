"""Store protocols: capability-split views over one repository object store.

Reflake v2 (§1/§12 of docs/architecture.md) keeps a *single* store surface per
adapter, but consumers depend on the narrowest capability they need:

- ``ObjectIO``      — immutable content IO: commits, trees, footers, blobs.
- ``RefCas``        — pure compare-and-swap branch pointers (§6).
- ``TreeQuery``     — tree-walk lookups and listings (§3).
- ``StoreInventory``— enumeration/deletion for gc and sync planning.

``ObjectStore`` is the composition every adapter (``LocalObjectStore`` /
``S3ObjectStore``) provides; services annotate against the sub-protocol that
matches their role so accidental coupling shows up in the type checker.
"""

from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Iterator, Protocol

from ..domain import BranchRefState, RepositoryObjectKind
from ..manifest import ManifestEntry


class ObjectIO(Protocol):
    """Immutable content-addressed object reads and writes."""

    # ── Commits ─────────────────────────────────────────────────────────

    def read_commit_bytes(self, commit_id: str) -> bytes | None: ...

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None: ...

    # ── Trees ───────────────────────────────────────────────────────────

    def read_tree_bytes(self, tree_hash: str) -> bytes | None: ...

    def write_tree_bytes(
        self,
        tree_hash: str,
        payload: bytes,
        *,
        if_missing: bool = True,
    ) -> None: ...

    def write_tree_file(
        self,
        tree_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None: ...

    # ── Footers ─────────────────────────────────────────────────────────

    def read_footer_bytes(self, footer_hash: str) -> bytes | None: ...

    def write_footer_file(
        self,
        footer_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None: ...

    # ── Blobs ───────────────────────────────────────────────────────────

    def read_blob_bytes(self, blob_hash: str) -> bytes: ...

    def open_blob(self, blob_hash: str) -> BinaryIO: ...

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None: ...

    def write_blob_stream(
        self,
        blob_hash: str,
        source: BinaryIO,
        *,
        if_missing: bool = True,
    ) -> None: ...

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool: ...

    def version_token(
        self, kind: RepositoryObjectKind, object_id: str
    ) -> str | None: ...


class RefCas(Protocol):
    """Pure compare-and-swap branch pointers — the only mutable state."""

    def branch_path(self, branch: str) -> Path: ...

    def read_branch_ref(self, branch: str) -> BranchRefState | None: ...

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None: ...

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_version_token: str | None,
        expected_commit_id: str | None = None,
    ) -> bool: ...


class TreeQuery(Protocol):
    """Tree-walk lookups: exact paths, prefixes, and full walks (§3)."""

    def iter_all_entries(self, tree_hash: str) -> Iterator[ManifestEntry]: ...

    def lookup_entry(
        self, tree_hash: str, logical_path: str
    ) -> ManifestEntry | None: ...

    def iter_entries_for_prefix(
        self, tree_hash: str, logical_prefix: str
    ) -> Iterator[ManifestEntry]: ...


class StoreInventory(Protocol):
    """Enumeration and deletion of stored objects (gc, sync planning)."""

    def iter_branches(self) -> Iterator[str]: ...

    def iter_object_ids(self, kind: RepositoryObjectKind) -> Iterator[str]: ...

    def delete_object(self, kind: RepositoryObjectKind, object_id: str) -> None: ...

    def object_path(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        """Local filesystem path of an object (local stores only)."""
        ...

    def object_uri(self, kind: RepositoryObjectKind, object_id: str) -> str:
        """Remote URI of an object (S3 stores only)."""
        ...


class ObjectStore(ObjectIO, RefCas, TreeQuery, StoreInventory, Protocol):
    """A complete repository object store: the composition of all capabilities."""

