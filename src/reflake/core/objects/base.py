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

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import BinaryIO, Protocol

from ..domain import BranchRefState, RepositoryObjectKind
from ..entry_codec import Entry


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


class RefCas(Protocol):
    """Pure compare-and-swap branch pointers — the only mutable state.

    A ref's content *is* its commit id, so the CAS expectation is the
    commit id itself: ``expected_commit_id=None`` means "must not exist"
    (branch creation). Adapters implement the check atomically (local
    ``fcntl`` lock, S3 conditional ``PutObject``).
    """

    def read_branch_ref(self, branch: str) -> BranchRefState | None: ...

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None: ...

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_commit_id: str | None,
    ) -> bool: ...


class TreeQuery(Protocol):
    """Tree-walk lookups: exact paths, prefixes, and full walks (§3)."""

    def iter_all_entries(self, tree_hash: str) -> Iterator[Entry]: ...

    def lookup_entry(
        self, tree_hash: str, logical_path: str
    ) -> Entry | None: ...

    def iter_entries_for_prefix(
        self, tree_hash: str, logical_prefix: str
    ) -> Iterator[Entry]: ...


class StoreInventory(Protocol):
    """Enumeration and deletion of stored objects (gc, sync planning)."""

    def iter_branches(self) -> Iterator[str]: ...

    def iter_object_ids(self, kind: RepositoryObjectKind) -> Iterator[str]: ...

    def delete_objects(
        self, kind: RepositoryObjectKind, object_ids: Iterable[str]
    ) -> int:
        """Delete many objects; returns the deleted count.

        Adapters batch where the backend allows it (S3 ``DeleteObjects``
        takes 1000 keys per request) so pruning 1M orphans is 1000 requests,
        not 1M.
        """
        ...


class HasLocalPath(Protocol):
    """Stores whose objects live on the local filesystem (sync sources/dests)."""

    def object_path(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        """Local filesystem path of an object."""
        ...


class HasRemoteURI(Protocol):
    """Stores whose objects live behind a remote URI (sync sources/dests)."""

    def object_uri(self, kind: RepositoryObjectKind, object_id: str) -> str:
        """Remote URI of an object."""
        ...


class ObjectStore(
    ObjectIO,
    RefCas,
    TreeQuery,
    StoreInventory,
    HasLocalPath,
    HasRemoteURI,
    Protocol,
):
    """A complete repository object store: the composition of all capabilities."""


# ── Narrow compositions: annotate each consumer with what it uses ────


class RefObjectStore(ObjectIO, RefCas, Protocol):
    """Immutable content + branch refs (e.g. ``RefManager``)."""


class ContentQueryStore(ObjectIO, TreeQuery, Protocol):
    """Immutable content + tree reads (e.g. query pruning)."""


class QueryRefStore(RefCas, TreeQuery, Protocol):
    """Branch refs + tree reads (e.g. ``StagingArea``)."""


class RepositoryStore(ObjectIO, RefCas, TreeQuery, StoreInventory, Protocol):
    """Everything a repository needs except transfer endpoints.

    ``HasLocalPath`` / ``HasRemoteURI`` stay separate: only sync planning
    touches them, via explicit capability checks (``repository_sync``),
    so local-only and S3-only adapters type-check without pretending to
    provide the other side's endpoint.
    """

