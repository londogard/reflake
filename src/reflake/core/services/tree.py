"""Tree writing: bottom-up tree construction, overlays, commits, and exports.

``TreeWriter`` replaces v1's ``SnapshotWriter`` (``services/snapshot.py``):
commits now produce a Merkle tree DAG instead of a full JSONL manifest.

- ``build_worktree_tree`` — full commit: walks the worktree, reuses unchanged
  parent leaves (no re-hash for same-size/same-content files) and unchanged
  subtrees by content-addressing, shards directories past
  ``MAX_TREE_ENTRIES`` into name-range subtrees, and prunes staged removals.
- ``build_from_entries`` — rebuild a tree from a sorted leaf-entry stream
  (used by ``verify``/``rm``/``mv``/``import`` and by the staged overlay).
- ``overlay_staged`` — ``commit --staged``: merge materialized additions and
  removals into the parent tree.
- ``write_commit_object`` — commit objects with ``{tree, parents, ...}`` and
  CAS ref advancement.
- ``export_derived_manifest`` — flatten a tree to a JSONL manifest with block
  offsets, cached per-client keyed by root-tree hash (never in the store).
"""

from __future__ import annotations

import json
import os
from bisect import bisect_right
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import msgspec
from blake3 import blake3

from ..client_state import LocalClientState
from ..domain import CommitObject, DiffEntry
from ..entry_codec import Entry, encode_leaf_parts
from ..hashing import blake3_digest_file
from ..manifest import FileEntry, walk_files
from ..objects import ObjectIO
from ..objects.query import TreeWalker
from ..objects.tree import (
    KIND_BLOB,
    KIND_BP,
    KIND_META,
    KIND_MP,
    KIND_SHARD,
    KIND_TREE,
    MAX_TREE_ENTRIES,
    validate_tree_node,
)
from ..repository_support import pointer_identity
from ..stat_cache import StatCache
from .refs import RefManager
from .tree_inspect import TreeInspector
from .tree_merge import TreeMerger

_Child = tuple[str, str]  # (name, serialized tree line)
_Frame = tuple[
    str,  # dir path
    list[_Child],  # children built so far
    dict[str, Entry] | None,  # parent children by name
    str | None,  # parent node hash
    frozenset[str],  # hashes known to exist (parent node + its subtrees)
]

#: Worker threads for hashing worktree files (blake3 releases the GIL, so
#: threads overlap file reads and hashing).
_HASH_WORKERS = min(8, os.cpu_count() or 1)


def _path_is_removed(path: str, removed: set[str]) -> bool:
    """True when *path* or any of its ancestors is in *removed*.

    Checks the path's ancestor prefixes (O(path depth) set lookups) instead
    of scanning every removal prefix (O(removals) string operations), so a
    large ``rm`` set does not turn splicing into quadratic work.
    """
    if not removed:
        return False
    if path in removed:
        return True
    prefix = ""
    for part in path.split("/")[:-1]:
        prefix = f"{prefix}/{part}" if prefix else part
        if prefix in removed:
            return True
    return False


def _leaf_line(
    kind: str,
    name: str,
    hash_value: str,
    size: int,
    source_uri: str | None,
    footer: str | None,
    source_etag: str | None = None,
) -> str:
    # Hot path (once per committed file): inputs are producer-guaranteed
    # (digest output, stat values, walk names, closed kind branch), so
    # serialize directly without building a validated Entry.
    return encode_leaf_parts(
        kind, name, hash_value, size, source_uri, footer, source_etag
    )


def _subtree_line(kind: str, name: str, hash_value: str) -> str:
    return msgspec.json.encode([kind, name, hash_value]).decode("utf-8")


def _entry_to_leaf_line(entry: Entry) -> str:
    return replace(entry, path=entry.path.rsplit("/", 1)[-1]).serialize()


class TreeWriter:
    def __init__(
        self,
        *,
        store: ObjectIO,
        refs: RefManager,
        client_state: LocalClientState | None = None,
        stat_cache: StatCache | None = None,
    ) -> None:
        self.store = store
        self.refs = refs
        self.client_state = client_state
        self.stat_cache = stat_cache
        # Share the store's content-addressed prefix cache: the same tree
        # object must never be parsed and held twice per process.
        tree_cache = getattr(store, "tree_cache", None)
        self._walker = TreeWalker(read_tree=store.read_tree_bytes, cache=tree_cache)
        self.inspector = TreeInspector(
            read_tree=store.read_tree_bytes,
            client_state=client_state,
            cache=tree_cache,
        )
        self.merger = TreeMerger(self)

    # ── Low-level tree object writing ────────────────────────────────────

    def _write_tree_bytes(
        self,
        payload: bytes,
        *,
        skip_hashes: frozenset[str] | set[str] = frozenset(),
    ) -> str:
        # Invariant guard (I11): never publish a node that would fail to
        # re-parse.  Past corruption bugs (duplicate/unsorted names) reached
        # the store because writes were unvalidated; the guard is cheap and
        # runs before hashing/short-circuiting so it also covers skipped
        # writes (defence in depth for the builder).
        validate_tree_node(payload)
        tree_hash = blake3(payload).hexdigest()
        if tree_hash in skip_hashes:
            # Result is byte-identical to a known node (e.g. one merge side):
            # no write, and no conditional-PUT round trip on S3.
            return tree_hash
        # object_exists check omitted: write_tree_bytes with if_missing=True
        # uses IfNoneMatch=* (S3) or path.exists() (local) as an atomic guard.
        # A 412 / early-return on conflict is handled inside write_tree_bytes.
        self.store.write_tree_bytes(tree_hash, payload, if_missing=True)
        return tree_hash

    def _write_plain_node(
        self,
        children: list[_Child],
        *,
        skip_hashes: frozenset[str] | set[str] = frozenset(),
    ) -> str:
        """Write one node without sharding (shard bodies, small directories)."""
        payload = b"\n".join(line.encode() for _, line in children) + b"\n"
        return self._write_tree_bytes(payload, skip_hashes=skip_hashes)

    def _build_children(
        self,
        children: list[_Child],
        *,
        already_sorted: bool = False,
        skip_hashes: frozenset[str] | set[str] = frozenset(),
    ) -> str:
        """Build (and shard) a tree object from children; returns its hash.

        Children are sorted by name here — directory frames close lazily
        during a walk, so callers may hand us out-of-order lists (e.g. root
        files appended while an earlier sibling directory is still open).
        Pass already_sorted=True when the caller guarantees sorted order.
        """
        if not already_sorted and len(children) > 1:
            children = sorted(children, key=lambda item: item[0])
        if len(children) <= MAX_TREE_ENTRIES:
            payload = b"\n".join(line.encode() for _, line in children) + b"\n"
            return self._write_tree_bytes(payload, skip_hashes=skip_hashes)

        shard_children: list[_Child] = []
        for start in range(0, len(children), MAX_TREE_ENTRIES):
            chunk = children[start : start + MAX_TREE_ENTRIES]
            # Inherit skip_hashes: a chunk that is byte-identical to a known
            # node (often the parent itself) must not be re-PUT.
            shard_hash = self._write_plain_node(chunk, skip_hashes=skip_hashes)
            shard_name = chunk[0][0]
            shard_children.append(
                (shard_name, _subtree_line(KIND_SHARD, shard_name, shard_hash))
            )
        return self._build_children(
            shard_children, already_sorted=True, skip_hashes=skip_hashes
        )

    # ── Full-commit builder (worktree walk + parent reuse) ───────────────

    def build_worktree_tree(
        self,
        *,
        worktree_root: Path,
        parent_tree: str | None,
        identity_mode: str,
        capture_footers: bool = False,
    ) -> str:
        """Build the root tree from a worktree walk.

        The worktree is the base of a full commit; the staged overlay is
        applied by the caller afterwards. Parent-only leaves and subtrees
        survive (absence is not deletion), and unchanged parent nodes are
        skipped by content hash. Staged removals/additions are *not* part of
        this builder — one overlay path handles them for both commit modes.
        """
        return self._build_worktree_inner(
            worktree_root=worktree_root,
            parent_tree=parent_tree,
            identity_mode=identity_mode,
            capture_footers=capture_footers,
        )

    def _build_worktree_inner(
        self,
        *,
        worktree_root: Path,
        parent_tree: str | None,
        identity_mode: str,
        capture_footers: bool,
    ) -> str:
        root_children: list[_Child] = []
        stack: list[_Frame] = []

        def parent_children(
            dir_path: str,
        ) -> tuple[dict[str, Entry] | None, str | None, frozenset[str]]:
            """Parent children for *dir_path* plus the parent node's hash.

            The hash lets the builder skip writing a node that is byte-
            identical to the parent (one failed conditional PUT per unchanged
            directory on S3 otherwise). The hash set covers the parent node
            *and its subtrees* (including shard bodies), so unchanged children
            of a sharded directory are not re-PUT either.
            """
            merged: dict[str, Entry] = {}
            node_hash: str | None = None
            known: set[str] = set()
            if parent_tree:
                # Must expand shards: a directory with >MAX_TREE_ENTRIES
                # entries stores shard pointers, and merging those as if they
                # were named children corrupts the new tree (duplicate paths,
                # lost parent-only entries).
                resolved = self._walker.resolve_directory(parent_tree, dir_path)
                if resolved is not None:
                    node_hash, entries = resolved
                    known.add(node_hash)
                    raw = self._walker.load_entries(node_hash)
                    if raw is not None:
                        known.update(entry.hash for entry in raw if entry.is_subtree)
                    for entry in entries:
                        merged[entry.path] = entry
            return (merged or None), node_hash, frozenset(known)

        root_parent, root_parent_hash, root_known = parent_children("")

        def open_frames(dir_path: str) -> None:
            while stack and not (
                dir_path == stack[-1][0] or dir_path.startswith(f"{stack[-1][0]}/")
            ):
                _close_frame(stack, root_children, self)
            parts = [p for p in dir_path.split("/") if p]
            for index in range(len(stack), len(parts)):
                frame_dir = "/".join(parts[: index + 1])
                frame_parent, frame_parent_hash, frame_known = parent_children(
                    frame_dir
                )
                stack.append(
                    (frame_dir, [], frame_parent, frame_parent_hash, frame_known)
                )

        def handle_directory(dir_path: str, files: list[FileEntry]) -> None:
            """Hash (in parallel) and materialize one directory's files."""
            if dir_path:
                open_frames(dir_path)
                frame = stack[-1]
                children: list[_Child] = frame[1]
                parent_by_name = frame[2]
            else:
                children = root_children
                parent_by_name = root_parent
            digests = (
                self._materialize_blobs(files) if identity_mode == "content" else {}
            )
            for file_entry in files:
                children.append(
                    self._materialize_worktree_file(
                        file_entry=file_entry,
                        name=file_entry.relative_path.rsplit("/", 1)[-1],
                        parent_by_name=parent_by_name,
                        identity_mode=identity_mode,
                        capture_footers=capture_footers,
                        prefetched_digest=digests.get(file_entry.path),
                    )
                )

        # Files arrive sorted by path, so every directory's files are one
        # contiguous batch — hash them together (parallel) and materialize.
        current_dir: str | None = None
        batch: list[FileEntry] = []
        for file_entry in walk_files(worktree_root):
            relative_path = file_entry.relative_path
            dir_path = relative_path.rsplit("/", 1)[0] if "/" in relative_path else ""
            if dir_path != current_dir:
                if batch:
                    assert current_dir is not None
                    handle_directory(current_dir, batch)
                current_dir = dir_path
                batch = []
            batch.append(file_entry)
        if batch:
            assert current_dir is not None
            handle_directory(current_dir, batch)

        while stack:
            _close_frame(stack, root_children, self)

        merged_root = _merge_parent_children(root_children, root_parent)
        if merged_root:
            return self._build_children(merged_root, skip_hashes=root_known)
        return self._write_tree_bytes(b"")

    def _materialize_blobs(
        self, files: list[FileEntry]
    ) -> dict[Path, str]:
        """Store every file's content once and return ``path -> digest``.

        Files whose ``(size, mtime_ns)`` match the client stat cache reuse the
        recorded digest (the store still confirms the blob exists); the rest
        are read **once** by ``materialize_blob_file`` — hashing and copying
        in the same pass — with up to ``_HASH_WORKERS`` files in flight
        (blake3 and the file I/O both release the GIL).

        The previous shape hashed every file in parallel and then read it
        again, single-threaded, to copy it into the store: 76 % of a local
        commit's wall time, and a 1.7x CPU multiplier on every byte.
        """
        resolved: dict[Path, str] = {}
        pending: list[FileEntry] = []
        for file_entry in files:
            cached = (
                self.stat_cache.lookup(
                    file_entry.relative_path,
                    size=file_entry.size,
                    mtime_ns=file_entry.mtime_ns,
                )
                if self.stat_cache is not None
                else None
            )
            if cached is not None:
                resolved[file_entry.path] = self.store.materialize_blob_file(
                    file_entry.path, known_digest=cached
                )
            else:
                pending.append(file_entry)

        if not pending:

            return resolved

        def store(file_entry: FileEntry) -> tuple[Path, str]:
            return file_entry.path, self.store.materialize_blob_file(file_entry.path)

        if len(pending) < 2 or _HASH_WORKERS < 2:
            stored = [store(file_entry) for file_entry in pending]
        else:
            with ThreadPoolExecutor(
                max_workers=min(_HASH_WORKERS, len(pending))
            ) as pool:
                stored = list(pool.map(store, pending))

        by_path = {file_entry.path: file_entry for file_entry in pending}
        for path, digest in stored:
            resolved[path] = digest
            file_entry = by_path[path]
            if self.stat_cache is not None:
                self.stat_cache.record(
                    file_entry.relative_path,
                    size=file_entry.size,
                    mtime_ns=file_entry.mtime_ns,
                    digest=digest,
                )
        return resolved

    def _materialize_worktree_file(
        self,
        *,
        file_entry: FileEntry,
        name: str,
        parent_by_name: dict[str, Entry] | None,
        identity_mode: str,
        capture_footers: bool,
        prefetched_digest: str | None = None,
    ) -> _Child:
        parent_entry = parent_by_name.get(name) if parent_by_name else None
        if parent_entry is not None and not parent_entry.is_subtree:
            if identity_mode == "content":
                if parent_entry.kind in {KIND_BLOB, KIND_BP} and (
                    parent_entry.size == file_entry.size
                    and (prefetched_digest or blake3_digest_file(file_entry.path))
                    == parent_entry.hash
                ):
                    return self._reuse_or_backfill_footer(
                        parent_entry=parent_entry,
                        name=name,
                        source_path=file_entry.path,
                        source_uri=None,
                        capture_footers=capture_footers,
                    )
            elif parent_entry.kind in {KIND_META, KIND_MP}:
                digest = self._pointer_digest(file_entry)
                marker = f"blake3:{digest}"
                identity = pointer_identity(size=file_entry.size, marker=marker)
                if identity == parent_entry.hash:
                    return self._reuse_or_backfill_footer(
                        parent_entry=parent_entry,
                        name=name,
                        source_path=file_entry.path,
                        source_uri=file_entry.path.as_uri(),
                        capture_footers=capture_footers,
                        source_etag=marker,
                    )

        if identity_mode == "content":
            identity_value = prefetched_digest or self.store.materialize_blob_file(
                file_entry.path
            )
            footer = self._capture_footer(file_entry.path, capture_footers)
            line = _leaf_line(
                KIND_BP if footer else KIND_BLOB,
                name,
                identity_value,
                file_entry.size,
                None,
                footer,
            )
        else:
            digest = self._pointer_digest(file_entry)
            marker = f"blake3:{digest}"
            identity_value = pointer_identity(
                size=file_entry.size,
                marker=marker,
            )
            footer = self._capture_footer(file_entry.path, capture_footers)
            line = _leaf_line(
                KIND_MP if footer else KIND_META,
                name,
                identity_value,
                file_entry.size,
                file_entry.path.as_uri(),
                footer,
                marker,
            )
        return name, line

    def _pointer_digest(self, file_entry: FileEntry) -> str:
        """Content digest for a pointer entry, via the client stat cache.

        A pointer does not store the bytes, but its *identity* is still the
        source version: a content digest, not a local mtime. That keeps the
        same bytes byte-identical across machines and makes drift checks and
        moves consistent.
        """
        cached = (
            self.stat_cache.lookup(
                file_entry.relative_path,
                size=file_entry.size,
                mtime_ns=file_entry.mtime_ns,
            )
            if self.stat_cache is not None
            else None
        )
        if cached is not None:
            return cached
        digest = blake3_digest_file(file_entry.path)
        if self.stat_cache is not None:
            self.stat_cache.record(
                file_entry.relative_path,
                size=file_entry.size,
                mtime_ns=file_entry.mtime_ns,
                digest=digest,
            )
        return digest

    def _reuse_or_backfill_footer(
        self,
        *,
        parent_entry: Entry,
        name: str,
        source_path: Path,
        source_uri: str | None,
        capture_footers: bool,
        source_etag: str | None = None,
    ) -> _Child:
        """Reuse a parent leaf, capturing a footer when newly enabled."""
        if parent_entry.footer is not None or not capture_footers:
            return name, parent_entry.serialize()
        footer = self._capture_footer(source_path, True)
        if footer is None:
            return name, parent_entry.serialize()
        kind = KIND_BP if parent_entry.kind == KIND_BLOB else KIND_MP
        return name, _leaf_line(
            kind,
            name,
            parent_entry.hash,
            parent_entry.size,
            source_uri,
            footer,
            source_etag,
        )

    def _capture_footer(self, source_path: Path, capture_footers: bool) -> str | None:
        if not capture_footers or source_path.suffix.lower() != ".parquet":
            return None
        from ..objects.footer import capture_footer_stats

        with source_path.open("rb") as handle:
            return capture_footer_stats(self.store, handle)


    # ── Rebuild from a sorted leaf stream (verify/rm/mv/import/overlay) ──

    def build_from_entries(self, entries: Iterator[Entry]) -> str:
        """Build a root tree from a *sorted* stream of ``Entry``."""
        root_children: list[_Child] = []
        stack: list[_Frame] = []
        previous_path: str | None = None

        for entry in entries:
            path = entry.path
            if previous_path is not None and path <= previous_path:
                raise ValueError(
                    f"Tree entries must be sorted by path; {path!r} after "
                    f"{previous_path!r}"
                )
            previous_path = path
            parts = path.split("/")
            dir_path = "/".join(parts[:-1])
            name = parts[-1]
            if not dir_path:
                root_children.append((name, _entry_to_leaf_line(entry)))
                continue
            while stack and not (
                dir_path == stack[-1][0] or dir_path.startswith(f"{stack[-1][0]}/")
            ):
                _close_frame(stack, root_children, self)
            parts_list = [p for p in dir_path.split("/") if p]
            for index in range(len(stack), len(parts_list)):
                frame_dir = "/".join(parts_list[: index + 1])
                stack.append((frame_dir, [], None, None, frozenset()))
            stack[-1][1].append((name, _entry_to_leaf_line(entry)))

        while stack:
            _close_frame(stack, root_children, self)

        if root_children:
            return self._build_children(root_children)
        return self._write_tree_bytes(b"")

    # ── Staged overlay (commit --staged) ─────────────────────────────────

    def three_way_merge(
        self,
        base_tree: str | None,
        ours_tree: str,
        theirs_tree: str,
    ) -> tuple[str, list[str]]:
        """Delegate to the extracted tree-level merger."""
        return self.merger.three_way_merge(base_tree, ours_tree, theirs_tree)

    def merge_trees(
        self,
        base_tree: str | None,
        ours_tree: str,
        theirs_tree: str,
    ) -> str:
        """Three-way merge that raises ``MergeConflictError`` on conflicts."""
        return self.merger.merge_trees(base_tree, ours_tree, theirs_tree)

    # ── Commits ──────────────────────────────────────────────────────────

    def splice_tree(
        self,
        parent_tree: str | None,
        additions: list[Entry],
        removed_prefixes: set[str],
    ) -> str:
        """Merge additions and removals into the parent tree via recursive
        path-splicing.

        Unlike full-manifest flattening, this only visits directories
        along the path of modified leaves. Untouched subtrees are reused
        purely by content hash in O(1).
        """
        if not parent_tree:
            if not additions:
                return self._write_tree_bytes(b"")
            sorted_adds = sorted(additions, key=lambda entry: entry.path)
            return self.build_from_entries(iter(sorted_adds))

        result = self._splice_directory(
            dir_path="",
            tree_hash=parent_tree,
            additions=additions,
            removed_prefixes=set(removed_prefixes),
        )
        if result is None:
            return self._write_tree_bytes(b"")
        return result

    def _split_additions(
        self, dir_path: str, additions: list[Entry]
    ) -> tuple[dict[str, Entry], dict[str, list[Entry]]]:
        """Split additions into this directory's leaves and child-directory sets."""
        local_leaf_additions: dict[str, Entry] = {}
        child_additions: dict[str, list[Entry]] = {}
        prefix_len = len(dir_path) + 1 if dir_path else 0
        for entry in additions:
            rel = entry.path[prefix_len:] if prefix_len else entry.path
            parts = rel.split("/", 1)
            if len(parts) == 1:
                local_leaf_additions[parts[0]] = entry
            else:
                child_additions.setdefault(parts[0], []).append(entry)
        return local_leaf_additions, child_additions

    def _touched_direct_names(
        self,
        dir_path: str,
        *,
        local_leaf_additions: dict[str, Entry],
        child_additions: dict[str, list[Entry]],
        removed_prefixes: set[str],
    ) -> set[str]:
        """Direct child names this splice can change in *dir_path*."""
        names: set[str] = set(local_leaf_additions) | set(child_additions)
        prefix_len = len(dir_path) + 1 if dir_path else 0
        for prefix in removed_prefixes:
            if prefix_len:
                if not prefix.startswith(f"{dir_path}/"):
                    continue
                rel = prefix[prefix_len:]
            else:
                rel = prefix
            if rel:
                names.add(rel.split("/", 1)[0])
        return names

    def _splice_directory(
        self,
        dir_path: str,
        tree_hash: str | None,
        additions: list[Entry],
        removed_prefixes: set[str],
    ) -> str | None:
        local_leaf_additions, child_additions = self._split_additions(
            dir_path, additions
        )
        conflicts = set(local_leaf_additions) & set(child_additions)
        if conflicts:
            joined = ", ".join(sorted(conflicts))
            raise ValueError(
                "Conflicting staged additions (a path and a path below it): "
                f"{joined}"
            )

        if tree_hash is None:
            kept, changed = self._splice_children(
                dir_path=dir_path,
                entries=[],
                local_leaf_additions=local_leaf_additions,
                child_additions=child_additions,
                removed_prefixes=removed_prefixes,
            )
        else:
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                raise ValueError(f"Unknown tree object: {tree_hash}")
            if any(entry.kind == KIND_SHARD for entry in entries):
                # A sharded directory (up to 100M children): only the shards
                # whose name range contains a touched name are loaded and
                # rewritten. The fallback keeps the old expand-everything
                # behaviour for the pathological case of more shards than a
                # node may hold.
                if len(entries) <= MAX_TREE_ENTRIES:
                    return self._splice_sharded_directory(
                        dir_path=dir_path,
                        tree_hash=tree_hash,
                        shard_entries=entries,
                        local_leaf_additions=local_leaf_additions,
                        child_additions=child_additions,
                        removed_prefixes=removed_prefixes,
                    )
                entries = self._expand_all_shards(entries)
            kept, changed = self._splice_children(
                dir_path=dir_path,
                entries=entries,
                local_leaf_additions=local_leaf_additions,
                child_additions=child_additions,
                removed_prefixes=removed_prefixes,
            )

        if not kept:
            return None
        if not changed and tree_hash is not None:
            return tree_hash
        return self._build_children(kept)

    def _expand_all_shards(self, entries: list[Entry]) -> list[Entry]:
        unpacked: list[Entry] = []
        for entry in entries:
            if entry.kind == KIND_SHARD:
                shard_entries = self._walker.load_entries(entry.hash)
                if shard_entries:
                    unpacked.extend(shard_entries)
            else:
                unpacked.append(entry)
        return unpacked

    def _splice_sharded_directory(
        self,
        *,
        dir_path: str,
        tree_hash: str,
        shard_entries: list[Entry],
        local_leaf_additions: dict[str, Entry],
        child_additions: dict[str, list[Entry]],
        removed_prefixes: set[str],
    ) -> str | None:
        """Rewrite only the shards of *dir_path* that a change touches."""
        shard_positions = [
            index
            for index, entry in enumerate(shard_entries)
            if entry.kind == KIND_SHARD
        ]
        shard_names = [shard_entries[index].path for index in shard_positions]

        def shard_index(name: str) -> int:
            # Ranges are [name_i, name_{i+1}); the first shard also absorbs
            # names sorting before it, the last absorbs everything after.
            index = bisect_right(shard_names, name) - 1
            return shard_positions[max(index, 0)]

        additions_by_index: dict[int, list[Entry]] = {}
        for name, entry in local_leaf_additions.items():
            additions_by_index.setdefault(shard_index(name), []).append(entry)
        for name, entries in child_additions.items():
            additions_by_index.setdefault(shard_index(name), []).extend(entries)

        touched = self._touched_direct_names(
            dir_path,
            local_leaf_additions=local_leaf_additions,
            child_additions=child_additions,
            removed_prefixes=removed_prefixes,
        )
        affected = {shard_index(name) for name in touched}

        kept: list[_Child] = []
        changed = False
        for index, shard in enumerate(shard_entries):
            if shard.kind != KIND_SHARD:
                # Mixed node (not produced by this writer): pass through.
                kept.append(
                    (
                        shard.path,
                        _subtree_line(shard.kind, shard.path, shard.hash),
                    )
                )
                continue
            if index not in affected:
                kept.append(
                    (shard.path, _subtree_line(KIND_SHARD, shard.path, shard.hash))
                )
                continue

            shard_content = self._walker.load_entries(shard.hash)
            if shard_content is None:
                raise ValueError(f"Unknown tree object: {shard.hash}")
            shard_local, shard_children = self._split_additions(
                dir_path, additions_by_index.get(index, [])
            )
            sub_kept, sub_changed = self._splice_children(
                dir_path=dir_path,
                entries=shard_content,
                local_leaf_additions=shard_local,
                child_additions=shard_children,
                removed_prefixes=removed_prefixes,
            )
            if not sub_kept:
                changed = True  # shard emptied out entirely
                continue
            if not sub_changed:
                kept.append(
                    (shard.path, _subtree_line(KIND_SHARD, shard.path, shard.hash))
                )
                continue
            changed = True
            # Unhandled additions were appended, so re-sort before writing a
            # plain node (the sharding builder is what normally sorts).
            sub_kept.sort(key=lambda item: item[0])
            # A shard body is a plain node: never run it through the builder
            # that shards (> MAX_TREE_ENTRIES => nested shard pointers and
            # corrupted lookups). If the range outgrew one node, split it
            # into equally sized sibling shards instead.
            group_count = -(-len(sub_kept) // MAX_TREE_ENTRIES)
            group_size = -(-len(sub_kept) // group_count)
            for start in range(0, len(sub_kept), group_size):
                chunk = sub_kept[start : start + group_size]
                chunk_hash = self._write_plain_node(chunk, skip_hashes={shard.hash})
                chunk_name = chunk[0][0]
                kept.append(
                    (
                        chunk_name,
                        _subtree_line(KIND_SHARD, chunk_name, chunk_hash),
                    )
                )

        if not kept:
            return None
        if not changed:
            return tree_hash
        if len(kept) > MAX_TREE_ENTRIES:
            # Pathological: even the shard pointers overflow a node (>100M
            # children in one directory). Rebuild through the generic path
            # rather than writing an unreadable nested layout.
            expanded = self._expand_all_shards(shard_entries)
            full_kept, _ = self._splice_children(
                dir_path=dir_path,
                entries=expanded,
                local_leaf_additions=local_leaf_additions,
                child_additions=child_additions,
                removed_prefixes=removed_prefixes,
            )
            if not full_kept:
                return None
            return self._build_children(full_kept)
        return self._build_children(kept, skip_hashes={tree_hash})

    def _splice_children(
        self,
        *,
        dir_path: str,
        entries: list[Entry],
        local_leaf_additions: dict[str, Entry],
        child_additions: dict[str, list[Entry]],
        removed_prefixes: set[str],
    ) -> tuple[list[_Child], bool]:
        """Merge additions/removals into one node's entries.

        Returns the kept children (with unhandled additions appended) and
        whether anything changed.
        """
        kept: list[_Child] = []
        changed = False
        handled_leaf_additions: set[str] = set()
        handled_child_dirs: set[str] = set()

        for entry in entries:
            name = entry.path
            full = f"{dir_path}/{name}" if dir_path else name

            if entry.is_leaf:
                if _path_is_removed(full, removed_prefixes):
                    changed = True
                    if name in local_leaf_additions:
                        new_leaf = local_leaf_additions[name]
                        kept.append((name, _entry_to_leaf_line(new_leaf)))
                        handled_leaf_additions.add(name)
                    elif name in child_additions:
                        # A staged directory replaces this committed leaf.
                        # Without this branch the tail loop would append a
                        # second entry with the same name and publish an
                        # unparseable node.
                        handled_child_dirs.add(name)
                        new_sub_hash = self._splice_directory(
                            dir_path=full,
                            tree_hash=None,
                            additions=child_additions[name],
                            removed_prefixes=removed_prefixes,
                        )
                        if new_sub_hash is not None:
                            kept.append(
                                (name, _subtree_line(KIND_TREE, name, new_sub_hash))
                            )
                    continue

                if name in local_leaf_additions:
                    new_leaf = local_leaf_additions[name]
                    kept.append((name, _entry_to_leaf_line(new_leaf)))
                    handled_leaf_additions.add(name)
                    # Conflicting nested adds would duplicate the name; the
                    # shape conflict check in _splice_directory raises first,
                    # this is defence in depth for hand-built callers.
                    handled_child_dirs.add(name)
                    changed = True
                    continue

                if name in child_additions:
                    # Staged files land in a new directory at this name: the
                    # leaf is replaced by a subtree (file -> directory).
                    handled_child_dirs.add(name)
                    new_sub_hash = self._splice_directory(
                        dir_path=full,
                        tree_hash=None,
                        additions=child_additions[name],
                        removed_prefixes=removed_prefixes,
                    )
                    if new_sub_hash is not None:
                        kept.append(
                            (name, _subtree_line(KIND_TREE, name, new_sub_hash))
                        )
                    changed = True
                    continue

                kept.append((name, entry.serialize()))
            else:
                is_completely_removed = _path_is_removed(full, removed_prefixes)
                if is_completely_removed and name not in child_additions:
                    changed = True
                    continue
                if name in local_leaf_additions:
                    # A leaf staged at a path holding a subtree replaces it
                    # (file <-> directory switches across branches). Without
                    # this the tail would append a second entry with the same
                    # name and the stream would no longer parse.
                    new_leaf = local_leaf_additions[name]
                    kept.append((name, _entry_to_leaf_line(new_leaf)))
                    handled_leaf_additions.add(name)
                    handled_child_dirs.add(name)
                    changed = True
                    continue

                has_additions = name in child_additions
                has_removals_inside = any(
                    prefix == full or prefix.startswith(f"{full}/")
                    for prefix in removed_prefixes
                )

                if (
                    not is_completely_removed
                    and not has_removals_inside
                    and not has_additions
                ):
                    kept.append(
                        (
                            entry.path,
                            _subtree_line(entry.kind, entry.path, entry.hash),
                        )
                    )
                    continue

                sub_adds = child_additions.get(name, [])
                handled_child_dirs.add(name)
                new_sub_hash = self._splice_directory(
                    dir_path=full,
                    tree_hash=entry.hash if not is_completely_removed else None,
                    additions=sub_adds,
                    removed_prefixes=removed_prefixes,
                )
                if new_sub_hash is None:
                    changed = True
                else:
                    if new_sub_hash != entry.hash:
                        changed = True
                    kept.append(
                        (
                            name,
                            _subtree_line(KIND_TREE, name, new_sub_hash),
                        )
                    )

        for name, new_leaf in local_leaf_additions.items():
            if name not in handled_leaf_additions:
                kept.append((name, _entry_to_leaf_line(new_leaf)))
                changed = True

        for child_name, sub_adds in child_additions.items():
            if child_name not in handled_child_dirs:
                child_dir = f"{dir_path}/{child_name}" if dir_path else child_name
                new_sub_hash = self._splice_directory(
                    dir_path=child_dir,
                    tree_hash=None,
                    additions=sub_adds,
                    removed_prefixes=removed_prefixes,
                )
                if new_sub_hash is not None:
                    kept.append(
                        (
                            child_name,
                            _subtree_line(KIND_TREE, child_name, new_sub_hash),
                        )
                    )
                    changed = True

        return kept, changed

    def overlay_staged(
        self,
        *,
        parent_tree: str,
        additions: list[Entry],
        removed_prefixes: set[str],
    ) -> str:
        """Merge materialized staged additions/removals into the parent tree
        via path-splicing."""
        return self.splice_tree(
            parent_tree=parent_tree,
            additions=additions,
            removed_prefixes=removed_prefixes,
        )

    def write_commit_object(
        self,
        *,
        branch: str,
        message: str,
        parent_commit: str | None = None,
        parents: list[str] | None = None,
        tree_hash: str,
        expected_commit_id: str | None,
        operation: str,
    ) -> str:
        """Create a commit object, persist it, and advance the branch ref via CAS.

        ``parents`` overrides ``parent_commit`` (used for merge commits with
        multiple parents); the CAS expectation stays the branch head, i.e.
        the first parent.
        """
        if parents is None:
            parents = [parent_commit] if parent_commit else []
        generation = 0
        if parents:
            generation = (
                max(self.refs.generation_of(parent_id) for parent_id in parents) + 1
            )
        created_at = datetime.now(UTC).isoformat()
        # The commit id hashes only content (not timestamp or generation), so
        # the same content always yields the same id across branches and
        # retries are idempotent. ``created_at`` is recorded in the stored
        # payload for humans, not in the hash; ``generation`` is DAG-derivable
        # and kept as a stored perf hint.
        #
        # The identity body stays on stdlib ``json`` (sorted keys, ensure_ascii)
        # on purpose: it is a frozen canonical form that must never change.
        # Everything else in this module serializes with msgspec.
        identity_body: dict[str, object] = {
            "message": message,
            "tree": tree_hash,
            "parents": parents,
        }
        canonical = json.dumps(
            identity_body, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        commit_id = blake3(canonical).hexdigest()
        commit_object = CommitObject(
            id=commit_id,
            message=message,
            tree=tree_hash,
            parents=tuple(parents),
            created_at=created_at,
            generation=generation,
        )
        stored = (
            msgspec.json.encode(
                {
                    "id": commit_id,
                    "message": message,
                    "tree": tree_hash,
                    "parents": parents,
                    "created_at": created_at,
                    "generation": generation,
                },
                order="deterministic",
            )
            + b"\n"
        )
        self.store.write_commit_bytes(
            commit_id,
            stored,
        )
        self.refs.cache_commit(commit_object)
        self.refs.update_branch_ref(
            branch=branch,
            commit_id=commit_id,
            expected_commit_id=expected_commit_id,
            operation=operation,
        )
        return commit_id

    # ── Derived manifest (optional per-client materialization) ───────────

    # ── Read-only inspection (delegated to TreeInspector) ────────────
    # ``TreeWriter`` keeps these thin forwarders so existing callers
    # (``repository.gc``, sync planning, VFS) are unaffected. New code
    # should use ``writer.inspector`` (or ``TreeInspector``) directly.

    def iter_tree_hashes(
        self, root_tree: str, _seen: set[str] | None = None
    ) -> Iterator[str]:
        """Yield every tree object hash reachable from *root_tree*."""
        yield from self.inspector.iter_tree_hashes(root_tree, _seen=_seen)

    def diff_trees(self, from_tree: str, to_tree: str) -> list[DiffEntry]:
        """Structural diff between two trees (unchanged subtrees skipped)."""
        return self.inspector.diff_trees(from_tree, to_tree)

    def export_derived_manifest(self, tree_hash: str) -> Path:
        """Flatten *tree_hash* into a JSONL manifest in client state."""
        return self.inspector.export_derived_manifest(tree_hash)


def _close_frame(
    stack: list[_Frame], root_children: list[_Child], writer: TreeWriter
) -> None:
    """Pop one open directory frame, build its subtree, attach to parent."""
    dir_path, children, parent_by_name, _, known_hashes = stack.pop()
    merged = _merge_parent_children(children, parent_by_name)
    if not merged:
        return
    # If the rebuilt node (or any of its shard bodies) is byte-identical to a
    # parent object, skip the write (avoids doomed conditional PUTs per
    # unchanged directory on S3).
    subtree_hash = writer._build_children(
        merged, skip_hashes=known_hashes
    )  # noqa: SLF001
    name = dir_path.rsplit("/", 1)[-1]
    line = _subtree_line(KIND_TREE, name, subtree_hash)
    if stack:
        stack[-1][1].append((name, line))
    else:
        root_children.append((name, line))


def _merge_parent_children(
    children: list[_Child], parent_by_name: dict[str, Entry] | None
) -> list[_Child]:
    """Fold parent-only entries into the worktree children (sorted by name).

    Files present in both worktree and parent were already resolved by
    ``_materialize_worktree_file``; parent-only leaves and subtrees survive.
    """
    if not parent_by_name:
        return children
    child_names = {name for name, _ in children}
    extra: list[_Child] = []
    for name, entry in parent_by_name.items():
        if name in child_names:
            continue
        if entry.is_subtree:
            extra.append((name, _subtree_line(entry.kind, name, entry.hash)))
        else:
            extra.append((name, entry.serialize()))
    merged = children + extra
    merged.sort(key=lambda item: item[0])
    return merged
