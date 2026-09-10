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
    leaf_to_tree_entry,
)
from ..repository_support import metadata_identity
from .refs import RefManager
from .tree_inspect import TreeInspector

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


def _parent_dir(path: str) -> str:
    parts = path.split("/")
    return "/".join(parts[:-1])


def _path_is_removed(path: str, removed: set[str]) -> bool:
    if path in removed:
        return True
    for prefix in removed:
        if path.startswith(f"{prefix}/"):
            return True
    return False


def _leaf_line(
    kind: str,
    name: str,
    hash_value: str,
    size: int,
    mtime_ns: int,
    source_uri: str | None,
    footer: str | None,
) -> str:
    # Hot path (once per committed file): inputs are producer-guaranteed
    # (digest output, stat values, walk names, closed kind branch), so
    # serialize directly without building a validated Entry.
    return encode_leaf_parts(kind, name, hash_value, size, mtime_ns, source_uri, footer)


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
        trust_mtime: bool = False,
    ) -> None:
        self.store = store
        self.refs = refs
        self.client_state = client_state
        self.trust_mtime = trust_mtime
        self._walker = TreeWalker(read_tree=store.read_tree_bytes)
        self.inspector = TreeInspector(
            read_tree=store.read_tree_bytes,
            client_state=client_state,
        )

    # ── Low-level tree object writing ────────────────────────────────────

    def _write_tree_bytes(
        self,
        payload: bytes,
        *,
        skip_hashes: frozenset[str] | set[str] = frozenset(),
    ) -> str:
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
        removed_paths: set[str],
        staged_additions: list[Entry] | None = None,
        capture_footers: bool = False,
    ) -> str:
        """Build the root tree from a worktree walk.

        Files unchanged since the parent commit are reused without re-hashing
        (blake3 mode re-hashes on same size, exactly like v1).  Parent-only
        leaves and subtrees survive the commit — matching v1's merge
        semantics, where deletions require explicit staging.  Staged additions
        join the effective parent (they survive full commits even when the
        source file is gone).  Staged removals are pruned in a final pass.
        """
        removed = set(removed_paths)
        root_hash, leftover_addition_dirs = self._build_worktree_inner(
            worktree_root=worktree_root,
            parent_tree=parent_tree,
            identity_mode=identity_mode,
            removed_paths=removed,
            staged_additions=staged_additions,
            capture_footers=capture_footers,
        )
        if removed:
            pruned = self._prune_tree(root_hash, removed)
            if pruned is None:
                root_hash = self._write_tree_bytes(b"")
            else:
                root_hash = pruned
        if staged_additions and leftover_addition_dirs:
            leftovers = [
                entry
                for entry in staged_additions
                if _parent_dir(entry.path) in leftover_addition_dirs
            ]
            if leftovers:
                leftovers.sort(key=lambda entry: entry.path)
                root_hash = self.overlay_staged(
                    parent_tree=root_hash,
                    additions=leftovers,
                    removed_prefixes=set(),
                )
        return root_hash

    def _build_worktree_inner(
        self,
        *,
        worktree_root: Path,
        parent_tree: str | None,
        identity_mode: str,
        removed_paths: set[str],
        staged_additions: list[Entry] | None,
        capture_footers: bool,
    ) -> tuple[str, set[str]]:
        """Build the tree; also report staged-addition dirs that never opened.

        Only directories with worktree files get frames, so additions under
        otherwise-empty directories are never merged into a parent frame;
        callers overlay them onto the built tree afterwards.
        """
        root_children: list[_Child] = []
        stack: list[_Frame] = []

        additions_by_dir: dict[str, dict[str, Entry]] = {}
        if staged_additions:
            for entry in staged_additions:
                parts = entry.path.split("/")
                dir_path = "/".join(parts[:-1])
                additions_by_dir.setdefault(dir_path, {})[parts[-1]] = (
                    leaf_to_tree_entry(entry)
                )

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
                        known.update(
                            entry.hash for entry in raw if entry.is_subtree
                        )
                    for entry in entries:
                        merged[entry.path] = entry
            additions = additions_by_dir.pop(dir_path, None)
            if additions:
                merged.update(additions)
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
            digests = self._prefetch_digests(
                files,
                parent_by_name=parent_by_name,
                identity_mode=identity_mode,
            )
            for file_entry in files:
                children.append(
                    self._materialize_worktree_file(
                        file_entry=file_entry,
                        name=file_entry.relative_path.rsplit("/", 1)[-1],
                        full_path=file_entry.relative_path,
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
            if _path_is_removed(file_entry.relative_path, removed_paths):
                continue
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

        leftover_addition_dirs = set(additions_by_dir)

        merged_root = _merge_parent_children(root_children, root_parent)
        if merged_root:
            return (
                self._build_children(merged_root, skip_hashes=root_known),
                leftover_addition_dirs,
            )
        return self._write_tree_bytes(b""), leftover_addition_dirs

    def _prefetch_digests(
        self,
        files: list[FileEntry],
        *,
        parent_by_name: dict[str, Entry] | None,
        identity_mode: str,
    ) -> dict[Path, str]:
        """Hash the files that cannot be reused, in parallel.

        Only content-mode files that actually need a digest are hashed: files
        matched by size+mtime when ``trust_mtime`` is enabled are reused
        without reading their bytes at all.
        """
        if identity_mode != "content":
            return {}
        candidates: list[Path] = []
        for file_entry in files:
            name = file_entry.relative_path.rsplit("/", 1)[-1]
            parent_entry = parent_by_name.get(name) if parent_by_name else None
            if self._can_reuse_without_hash(parent_entry, file_entry):
                continue
            candidates.append(file_entry.path)
        if not candidates:
            return {}
        if len(candidates) < 2 or _HASH_WORKERS < 2:
            return {path: blake3_digest_file(path) for path in candidates}
        with ThreadPoolExecutor(
            max_workers=min(_HASH_WORKERS, len(candidates))
        ) as pool:
            return {
                path: digest
                for path, digest in zip(
                    candidates, pool.map(blake3_digest_file, candidates), strict=True
                )
            }

    def _can_reuse_without_hash(
        self, parent_entry: Entry | None, file_entry: FileEntry
    ) -> bool:
        """True when a content entry can be reused from size+mtime alone."""
        return (
            self.trust_mtime
            and parent_entry is not None
            and not parent_entry.is_subtree
            and parent_entry.kind in {KIND_BLOB, KIND_BP}
            and parent_entry.size == file_entry.size
            and parent_entry.mtime_ns == file_entry.mtime_ns
        )

    def _materialize_worktree_file(
        self,
        *,
        file_entry: FileEntry,
        name: str,
        full_path: str,
        parent_by_name: dict[str, Entry] | None,
        identity_mode: str,
        capture_footers: bool,
        prefetched_digest: str | None = None,
    ) -> _Child:
        parent_entry = parent_by_name.get(name) if parent_by_name else None
        if parent_entry is not None and not parent_entry.is_subtree:
            if identity_mode == "content":
                if parent_entry.kind in {KIND_BLOB, KIND_BP} and (
                    self._can_reuse_without_hash(parent_entry, file_entry)
                    or (
                        parent_entry.size == file_entry.size
                        and (
                            prefetched_digest
                            or blake3_digest_file(file_entry.path)
                        )
                        == parent_entry.hash
                    )
                ):
                    return self._reuse_or_backfill_footer(
                        parent_entry=parent_entry,
                        name=name,
                        source_path=file_entry.path,
                        source_uri=None,
                        capture_footers=capture_footers,
                    )
            elif (
                parent_entry.kind in {KIND_META, KIND_MP}
                and metadata_identity(full_path, file_entry.size) == parent_entry.hash
            ):
                return self._reuse_or_backfill_footer(
                    parent_entry=parent_entry,
                    name=name,
                    source_path=file_entry.path,
                    source_uri=file_entry.path.as_uri(),
                    capture_footers=capture_footers,
                )

        if identity_mode == "content":
            identity_value = prefetched_digest or blake3_digest_file(file_entry.path)
            self.store.write_blob_file(identity_value, file_entry.path, if_missing=True)
            footer = self._capture_footer(file_entry.path, capture_footers)
            line = _leaf_line(
                KIND_BP if footer else KIND_BLOB,
                name,
                identity_value,
                file_entry.size,
                file_entry.mtime_ns,
                None,
                footer,
            )
        else:
            identity_value = metadata_identity(full_path, file_entry.size)
            footer = self._capture_footer(file_entry.path, capture_footers)
            line = _leaf_line(
                KIND_MP if footer else KIND_META,
                name,
                identity_value,
                file_entry.size,
                file_entry.mtime_ns,
                file_entry.path.as_uri(),
                footer,
            )
        return name, line

    def _reuse_or_backfill_footer(
        self,
        *,
        parent_entry: Entry,
        name: str,
        source_path: Path,
        source_uri: str | None,
        capture_footers: bool,
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
            parent_entry.mtime_ns,
            source_uri,
            footer,
        )

    def _capture_footer(self, source_path: Path, capture_footers: bool) -> str | None:
        if not capture_footers or source_path.suffix.lower() != ".parquet":
            return None
        from ..objects.footer import capture_footer_stats

        with source_path.open("rb") as handle:
            return capture_footer_stats(self.store, handle)

    def _prune_tree(self, root_tree: str, removed: set[str]) -> str | None:
        """Rebuild the tree DAG, dropping entries under removal prefixes.

        Returns ``None`` when the whole tree is removed.  Subtrees untouched
        by any removal are reused by hash.  Sharded directories are pruned
        shard by shard: only the ranges that can contain a removed name are
        loaded, so removing a file from a million-entry directory stays
        O(touched shards), not O(children).
        """

        def removal_ranges(dir_path: str) -> list[tuple[str, str]]:
            """``(low, high)`` name ranges of the removals inside *dir_path*."""
            prefix_len = len(dir_path) + 1 if dir_path else 0
            ranges: list[tuple[str, str]] = []
            for prefix in removed:
                if prefix_len:
                    if not prefix.startswith(f"{dir_path}/"):
                        continue
                    rel = prefix[prefix_len:]
                else:
                    rel = prefix
                if rel:
                    # Every name starting with *rel* sorts inside this range.
                    ranges.append((rel, rel + "\U0010ffff"))
            return ranges

        def prune_children(
            dir_path: str, entries: list[Entry]
        ) -> tuple[list[_Child], bool]:
            kept: list[_Child] = []
            changed = False
            for entry in entries:
                full = f"{dir_path}/{entry.path}" if dir_path else entry.path
                if _path_is_removed(full, removed):
                    changed = True
                    continue
                if entry.is_subtree:
                    needs_rebuild = any(
                        prefix.startswith(f"{full}/") for prefix in removed
                    )
                    if not needs_rebuild:
                        kept.append(
                            (
                                entry.path,
                                _subtree_line(entry.kind, entry.path, entry.hash),
                            )
                        )
                        continue
                    new_hash = prune(full, entry.hash)
                    if new_hash is None:
                        changed = True
                        continue
                    if new_hash != entry.hash:
                        changed = True
                    kept.append(
                        (
                            entry.path,
                            _subtree_line(entry.kind, entry.path, new_hash),
                        )
                    )
                else:
                    kept.append((entry.path, entry.serialize()))
            return kept, changed

        def prune(dir_path: str, tree_hash: str) -> str | None:
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                raise ValueError(f"Unknown tree object: {tree_hash}")
            if any(entry.kind == KIND_SHARD for entry in entries):
                # Shards stand in for name ranges of this directory's
                # children, so a removal prefix must be tested against the
                # real names, not the shard pointer names. A shard only
                # shrinks here, so the rewritten body never needs splitting.
                ranges = removal_ranges(dir_path)
                kept: list[_Child] = []
                changed = False
                for index, shard in enumerate(entries):
                    if shard.kind != KIND_SHARD:
                        kept.append(
                            (
                                shard.path,
                                _subtree_line(shard.kind, shard.path, shard.hash),
                            )
                        )
                        continue
                    upper = (
                        entries[index + 1].path
                        if index + 1 < len(entries)
                        else None
                    )
                    touched = any(
                        (upper is None or low < upper) and high > shard.path
                        for low, high in ranges
                    )
                    if not touched:
                        kept.append(
                            (
                                shard.path,
                                _subtree_line(KIND_SHARD, shard.path, shard.hash),
                            )
                        )
                        continue
                    body = self._walker.load_entries(shard.hash)
                    if body is None:
                        raise ValueError(f"Unknown tree object: {shard.hash}")
                    body_kept, body_changed = prune_children(dir_path, body)
                    if not body_kept:
                        changed = True
                        continue
                    if not body_changed:
                        kept.append(
                            (
                                shard.path,
                                _subtree_line(KIND_SHARD, shard.path, shard.hash),
                            )
                        )
                        continue
                    changed = True
                    body_kept.sort(key=lambda item: item[0])
                    body_hash = self._write_plain_node(
                        body_kept, skip_hashes={shard.hash}
                    )
                    shard_name = body_kept[0][0]
                    kept.append(
                        (
                            shard_name,
                            _subtree_line(KIND_SHARD, shard_name, body_hash),
                        )
                    )
                if not kept:
                    return None
                if not changed:
                    return tree_hash
                return self._build_children(kept, skip_hashes={tree_hash})

            kept, changed = prune_children(dir_path, entries)
            if not kept:
                return None
            if not changed:
                return tree_hash
            return self._build_children(kept)

        return prune("", root_tree)

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
        """Metadata-only, tree-level 3-way merge (docs/architecture.md §9).

        Walks *directory nodes*, never a flattened leaf stream: identical
        subtrees are reused by content hash in O(1), and a directory whose
        content is unchanged on one side is taken from the other side without
        descending at all. Merging two branches that each changed one file
        therefore costs O(path depth + changed directories), not O(files).

        Rules per path: identical on both sides wins; a side equal to the base
        is "unchanged" and yields the other side; anything else is a conflict.
        Returns ``(merged_tree_hash, conflict_paths)``.
        """
        conflicts: list[str] = []
        merged = self._merge_directory(
            dir_path="",
            base_hash=base_tree,
            ours_hash=ours_tree,
            theirs_hash=theirs_tree,
            conflicts=conflicts,
        )
        if merged is None:
            merged = self._write_tree_bytes(b"")
        return merged, conflicts

    def _children_by_name(self, tree_hash: str) -> dict[str, Entry]:
        return {
            entry.path: entry for entry in self._walker.load_children(tree_hash)
        }

    def _merge_directory(
        self,
        *,
        dir_path: str,
        base_hash: str | None,
        ours_hash: str | None,
        theirs_hash: str | None,
        conflicts: list[str],
    ) -> str | None:
        """Merge one directory node; returns its hash or ``None`` if empty."""
        # O(1) fast paths: content-addressed hashes decide whole subtrees.
        if ours_hash == theirs_hash:
            return ours_hash
        if ours_hash == base_hash:
            return theirs_hash
        if theirs_hash == base_hash:
            return ours_hash

        # Both sides diverged. A subtree present on only one side was deleted
        # by the other and modified here — a conflict, reported at dir level.
        if ours_hash is None or theirs_hash is None:
            conflicts.append(dir_path)
            return ours_hash if ours_hash is not None else theirs_hash

        # Sharded directories: when both sides share shard boundaries (the
        # common case, since both derive from the base), compare shard ranges
        # pairwise and recurse only into the ranges that differ — never load
        # the whole directory.
        handled, merged = self._merge_sharded(
            dir_path=dir_path,
            base_hash=base_hash,
            ours_hash=ours_hash,
            theirs_hash=theirs_hash,
            conflicts=conflicts,
        )
        if handled:
            return merged

        ours_children = self._children_by_name(ours_hash)
        theirs_children = self._children_by_name(theirs_hash)
        base_children: dict[str, Entry] = (
            {} if base_hash is None else self._children_by_name(base_hash)
        )

        merged_children: list[_Child] = []
        for name in sorted(
            set(base_children) | set(ours_children) | set(theirs_children)
        ):
            path = f"{dir_path}/{name}" if dir_path else name
            merged = self._merge_child(
                path=path,
                base=base_children.get(name),
                ours=ours_children.get(name),
                theirs=theirs_children.get(name),
                conflicts=conflicts,
            )
            if merged is None:
                continue
            if merged.is_subtree:
                line = _subtree_line(merged.kind, merged.path, merged.hash)
            else:
                line = _entry_to_leaf_line(merged)
            merged_children.append((name, line))

        if not merged_children:
            return None
        return self._build_children(
            merged_children,
            skip_hashes={
                tree_hash
                for tree_hash in (ours_hash, theirs_hash)
                if tree_hash is not None
            },
        )

    def _merge_sharded(
        self,
        *,
        dir_path: str,
        base_hash: str | None,
        ours_hash: str | None,
        theirs_hash: str | None,
        conflicts: list[str],
    ) -> tuple[bool, str | None]:
        """Shard-range merge; ``(False, None)`` when not applicable.

        Requires all three sides to be sharded with identical range names —
        i.e. the ranges were not re-balanced since the merge base. Otherwise
        the caller falls back to expanding the directory's children.
        """
        if base_hash is None or ours_hash is None or theirs_hash is None:
            return False, None
        base_node = self._walker.load_entries(base_hash)
        ours_node = self._walker.load_entries(ours_hash)
        theirs_node = self._walker.load_entries(theirs_hash)
        if base_node is None or ours_node is None or theirs_node is None:
            return False, None
        if not all(entry.kind == KIND_SHARD for entry in base_node):
            return False, None
        if not all(entry.kind == KIND_SHARD for entry in ours_node):
            return False, None
        if not all(entry.kind == KIND_SHARD for entry in theirs_node):
            return False, None
        if [e.path for e in ours_node] != [e.path for e in theirs_node] or [
            e.path for e in base_node
        ] != [e.path for e in theirs_node]:
            return False, None

        merged_shards: list[_Child] = []
        for ours_shard, theirs_shard, base_shard in zip(
            ours_node, theirs_node, base_node, strict=True
        ):
            if ours_shard.hash == theirs_shard.hash:
                merged_hash: str | None = ours_shard.hash
            elif ours_shard.hash == base_shard.hash:
                merged_hash = theirs_shard.hash
            elif theirs_shard.hash == base_shard.hash:
                merged_hash = ours_shard.hash
            else:
                merged_hash = self._merge_directory(
                    dir_path=dir_path,
                    base_hash=base_shard.hash,
                    ours_hash=ours_shard.hash,
                    theirs_hash=theirs_shard.hash,
                    conflicts=conflicts,
                )
            if merged_hash is None:
                continue  # the whole range was removed on both sides
            merged_shards.append(
                (
                    ours_shard.path,
                    _subtree_line(KIND_SHARD, ours_shard.path, merged_hash),
                )
            )

        if not merged_shards:
            return True, None
        return True, self._build_children(
            merged_shards, skip_hashes={ours_hash, theirs_hash}
        )

    def _merge_child(
        self,
        *,
        path: str,
        base: Entry | None,
        ours: Entry | None,
        theirs: Entry | None,
        conflicts: list[str],
    ) -> Entry | None:
        """Merge one named child (leaf or subtree); ``None`` means removed."""

        def equal(left: Entry | None, right: Entry | None) -> bool:
            return (
                left is not None
                and right is not None
                and left.kind == right.kind
                and left.hash == right.hash
                and left.size == right.size
            )

        if equal(ours, theirs):
            return ours
        if ours is None or theirs is None:
            present = ours if ours is not None else theirs
            assert present is not None
            if base is None or equal(present, base):
                # Added by one side, or deleted by the side without it while
                # the other side left it untouched.
                return present if base is None else None
            # Modified on one side, deleted on the other.
            conflicts.append(path)
            return present

        if base is not None and equal(ours, base):
            return theirs
        if base is not None and equal(theirs, base):
            return ours

        if ours.is_subtree and theirs.is_subtree:
            merged_hash = self._merge_directory(
                dir_path=path,
                base_hash=(
                    base.hash if base is not None and base.is_subtree else None
                ),
                ours_hash=ours.hash,
                theirs_hash=theirs.hash,
                conflicts=conflicts,
            )
            if merged_hash is None:
                return None
            return Entry(path=ours.path, kind=ours.kind, hash=merged_hash)

        # Both sides changed the same path differently (including shape
        # changes such as file → directory).
        conflicts.append(path)
        return ours

    def merge_trees(
        self,
        base_tree: str | None,
        ours_tree: str,
        theirs_tree: str,
    ) -> str:
        """Three-way merge that raises ``MergeConflictError`` on conflicts."""
        from ..domain import MergeConflictError

        merged_tree, conflicts = self.three_way_merge(base_tree, ours_tree, theirs_tree)
        if conflicts:
            raise MergeConflictError(paths=sorted(conflicts))
        return merged_tree

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
                    continue

                if name in local_leaf_additions:
                    new_leaf = local_leaf_additions[name]
                    kept.append((name, _entry_to_leaf_line(new_leaf)))
                    handled_leaf_additions.add(name)
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
                max(
                    self.refs.read_commit(parent_id).generation for parent_id in parents
                )
                + 1
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
        self.refs.cache_commit(commit_object)
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

    def iter_leaf_refs(
        self,
        root_tree: str,
        memo: dict[str, tuple[tuple[str | None, str | None], ...]] | None = None,
    ) -> Iterator[tuple[str | None, str | None]]:
        """Yield ``(blob_hash, footer_hash)`` for every leaf in the tree DAG."""
        yield from self.inspector.iter_leaf_refs(root_tree, memo=memo)

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
    subtree_hash = writer._build_children(merged, skip_hashes=known_hashes)  # noqa: SLF001
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
