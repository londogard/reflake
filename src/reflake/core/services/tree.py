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
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import msgspec
from blake3 import blake3

from ..client_state import LocalClientState
from ..domain import CommitObject
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
from .tree_inspect import DERIVED_BLOCK_ENTRY_COUNT as DERIVED_BLOCK_ENTRY_COUNT
from .tree_inspect import TreeInspector

_Child = tuple[str, str]  # (name, serialized tree line)
_Frame = tuple[str, list[_Child], dict[str, Entry] | None]


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
    ) -> None:
        self.store = store
        self.refs = refs
        self.client_state = client_state
        self._walker = TreeWalker(read_tree=store.read_tree_bytes)
        self.inspector = TreeInspector(
            read_tree=store.read_tree_bytes,
            client_state=client_state,
        )

    # ── Low-level tree object writing ────────────────────────────────────

    def _write_tree_bytes(self, payload: bytes) -> str:
        tree_hash = blake3(payload).hexdigest()
        # object_exists check omitted: write_tree_bytes with if_missing=True
        # uses IfNoneMatch=* (S3) or path.exists() (local) as an atomic guard.
        # A 412 / early-return on conflict is handled inside write_tree_bytes.
        self.store.write_tree_bytes(tree_hash, payload, if_missing=True)
        return tree_hash

    def _build_children(
        self, children: list[_Child], *, already_sorted: bool = False
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
            return self._write_tree_bytes(payload)

        shard_children: list[_Child] = []
        for start in range(0, len(children), MAX_TREE_ENTRIES):
            chunk = children[start : start + MAX_TREE_ENTRIES]
            shard_hash = self._build_children(chunk, already_sorted=True)
            shard_name = chunk[0][0]
            shard_children.append(
                (shard_name, _subtree_line(KIND_SHARD, shard_name, shard_hash))
            )
        return self._build_children(shard_children, already_sorted=True)

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

        def parent_children(dir_path: str) -> dict[str, Entry] | None:
            merged: dict[str, Entry] = {}
            if parent_tree:
                entries = self._walker.resolve_subtree(parent_tree, dir_path)
                if entries:
                    for entry in entries:
                        merged[entry.path] = entry
            additions = additions_by_dir.pop(dir_path, None)
            if additions:
                merged.update(additions)
            return merged or None

        root_parent = parent_children("")

        def open_frames(dir_path: str) -> None:
            while stack and not (
                dir_path == stack[-1][0] or dir_path.startswith(f"{stack[-1][0]}/")
            ):
                _close_frame(stack, root_children, self)
            parts = [p for p in dir_path.split("/") if p]
            for index in range(len(stack), len(parts)):
                frame_dir = "/".join(parts[: index + 1])
                stack.append((frame_dir, [], parent_children(frame_dir)))

        for file_entry in walk_files(worktree_root):
            relative_path = file_entry.relative_path
            if _path_is_removed(relative_path, removed_paths):
                continue
            parts = relative_path.split("/")
            dir_path = "/".join(parts[:-1])
            name = parts[-1]
            if dir_path:
                open_frames(dir_path)
                frame = stack[-1]
                frame[1].append(
                    self._materialize_worktree_file(
                        file_entry=file_entry,
                        name=name,
                        full_path=relative_path,
                        parent_by_name=frame[2],
                        identity_mode=identity_mode,
                        capture_footers=capture_footers,
                    )
                )
            else:
                root_children.append(
                    self._materialize_worktree_file(
                        file_entry=file_entry,
                        name=name,
                        full_path=relative_path,
                        parent_by_name=root_parent,
                        identity_mode=identity_mode,
                        capture_footers=capture_footers,
                    )
                )

        while stack:
            _close_frame(stack, root_children, self)

        leftover_addition_dirs = set(additions_by_dir)

        merged_root = _merge_parent_children(root_children, root_parent)
        if merged_root:
            return self._build_children(merged_root), leftover_addition_dirs
        return self._write_tree_bytes(b""), leftover_addition_dirs

    def _materialize_worktree_file(
        self,
        *,
        file_entry: FileEntry,
        name: str,
        full_path: str,
        parent_by_name: dict[str, Entry] | None,
        identity_mode: str,
        capture_footers: bool,
    ) -> _Child:
        parent_entry = parent_by_name.get(name) if parent_by_name else None
        if parent_entry is not None and not parent_entry.is_subtree:
            if identity_mode == "content":
                if (
                    parent_entry.kind in {KIND_BLOB, KIND_BP}
                    and parent_entry.size == file_entry.size
                    and blake3_digest_file(file_entry.path) == parent_entry.hash
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
            identity_value = blake3_digest_file(file_entry.path)
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
        by any removal are reused by hash.
        """

        def prune(dir_path: str, tree_hash: str) -> str | None:
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                raise ValueError(f"Unknown tree object: {tree_hash}")
            kept: list[_Child] = []
            changed = False
            for entry in entries:
                full = f"{dir_path}/{entry.path}" if dir_path else entry.path
                if _path_is_removed(full, removed):
                    changed = True
                    continue
                if entry.is_subtree:
                    child_dir = dir_path if entry.kind == KIND_SHARD else full
                    needs_rebuild = any(
                        _path_is_removed(full, {prefix}) is False
                        and (prefix == full or prefix.startswith(f"{full}/"))
                        for prefix in removed
                    )
                    if not needs_rebuild:
                        kept.append(
                            (
                                entry.path,
                                _subtree_line(entry.kind, entry.path, entry.hash),
                            )
                        )
                        continue
                    new_hash = prune(child_dir, entry.hash)
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
                stack.append((frame_dir, [], None))
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
        """Metadata-only 3-way merge (docs/architecture.md §9).

        Streams the base/ours/theirs trees as sorted leaf entries and applies
        standard merge rules: take ours/theirs where only one side changed,
        auto-keep identical additions, and report a conflict for every path
        modified on both sides.  Returns ``(merged_tree_hash, conflict_paths)``.
        """
        base_iter = (
            iter(()) if base_tree is None else self._walker.iter_all_entries(base_tree)
        )
        ours_iter = self._walker.iter_all_entries(ours_tree)
        theirs_iter = self._walker.iter_all_entries(theirs_tree)

        conflicts: list[str] = []

        def same(left: Entry | None, right: Entry | None) -> bool:
            if left is None or right is None:
                return left is right
            return left.hash == right.hash and left.size == right.size

        base = next(base_iter, None)
        ours = next(ours_iter, None)
        theirs = next(theirs_iter, None)

        def merged_stream() -> Iterator[Entry]:
            nonlocal base, ours, theirs

            def advance() -> None:
                nonlocal base, ours, theirs
                path = min(p.path for p in (base, ours, theirs) if p is not None)
                if base is not None and base.path == path:
                    base = next(base_iter, None)
                if ours is not None and ours.path == path:
                    ours = next(ours_iter, None)
                if theirs is not None and theirs.path == path:
                    theirs = next(theirs_iter, None)

            while base is not None or ours is not None or theirs is not None:
                path = min(p.path for p in (base, ours, theirs) if p is not None)
                b = base if base is not None and base.path == path else None
                o = ours if ours is not None and ours.path == path else None
                t = theirs if theirs is not None and theirs.path == path else None

                if o is not None and t is not None:
                    if same(o, t):
                        yield o
                    elif b is not None and same(o, b):
                        yield t
                    elif b is not None and same(t, b):
                        yield o
                    else:
                        conflicts.append(path)
                        yield o
                elif o is not None:
                    if b is None or not same(o, b):
                        if b is not None:
                            conflicts.append(path)  # we changed; they removed
                        yield o
                elif t is not None:
                    if b is None or not same(t, b):
                        if b is not None:
                            conflicts.append(path)  # they changed; we removed
                        yield t
                # base-only: both sides removed — drop

                advance()

        merged_tree = self.build_from_entries(merged_stream())
        return merged_tree, conflicts

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

    def _splice_directory(
        self,
        dir_path: str,
        tree_hash: str | None,
        additions: list[Entry],
        removed_prefixes: set[str],
    ) -> str | None:
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

        kept: list[_Child] = []
        changed = False

        if tree_hash is not None:
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                raise ValueError(f"Unknown tree object: {tree_hash}")

            if any(e.kind == KIND_SHARD for e in entries):
                unpacked: list[Entry] = []
                for e in entries:
                    shard_entries = self._walker.load_entries(e.hash)
                    if shard_entries:
                        unpacked.extend(shard_entries)
                entries = unpacked

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

        else:
            for name, new_leaf in local_leaf_additions.items():
                kept.append((name, _entry_to_leaf_line(new_leaf)))
                changed = True

            for child_name, sub_adds in child_additions.items():
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

        if not kept:
            return None
        if not changed and tree_hash is not None:
            return tree_hash
        return self._build_children(kept)

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

    def iter_leaf_refs(
        self,
        root_tree: str,
        _seen_trees: set[str] | None = None,
    ) -> Iterator[tuple[str | None, str | None]]:
        """Yield ``(blob_hash, footer_hash)`` for every leaf in the tree DAG."""
        yield from self.inspector.iter_leaf_refs(root_tree, _seen_trees=_seen_trees)

    def export_derived_manifest(self, tree_hash: str) -> Path:
        """Flatten *tree_hash* into a JSONL manifest with block offsets."""
        return self.inspector.export_derived_manifest(tree_hash)

    def lookup_derived_entry(self, tree_hash: str, logical_path: str) -> Entry | None:
        """Point lookup through the cached derived manifest (optional path)."""
        return self.inspector.lookup_derived_entry(tree_hash, logical_path)


def _close_frame(
    stack: list[_Frame], root_children: list[_Child], writer: TreeWriter
) -> None:
    """Pop one open directory frame, build its subtree, attach to parent."""
    dir_path, children, parent_by_name = stack.pop()
    merged = _merge_parent_children(children, parent_by_name)
    if not merged:
        return
    subtree_hash = writer._build_children(merged)  # noqa: SLF001
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
