"""Tree-level three-way merge (extracted from TreeWriter).

The merge is the read-mostly half of the tree service: it walks directory
nodes, short-circuits identical content hashes, and asks the writer to build
merged nodes. Keeping it separate makes the write path (builder/splicer) and
the merge path independently testable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..entry_codec import Entry
from ..objects.tree import KIND_SHARD

if TYPE_CHECKING:  # pragma: no cover
    from .tree import TreeWriter


def _subtree_line(kind: str, name: str, hash_value: str) -> str:
    from .tree import _subtree_line as impl

    return impl(kind, name, hash_value)


def _entry_to_leaf_line(entry: Entry) -> str:
    from .tree import _entry_to_leaf_line as impl

    return impl(entry)


class TreeMerger:
    """Three-way merge over content-addressed tree nodes."""

    def __init__(self, writer: TreeWriter) -> None:
        self.writer = writer

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
            merged = self.writer._write_tree_bytes(b"")
        return merged, conflicts

    def _children_by_name(self, tree_hash: str) -> dict[str, Entry]:
        return {
            entry.path: entry
            for entry in self.writer._walker.load_children(tree_hash)
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

        merged_children: list[tuple[str, str]] = []
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
        return self.writer._build_children(
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
        base_node = self.writer._walker.load_entries(base_hash)
        ours_node = self.writer._walker.load_entries(ours_hash)
        theirs_node = self.writer._walker.load_entries(theirs_hash)
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

        merged_shards: list[tuple[str, str]] = []
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
        return True, self.writer._build_children(
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
            if left is None or right is None:
                return left is None and right is None
            return (
                left.kind == right.kind
                and left.hash == right.hash
                and left.size == right.size
            )

        # Deleting the same path on both sides is not a conflict: the union
        # of both sides simply does not contain it. (This used to fall through
        # to the "present on one side" branch and trip an assertion.)
        if ours is None and theirs is None:
            return None
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
                base_hash=(base.hash if base is not None and base.is_subtree else None),
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


