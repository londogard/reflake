"""Read-only tree inspection: GC enumeration and derived-manifest export.

Split out of ``TreeWriter`` (``services/tree.py``) so write-path and
read-path responsibilities live in separate classes:

- ``TreeInspector`` — ``iter_tree_hashes`` / ``iter_leaf_refs`` (reachable-set
  walks shared by GC and sync planning) plus ``export_derived_manifest``, the
  optional client-side JSONL flattening of a tree.
- ``TreeWriter`` keeps thin delegating methods with the same names so
  existing callers (``repository.gc``, sync planning, VFS) are unaffected.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile

from ..client_state import LocalClientState
from ..domain import DiffEntry
from ..entry_codec import Entry
from ..objects.query import TreeWalker


class TreeInspector:
    """Read-only views over a content-addressed tree DAG."""

    def __init__(
        self,
        *,
        read_tree: Callable[[str], bytes | None],
        client_state: LocalClientState | None = None,
    ) -> None:
        self._walker = TreeWalker(read_tree=read_tree)
        self.client_state = client_state

    # ── GC enumeration ───────────────────────────────────────────────

    def iter_tree_hashes(
        self, root_tree: str, _seen: set[str] | None = None
    ) -> Iterator[str]:
        """Yield every tree object hash reachable from *root_tree*.

        Pass a shared ``_seen`` set to memoize across calls (e.g. GC
        walking many commits over shared subtrees).
        """
        seen: set[str] = _seen if _seen is not None else set()
        stack = [root_tree]
        while stack:
            tree_hash = stack.pop()
            if tree_hash in seen:
                continue
            seen.add(tree_hash)
            yield tree_hash
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                continue
            for entry in reversed(entries):
                if entry.is_subtree:
                    stack.append(entry.hash)

    def iter_leaf_refs(
        self,
        root_tree: str,
        memo: dict[str, tuple[tuple[str | None, str | None], ...]] | None = None,
    ) -> Iterator[tuple[str | None, str | None]]:
        """Yield ``(blob_hash, footer_hash)`` for every leaf in the tree DAG.

        Metadata-only leaves yield ``(None, None)`` — they reference no
        canonical object. Used by GC to compute the reachable set.

        Pass a shared *memo* to walk subtrees shared across commits only
        once (GC over a DAG, sync planning over many commits).
        """
        from ..objects.tree import KIND_BLOB, KIND_BP

        leaf_refs_memo: dict[str, tuple[tuple[str | None, str | None], ...]] = (
            {} if memo is None else memo
        )
        yield from self._collect_leaf_refs(
            root_tree, leaf_refs_memo, KIND_BLOB, KIND_BP
        )

    def _collect_leaf_refs(
        self,
        root_tree: str,
        memo: dict[str, tuple[tuple[str | None, str | None], ...]],
        kind_blob: str,
        kind_bp: str,
    ) -> tuple[tuple[str | None, str | None], ...]:
        """Memoized, iterative collection of a tree's leaf references."""
        stack = [root_tree]
        while stack:
            tree_hash = stack[-1]
            if tree_hash in memo:
                stack.pop()
                continue
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                memo[tree_hash] = ()
                stack.pop()
                continue
            subtree_hashes = [entry.hash for entry in entries if entry.is_subtree]
            unresolved = [h for h in subtree_hashes if h not in memo]
            if unresolved:
                stack.extend(unresolved)
                continue
            refs: list[tuple[str | None, str | None]] = []
            for entry in entries:
                if entry.is_subtree:
                    refs.extend(memo[entry.hash])
                elif entry.kind in (kind_blob, kind_bp):
                    refs.append((entry.hash, entry.footer))
                elif entry.footer is not None:
                    refs.append((None, entry.footer))
            memo[tree_hash] = tuple(refs)
            stack.pop()
        return memo[root_tree]

    # ── Structural diff ─────────────────────────────────────────────

    def diff_trees(self, from_tree: str, to_tree: str) -> list[DiffEntry]:
        """Diff two trees structurally: identical subtrees are skipped by hash.

        Only directories that actually differ are read; unchanged subtrees are
        never enumerated, so a one-file change costs O(depth + changed
        directories + changed leaves) instead of O(files).
        """
        changes: list[DiffEntry] = []
        self._diff_trees(from_tree, to_tree, "", changes)
        return changes

    def _diff_trees(
        self,
        from_tree: str,
        to_tree: str,
        prefix: str,
        changes: list[DiffEntry],
    ) -> None:
        if from_tree == to_tree:
            return
        from_children = {
            entry.path: entry for entry in self._walker.load_children(from_tree)
        }
        to_children = {
            entry.path: entry for entry in self._walker.load_children(to_tree)
        }
        for name in sorted(set(from_children) | set(to_children)):
            path = f"{prefix}/{name}" if prefix else name
            before = from_children.get(name)
            after = to_children.get(name)
            if before is None:
                self._emit_change(changes, path, "added", before=None, after=after)
            elif after is None:
                self._emit_change(changes, path, "removed", before=before, after=None)
            elif before.is_subtree and after.is_subtree:
                if before.hash != after.hash:
                    self._diff_trees(before.hash, after.hash, path, changes)
            elif before.is_subtree or after.is_subtree:
                # Shape change (file ↔ directory): flatten both sides.
                self._emit_change(changes, path, "removed", before=before, after=None)
                self._emit_change(changes, path, "added", before=None, after=after)
            elif before.hash != after.hash or before.size != after.size:
                changes.append(
                    DiffEntry(
                        path=path,
                        change="modified",
                        before_hash=before.hash,
                        after_hash=after.hash,
                        before_size=before.size,
                        after_size=after.size,
                    )
                )

    def _emit_change(
        self,
        changes: list[DiffEntry],
        path: str,
        change: str,
        *,
        before: Entry | None,
        after: Entry | None,
    ) -> None:
        """Append a change, expanding subtrees into their leaves in path order."""
        if (before is not None and before.is_subtree) or (
            after is not None and after.is_subtree
        ):
            pending: list[tuple[str, Entry | None, Entry | None]] = []
            if before is not None and before.is_subtree:
                pending.extend(
                    (f"{path}/{leaf.path}", leaf, None)
                    for leaf in self._walker.iter_all_entries(before.hash)
                )
            if after is not None and after.is_subtree:
                pending.extend(
                    (f"{path}/{leaf.path}", None, leaf)
                    for leaf in self._walker.iter_all_entries(after.hash)
                )
            for leaf_path, before_leaf, after_leaf in sorted(
                pending, key=lambda item: item[0]
            ):
                self._emit_change(
                    changes,
                    leaf_path,
                    change,
                    before=before_leaf,
                    after=after_leaf,
                )
            return
        changes.append(
            DiffEntry(
                path=path,
                change=change,
                before_hash=before.hash if before is not None else None,
                after_hash=after.hash if after is not None else None,
                before_size=before.size if before is not None else None,
                after_size=after.size if after is not None else None,
            )
        )

    # ── Derived manifest (optional per-client materialization) ───────

    def export_derived_manifest(self, tree_hash: str) -> Path:
        """Flatten *tree_hash* into a JSONL manifest in client state.

        The artifact is a readable, streamable view of one tree — handy for
        grepping/eyeballing a snapshot — cached per client keyed by the root
        tree hash (content-addressed ⇒ never stale) and never written to the
        shared store.
        """
        if self.client_state is None:
            raise RuntimeError(
                "TreeInspector has no client_state for derived exports"
            )
        cache_path = self.client_state.derived_manifest_path(tree_hash)
        if cache_path.exists():
            return cache_path
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(
            mode="wb", suffix=".jsonl", delete=False
        ) as temp:
            temp_path = Path(temp.name)
        try:
            with temp_path.open("wb") as handle:
                for entry in self._walker.iter_all_entries(tree_hash):
                    handle.write(entry.serialize().encode("utf-8") + b"\n")
            temp_path.replace(cache_path)
        finally:
            temp_path.unlink(missing_ok=True)
        return cache_path
