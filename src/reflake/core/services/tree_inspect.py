"""Read-only tree inspection: GC enumeration and derived manifests.

Split out of ``TreeWriter`` (``services/tree.py``) so write-path and
read-path responsibilities live in separate classes:

- ``TreeInspector`` — ``iter_tree_hashes`` / ``iter_leaf_refs`` (GC
  reachable-set walks) plus the optional client-side derived-manifest
  cache (``export_derived_manifest`` / ``lookup_derived_entry``).
- ``TreeWriter`` keeps thin delegating methods with the same names so
  existing callers (``repository.gc``, sync planning, VFS) are unaffected.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile

from ..client_state import LocalClientState
from ..entry_codec import Entry
from ..manifest import ManifestWriter
from ..objects.query import TreeWalker

#: Derived-manifest block size for the optional client-side point-lookup cache.
DERIVED_BLOCK_ENTRY_COUNT = 4096


def _lookup_block_index(paths: list[str], logical_path: str) -> int | None:
    """Binary search for the block that may contain *logical_path*."""
    low, high = 0, len(paths)
    while low < high:
        mid = (low + high) // 2
        if paths[mid] <= logical_path:
            low = mid + 1
        else:
            high = mid
    if low == 0:
        return None
    return low - 1


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
        _seen_trees: set[str] | None = None,
    ) -> Iterator[tuple[str | None, str | None]]:
        """Yield ``(blob_hash, footer_hash)`` for every leaf in the tree DAG.

        Metadata-only leaves yield ``(None, None)`` — they reference no
        canonical object. Used by GC to compute the reachable set.
        """
        from ..objects.tree import KIND_BLOB, KIND_BP

        seen_trees: set[str] = (
            _seen_trees if _seen_trees is not None else set()
        )
        stack = [root_tree]
        while stack:
            tree_hash = stack.pop()
            if tree_hash in seen_trees:
                continue
            seen_trees.add(tree_hash)
            entries = self._walker.load_entries(tree_hash)
            if entries is None:
                continue
            for entry in entries:
                if entry.is_subtree:
                    stack.append(entry.hash)
                elif entry.kind in (KIND_BLOB, KIND_BP):
                    yield entry.hash, entry.footer
                elif entry.footer is not None:
                    yield None, entry.footer

    # ── Derived manifest (optional per-client materialization) ───────

    def export_derived_manifest(self, tree_hash: str) -> Path:
        """Flatten *tree_hash* into a JSONL manifest with block offsets.

        The artifact (and its block index sidecar) is cached in local client
        state keyed by the root-tree hash (content-addressed ⇒ cache-safe) and
        is never written to the shared store.
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
        writer = ManifestWriter(
            temp_path, block_entry_count=DERIVED_BLOCK_ENTRY_COUNT
        )
        writer.write_entries(
            (entry.path, entry.serialize())
            for entry in self._walker.iter_all_entries(tree_hash)
        )
        index = writer.build_index()
        if index is not None:
            self.client_state.write_derived_index(tree_hash, index)
        temp_path.replace(cache_path)
        return cache_path

    def lookup_derived_entry(
        self, tree_hash: str, logical_path: str
    ) -> Entry | None:
        """Point lookup through the cached derived manifest (optional path).

        Binary-searches the block index and range-reads one slice of the
        JSONL manifest. Falls back to a tree-walk lookup when the derived
        manifest has not been materialized.
        """
        if self.client_state is None:
            return None
        cache_path = self.client_state.derived_manifest_path(tree_hash)
        if not cache_path.exists():
            return None

        index = self.client_state.read_derived_index(tree_hash)
        if index is None or index.is_empty:
            return None
        paths = [block.first_path for block in index.blocks]
        block_index = _lookup_block_index(paths, logical_path)
        if block_index is None:
            return None
        block = index.blocks[block_index]
        end = (
            index.blocks[block_index + 1].offset
            if block_index + 1 < len(index.blocks)
            else index.manifest_size
        )
        with cache_path.open("rb") as handle:
            handle.seek(block.offset)
            slice_bytes = handle.read(end - block.offset)
        for raw_line in slice_bytes.decode("utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            entry = Entry.parse(line)
            if entry.path == logical_path:
                return entry
            if entry.path > logical_path:
                break
        return None
