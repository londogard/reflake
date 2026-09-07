"""Tree walkers: exact lookups, prefix listings, and full walks over tree DAGs.

This is the single lookup core of Reflake v2 (§3 of docs/architecture.md):

- exact-path lookups descend the path chain, binary-searching each level in a
  (cached or freshly fetched) tree object — 0–1 GETs warm;
- prefix/bulk listings walk the subtree under the prefix, streaming matches;
- full walks flatten a tree into leaf ``Entry`` objects (the drop-in
  replacement for v1's ``iter_manifest_entries``).

Trees are content-addressed, so the cache is never stale: a hash always maps to
the same bytes.
"""

from __future__ import annotations

from bisect import bisect_left
from collections import OrderedDict
from collections.abc import Callable, Iterator
from dataclasses import replace

from ..entry_codec import Entry
from .tree import (
    KIND_SHARD,
    KIND_TREE,
    parse_tree_object,
)


def _leaf_to_manifest(entry: Entry, path: str) -> Entry:
    return replace(entry, path=path)


class TreeCache:
    """LRU cache of parsed tree objects, keyed by content hash.

    Content-addressing makes the cache safe: a hash always maps to identical
    bytes, so cached entries can never go stale.  ``pin`` marks hashes that
    must survive eviction (e.g. the current HEAD root tree).
    """

    def __init__(self, maxsize: int = 2048) -> None:
        self._entries: OrderedDict[str, list[Entry]] = OrderedDict()
        self._maxsize = maxsize
        self._pinned: set[str] = set()

    def get(self, tree_hash: str) -> list[Entry] | None:
        entries = self._entries.get(tree_hash)
        if entries is not None:
            if tree_hash not in self._pinned:
                self._entries.move_to_end(tree_hash)
            return entries
        return None

    def put(self, tree_hash: str, entries: list[Entry]) -> None:
        self._entries[tree_hash] = entries
        if tree_hash not in self._pinned:
            self._entries.move_to_end(tree_hash)
        self._evict()

    def pin(self, tree_hash: str) -> None:
        self._pinned.add(tree_hash)

    def unpin(self, tree_hash: str) -> None:
        self._pinned.discard(tree_hash)

    def _evict(self) -> None:
        while len(self._entries) > self._maxsize:
            oldest = next(iter(self._entries))
            if oldest in self._pinned:
                # Everything left is pinned; stop evicting.
                break
            self._entries.pop(oldest)


def _descend(entries: list[Entry], part: str) -> Entry | None:
    """Find the entry to descend into for *part*.

    Returns an exact match, or — when the tree is sharded — the name-range
    shard whose range contains *part*.
    """
    idx = bisect_left(entries, part, key=lambda entry: entry.path)
    if idx < len(entries) and entries[idx].path == part:
        return entries[idx]
    previous = idx - 1
    if previous >= 0 and entries[previous].kind == KIND_SHARD:
        return entries[previous]
    return None


class TreeWalker:
    """Tree-walk lookups over a store's ``read_tree_bytes`` callable."""

    def __init__(
        self,
        read_tree: Callable[[str], bytes | None],
        cache: TreeCache | None = None,
    ) -> None:
        self._read_tree = read_tree
        self._cache = cache or TreeCache()

    def _load(self, tree_hash: str) -> list[Entry] | None:
        cached = self._cache.get(tree_hash)
        if cached is not None:
            return cached
        payload = self._read_tree(tree_hash)
        if payload is None:
            return None
        entries = parse_tree_object(payload)
        self._cache.put(tree_hash, entries)
        return entries

    def load_entries(self, tree_hash: str) -> list[Entry] | None:
        """Parse (and cache) a tree object's entries; ``None`` if unknown."""
        return self._load(tree_hash)

    def resolve_subtree(
        self, root_tree_hash: str, directory_path: str
    ) -> list[Entry] | None:
        """Return the entries of the subtree at *directory_path*.

        Returns ``None`` when the directory does not exist in the tree.
        Sharded trees are navigated transparently.
        """
        parts = [p for p in directory_path.strip("/").split("/") if p]
        current = root_tree_hash
        for part in parts:
            entries = self._load(current)
            if entries is None:
                raise ValueError(f"Unknown tree object: {current}")
            target = _descend(entries, part)
            if target is None or not target.is_subtree:
                return None
            current = target.hash
        return self._load(current)

    def _iter_entries(
        self, tree_hash: str
    ) -> Iterator[tuple[str, Entry]]:
        """Pre-order walk of a tree DAG, yielding ``(full_path, entry)``.

        Entries are yielded in ascending path order.  A frame stack resumes a
        tree after descending into each subtree, so leaves and subtrees
        interleave correctly by name (shard subtrees expand in place).
        """
        stack: list[tuple[str, str, int]] = [(tree_hash, "", 0)]
        while stack:
            current_hash, prefix, resume_index = stack.pop()
            entries = self._load(current_hash)
            if entries is None:
                raise ValueError(f"Unknown tree object: {current_hash}")
            index = resume_index
            while index < len(entries) and not entries[index].is_subtree:
                entry = entries[index]
                yield prefix + entry.path, entry
                index += 1
            if index < len(entries):
                subtree = entries[index]
                stack.append((current_hash, prefix, index + 1))
                child_prefix = (
                    prefix
                    if subtree.kind == KIND_SHARD
                    else f"{prefix}{subtree.path}/"
                )
                stack.append((subtree.hash, child_prefix, 0))

    def iter_all_entries(self, tree_hash: str) -> Iterator[Entry]:
        """Flatten a tree into leaf entries in ascending path order."""
        for path, entry in self._iter_entries(tree_hash):
            yield _leaf_to_manifest(entry, path)

    def lookup_entry(self, tree_hash: str, logical_path: str) -> Entry | None:
        """Exact-path lookup: descend the path chain via cached tree objects."""
        normalized = logical_path.strip("/")
        if not normalized:
            return None
        parts = normalized.split("/")
        current_hash = tree_hash
        part_index = 0
        while part_index < len(parts):
            entries = self._load(current_hash)
            if entries is None:
                raise ValueError(f"Unknown tree object: {current_hash}")
            target = _descend(entries, parts[part_index])
            if target is None:
                return None
            if target.is_subtree:
                current_hash = target.hash
                if target.kind == KIND_TREE:
                    part_index += 1
                # KIND_SHARD: same component, continue searching inside the shard
                continue
            if part_index == len(parts) - 1:
                return _leaf_to_manifest(target, normalized)
            return None
        return None

    def iter_entries_for_prefix(
        self, tree_hash: str, logical_prefix: str
    ) -> Iterator[Entry]:
        """Stream leaf entries whose path starts with *logical_prefix*.

        Subtrees whose path prefix cannot overlap the target prefix are
        skipped, so the walk is O(matches + depth) rather than O(N).
        """
        normalized = logical_prefix.strip("/")
        stack: list[tuple[str, str, int]] = [(tree_hash, "", 0)]
        while stack:
            current_hash, prefix, resume_index = stack.pop()
            entries = self._load(current_hash)
            if entries is None:
                raise ValueError(f"Unknown tree object: {current_hash}")
            index = resume_index
            while index < len(entries) and not entries[index].is_subtree:
                entry = entries[index]
                path = prefix + entry.path
                if not normalized or path.startswith(normalized):
                    yield _leaf_to_manifest(entry, path)
                index += 1
            if index < len(entries):
                subtree = entries[index]
                child_prefix = (
                    prefix
                    if subtree.kind == KIND_SHARD
                    else f"{prefix}{subtree.path}/"
                )
                if normalized and not (
                    child_prefix.startswith(normalized)
                    or normalized.startswith(child_prefix)
                ):
                    # No match can live under this subtree; skip it entirely.
                    index += 1
                    stack.append((current_hash, prefix, index))
                    continue
                stack.append((current_hash, prefix, index + 1))
                stack.append((subtree.hash, child_prefix, 0))
