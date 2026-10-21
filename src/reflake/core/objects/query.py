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

from ..entry_codec import Entry, matches_logical_path
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
    bytes, so cached entries can never go stale.

    The bound is on **entries**, not nodes: a node may hold up to
    ``MAX_TREE_ENTRIES`` (2 048) entries and a parsed entry costs a few
    hundred bytes, so a node-count bound let wide directories push the cache
    into the gigabytes. The default budget is ~100 MB at the measured
    ~400 B/entry.
    """

    def __init__(self, max_entries: int = 250_000) -> None:
        self._entries: OrderedDict[str, list[Entry]] = OrderedDict()
        self._max_entries = max_entries
        self._count = 0

    def get(self, tree_hash: str) -> list[Entry] | None:
        entries = self._entries.get(tree_hash)
        if entries is not None:
            self._entries.move_to_end(tree_hash)
        return entries

    def put(self, tree_hash: str, entries: list[Entry]) -> None:
        previous = self._entries.get(tree_hash)
        if previous is not None:
            self._count -= len(previous)
        self._entries[tree_hash] = entries
        self._entries.move_to_end(tree_hash)
        self._count += len(entries)
        self._evict()

    def _evict(self) -> None:
        while self._count > self._max_entries and len(self._entries) > 1:
            _, evicted = self._entries.popitem(last=False)
            self._count -= len(evicted)


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


def _shard_may_contain(
    entries: list[Entry], index: int, logical_prefix: str, child_prefix: str
) -> bool:
    """True when shard ``entries[index]`` can hold a path under *logical_prefix*.

    Shard pointers are named after the first name they contain and their
    ranges are contiguous, so the range of shard *i* is
    ``[entries[i].path, entries[i+1].path)`` — the first absorbs everything
    below its name, the last everything above. Only the *next path component*
    inside the shard matters, because a shard holds direct children of its
    parent directory.
    """
    remainder = logical_prefix[len(child_prefix) :]
    component = remainder.split("/", 1)[0]
    if not component:
        return True
    lower = entries[index].path
    upper = entries[index + 1].path if index + 1 < len(entries) else None
    if component < lower:
        return index == 0
    return upper is None or component < upper


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

    def expand_shards(self, entries: list[Entry]) -> list[Entry]:
        """Return *entries* with every shard pointer replaced by its children.

        A directory node that exceeds ``MAX_TREE_ENTRIES`` stores name-range
        shards (``s`` entries) instead of its real children. Callers that must
        reason about a directory's *children* (merging a parent directory into
        a new tree, pruning, removing) have to expand those shards first,
        otherwise they see a handful of shard pointers named after the first
        entry of each range. Shards never nest, so one pass suffices.
        """
        if not any(entry.kind == KIND_SHARD for entry in entries):
            return entries
        expanded: list[Entry] = []
        for entry in entries:
            if entry.kind != KIND_SHARD:
                expanded.append(entry)
                continue
            shard_entries = self._load(entry.hash)
            if shard_entries is None:
                raise ValueError(f"Unknown tree object: {entry.hash}")
            expanded.extend(shard_entries)
        return expanded

    def resolve_directory(
        self, root_tree_hash: str, directory_path: str
    ) -> tuple[str, list[Entry]] | None:
        """Return ``(node_hash, children)`` for the directory at *directory_path*.

        Children are expanded to real files/subdirectories (shards unpacked).
        ``None`` when the directory does not exist in the tree.
        """
        parts = [p for p in directory_path.strip("/").split("/") if p]
        current = root_tree_hash
        for part in parts:
            entries = self.expand_shards(self._load_or_raise(current))
            target = _descend(entries, part)
            if target is None or not target.is_subtree:
                return None
            current = target.hash
        return current, self.expand_shards(self._load_or_raise(current))

    def resolve_directory_children(
        self, root_tree_hash: str, directory_path: str
    ) -> list[Entry] | None:
        """Return the *real* children of the directory at *directory_path*.

        Unlike a raw node load, this expands name-range shards so every entry
        is an actual file or subdirectory of the directory. Returns ``None``
        when the directory does not exist in the tree.
        """
        resolved = self.resolve_directory(root_tree_hash, directory_path)
        if resolved is None:
            return None
        return resolved[1]

    def load_children(self, tree_hash: str) -> list[Entry]:
        """Load a directory node's entries with shard pointers expanded.

        The result has one entry per real child (leaf or ``t`` subtree), keyed
        by name — the form merge and splice operations need.
        """
        return self.expand_shards(self._load_or_raise(tree_hash))

    def _load_or_raise(self, tree_hash: str) -> list[Entry]:
        entries = self._load(tree_hash)
        if entries is None:
            raise ValueError(f"Unknown tree object: {tree_hash}")
        return entries

    def _iter_entries(self, tree_hash: str) -> Iterator[tuple[str, Entry]]:
        """Pre-order walk of a tree DAG, yielding ``(full_path, entry)``.

        Entries are yielded in ascending path order.  A frame stack resumes a
        tree after descending into each subtree, so leaves and subtrees
        interleave correctly by name (shard subtrees expand in place).
        """
        stack: list[tuple[str, str, int]] = [(tree_hash, "", 0)]
        while stack:
            current_hash, prefix, resume_index = stack.pop()
            entries = self._load_or_raise(current_hash)
            index = resume_index
            while index < len(entries) and not entries[index].is_subtree:
                entry = entries[index]
                yield prefix + entry.path, entry
                index += 1
            if index < len(entries):
                subtree = entries[index]
                stack.append((current_hash, prefix, index + 1))
                child_prefix = (
                    prefix if subtree.kind == KIND_SHARD else f"{prefix}{subtree.path}/"
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
            entries = self._load_or_raise(current_hash)
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
        """Stream leaf entries under *logical_prefix*.

        Matching is **path-component aware**, not raw string prefix: prefix
        ``data/raw`` matches ``data/raw/x.parquet`` and an entry named exactly
        ``data/raw`` — never ``data/rawish/...``. This is the shared
        ``entry_codec.matches_logical_path`` rule, so staged overlays and
        committed trees agree.

        Subtrees whose path cannot contain the prefix are skipped, so the walk
        is O(matches + depth) rather than O(N).
        """
        normalized = logical_prefix.strip("/")
        stack: list[tuple[str, str, int]] = [(tree_hash, "", 0)]
        while stack:
            current_hash, prefix, resume_index = stack.pop()
            entries = self._load_or_raise(current_hash)
            index = resume_index
            while index < len(entries) and not entries[index].is_subtree:
                entry = entries[index]
                path = prefix + entry.path
                if not normalized or matches_logical_path(path, normalized):
                    yield _leaf_to_manifest(entry, path)
                index += 1
            if index < len(entries):
                subtree = entries[index]
                child_prefix = (
                    prefix if subtree.kind == KIND_SHARD else f"{prefix}{subtree.path}/"
                )
                skip = normalized and not (
                    child_prefix.startswith(f"{normalized}/")
                    or normalized.startswith(child_prefix)
                )
                if not skip and subtree.kind == KIND_SHARD and normalized:
                    # A shard stands for a contiguous name range of *this*
                    # directory, so a prefix either falls inside the range or
                    # cannot match anything in the shard. Without this test a
                    # prefix listing over a wide directory fetched every shard
                    # (O(N) instead of O(matches)).
                    skip = not _shard_may_contain(
                        entries, index, normalized, child_prefix
                    )
                if skip:
                    # No match can live under this subtree; skip it entirely.
                    index += 1
                    stack.append((current_hash, prefix, index))
                    continue
                stack.append((current_hash, prefix, index + 1))
                stack.append((subtree.hash, child_prefix, 0))
