from __future__ import annotations

import heapq
from collections.abc import Callable
from dataclasses import replace
from pathlib import PurePosixPath

from blake3 import blake3

from .domain import CommitObject
from .entry_codec import Entry

CommitReader = Callable[[str], CommitObject]


def metadata_identity(relative_path: str, size: int) -> str:
    payload = f"{relative_path}\n{size}".encode()
    return blake3(payload).hexdigest()


def is_ancestor_commit(
    ancestor: str,
    descendant: str,
    *,
    read_commit: Callable[[str], CommitObject | None],
) -> bool:
    """True when *ancestor* is reachable from *descendant* (merge-aware).

    ``read_commit`` may return ``None`` for unknown commits (e.g. checking a
    remote head against a partial local history).  Uses the ``generation``
    counter to prune branches that cannot reach the ancestor: a commit's
    ancestors always have strictly smaller generations.
    """
    if ancestor == descendant:
        return True
    ancestor_commit = read_commit(ancestor)
    target_generation = (
        ancestor_commit.generation if ancestor_commit is not None else -1
    )
    seen = {descendant}
    stack = [descendant]
    while stack:
        commit_id = stack.pop()
        if commit_id == ancestor:
            return True
        commit = read_commit(commit_id)
        if commit is None:
            continue
        if commit.generation <= target_generation:
            continue
        for parent_id in commit.parents:
            if parent_id not in seen:
                seen.add(parent_id)
                stack.append(parent_id)
    return False


def merge_base_commit(
    commit_a: str, commit_b: str, *, read_commit: CommitReader
) -> CommitObject | None:
    """Best common ancestor of two commits (highest generation), or ``None``.

    Generation-ordered frontier walk: both sides expand from the highest
    generation down, marking each commit with the side(s) that reach it. The
    first commit marked from *both* sides is a common ancestor of maximal
    generation, and the walk stops there — cost is O(distance since
    divergence), not O(full history), and only the frontier is held in memory.
    """
    if commit_a == commit_b:
        return read_commit(commit_a)

    flag_a, flag_b = 1, 2
    marked: dict[str, int] = {}
    heap: list[tuple[int, str]] = []

    def visit(commit_id: str, flag: int) -> None:
        previous = marked.get(commit_id, 0)
        combined = previous | flag
        if combined == previous:
            return
        marked[commit_id] = combined
        heapq.heappush(heap, (-read_commit(commit_id).generation, commit_id))

    visit(commit_a, flag_a)
    visit(commit_b, flag_b)

    while heap:
        _, commit_id = heapq.heappop(heap)
        side = marked[commit_id]
        if side == flag_a | flag_b:
            return read_commit(commit_id)
        for parent_id in read_commit(commit_id).parents:
            visit(parent_id, side)
    return None


def normalize_repository_path(path: str) -> str:
    return _sanitize_path_component(path, "Path")


def _sanitize_path_component(value: str, context: str) -> str:
    _validate_no_binary(value)
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{context} cannot be empty")
    if stripped.startswith("/"):
        raise ValueError(f"{context} cannot be absolute")
    normalized = stripped.strip("/")
    if not normalized:
        raise ValueError(f"{context} cannot be empty")
    if (
        normalized in (".", "..")
        or normalized.startswith("../")
        or "/../" in normalized
        or normalized.endswith("/..")
    ):
        raise ValueError(f"{context} cannot traverse outside repository root")
    if "//" in normalized:
        raise ValueError(f"{context} contains empty components")
    return normalized


def _validate_no_binary(token: str, *, context: str = "") -> None:
    prefix = f"{context}: " if context else ""
    if "\x00" in token:
        raise ValueError(f"{prefix}Path contains null bytes")
    for ch in token:
        if 0 < ord(ch) < 32:
            raise ValueError(f"{prefix}Path contains control characters")


def normalize_logical_paths(paths: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for path in paths:
        value = normalize_repository_path(path)
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def matches_logical_path(entry_path: str, logical_path: str) -> bool:
    return entry_path == logical_path or entry_path.startswith(f"{logical_path}/")


def move_logical_path(
    entry_path: str,
    *,
    source_path: str,
    destination_path: str,
) -> str:
    if entry_path == source_path:
        return destination_path
    suffix = entry_path[len(source_path) :]
    return f"{destination_path}{suffix}"


def relocate_manifest_entry(
    entry: Entry,
    destination_path: str,
) -> Entry:
    if entry.identity_mode == "pointer":
        identity_value = metadata_identity(destination_path, entry.size)
        return replace(entry, path=destination_path, hash=identity_value)
    return replace(entry, path=destination_path)


def normalize_s3_import_path(
    *,
    key: str,
    prefix: str,
    size: int,
) -> str | None:
    if key.endswith("/") and size == 0:
        return None
    normalized_key = key.strip("/")
    if not normalized_key:
        return None
    normalized_prefix = prefix.strip("/")
    if normalized_prefix:
        if normalized_key == normalized_prefix:
            relative_path = normalized_key.rsplit("/", maxsplit=1)[-1]
        elif normalized_key.startswith(f"{normalized_prefix}/"):
            relative_path = normalized_key[len(normalized_prefix) + 1 :]
        else:
            raise ValueError(f"S3 key '{key}' is outside import prefix '{prefix}'")
    else:
        relative_path = normalized_key
    return normalize_repository_path(relative_path)


def normalize_import_patterns(path_patterns: list[str] | None) -> list[str]:
    patterns: list[str] = []
    for pattern in path_patterns or []:
        patterns.append(_sanitize_path_component(pattern, "Import path filter"))
    return patterns


def matches_import_patterns(relative_path: str, path_patterns: list[str]) -> bool:
    if not path_patterns:
        return True
    path = PurePosixPath(relative_path)
    return any(match_import_pattern(path, pattern) for pattern in path_patterns)


def match_import_pattern(path: PurePosixPath, pattern: str) -> bool:
    if path.match(pattern):
        return True
    if pattern.startswith("**/"):
        pattern_suffix = pattern[len("**/") :]
        return len(path.parts) == 1 and path.match(pattern_suffix)
    return False
