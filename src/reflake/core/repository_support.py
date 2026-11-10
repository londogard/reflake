from __future__ import annotations

import heapq
from collections.abc import Callable
from dataclasses import replace
from pathlib import PurePosixPath

from blake3 import blake3

from .domain import CommitObject
from .entry_codec import Entry

CommitReader = Callable[[str], CommitObject]


def pointer_identity(*, size: int, marker: str | None) -> str:
    """Revision identity for a pointer (unverifiable) entry.

    A pointer entry makes no claim about canonical bytes; it claims "this
    source object *version*". The identity is a hash of the source's version
    marker (S3 ETag, or a content digest for local/fsspec sources) plus its
    size — and **not** of the repository path, so moving a pointer keeps its
    identity while re-importing modified content produces a new one.

    The marker is stored on the entry (``source_etag``). Drift detection
    recomputes it from the source: metadata-only for S3, a content re-read for
    local files (mtime is not an identity).
    """
    body = f"{marker or 'size'}\n{size}"
    return blake3(body.encode()).hexdigest()


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
    ancestors always have strictly smaller generations.  Commits without a
    stored generation (pre-0.2 objects) get one computed from the DAG, so
    legacy histories are not silently misclassified.
    """
    if ancestor == descendant:
        return True
    generations = _GenerationProvider(read_commit)
    ancestor_commit = read_commit(ancestor)
    target_generation = generations.of(ancestor) if ancestor_commit is not None else -1
    seen = {descendant}
    stack = [descendant]
    while stack:
        commit_id = stack.pop()
        if commit_id == ancestor:
            return True
        commit = read_commit(commit_id)
        if commit is None:
            continue
        generation = generations.of(commit_id)
        if 0 < generation <= target_generation:
            continue
        for parent_id in commit.parents:
            if parent_id not in seen:
                seen.add(parent_id)
                stack.append(parent_id)
    return False


class _GenerationProvider:
    """Stored generations where available, DAG-derived ones when absent.

    Root commits legitimately have generation 0, so ``None`` (missing field)
    is the only "unknown" marker.  The derived rule matches the stored one:
    a root is 0 and every child is ``1 + max(parent generations)``.
    """

    def __init__(self, read_commit: Callable[[str], CommitObject | None]) -> None:
        self._read = read_commit
        self._memo: dict[str, int] = {}

    def of(self, commit_id: str) -> int:
        cached = self._memo.get(commit_id)
        if cached is not None:
            return cached
        commit = self._read(commit_id)
        if commit is None:
            self._memo[commit_id] = 0
            return 0
        generation = commit.generation
        if generation is None:
            parent_generations = [self.of(parent) for parent in commit.parents]
            generation = 1 + max(parent_generations) if parent_generations else 0
        self._memo[commit_id] = generation
        return generation


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
    generations = _GenerationProvider(read_commit)

    def visit(commit_id: str, flag: int) -> None:
        previous = marked.get(commit_id, 0)
        combined = previous | flag
        if combined == previous:
            return
        marked[commit_id] = combined
        heapq.heappush(heap, (-generations.of(commit_id), commit_id))

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


def validate_branch_name(name: str) -> str:
    """Validate a branch name, allowing hierarchical ``feature/x`` names."""
    return _validate_ref_name(name, label="Branch")


def validate_tag_name(name: str) -> str:
    """Validate a tag name, allowing hierarchical ``release/v1`` names."""
    return _validate_ref_name(name, label="Tag")


def _validate_ref_name(name: str, *, label: str) -> str:
    value = name.strip()
    _validate_no_binary(value)
    if not value:
        raise ValueError(f"{label} name cannot be empty")
    if value.startswith("/") or value.endswith("/"):
        raise ValueError(f"{label} name cannot start or end with '/'")
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"{label} name contains an invalid path segment")
    if ".." in value:
        raise ValueError(f"{label} name cannot contain consecutive dots")
    if any(part.startswith(".") for part in parts):
        raise ValueError(f"{label} name segments cannot start with '.'")
    if any(part.startswith("-") for part in parts):
        raise ValueError(f"{label} name segments cannot start with '-'")
    forbidden = set('\\:*?"<>|')
    if any(char in forbidden for char in value):
        raise ValueError(f"{label} name contains an invalid character")
    if value.endswith(".lock"):
        raise ValueError(f"{label} name cannot end with '.lock'")
    # Local repositories store refs as files, so a name longer than the
    # filesystem's NAME_MAX surfaces as a raw OSError. Reject it up front, on
    # every backend, so S3 and local behave identically.
    if len(value.encode()) > 200:
        raise ValueError(
            f"{label} name is too long ({len(value.encode())} bytes; max 200)"
        )
    return value


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
    if "\\" in normalized:
        # Logical paths are POSIX. A backslash is a path separator on
        # Windows and a traversal vector if it reaches a filesystem join.
        raise ValueError(f"{context} must use '/' separators")
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
        identity_value = pointer_identity(
            size=entry.size,
            marker=entry.source_etag,
        )
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
