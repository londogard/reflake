from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import msgspec

RepositoryObjectKind = Literal["blob", "commit", "ref", "tree", "footer"]


@dataclass(frozen=True)
class BranchRefState:
    branch: str
    commit_id: str | None


@dataclass(frozen=True)
class CommitObject:
    id: str
    message: str
    tree: str
    parents: tuple[str, ...] = ()
    created_at: str = ""
    #: DAG-depth perf hint. ``None`` means the stored object predates the
    #: field (legacy); the value is then derived from the parent DAG.  A root
    #: commit legitimately has generation 0.
    generation: int | None = None

    @property
    def first_parent(self) -> str | None:
        return self.parents[0] if self.parents else None


@dataclass(frozen=True)
class DiffEntry:
    path: str
    change: str
    before_hash: str | None
    after_hash: str | None
    before_size: int | None
    after_size: int | None


@dataclass(frozen=True)
class GcResult:
    reachable_commits: int
    reachable_trees: int
    reachable_blobs: int
    reachable_footers: int
    orphan_commits: int
    orphan_trees: int
    orphan_blobs: int
    orphan_footers: int
    pruned: bool
    #: Orphans skipped because they are younger than the grace window
    #: (they may belong to a writer that has not CAS'd its ref yet).
    skipped_young: int = 0


@dataclass(frozen=True)
class VerifyResult:
    commit_id: str
    verified_entries: int
    candidate_entries: int
    total_entries: int
    created_commit: bool
    dry_run: bool
    #: Pointer entries whose source metadata no longer matches what was
    #: imported (size / ETag / last-modified changed, or source missing).
    drifted_paths: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MergeResult:
    source_ref: str
    target_ref: str
    commit_id: str
    updated: bool


@dataclass(frozen=True)
class ResetResult:
    """Result of moving a branch pointer to an existing commit."""

    ref: str
    commit_id: str
    previous_commit_id: str | None
    updated: bool


@dataclass(frozen=True)
class RemoveResult:
    ref: str
    commit_id: str
    removed_paths: list[str]


@dataclass(frozen=True)
class MoveResult:
    ref: str
    commit_id: str
    source_path: str
    destination_path: str
    moved_paths: list[str]


class StageChange(msgspec.Struct):
    path: str
    action: str
    identity_mode: str | None = None
    source_uri: str | None = None
    blob_hash: str | None = None
    size: int | None = None
    #: Source size/mtime observed when the change was staged. Staging is a
    #: recipe (content is read at commit time); these let the CLI warn when
    #: the recipe no longer matches the source instead of silently
    #: committing different bytes.
    mtime_ns: int | None = None


@dataclass(frozen=True)
class StageStatus:
    ref: str
    added: list[str]
    removed: list[str]
    working_tree_added: list[str] = field(default_factory=list)
    working_tree_removed: list[str] = field(default_factory=list)
    working_tree_modified: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        object.__setattr__(self, "added", list(self.added))
        object.__setattr__(self, "removed", list(self.removed))
        object.__setattr__(self, "working_tree_added", list(self.working_tree_added))
        object.__setattr__(
            self, "working_tree_removed", list(self.working_tree_removed)
        )
        object.__setattr__(
            self, "working_tree_modified", list(self.working_tree_modified)
        )


@dataclass(frozen=True)
class FetchResult:
    remote_uri: str
    branch: str
    fetched_commits: int
    fetched_blobs: int


@dataclass(frozen=True)
class PushResult:
    source_branch: str
    remote_uri: str
    pushed_commits: int
    pushed_blobs: int
    updated: bool


@dataclass(frozen=True)
class PullResult:
    source_branch: str
    remote_uri: str
    pulled_commits: int
    pulled_blobs: int
    updated: bool
