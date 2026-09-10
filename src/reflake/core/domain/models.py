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
    generation: int = 0

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


@dataclass(frozen=True)
class VerifyResult:
    commit_id: str
    verified_entries: int
    candidate_entries: int
    total_entries: int
    created_commit: bool
    dry_run: bool


@dataclass(frozen=True)
class MergeResult:
    source_ref: str
    target_ref: str
    commit_id: str
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
