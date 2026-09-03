"""Branch ref management: resolution, caching, fast-forward, and CAS updates.

Owns the two hot caches (resolved refs and commit objects) so that every
read and every mutation flows through one consistent place.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path

from ..client_state import LocalClientState
from ..domain import (
    BranchRefState,
    CommitObject,
    EmptyBranchError,
    NonFastForwardError,
    RefConflictError,
    UnknownCommitError,
    UnknownRefError,
)
from ..objects import ObjectStore
from ..repository_support import is_ancestor_commit


class _BoundedCache(OrderedDict[str, CommitObject]):
    """Ordered cache that evicts the least-recently-used entry when over capacity.

    ``__getitem__`` and ``__setitem__`` both refresh recency, so hot commits
    stay cached while long walks over many commits stay memory-bounded.
    """

    def __init__(self, maxsize: int = 512) -> None:
        super().__init__()
        self._maxsize = maxsize

    def __getitem__(self, key: str) -> CommitObject:
        value = super().__getitem__(key)
        super().move_to_end(key)
        return value

    def __setitem__(self, key: str, value: CommitObject) -> None:
        if key in self:
            del self[key]
        super().__setitem__(key, value)
        if len(self) > self._maxsize:
            self.popitem(last=False)


class RefManager:
    """Owns branch refs, commit reads, and their caches.

    All branch resolution, fast-forward/CAS updates, and commit-object reads
    flow through this collaborator so the caches stay consistent.
    """

    def __init__(
        self,
        *,
        store: ObjectStore,
        client_state: LocalClientState,
        default_branch: str = "main",
    ) -> None:
        self.store = store
        self.client_state = client_state
        self._resolved_ref_cache: dict[str, BranchRefState] = {}
        self._commit_cache = _BoundedCache()
        self._ensure_head(default_branch)

    def _ensure_head(self, default_branch: str) -> None:
        self.client_state.ensure_current_branch(default_branch)
        if self.store.read_branch_ref(default_branch) is None:
            self.store.write_branch_ref(default_branch, None)

    def current_branch(self) -> str:
        return self.client_state.current_branch()

    def set_current_branch(self, branch: str) -> None:
        self.ensure_branch_exists(branch)
        self.client_state.set_current_branch(branch)

    def head_commit(self) -> str | None:
        branch_ref = self.store.read_branch_ref(self.current_branch())
        if branch_ref is None:
            return None
        return branch_ref.commit_id

    def branch_path(self, branch: str) -> Path:
        return self.store.branch_path(branch)

    def resolve_ref(self, branch_or_commit: str) -> str:
        cached_branch_ref = self._resolved_ref_cache.get(branch_or_commit)
        if cached_branch_ref is not None:
            current_token = self.store.version_token("ref", branch_or_commit)
            if current_token == cached_branch_ref.version_token:
                if not cached_branch_ref.commit_id:
                    raise EmptyBranchError(branch_or_commit)
                return cached_branch_ref.commit_id

        branch_ref = self.store.read_branch_ref(branch_or_commit)
        if branch_ref is not None:
            self._resolved_ref_cache[branch_or_commit] = branch_ref
            if not branch_ref.commit_id:
                raise EmptyBranchError(branch_or_commit)
            return branch_ref.commit_id
        if self.store.object_exists("commit", branch_or_commit):
            return branch_or_commit
        raise UnknownRefError(branch_or_commit)

    def branch(self, name: str) -> Path:
        if not name or "/" in name or name.startswith("."):
            raise ValueError("Invalid branch name")
        if self.store.read_branch_ref(name) is not None:
            raise ValueError(f"Branch already exists: {name}")
        head_commit = self.head_commit() or ""
        if not self.store.compare_and_set_branch_ref(
            name,
            head_commit or None,
            expected_version_token=None,
            expected_commit_id=None,
        ):
            raise ValueError(f"Branch already exists: {name}")
        created_state = self.store.read_branch_ref(name)
        if created_state is not None:
            self.client_state.write_branch_snapshot(
                name,
                commit_id=created_state.commit_id,
                version_token=created_state.version_token,
            )
        self._resolved_ref_cache.pop(name, None)
        return self.branch_path(name)

    def read_commit(self, commit_id: str) -> CommitObject:
        try:
            return self._commit_cache[commit_id]
        except KeyError:
            pass
        commit_payload = self.store.read_commit_bytes(commit_id)
        if commit_payload is None:
            raise UnknownCommitError(commit_id)
        data = json.loads(commit_payload.decode("utf-8"))
        parents_raw = data.get("parents") or []
        commit = CommitObject(
            id=str(data["id"]),
            message=str(data["message"]),
            tree=str(data["tree"]),
            parents=tuple(str(p) for p in parents_raw),
            created_at=str(data["created_at"]),
            branch=str(data["branch"]),
            generation=int(data.get("generation", 0)),
        )
        self._commit_cache[commit_id] = commit
        return commit

    def cache_commit(self, commit: CommitObject) -> None:
        """Insert a freshly created commit into the cache."""
        self._commit_cache[commit.id] = commit

    def ensure_branch_exists(self, branch: str) -> None:
        self.require_branch_state(branch)

    def require_branch_state(self, branch: str) -> BranchRefState:
        cached_state = self.client_state.read_branch_snapshot(branch)
        if cached_state is not None:
            return cached_state

        branch_state = self.store.read_branch_ref(branch)
        if branch_state is None:
            raise UnknownRefError(branch)
        self.client_state.write_branch_snapshot(
            branch,
            commit_id=branch_state.commit_id,
            version_token=branch_state.version_token,
        )
        return branch_state

    def require_commit_for_metadata_mutation(self, branch: str) -> CommitObject:
        branch_state = self.require_branch_state(branch)
        if not branch_state.commit_id:
            raise FileNotFoundError(
                f"Branch '{branch}' has no committed manifest to mutate"
            )
        return self.read_commit(branch_state.commit_id)

    def branch_head_commit(self, branch: str) -> str | None:
        branch_ref = self.store.read_branch_ref(branch)
        if branch_ref is None:
            return None
        return branch_ref.commit_id

    def is_ancestor(self, *, ancestor_commit: str, descendant_commit: str) -> bool:
        return is_ancestor_commit(
            ancestor_commit,
            descendant_commit,
            read_commit=self.read_commit,
        )

    def fast_forward_branch(
        self,
        branch: str,
        target_commit: str,
        *,
        operation: str,
    ) -> bool:
        """Advance a branch only if its current head is an ancestor of target."""
        branch_state = self.require_branch_state(branch)
        current_commit = branch_state.commit_id
        if current_commit == target_commit:
            return False
        if current_commit and not self.is_ancestor(
            ancestor_commit=current_commit,
            descendant_commit=target_commit,
        ):
            raise NonFastForwardError(
                branch=branch,
                current_commit=current_commit,
                target_commit=target_commit,
            )
        self.update_branch_ref(
            branch=branch,
            commit_id=target_commit,
            expected_version_token=branch_state.version_token,
            expected_commit_id=current_commit,
            operation=operation,
        )
        return True

    def update_branch_ref(
        self,
        *,
        branch: str,
        commit_id: str | None,
        expected_version_token: str | None,
        expected_commit_id: str | None,
        operation: str,
    ) -> None:
        updated = self.store.compare_and_set_branch_ref(
            branch,
            commit_id,
            expected_version_token=expected_version_token,
            expected_commit_id=expected_commit_id,
        )
        if updated:
            self._resolved_ref_cache.pop(branch, None)
            current_state = self.store.read_branch_ref(branch)
            if current_state is not None:
                self._resolved_ref_cache[branch] = current_state
                self.client_state.write_branch_snapshot(
                    branch,
                    commit_id=current_state.commit_id,
                    version_token=current_state.version_token,
                )
            self.client_state.append_reflog(
                branch, expected_commit_id, commit_id, operation
            )
            return
        current_state = self.store.read_branch_ref(branch)
        if current_state is not None:
            self.client_state.write_branch_snapshot(
                branch,
                commit_id=current_state.commit_id,
                version_token=current_state.version_token,
            )
        raise RefConflictError(
            branch=branch,
            operation=operation,
            expected_commit_id=expected_commit_id,
            current_commit_id=current_state.commit_id if current_state else None,
        )
