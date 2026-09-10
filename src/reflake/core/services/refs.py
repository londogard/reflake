"""Branch ref management: resolution, caching, fast-forward, and CAS updates.

Owns the two hot caches (resolved refs and commit objects) so that every
read and every mutation flows through one consistent place.
"""

from __future__ import annotations

from collections import OrderedDict

import msgspec

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
from ..objects import RefObjectStore
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
        store: RefObjectStore,
        client_state: LocalClientState,
        default_branch: str = "main",
    ) -> None:
        self.store = store
        self.client_state = client_state
        self._commit_cache = _BoundedCache()
        # Only client-local state is touched here: opening a repository must
        # never write to the shared store (no ref creation on open).
        self.client_state.ensure_current_branch(default_branch)

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

    def resolve_ref(self, branch_or_commit: str) -> str:
        # Read-through on every call: ref reads are cheap (local file, one
        # S3 GET) and a validated cache would need the version tokens we
        # deleted. Commit objects stay cached — they are immutable.
        branch_ref = self.store.read_branch_ref(branch_or_commit)
        if branch_ref is not None:
            if not branch_ref.commit_id:
                raise EmptyBranchError(branch_or_commit)
            return branch_ref.commit_id
        if self.store.object_exists("commit", branch_or_commit):
            return branch_or_commit
        raise UnknownRefError(branch_or_commit)

    def branch(self, name: str) -> str:
        if not name or "/" in name or name.startswith("."):
            raise ValueError("Invalid branch name")
        if self.store.read_branch_ref(name) is not None:
            raise ValueError(f"Branch already exists: {name}")
        head_commit = self.head_commit()
        if not self.store.compare_and_set_branch_ref(
            name,
            head_commit,
            expected_commit_id=None,
        ):
            raise ValueError(f"Branch already exists: {name}")
        created_state = self.store.read_branch_ref(name)
        if created_state is not None:
            self.client_state.write_branch_snapshot(
                name,
                commit_id=created_state.commit_id,
            )
        return name

    def read_commit(self, commit_id: str) -> CommitObject:
        try:
            return self._commit_cache[commit_id]
        except KeyError:
            pass
        commit_payload = self.store.read_commit_bytes(commit_id)
        if commit_payload is None:
            raise UnknownCommitError(commit_id)
        data = msgspec.json.decode(commit_payload)
        parents_raw = data.get("parents") or []
        commit = CommitObject(
            id=str(data["id"]),
            message=str(data["message"]),
            tree=str(data["tree"]),
            parents=tuple(str(p) for p in parents_raw),
            created_at=str(data["created_at"]),
            generation=int(data.get("generation", 0)),
        )
        self._commit_cache[commit_id] = commit
        return commit

    def cache_commit(self, commit: CommitObject) -> None:
        """Insert a freshly created commit into the cache."""
        self._commit_cache[commit.id] = commit

    def ensure_branch_exists(self, branch: str) -> None:
        """Validate that *branch* can be operated on.

        A branch with no ref yet is *unborn* — tolerated only for the current
        branch, mirroring git: staging into a fresh repository works, the ref
        is created by the first commit. Any other unknown branch is an error.
        """
        if self.store.read_branch_ref(branch) is not None:
            return
        if branch == self.current_branch():
            return
        raise UnknownRefError(branch)

    def require_branch_state(
        self, branch: str, *, allow_unborn: bool = False
    ) -> BranchRefState:
        cached_state = self.client_state.read_branch_snapshot(branch)
        if cached_state is not None:
            return cached_state

        branch_state = self.store.read_branch_ref(branch)
        if branch_state is None:
            if allow_unborn:
                # Unborn branch: expectations are "must not exist yet", and
                # nothing is cached so a peer's creation is observed next call.
                return BranchRefState(branch=branch, commit_id=None)
            raise UnknownRefError(branch)
        self.client_state.write_branch_snapshot(
            branch,
            commit_id=branch_state.commit_id,
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
            expected_commit_id=current_commit,
            operation=operation,
        )
        return True

    def update_branch_ref(
        self,
        *,
        branch: str,
        commit_id: str | None,
        expected_commit_id: str | None,
        operation: str,
    ) -> None:
        updated = self.store.compare_and_set_branch_ref(
            branch,
            commit_id,
            expected_commit_id=expected_commit_id,
        )
        if updated:
            current_state = self.store.read_branch_ref(branch)
            if current_state is not None:
                self.client_state.write_branch_snapshot(
                    branch,
                    commit_id=current_state.commit_id,
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
            )
        raise RefConflictError(
            branch=branch,
            operation=operation,
            expected_commit_id=expected_commit_id,
            current_commit_id=current_state.commit_id if current_state else None,
        )
