"""Push, pull, and fetch operations for syncing between local and S3 repos."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .repository import ReflakeRepository


from .domain import FetchResult, PullResult, PushResult, RepositoryObjectKind
from .objects.base import ObjectIO, ObjectStore
from .objects.backends import BlobTransferBackend
from .repository_support import is_ancestor_commit


@dataclass(frozen=True)
class TransferItem:
    """One (src → dst) copy in a transfer plan (docs/architecture.md §7)."""

    kind: RepositoryObjectKind
    object_id: str
    local_path: str
    remote_uri: str


def _build_plan(
    src_repo: ReflakeRepository,
    dst_repo: ReflakeRepository,
    commit_ids: list[str],
    *,
    direction: str,
) -> list[TransferItem]:
    """Compute the exact missing-object set for *commit_ids* on the destination."""
    items: list[TransferItem] = []
    seen: set[tuple[str, str]] = set()

    def add(kind: RepositoryObjectKind, object_id: str) -> None:
        key = (kind, object_id)
        if key in seen or dst_repo.store.object_exists(kind, object_id):
            return
        seen.add(key)
        if direction == "upload":
            local_path = src_repo.store.object_path(kind, object_id)
        else:
            local_path = dst_repo.store.object_path(kind, object_id)
        remote_store = (
            dst_repo.store if direction == "upload" else src_repo.store
        )
        items.append(
            TransferItem(
                kind=kind,
                object_id=object_id,
                local_path=str(local_path),
                remote_uri=remote_store.object_uri(kind, object_id),
            )
        )

    for commit_id in commit_ids:
        commit_obj = src_repo.read_commit(commit_id)
        for tree_hash in src_repo.tree_writer.iter_tree_hashes(commit_obj.tree):
            add("tree", tree_hash)
        for blob_hash, footer_hash in src_repo.tree_writer.iter_leaf_refs(
            commit_obj.tree
        ):
            if blob_hash:
                add("blob", blob_hash)
            if footer_hash:
                add("footer", footer_hash)
        add("commit", commit_id)
    return items


def _copy_item(
    item: TransferItem,
    *,
    local_store: ObjectIO,
    remote_store: ObjectIO,
    direction: str,
) -> None:
    kind, object_id = item.kind, item.object_id
    if direction == "upload":
        if kind == "blob":
            with open(item.local_path, "rb") as handle:
                remote_store.write_blob_stream(object_id, handle, if_missing=True)
        elif kind == "tree":
            remote_store.write_tree_file(object_id, item.local_path, if_missing=True)
        elif kind == "footer":
            remote_store.write_footer_file(object_id, item.local_path, if_missing=True)
        else:
            remote_store.write_commit_bytes(
                object_id, Path(item.local_path).read_bytes(), if_missing=True
            )
        return
    # download
    if kind == "blob":
        source = remote_store.open_blob(object_id)
        try:
            local_store.write_blob_stream(object_id, source, if_missing=True)
        finally:
            source.close()
    else:
        if kind == "tree":
            payload = remote_store.read_tree_bytes(object_id)
            write = local_store.write_tree_file
        elif kind == "footer":
            payload = remote_store.read_footer_bytes(object_id)
            write = local_store.write_footer_file
        else:
            payload = remote_store.read_commit_bytes(object_id)
            if payload is not None:
                local_store.write_commit_bytes(
                    object_id, payload, if_missing=True
                )
            return
        if payload is None:
            return
        with NamedTemporaryFile(mode="wb", suffix=".obj", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(payload)
        try:
            write(object_id, tmp_path, if_missing=True)
        finally:
            tmp_path.unlink(missing_ok=True)


def execute_transfer_plan(
    items: list[TransferItem],
    *,
    local_store: ObjectIO,
    remote_store: ObjectIO,
    direction: str,
    batch_backend: BlobTransferBackend | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> int:
    """Execute a transfer plan in one batch (s5cmd) or per-object (boto3).

    Returns the number of transferred items.
    """
    if not items:
        return 0
    if batch_backend is not None and batch_backend.supports_batch():
        transferred = batch_backend.upload_batch(
            [(item.local_path, item.remote_uri) for item in items],
            if_not_exists=direction == "upload",
        )
        if progress is not None:
            progress(transferred, len(items))
        return transferred

    for index, item in enumerate(items):
        _copy_item(
            item,
            local_store=local_store,
            remote_store=remote_store,
            direction=direction,
        )
        if progress is not None:
            progress(index + 1, len(items))
    return len(items)


def _collect_commits_to_push(
    src_repo: ReflakeRepository, dst_repo: ReflakeRepository, commit_id: str
) -> list[str]:
    """Commits reachable from *commit_id* that are missing on the destination.

    Merge-aware: every parent edge is traversed.  Results are ordered oldest
    first (parents before children) by generation.
    """
    commits: list[str] = []
    seen: set[str] = set()
    stack: list[str] = [commit_id]
    while stack:
        current = stack.pop()
        if current in seen or dst_repo.store.object_exists("commit", current):
            continue
        seen.add(current)
        commits.append(current)
        stack.extend(src_repo.read_commit(current).parents)
    commits.sort(key=lambda cid: src_repo.read_commit(cid).generation)
    return commits


def _is_ancestor_in(
    ancestor: str,
    descendant: str,
    repo: ReflakeRepository,
) -> bool:
    """Check whether *ancestor* is an ancestor of *descendant* in *repo*.

    Commits missing from *repo* (e.g. a remote-only head) are treated as
    unreachable rather than fatal.
    """
    from .domain import UnknownCommitError

    def read_commit(commit_id: str):
        try:
            return repo.read_commit(commit_id)
        except UnknownCommitError:
            return None

    return is_ancestor_commit(ancestor, descendant, read_commit=read_commit)


def _verify_push_fast_forward(
    local_repo: ReflakeRepository,
    remote_repo: ReflakeRepository,
    branch: str,
    local_commit_id: str,
) -> bool:
    """Verify that the remote head is an ancestor of the local head.

    Returns False when already up to date.  Raises NonFastForwardError
    when the histories have diverged.
    """
    from .domain import NonFastForwardError

    remote_state = remote_repo.store.read_branch_ref(branch)
    remote_head = remote_state.commit_id if remote_state else None
    if remote_head == local_commit_id:
        return False
    if remote_head is not None and not _is_ancestor_in(
        remote_head,
        local_commit_id,
        local_repo,
    ):
        raise NonFastForwardError(
            branch=branch,
            current_commit=remote_head,
            target_commit=local_commit_id,
        )
    return True


def _verify_pull_fast_forward(
    local_repo: ReflakeRepository,
    remote_repo: ReflakeRepository,
    branch: str,
    remote_commit_id: str,
) -> bool:
    """Verify that the local head is an ancestor of the remote head.

    Returns False when already up to date.  Raises NonFastForwardError
    when the histories have diverged.
    """
    from .domain import NonFastForwardError

    local_state = local_repo.store.read_branch_ref(branch)
    local_head = local_state.commit_id if local_state else None
    if local_head == remote_commit_id:
        return False
    if local_head is not None and not _is_ancestor_in(
        local_head,
        remote_commit_id,
        remote_repo,
    ):
        raise NonFastForwardError(
            branch=branch,
            current_commit=local_head,
            target_commit=remote_commit_id,
        )
    return True


def push(
    repo: ReflakeRepository,
    remote_uri: str,
    ref: str | None = None,
    *,
    blob_transfer: str | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> PushResult:
    """Push commits, trees, and blobs from local repo to a remote S3 repo.

    Rejects divergent history *before* copying any objects.  The final
    ref update uses CAS; a race between the pre-check and the CAS will
    result in a RefConflictError rather than a silent overwrite.

    Args:
        repo: Local ReflakeRepository.
        remote_uri: S3 URI of the remote repository (e.g. s3://bucket/prefix).
        ref: Branch to push (default: current branch).
        blob_transfer: Blob transfer backend name (e.g. "boto3", "s5cmd").
    """
    from .repository import open_repository

    branch = ref or repo.current_branch()
    if not repo.store.object_exists("ref", branch):
        raise ValueError(f"Unknown branch: {branch}")
    branch_state = repo.store.read_branch_ref(branch)
    if branch_state is None or branch_state.commit_id is None:
        raise ValueError(f"Branch '{branch}' has no commits to push")

    local_commit_id = branch_state.commit_id

    # Open remote repo
    remote_repo = open_repository(
        remote_uri,
        worktree=repo.root,
        blob_transfer=blob_transfer,
    )

    # Ensure remote branch exists
    if remote_repo.store.read_branch_ref(branch) is None:
        remote_repo.store.write_branch_ref(branch, None)

    # ── Pre-check: reject divergent history before copying any objects ──
    can_fast_forward = _verify_push_fast_forward(
        repo,
        remote_repo,
        branch,
        local_commit_id,
    )
    if not can_fast_forward:
        return PushResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pushed_commits=0,
            pushed_blobs=0,
            updated=False,
        )

    commits_to_push = _collect_commits_to_push(repo, remote_repo, local_commit_id)
    if not commits_to_push:
        # All objects already exist; CAS the branch ref for idempotency.
        updated = remote_repo.fast_forward_branch(
            branch, local_commit_id, operation="push"
        )
        return PushResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pushed_commits=0,
            pushed_blobs=0,
            updated=updated,
        )

    # Publish immutable objects first (plan-then-batch, §7). The ref moves
    # last, after the remote verifies that the update is a fast-forward.
    plan = _build_plan(
        repo,
        remote_repo,
        commits_to_push,
        direction="upload",
    )
    execute_transfer_plan(
        plan,
        local_store=repo.store,
        remote_store=remote_repo.store,
        direction="upload",
        batch_backend=getattr(remote_repo.store, "transfer_backend", None),
        progress=progress,
    )
    pushed_blobs = sum(1 for item in plan if item.kind == "blob")

    updated = remote_repo.fast_forward_branch(branch, local_commit_id, operation="push")

    return PushResult(
        source_branch=branch,
        remote_uri=remote_uri,
        pushed_commits=len(commits_to_push),
        pushed_blobs=pushed_blobs,
        updated=updated,
    )


def pull(
    repo: ReflakeRepository,
    remote_uri: str,
    ref: str | None = None,
    *,
    blob_transfer: str | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> PullResult:
    """Pull commits, trees, and blobs from a remote S3 repo to local.

    Rejects divergent history *before* copying any objects.  The final
    ref update uses CAS; a race between the pre-check and the CAS will
    result in a RefConflictError rather than a silent overwrite.

    Args:
        repo: Local ReflakeRepository.
        remote_uri: S3 URI of the remote repository.
        ref: Branch to pull (default: current branch).
        blob_transfer: Blob transfer backend name (e.g. "boto3", "s5cmd").
    """
    from .repository import open_repository

    branch = ref or repo.current_branch()

    # Open remote repo
    remote_repo = open_repository(
        remote_uri, worktree=repo.root, blob_transfer=blob_transfer
    )

    # Resolve remote ref
    remote_commit_id = remote_repo.resolve_ref(branch)

    # ── Pre-check: reject divergent history before copying any objects ──
    # Ensure local branch exists for the check
    if repo.store.read_branch_ref(branch) is None:
        repo.store.write_branch_ref(branch, None)

    can_fast_forward = _verify_pull_fast_forward(
        repo,
        remote_repo,
        branch,
        remote_commit_id,
    )
    if not can_fast_forward:
        return PullResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pulled_commits=0,
            pulled_blobs=0,
            updated=False,
        )

    commits_to_pull = _collect_commits_to_push(remote_repo, repo, remote_commit_id)
    if not commits_to_pull:
        updated = repo.fast_forward_branch(branch, remote_commit_id, operation="pull")
        return PullResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pulled_commits=0,
            pulled_blobs=0,
            updated=updated,
        )

    plan = _build_plan(
        remote_repo,
        repo,
        commits_to_pull,
        direction="download",
    )
    execute_transfer_plan(
        plan,
        local_store=repo.store,
        remote_store=remote_repo.store,
        direction="download",
        batch_backend=getattr(remote_repo.store, "transfer_backend", None),
        progress=progress,
    )
    pulled_blobs = sum(1 for item in plan if item.kind == "blob")

    # CAS the local branch ref (branch was ensured to exist during pre-check).
    updated = repo.fast_forward_branch(branch, remote_commit_id, operation="pull")

    return PullResult(
        source_branch=branch,
        remote_uri=remote_uri,
        pulled_commits=len(commits_to_pull),
        pulled_blobs=pulled_blobs,
        updated=updated,
    )


def fetch(
    repo: ReflakeRepository,
    remote_uri: str,
    ref: str | None = None,
    *,
    blob_transfer: str | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> FetchResult:
    """Fetch objects from a remote S3 repo without updating local branch refs.

    Args:
        repo: Local ReflakeRepository.
        remote_uri: S3 URI of the remote repository.
        ref: Branch to fetch (default: current branch).
        blob_transfer: Blob transfer backend name (e.g. "boto3", "s5cmd").
    """
    from .repository import open_repository

    branch = ref or repo.current_branch()
    remote_repo = open_repository(
        remote_uri, worktree=repo.root, blob_transfer=blob_transfer
    )
    remote_commit_id = remote_repo.resolve_ref(branch)

    commits_to_fetch = _collect_commits_to_push(remote_repo, repo, remote_commit_id)

    plan = _build_plan(
        remote_repo,
        repo,
        commits_to_fetch,
        direction="download",
    )
    execute_transfer_plan(
        plan,
        local_store=repo.store,
        remote_store=remote_repo.store,
        direction="download",
        batch_backend=getattr(remote_repo.store, "transfer_backend", None),
        progress=progress,
    )
    fetched_blobs = sum(1 for item in plan if item.kind == "blob")

    return FetchResult(
        remote_uri=remote_uri,
        branch=branch,
        fetched_commits=len(commits_to_fetch),
        fetched_blobs=fetched_blobs,
    )
