"""Push, pull, and fetch operations for syncing between local and S3 repos."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .repository import ReflakeRepository


from .concurrency import run_bounded
from .domain import (
    FetchResult,
    PullResult,
    PushResult,
    RepositoryObjectKind,
    TransferEndpointError,
)
from .objects.backends import (
    BlobTransferBackend,
    TransferDirection,
    TransferItem,
    TransferPlan,
)
from .objects.base import HasLocalPath, HasRemoteURI, ObjectIO, RepositoryStore
from .repository_support import is_ancestor_commit


def _require_local_path(store: object, *, role: str) -> HasLocalPath:
    """Return *store* as a local-path endpoint or raise a clear error."""
    if not hasattr(store, "object_path"):
        raise TransferEndpointError(
            f"Transfer plan needs a local filesystem path for the {role} "
            f"store, but {type(store).__name__} has no object_path "
            "(sync requires one local and one S3 repository)"
        )
    return store  # type: ignore[return-value]


def _require_remote_uri(store: object, *, role: str) -> HasRemoteURI:
    """Return *store* as a remote-URI endpoint or raise a clear error."""
    if not hasattr(store, "object_uri"):
        raise TransferEndpointError(
            f"Transfer plan needs a remote URI for the {role} "
            f"store, but {type(store).__name__} has no object_uri "
            "(sync requires one local and one S3 repository)"
        )
    return store  # type: ignore[return-value]


#: Above this many candidate objects, transfer planning switches from
#: per-object existence probes (one HEAD per object on S3) to a single
#: paginated inventory listing per object kind. Below it, probes are cheaper
#: than listing a potentially huge prefix.
_INVENTORY_THRESHOLD = 64


def _missing_ids(
    store: RepositoryStore, kind: RepositoryObjectKind, object_ids: set[str]
) -> set[str]:
    """Return the subset of *object_ids* missing from *store*."""
    if len(object_ids) <= _INVENTORY_THRESHOLD:
        return {
            object_id
            for object_id in object_ids
            if not store.object_exists(kind, object_id)
        }
    # Filter the (streamed) store listing against the candidate set instead of
    # materializing every stored id: peak memory is the candidates, not the
    # repository.
    present = {
        object_id
        for object_id in store.iter_object_ids(kind)
        if object_id in object_ids
    }
    return {object_id for object_id in object_ids if object_id not in present}


def _collect_candidates(
    src_repo: ReflakeRepository, commit_ids: list[str]
) -> dict[RepositoryObjectKind, set[str]]:
    """Every object reachable from *commit_ids* (deduplicated, no I/O probes).

    The commit DAG is walked with one shared ``seen`` set and leaf references
    are harvested from each unique tree node exactly once, so a 100-commit
    push over a 1M-entry tree reads the tree DAG once — not once per commit —
    and keeps no per-subtree memo.
    """
    candidates: dict[RepositoryObjectKind, set[str]] = {
        "commit": set(),
        "tree": set(),
        "blob": set(),
        "footer": set(),
    }
    inspector = src_repo.tree_writer.inspector
    seen_trees: set[str] = set()
    for commit_id in commit_ids:
        commit_obj = src_repo.read_commit(commit_id)
        for tree_hash in inspector.iter_tree_hashes(commit_obj.tree, _seen=seen_trees):
            candidates["tree"].add(tree_hash)
        candidates["commit"].add(commit_id)

    for tree_hash in candidates["tree"]:
        entries = inspector.load_entries(tree_hash)
        if entries is None:
            continue
        for entry in entries:
            if entry.is_subtree:
                continue
            if entry.blob_hash:
                candidates["blob"].add(entry.blob_hash)
            if entry.footer:
                candidates["footer"].add(entry.footer)
    return candidates


def _build_plan(
    src_repo: ReflakeRepository,
    dst_repo: ReflakeRepository,
    commit_ids: list[str],
    *,
    direction: TransferDirection,
    include_blobs: bool = True,
) -> TransferPlan:
    """Compute the exact missing-object set for *commit_ids* on the destination.

    Existence checks are adaptive: small plans use per-object probes, larger
    plans list the destination's object ids once per kind (one paginated
    ``list_objects_v2`` instead of N HEAD requests).
    """
    candidates = _collect_candidates(src_repo, commit_ids)
    if not include_blobs:
        candidates["blob"] = set()

    if direction == "upload":
        local_store = _require_local_path(src_repo.store, role="source")
        remote_store = _require_remote_uri(dst_repo.store, role="target")
    else:
        local_store = _require_local_path(dst_repo.store, role="target")
        remote_store = _require_remote_uri(src_repo.store, role="source")

    counts: Counter[str] = Counter()

    def iter_items() -> Iterator[TransferItem]:
        """Stream missing objects; the plan is never fully materialized."""
        for kind, object_ids in candidates.items():
            for object_id in sorted(
                _missing_ids(dst_repo.store, kind, object_ids)
            ):
                counts[kind] += 1
                yield TransferItem(
                    kind=kind,
                    object_id=object_id,
                    local_path=str(local_store.object_path(kind, object_id)),
                    remote_uri=remote_store.object_uri(kind, object_id),
                )

    return TransferPlan(direction=direction, items=iter_items(), counts=counts)


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
            remote_store.write_blob_file(
                object_id, item.local_path, if_missing=True
            )
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
                local_store.write_commit_bytes(object_id, payload, if_missing=True)
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
    plan: TransferPlan,
    *,
    local_store: ObjectIO,
    remote_store: ObjectIO,
    batch_backend: BlobTransferBackend | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> int:
    """Execute a transfer plan in one batch (s5cmd) or per-object (boto3).

    Returns the number of transferred items.
    """
    if plan.total == 0:
        return 0
    if batch_backend is not None and batch_backend.supports_batch():
        transferred = batch_backend.transfer(plan)
        if progress is not None:
            progress(transferred, plan.total or transferred)
        return transferred

    def copy(item: TransferItem) -> None:
        _copy_item(
            item,
            local_store=local_store,
            remote_store=remote_store,
            direction=plan.direction,
        )

    count = run_bounded(copy, plan.items)
    if progress is not None:
        progress(count, plan.total or count)
    return count


def _collect_commits_to_push(
    src_repo: ReflakeRepository, dst_repo: ReflakeRepository, commit_id: str
) -> list[str]:
    """Commits reachable from *commit_id* that are missing on the destination.

    Merge-aware: every parent edge is traversed.  Results are ordered oldest
    first (parents before children) by generation.  Missing commits are found
    with the same adaptive existence strategy as object planning.
    """
    reachable: set[str] = set()
    stack: list[str] = [commit_id]
    while stack:
        current = stack.pop()
        if current in reachable:
            continue
        reachable.add(current)
        stack.extend(src_repo.read_commit(current).parents)

    missing = _missing_ids(dst_repo.store, "commit", reachable)
    commits = list(missing)
    commits.sort(key=lambda cid: src_repo.refs.generation_of(cid))
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


def _push_tag(
    repo: ReflakeRepository,
    remote_uri: str,
    tag: str,
    commit_id: str,
    *,
    blob_transfer: str | None,
    progress: Callable[[int, int], None] | None,
) -> PushResult:
    """Push a tag's commit lineage, then create the remote tag (create-only).

    Tags are immutable: an existing remote tag at the same commit is a no-op,
    one at a different commit is an error, and the ref is never retargeted.
    The ref is written last, after its commit and blobs exist remotely.
    """
    from .repository import open_repository

    remote_repo = open_repository(
        remote_uri,
        worktree=repo.root,
        blob_transfer=blob_transfer,
        validate_format=False,
        must_exist=False,
    )
    remote_repo._ensure_repo_format_marker()
    remote_commit = remote_repo.store.read_tag_ref(tag)
    if remote_commit is not None:
        if remote_commit == commit_id:
            return PushResult(
                source_branch=tag,
                remote_uri=remote_uri,
                pushed_commits=0,
                pushed_blobs=0,
                updated=False,
            )
        raise ValueError(
            f"Tag '{tag}' already exists on the remote at {remote_commit}; "
            "tags are immutable"
        )

    commits_to_push = _collect_commits_to_push(repo, remote_repo, commit_id)
    pushed_blobs = 0
    # Even when every commit already exists remotely, the tip's tree may
    # reference objects a crashed transfer never copied. Always plan from the
    # tag's tip, then transfer only what is actually missing.
    plan_ids = commits_to_push or [commit_id]
    plan = _build_plan(repo, remote_repo, plan_ids, direction="upload")
    execute_transfer_plan(
        plan,
        local_store=repo.store,
        remote_store=remote_repo.store,
        batch_backend=getattr(remote_repo.store, "transfer_backend", None),
        progress=progress,
    )
    pushed_blobs = plan.counts["blob"]

    if not remote_repo.store.create_tag_ref(tag, commit_id):
        winner = remote_repo.store.read_tag_ref(tag)
        if winner != commit_id:
            raise ValueError(
                f"Tag '{tag}' already exists on the remote at {winner}; "
                "tags are immutable"
            )
        return PushResult(
            source_branch=tag,
            remote_uri=remote_uri,
            pushed_commits=0,
            pushed_blobs=0,
            updated=False,
        )

    return PushResult(
        source_branch=tag,
        remote_uri=remote_uri,
        pushed_commits=len(commits_to_push),
        pushed_blobs=pushed_blobs,
        updated=True,
    )


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

    name = ref or repo.current_branch()
    branch_state = repo.store.read_branch_ref(name)
    if branch_state is None:
        tag_commit = repo.store.read_tag_ref(name)
        if tag_commit is None:
            raise ValueError(f"Unknown ref: {name}")
        return _push_tag(
            repo,
            remote_uri,
            name,
            tag_commit,
            blob_transfer=blob_transfer,
            progress=progress,
        )
    if branch_state.commit_id is None:
        raise ValueError(f"Branch '{name}' has no commits to push")

    branch = name
    local_commit_id = branch_state.commit_id

    # Open remote repo. A push destination is about to be written by this
    # version, so it is not required to carry a format marker yet — the marker
    # is written below (create-only).
    remote_repo = open_repository(
        remote_uri,
        worktree=repo.root,
        blob_transfer=blob_transfer,
        validate_format=False,
        must_exist=False,
    )
    remote_repo._ensure_repo_format_marker()

    # Ensure remote branch exists (CAS: create only when still absent).
    if remote_repo.store.read_branch_ref(branch) is None:
        remote_repo.store.compare_and_set_branch_ref(
            branch, None, expected_commit_id=None
        )

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

    # Plan from the new commits when there are any. When every commit already
    # exists (a previous transfer crashed before the ref CAS), plan from the
    # tip instead: its tree still references every blob the branch needs, so a
    # retry can repair the incomplete transfer instead of advancing the ref
    # over missing content.
    plan_ids = commits_to_push or [local_commit_id]
    plan = _build_plan(repo, remote_repo, plan_ids, direction="upload")
    execute_transfer_plan(
        plan,
        local_store=repo.store,
        remote_store=remote_repo.store,
        batch_backend=getattr(remote_repo.store, "transfer_backend", None),
        progress=progress,
    )
    pushed_blobs = plan.counts["blob"]

    updated = remote_repo.fast_forward_branch(branch, local_commit_id, operation="push")

    return PushResult(
        source_branch=branch,
        remote_uri=remote_uri,
        pushed_commits=len(commits_to_push),
        pushed_blobs=pushed_blobs,
        updated=updated,
    )


def _pull_tag(
    repo: ReflakeRepository,
    remote_repo: ReflakeRepository,
    remote_uri: str,
    tag: str,
    commit_id: str,
    *,
    progress: Callable[[int, int], None] | None,
) -> PullResult:
    """Download a tag's commit lineage, then create the local tag.

    Symmetric with ``_push_tag``: same commit is a no-op, a different commit
    is an error (tags never move), and the local ref is created only after
    the objects it points at exist locally.
    """
    local_commit = repo.store.read_tag_ref(tag)
    if local_commit is not None:
        if local_commit == commit_id:
            return PullResult(
                source_branch=tag,
                remote_uri=remote_uri,
                pulled_commits=0,
                pulled_blobs=0,
                updated=False,
            )
        raise ValueError(
            f"Tag '{tag}' already exists locally at {local_commit}; "
            "tags are immutable"
        )

    commits_to_pull = _collect_commits_to_push(remote_repo, repo, commit_id)
    plan_ids = commits_to_pull or [commit_id]
    plan = _build_plan(remote_repo, repo, plan_ids, direction="download")
    execute_transfer_plan(
        plan,
        local_store=repo.store,
        remote_store=remote_repo.store,
        batch_backend=getattr(remote_repo.store, "transfer_backend", None),
        progress=progress,
    )
    pulled_blobs = plan.counts["blob"]

    if not repo.store.create_tag_ref(tag, commit_id):
        winner = repo.store.read_tag_ref(tag)
        if winner != commit_id:
            raise ValueError(
                f"Tag '{tag}' already exists locally at {winner}; "
                "tags are immutable"
            )
        return PullResult(
            source_branch=tag,
            remote_uri=remote_uri,
            pulled_commits=0,
            pulled_blobs=0,
            updated=False,
        )

    return PullResult(
        source_branch=tag,
        remote_uri=remote_uri,
        pulled_commits=len(commits_to_pull),
        pulled_blobs=pulled_blobs,
        updated=True,
    )


def pull(
    repo: ReflakeRepository,
    remote_uri: str,
    ref: str | None = None,
    *,
    blob_transfer: str | None = None,
    progress: Callable[[int, int], None] | None = None,
    metadata_only: bool = False,
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

    name = ref or repo.current_branch()

    # Open remote repo
    remote_repo = open_repository(
        remote_uri, worktree=repo.root, blob_transfer=blob_transfer
    )

    if remote_repo.store.read_branch_ref(name) is None:
        tag_commit = remote_repo.store.read_tag_ref(name)
        if tag_commit is not None:
            return _pull_tag(
                repo,
                remote_repo,
                remote_uri,
                name,
                tag_commit,
                progress=progress,
            )

    # Resolve remote ref
    remote_commit_id = remote_repo.resolve_ref(name)
    branch = name

    # ── Pre-check: reject divergent history before copying any objects ──
    # Ensure the local branch exists for the check (CAS: create if absent).
    if repo.store.read_branch_ref(branch) is None:
        repo.store.compare_and_set_branch_ref(branch, None, expected_commit_id=None)

    can_fast_forward = _verify_pull_fast_forward(
        repo,
        remote_repo,
        branch,
        remote_commit_id,
    )
    partial = repo.client_state.is_partial(branch)
    if not can_fast_forward and not partial:
        return PullResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pulled_commits=0,
            pulled_blobs=0,
            updated=False,
        )

    commits_to_pull = _collect_commits_to_push(remote_repo, repo, remote_commit_id)
    if not commits_to_pull and not partial:
        updated = repo.fast_forward_branch(branch, remote_commit_id, operation="pull")
        return PullResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pulled_commits=0,
            pulled_blobs=0,
            updated=updated,
        )

    # A metadata-only pull leaves the branch at the right commit but without
    # blobs. Planning the tip's reachable graph (not just "new commits") is
    # what makes the next ordinary pull hydrate it; the partial marker tells
    # us to do that even when the heads already match.
    plan_ids = commits_to_pull or [remote_commit_id]
    plan = _build_plan(
        remote_repo,
        repo,
        plan_ids,
        direction="download",
        include_blobs=not metadata_only,
    )
    execute_transfer_plan(
        plan,
        local_store=repo.store,
        remote_store=remote_repo.store,
        batch_backend=getattr(remote_repo.store, "transfer_backend", None),
        progress=progress,
    )
    pulled_blobs = plan.counts["blob"]
    if metadata_only:
        repo.client_state.mark_partial(branch)
    else:
        repo.client_state.clear_partial(branch)

    # CAS the local branch ref (branch was ensured to exist during pre-check).
    previous_state = repo.store.read_branch_ref(branch)
    previous_commit = previous_state.commit_id if previous_state else None
    updated = repo.fast_forward_branch(branch, remote_commit_id, operation="pull")
    if repo.current_branch() == branch:
        repo.client_state.mark_worktree_stale(
            branch, previous_commit_id=previous_commit
        )

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
    metadata_only: bool = False,
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

    # Fetch is also the repair command: always plan from the remote tip so a
    # previous interrupted transfer can be completed, and a metadata-only
    # fetch can be hydrated later.
    plan_ids = commits_to_fetch or [remote_commit_id]
    plan = _build_plan(
        remote_repo,
        repo,
        plan_ids,
        direction="download",
        include_blobs=not metadata_only,
    )
    execute_transfer_plan(
        plan,
        local_store=repo.store,
        remote_store=remote_repo.store,
        batch_backend=getattr(remote_repo.store, "transfer_backend", None),
        progress=progress,
    )
    fetched_blobs = plan.counts["blob"]
    if metadata_only:
        repo.client_state.mark_partial(branch)
    else:
        repo.client_state.clear_partial(branch)

    return FetchResult(
        remote_uri=remote_uri,
        branch=branch,
        commit_id=remote_commit_id,
        fetched_commits=len(commits_to_fetch),
        fetched_blobs=fetched_blobs,
    )
