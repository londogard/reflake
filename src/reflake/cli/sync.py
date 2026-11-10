"""Sync commands: push, pull, fetch, clone, transfer."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..core import (
    NotARepositoryError,
    ReflakeRepository,
    fetch,
    init_config,
    init_repository,
    open_repository,
    pull,
    push,
)
from .args import CloneArgs, FetchArgs, PullArgs, PushArgs
from .helpers import progress_printer


def handle_push(command: PushArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root, blob_transfer=command.transfer_backend)
    result = push(
        repo,
        command.remote,
        ref=command.ref,
        blob_transfer=command.transfer_backend,
        progress=progress_printer("pushing", enabled=not as_json),
    )
    if as_json:
        print(json.dumps(asdict(result), indent=2))
    else:
        if result.updated:
            print(
                f"Pushed {result.pushed_commits} commit(s), "
                f"{result.pushed_blobs} blob(s) to {command.remote}"
            )
        else:
            print("Everything up-to-date")
    return 0


def handle_pull(command: PullArgs, repo_root: str, as_json: bool) -> int:
    # Pull bootstraps the local repo (clone workflow).
    repo = _open_or_init(repo_root, blob_transfer=command.transfer_backend)
    result = pull(
        repo,
        command.remote,
        ref=command.ref,
        blob_transfer=command.transfer_backend,
        metadata_only=command.metadata_only,
        progress=progress_printer("pulling", enabled=not as_json),
    )
    if as_json:
        print(json.dumps(asdict(result), indent=2))
    else:
        if result.updated:
            print(
                f"Pulled {result.pulled_commits} commit(s), "
                f"{result.pulled_blobs} blob(s) from {command.remote}"
            )
        else:
            print("Already up-to-date")
    return 0


def handle_fetch(command: FetchArgs, repo_root: str, as_json: bool) -> int:
    # Fetch bootstraps the local repo (clone workflow).
    repo = _open_or_init(repo_root, blob_transfer=command.transfer_backend)
    result = fetch(
        repo,
        command.remote,
        ref=command.ref,
        blob_transfer=command.transfer_backend,
        metadata_only=command.metadata_only,
        progress=progress_printer("fetching", enabled=not as_json),
    )
    if as_json:
        print(json.dumps(asdict(result), indent=2))
    else:
        print(
            f"Fetched {result.fetched_commits} commit(s), "
            f"{result.fetched_blobs} blob(s) from {command.remote}"
        )
    return 0


def handle_clone(command: CloneArgs, repo_root: str, as_json: bool) -> int:
    """Clone a remote dataset as a *virtual* worktree.

    The clone points a local working directory at the remote repository
    (``.reflake/config.json`` with the S3 backend) instead of copying objects:
    reads go straight to the remote, so a clone is instant and a checkout
    costs nothing. ``--with-data`` additionally materializes every file.
    Use ``pull`` when you want a full local mirror instead.
    """
    if not command.remote.startswith("s3://"):
        raise ValueError(
            "clone needs an s3:// remote; use `pull <remote>` to populate an "
            "existing local repository"
        )
    from ..core.objects import parse_s3_uri

    bucket, prefix = parse_s3_uri(command.remote)
    target = Path(command.directory).resolve()
    target.mkdir(parents=True, exist_ok=True)
    config_path = target / ".reflake" / "config.json"
    if not config_path.exists():
        init_config(
            target,
            backend="s3",
            s3_bucket=bucket,
            s3_prefix=prefix,
        ).save(target)

    repo = open_repository(str(target), blob_transfer=command.transfer_backend)
    ref = command.ref or repo.current_branch()
    if repo.store.read_branch_ref(ref) is None:
        raise ValueError(
            f"clone --ref expects a branch; '{ref}' is a tag or commit. "
            "Clone the default branch, then read the tag with `cat`/`log`."
        )
    commit_id = repo.resolve_ref(ref)

    # A fresh clone owns the branch locally: pin the snapshot so the next
    # commit CASes the commit it actually saw.
    repo.set_current_branch(ref)
    repo.client_state.write_branch_snapshot(ref, commit_id=commit_id)

    restored: list[str] = []
    if command.with_data:
        restored = repo.restore_files(ref)

    payload = {
        "directory": str(target),
        "remote": command.remote,
        "branch": ref,
        "commit_id": commit_id,
        "restored": len(restored),
    }
    if as_json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"Cloned {command.remote} into {target}")
        print(f"  branch {ref} at {commit_id[:10]} (metadata only)")
        if command.with_data:
            print(f"  restored {len(restored)} file(s)")
        else:
            print("  files are read on demand: `reflake cat <path>`, VFS, query")
    return 0


def _open_or_init(root: str, **kwargs: Any) -> ReflakeRepository:
    """Open a local repo, initializing it first when missing (clone flow)."""
    try:
        return open_repository(root, **kwargs)
    except NotARepositoryError:
        init_repository(root)
        return open_repository(root, **kwargs)
