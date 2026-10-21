"""Worktree commands: commit, add, rm, mv, status."""

from __future__ import annotations

import json
import sys
from typing import Any

from ..core import StageStatus, open_repository
from ..core.config import BaseConfig
from .args import AddArgs, CommitArgs, MoveArgs, RmArgs, StatusArgs
from .helpers import Counter


def handle_commit(command: CommitArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    branch = repo.current_branch()
    staged = repo.staging.load(branch)
    drifted = _staged_source_drift(staged)
    if drifted:
        shown = "\n".join(f"  changed: {path}" for path in drifted[:10])
        extra = f"\n  … ({len(drifted)} total)" if len(drifted) > 10 else ""
        print(
            "note: these sources changed since `add`; the snapshot taken at "
            f"`add` is committed as-is (run `add` again to update):\n"
            f"{shown}{extra}",
            file=sys.stderr,
        )
    staged_pointer_entries = any(
        change.identity_mode == "pointer" for change in staged.values()
    )
    commit_id = repo.commit(
        command.message,
        staged_only=command.staged_only,
    )
    stats = commit_id.stats
    if as_json:
        print(
            json.dumps(
                {
                    "commit_id": str(commit_id),
                    "branch": branch,
                    "parent_commit_id": stats.parent_commit_id,
                    "tree": stats.tree,
                    "added": stats.added,
                    "removed": stats.removed,
                    "modified": stats.modified,
                },
                indent=2,
            )
        )
    else:
        summary = _describe_stats(stats)
        print(f"[{branch} {str(commit_id)[:10]}] {command.message}")
        if summary:
            print(summary)
    config = BaseConfig.load(repo_root)
    pointer_commit = staged_pointer_entries or (
        config is not None and config.identity == "pointer"
    )
    if pointer_commit:
        print(
            "⚠  Metadata-only identity: this revision is unverifiable "
            "until `reflake identity promote` is run. "
            "You must retain source objects for future verification.",
            file=sys.stderr,
        )
    return 0


def _describe_stats(stats: object) -> str:
    parts: list[str] = []
    for label in ("added", "modified", "removed"):
        count = getattr(stats, label)
        if count:
            parts.append(f"{count} {label}")
    return " " + ", ".join(parts) if parts else " (no content changes)"


def handle_add(command: AddArgs, repo_root: str, as_json: bool) -> int:
    progress = Counter("staging", enabled=not as_json)
    stage = open_repository(repo_root).add(
        paths=command.paths,
        identity_mode=command.identity,
        destination_path=command.destination_path,
        progress=progress,
    )
    progress.finish()
    if as_json:
        print(json.dumps(_stage_payload(stage), indent=2))
    else:
        _print_status(stage)
    if command.identity == "pointer":
        print(
            "⚠  Metadata-only identity: revisions are unverifiable "
            "until `reflake identity promote` is run. "
            "You must retain source objects for future verification.",
            file=sys.stderr,
        )
    return 0


def handle_rm(command: RmArgs, repo_root: str, as_json: bool) -> int:
    stage = open_repository(repo_root).rm(
        paths=command.paths,
        worktree=not command.keep_worktree,
    )
    if as_json:
        print(json.dumps(_stage_payload(stage), indent=2))
    else:
        _print_status(stage)
    return 0


def handle_mv(command: MoveArgs, repo_root: str, as_json: bool) -> int:
    stage = open_repository(repo_root).move_staged(
        source_path=command.source_path,
        destination_path=command.destination_path,
        worktree=not command.keep_worktree,
    )
    if as_json:
        print(json.dumps(_stage_payload(stage), indent=2))
    else:
        _print_status(stage)
    return 0


def handle_status(command: StatusArgs, repo_root: str, as_json: bool) -> int:
    stage = open_repository(repo_root, must_exist=True).status(working_tree=True)
    if as_json:
        print(json.dumps(_stage_payload(stage), indent=2))
    else:
        _print_status(stage)
    return 0


def _print_status(stage: StageStatus) -> None:
    if stage.skipped_symlinks:
        shown = ", ".join(stage.skipped_symlinks[:5])
        more = "" if len(stage.skipped_symlinks) <= 5 else ", …"
        print(
            f"note: {len(stage.skipped_symlinks)} symbolic link(s) are not "
            f"versioned ({shown}{more})",
            file=sys.stderr,
        )
    if (
        stage.working_tree_added
        or stage.working_tree_removed
        or stage.working_tree_modified
    ):
        if stage.working_tree_added:
            print("Working tree changes:")
            for path in stage.working_tree_added:
                print(f"  added:    {path}")
        if stage.working_tree_modified:
            for path in stage.working_tree_modified:
                print(f"  modified: {path}")
        if stage.working_tree_removed:
            for path in stage.working_tree_removed:
                print(f"  removed:  {path}")
        print()

    if stage.added or stage.removed:
        print("Staged changes:")
        for path in stage.added:
            print(f"  added:    {path}")
        for path in stage.removed:
            print(f"  removed:  {path}")
    elif (
        not stage.working_tree_added
        and not stage.working_tree_removed
        and not stage.working_tree_modified
    ):
        print("Nothing to commit, working tree clean")


def _stage_payload(stage: StageStatus) -> dict[str, object]:
    return {
        "ref": stage.ref,
        "added": stage.added,
        "removed": stage.removed,
        "working_tree_added": stage.working_tree_added,
        "working_tree_removed": stage.working_tree_removed,
        "working_tree_modified": stage.working_tree_modified,
        "skipped_symlinks": stage.skipped_symlinks,
    }


def _staged_source_drift(staged: dict[str, Any]) -> list[str]:
    """Staged adds whose local source changed since `add` (recipe drift).

    Staging stores a recipe, not a frozen snapshot: commit reads the content
    at commit time.  Surface that loudly instead of committing different
    bytes than the user saw when staging.
    """
    from pathlib import Path
    from urllib.parse import unquote, urlparse

    drifted: list[str] = []
    for change in staged.values():
        if change.action != "add":
            continue
        source_uri = change.source_uri
        if not source_uri or not source_uri.startswith("file:"):
            continue
        if change.size is None or change.mtime_ns is None:
            continue
        try:
            stat = Path(unquote(urlparse(source_uri).path)).stat()
        except OSError:
            continue  # missing sources fail loudly at commit time
        if stat.st_size != change.size or stat.st_mtime_ns != change.mtime_ns:
            drifted.append(change.path)
    return sorted(drifted)
