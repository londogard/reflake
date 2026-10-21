"""History and branch commands: branch, tag, diff, merge, log, list, cat,
reflog, branches, checkout, restore, reset."""

from __future__ import annotations

import json
import sys

from ..core import open_repository
from .args import (
    BranchArgs,
    BranchesArgs,
    CatArgs,
    CheckoutArgs,
    DiffArgs,
    ListArgs,
    LogArgs,
    MergeArgs,
    ReflogArgs,
    ResetArgs,
    RestoreArgs,
    TagArgs,
)
from .helpers import flatten_option_values


def handle_branch(command: BranchArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    if command.delete:
        name = repo.delete_branch(command.name)
        if as_json:
            print(json.dumps({"deleted": name}, indent=2))
        else:
            print(f"Deleted branch '{name}'")
        return 0
    repo.branch(command.name)
    if as_json:
        print(
            json.dumps(
                {
                    "branch": command.name,
                    "commit_id": repo.refs.branch_head_commit(command.name),
                },
                indent=2,
            )
        )
    else:
        print(f"Created branch '{command.name}'")
    return 0


def handle_tag(command: TagArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    if command.list_tags or (command.name is None and not command.delete):
        if command.name is not None:
            raise ValueError("--list does not take a tag name")
        tags = repo.tags()
        if as_json:
            print(json.dumps(tags, indent=2))
        else:
            for tag in tags:
                print(
                    f"{tag['tag']}: {tag['commit_id']} {tag['message'] or ''}".rstrip()
                )
        return 0
    if command.name is None:
        raise ValueError("Tag name is required with --delete")
    if command.delete:
        name = repo.delete_tag(command.name)
        if as_json:
            print(json.dumps({"deleted": name}, indent=2))
        else:
            print(f"Deleted tag '{name}'")
        return 0
    commit_id = repo.tag(command.name, command.ref)
    if as_json:
        print(json.dumps({"tag": command.name, "commit_id": commit_id}, indent=2))
    else:
        print(f"Tagged '{command.name}' at {commit_id}")
    return 0


def handle_diff(command: DiffArgs, repo_root: str, as_json: bool) -> int:
    changes = open_repository(repo_root).diff(
        command.from_ref,
        command.to_ref,
    )
    if as_json:
        payload = [
            {
                "path": change.path,
                "change": change.change,
                "before_hash": change.before_hash,
                "after_hash": change.after_hash,
                "before_size": change.before_size,
                "after_size": change.after_size,
            }
            for change in changes
        ]
        print(json.dumps(payload, indent=2))
    else:
        for c in changes:
            print(f"  {c.change}: {c.path}")
    return 0


def handle_merge(command: MergeArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    result = repo.merge(
        command.source_ref,
        command.target_ref or repo.current_branch(),
    )
    if as_json:
        print(
            json.dumps(
                {
                    "source_ref": result.source_ref,
                    "target_ref": result.target_ref,
                    "commit_id": result.commit_id,
                    "updated": result.updated,
                },
                indent=2,
            )
        )
    else:
        if result.updated:
            print(
                f"Merged '{result.source_ref}' into "
                f"'{result.target_ref}' ({result.commit_id})"
            )
        else:
            print(
                f"'{result.target_ref}' is already up to date "
                f"with '{result.source_ref}'"
            )
    return 0


def handle_log(command: LogArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    ref = command.ref or repo.current_branch()
    commits = list(repo.log(ref))
    if as_json:
        payload = [
            {
                "id": c.id,
                "message": c.message,
                "tree": c.tree,
                "parents": list(c.parents),
                "created_at": c.created_at,
            }
            for c in commits
        ]
        print(json.dumps(payload, indent=2))
    else:
        for i, c in enumerate(commits):
            if i > 0:
                print()
            print(f"commit {c.id}")
            print(f"Date:   {c.created_at}")
            for p in c.parents:
                print(f"Parent: {p}")
            print()
            msg_indented = "\n".join(f"    {line}" for line in c.message.splitlines())
            print(msg_indented)
    return 0


def handle_list(command: ListArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    entries = repo.resolve_entries_for_prefix(
        command.ref or repo.current_branch(),
        command.path,
    )
    if as_json:
        payload = [
            {
                "path": entry.path,
                "hash": entry.hash,
                "size": entry.size,
                "identity_mode": entry.identity_mode,
                "blob_hash": entry.blob_hash,
                "source_uri": entry.source_uri,
            }
            for entry in entries.values()
        ]
        print(json.dumps(payload, indent=2))
    else:
        for path, entry in sorted(entries.items()):
            mode = "content" if entry.is_verified else "pointer"
            print(f"{entry.size:>12}  {mode:<7}  {path}")
    return 0


def handle_cat(command: CatArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    ref = command.ref or repo.current_branch()
    sys.stdout.buffer.write(repo.cat(ref, command.path))
    return 0


def handle_reflog(command: ReflogArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    branch_name = command.branch or repo.current_branch()
    entries = list(repo.reflog(branch_name))
    if as_json:
        payload = []
        for line in entries:
            old_id, new_id, operation, timestamp = line.split(" ", 3)
            payload.append(
                {
                    "old": old_id,
                    "new": new_id,
                    "operation": operation,
                    "timestamp": timestamp,
                }
            )
        print(json.dumps(payload, indent=2))
    else:
        for line in entries:
            print(line)
    return 0


def handle_branches(command: BranchesArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    datasets = repo.branches()
    if as_json:
        print(json.dumps(datasets, indent=2))
    else:
        for dataset in datasets:
            head = dataset["commit_id"] or "<empty>"
            print((f"{dataset['branch']}: {head} {dataset['message'] or ''}").rstrip())
    return 0


def handle_checkout(command: CheckoutArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    repo.set_current_branch(command.name)
    if as_json:
        print(json.dumps({"branch": command.name}, indent=2))
    else:
        print(f"Switched to branch '{command.name}'")
        # checkout is metadata-only by design (no downloads); say so, because
        # the worktree still holds the previous branch's files.
        print(
            "note: branch switch only — the worktree was not modified. "
            f"Run `reflake restore --ref {command.name}` to materialize it.",
            file=sys.stderr,
        )
    return 0


def handle_restore(command: RestoreArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    ref = command.ref or repo.current_branch()
    restored = repo.restore_files(
        ref,
        paths=flatten_option_values(command.path) or None,
        force=command.force,
    )
    if as_json:
        print(json.dumps({"restored": restored}, indent=2))
    elif not restored:
        print("Nothing to restore")
    else:
        rel_paths = "\n".join(f"  restored: {p}" for p in restored)
        print(f"Restored {len(restored)} file(s) from '{ref}':\n{rel_paths}")
    return 0


def handle_reset(command: ResetArgs, repo_root: str, as_json: bool) -> int:
    repo = open_repository(repo_root)
    branch_name = command.branch or repo.current_branch()
    staged = repo.staging.load(branch_name)
    result = repo.reset(command.ref, branch=command.branch)
    if staged:
        print(
            f"⚠  Branch '{result.ref}' still has staged changes; "
            "`commit --staged` will apply them onto the new parent.",
            file=sys.stderr,
        )
    if as_json:
        print(
            json.dumps(
                {
                    "ref": result.ref,
                    "commit_id": result.commit_id,
                    "previous_commit_id": result.previous_commit_id,
                    "updated": result.updated,
                },
                indent=2,
            )
        )
    elif result.updated:
        print(f"Moved '{result.ref}' to {result.commit_id}")
    else:
        print(f"'{result.ref}' is already at {result.commit_id}")
    return 0
