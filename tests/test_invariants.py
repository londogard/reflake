"""Randomized operation sequences must never break repository invariants.

This is the class-level guard for the corruption bugs that shipped twice
(v0.2.0: sharded directories merged as children; v0.2.1: staged directory
splice wrote duplicate names). Whatever sequence of commits, staged
add/remove/move, resets, branch switches and merges a user performs, every
tree reachable from every branch must:

- exist and re-parse (sorted, duplicate-free, single-component names),
- reference subtree/blob/footer objects that exist,
- belong to a commit DAG whose commits all exist.

The tree fanout is monkeypatched to 3 so sharding engages immediately.
"""

from __future__ import annotations

import random
from pathlib import Path

import pytest

import reflake.core.services.tree as tree_module
from reflake.core import (
    MergeConflictError,
    NonFastForwardError,
    ReflakeRepository,
    create_repository,
)
from reflake.core.objects.tree import parse_tree_object

FILE_PATHS = (
    "a.txt",
    "b.txt",
    "d1/x.txt",
    "d1/y.txt",
    "d2/z.txt",
    "d2/deep/w.txt",
)
BRANCH_POOL = ("feature", "feature/x", "fix", "tmp")


def assert_repository_invariants(repo: ReflakeRepository) -> None:
    for branch in repo.store.iter_branches():
        state = repo.store.read_branch_ref(branch)
        if state is None or state.commit_id is None:
            continue

        # Commit DAG: every commit parses and every parent exists.
        pending = [state.commit_id]
        commits_seen: set[str] = set()
        while pending:
            commit_id = pending.pop()
            if commit_id in commits_seen:
                continue
            commits_seen.add(commit_id)
            commit = repo.read_commit(commit_id)
            pending.extend(commit.parents)
            assert list(repo.store.iter_all_entries(commit.tree)) is not None

        # Tree DAG from the branch head: parses, references exist.
        seen_trees: set[str] = set()
        stack = [repo.read_commit(state.commit_id).tree]
        while stack:
            tree_hash = stack.pop()
            if tree_hash in seen_trees:
                continue
            seen_trees.add(tree_hash)
            payload = repo.store.read_tree_bytes(tree_hash)
            assert payload is not None, f"missing tree {tree_hash} in {branch}"
            entries = parse_tree_object(payload)  # raises on unsorted/duplicate
            for entry in entries:
                if entry.is_subtree:
                    stack.append(entry.hash)
                else:
                    if entry.blob_hash:
                        assert repo.store.object_exists("blob", entry.blob_hash)
                    if entry.footer:
                        assert repo.store.object_exists("footer", entry.footer)


@pytest.mark.parametrize("seed", [20260924, 7, 42])
def test_random_operation_sequences_keep_repository_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, seed: int
) -> None:
    monkeypatch.setattr(tree_module, "MAX_TREE_ENTRIES", 3)
    random.seed(seed)

    repo = create_repository(str(tmp_path))
    external = tmp_path.parent / f"{tmp_path.name}-external"
    external.mkdir(exist_ok=True)
    known_commits: list[str] = []

    def worktree_file(relative: str) -> Path:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    for relative in FILE_PATHS:
        worktree_file(relative).write_text(f"seed {relative}")

    for step in range(140):
        operation = random.choice(
            (
                "edit",
                "commit",
                "add_file",
                "add_external",
                "rm",
                "mv",
                "commit_staged",
                "branch",
                "checkout",
                "reset",
                "merge",
            )
        )
        try:
            if operation == "edit":
                relative = random.choice(FILE_PATHS)
                worktree_file(relative).write_text(f"edit {step} {relative}")
            elif operation == "commit":
                known_commits.append(repo.commit(f"commit {step}"))
            elif operation == "add_file":
                relative = random.choice(FILE_PATHS)
                worktree_file(relative).write_text(f"staged {step} {relative}")
                repo.add([relative])
            elif operation == "add_external":
                source = external / f"incoming_{step}.txt"
                source.write_text(f"external {step}")
                repo.add(
                    [str(source)],
                    destination_path=random.choice(
                        ("imports/x.txt", "d1/x.txt", "a.txt", "new/dir/y.txt")
                    ),
                )
            elif operation == "rm":
                repo.rm(
                    [
                        random.choice(
                            ("a.txt", "d1", "d2/deep", "imports", "missing.txt")
                        )
                    ]
                )
            elif operation == "mv":
                source, destination = random.choice(
                    (
                        ("a.txt", "renamed.txt"),
                        ("d1", "moved"),
                        ("d2/z.txt", "d1/z.txt"),
                        ("imports", "archive"),
                        ("missing.txt", "nowhere.txt"),
                    )
                )
                repo.move_staged(source, destination)
            elif operation == "commit_staged":
                known_commits.append(repo.commit(f"staged {step}", staged_only=True))
            elif operation == "branch":
                repo.branch(random.choice(BRANCH_POOL))
            elif operation == "checkout":
                heads = [
                    entry["branch"]
                    for entry in repo.branches()
                    if entry["commit_id"] is not None
                ]
                if heads:
                    repo.refs.set_current_branch(random.choice(heads))
            elif operation == "reset":
                heads = [
                    entry["commit_id"]
                    for entry in repo.branches()
                    if entry["commit_id"] is not None
                ]
                targets = known_commits + heads
                if targets:
                    repo.reset(random.choice(targets))
            elif operation == "merge":
                source, target = random.sample(BRANCH_POOL, 2)
                repo.merge(source, target)
        except (
            MergeConflictError,
            NonFastForwardError,
            FileNotFoundError,
            ValueError,
            KeyError,
        ):
            # Legitimate refusals (conflicts, collisions, missing sources,
            # file-parent destinations, already-existing branches). The
            # invariant is that the repository stays *valid*, not that every
            # operation succeeds.
            pass

        known_commits = [
            commit
            for commit in known_commits
            if _commit_exists(repo, commit)
        ]
        assert_repository_invariants(repo)

    # At least something happened.
    assert any(entry["commit_id"] for entry in repo.branches())


def _commit_exists(repo: ReflakeRepository, commit_id: str) -> bool:
    from reflake.core import UnknownCommitError

    try:
        repo.read_commit(commit_id)
    except UnknownCommitError:
        return False
    return True
