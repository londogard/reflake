"""Robustness: legacy commits (no generation) and corrupt commit objects."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from reflake.core import CorruptCommitError, create_repository


def _strip_generation(store, commit_id: str) -> None:
    payload = store.read_commit_bytes(commit_id)
    assert payload is not None
    data = json.loads(payload)
    data.pop("generation", None)
    store.write_commit_bytes(commit_id, json.dumps(data).encode())


def test_ancestor_checks_work_for_legacy_commits(tmp_path: Path) -> None:
    """Commits without a stored generation must not break ancestry walks.

    Regression: deriving "generation <= 0 means unknown" silently pruned
    every legacy link and made ancestors look unreachable.
    """
    (tmp_path / "a.txt").write_text("one")
    repo = create_repository(str(tmp_path))
    first = repo.commit("first")
    (tmp_path / "b.txt").write_text("two")
    second = repo.commit("second")
    (tmp_path / "c.txt").write_text("three")
    third = repo.commit("third")

    for commit_id in (first, second, third):
        _strip_generation(repo.store, commit_id)

    # Fresh instance: no caches carry the pre-strip commit objects.
    reopened = create_repository(str(tmp_path))
    assert reopened.refs.is_ancestor(ancestor_commit=first, descendant_commit=third)
    assert reopened.refs.is_ancestor(ancestor_commit=second, descendant_commit=third)
    assert not reopened.refs.is_ancestor(ancestor_commit=third, descendant_commit=first)
    assert reopened.refs.generation_of(first) == 0
    assert reopened.refs.generation_of(third) == 2

    # log must still list the whole chain, newest first.
    messages = [commit.message for commit in reopened.log("main")]
    assert messages == ["third", "second", "first"]


def test_merge_base_works_for_legacy_commits(tmp_path: Path) -> None:
    from reflake.core.repository_support import merge_base_commit

    (tmp_path / "base.txt").write_text("base")
    repo = create_repository(str(tmp_path))
    base = repo.commit("base")

    repo.refs.branch("feature")
    repo.refs.set_current_branch("feature")
    (tmp_path / "f.txt").write_text("f")
    feature = repo.commit("feature commit")

    repo.refs.set_current_branch("main")
    (tmp_path / "m.txt").write_text("m")
    main = repo.commit("main commit")

    for commit_id in (base, feature, main):
        _strip_generation(repo.store, commit_id)

    reopened = create_repository(str(tmp_path))
    base_commit = merge_base_commit(
        feature, main, read_commit=reopened.refs.read_commit
    )
    assert base_commit is not None
    assert base_commit.id == base


def test_corrupt_commit_object_raises_domain_error(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("one")
    repo = create_repository(str(tmp_path))
    repo.commit("first")

    corrupt_id = "f" * 64
    repo.store.write_commit_bytes(corrupt_id, b"{not json")
    with pytest.raises(CorruptCommitError):
        repo.read_commit(corrupt_id)

    missing_fields_id = "e" * 64
    repo.store.write_commit_bytes(missing_fields_id, b'{"message": "nope"}')
    with pytest.raises(CorruptCommitError):
        repo.read_commit(missing_fields_id)
