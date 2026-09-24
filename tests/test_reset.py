"""`reset`: moving a branch pointer backwards/forwards (undo/redo)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from reflake import run_cli
from reflake.core import UnknownRefError, create_repository


def test_reset_moves_branch_backwards_and_forwards(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("one")
    repo = create_repository(str(tmp_path))
    first = repo.commit("first")
    (tmp_path / "b.txt").write_text("two")
    second = repo.commit("second")

    result = repo.reset(first)
    assert result.updated is True
    assert result.previous_commit_id == second
    assert repo.head_commit() == first
    tree = repo.read_commit(first).tree
    assert sorted(e.path for e in repo.store.iter_all_entries(tree)) == ["a.txt"]

    # Redo back to the newest commit.
    result = repo.reset(second)
    assert result.updated is True
    assert repo.head_commit() == second

    # Idempotent when already there.
    result = repo.reset(second)
    assert result.updated is False
    assert repo.head_commit() == second

    operations = [line.split(" ")[2] for line in repo.reflog("main")]
    assert "reset" in operations


def test_reset_rejects_unknown_ref(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("one")
    repo = create_repository(str(tmp_path))
    repo.commit("first")
    with pytest.raises(UnknownRefError):
        repo.reset("does-not-exist")


def test_reset_uses_fresh_ref_read_and_cas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A concurrent advance must surface as RefConflictError, never overwrite."""
    from reflake.core import RefConflictError

    (tmp_path / "a.txt").write_text("one")
    repo = create_repository(str(tmp_path))
    first = repo.commit("first")
    (tmp_path / "b.txt").write_text("two")
    repo.commit("second")

    monkeypatch.setattr(
        repo.store, "compare_and_set_branch_ref", lambda *a, **k: False
    )
    with pytest.raises(RefConflictError):
        repo.reset(first)


def test_cli_reset_json_and_staged_warning(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("one")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "first"]) == 0
    first = capsys.readouterr().out.strip()
    (tmp_path / "b.txt").write_text("two")
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "second"]) == 0
    capsys.readouterr()

    # Staged changes survive a reset with a warning.
    assert run_cli(["--repo", str(tmp_path), "add", "b.txt"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "--json", "reset", first]) == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["commit_id"] == first
    assert payload["updated"] is True
    assert "staged changes" in captured.err
