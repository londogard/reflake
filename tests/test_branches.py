"""Branch ergonomics: hierarchical names, deletion, and JSON output."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from reflake import run_cli
from reflake.core import EmptyBranchError, UnknownRefError, create_repository


def test_hierarchical_branch_names_are_isolated(tmp_path: Path) -> None:
    (tmp_path / "base.txt").write_text("base")
    repo = create_repository(str(tmp_path))
    base = repo.commit("base")

    repo.branch("feature/experiment")
    repo.refs.set_current_branch("feature/experiment")
    (tmp_path / "feature.txt").write_text("feature")
    feature = repo.commit("feature commit")

    repo.refs.set_current_branch("main")
    (tmp_path / "main.txt").write_text("main")
    main = repo.commit("main commit")

    heads = {entry["branch"]: entry["commit_id"] for entry in repo.branches()}
    assert heads["feature/experiment"] == feature
    assert heads["main"] == main
    assert base != feature and base != main

    # Per-branch staging files must not collide for nested names.
    repo.refs.set_current_branch("feature/experiment")
    (tmp_path / "staged.txt").write_text("staged")
    repo.add(["staged.txt"])
    assert repo.status().added == ["staged.txt"]
    assert repo.status(ref="main").added == []


def test_branch_name_validation(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("a")
    repo = create_repository(str(tmp_path))
    repo.commit("c")
    for bad in (
        "feature//x",
        "/leading",
        "trailing/",
        "feature/../x",
        ".hidden",
        "a..b",
    ):
        with pytest.raises(ValueError):
            repo.branch(bad)


def test_delete_branch_cas_and_recreate(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("a")
    repo = create_repository(str(tmp_path))
    repo.commit("first")
    repo.branch("temp")

    with pytest.raises(ValueError, match="current branch"):
        repo.delete_branch("main")

    repo.delete_branch("temp")
    with pytest.raises(EmptyBranchError):
        repo.resolve_ref("temp")

    # The name is free again.
    repo.branch("temp")
    assert repo.resolve_ref("temp")

    with pytest.raises(UnknownRefError):
        repo.delete_branch("never-existed")


def test_cli_branch_json_and_delete(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("a")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "first"]) == 0
    commit_id = capsys.readouterr().out.strip()

    assert (
        run_cli(["--repo", str(tmp_path), "--json", "branch", "feature/x"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"branch": "feature/x", "commit_id": commit_id}

    assert (
        run_cli(["--repo", str(tmp_path), "--json", "checkout", "feature/x"]) == 0
    )
    assert json.loads(capsys.readouterr().out) == {"branch": "feature/x"}

    assert run_cli(["--repo", str(tmp_path), "checkout", "main"]) == 0
    capsys.readouterr()

    assert (
        run_cli(["--repo", str(tmp_path), "--json", "branch", "-d", "feature/x"])
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {"deleted": "feature/x"}
