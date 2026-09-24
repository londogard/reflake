"""Ignore rules: `.reflakeignore` plus built-in defaults."""

from __future__ import annotations

from pathlib import Path

from reflake.core import create_repository
from reflake.core.ignore import IgnoreRules


def test_basename_patterns_match_any_component() -> None:
    rules = IgnoreRules(["*.tmp"])
    assert rules.ignores("a.tmp", is_dir=False)
    assert rules.ignores("deep/nested/b.tmp", is_dir=False)
    assert not rules.ignores("a.tmp.bak", is_dir=False)


def test_directory_patterns_prune_only_directories() -> None:
    rules = IgnoreRules(["scratch/"])
    assert rules.ignores("scratch", is_dir=True)
    assert rules.ignores("data/scratch", is_dir=True)
    assert not rules.ignores("scratch", is_dir=False)


def test_anchored_patterns_are_relative_to_root() -> None:
    rules = IgnoreRules(["data/raw/*.csv"])
    assert rules.ignores("data/raw/a.csv", is_dir=False)
    assert not rules.ignores("other/data/raw/a.csv", is_dir=False)


def test_double_star_matches_across_directories() -> None:
    rules = IgnoreRules(["**/*.parquet"])
    assert rules.ignores("a.parquet", is_dir=False)
    assert rules.ignores("a/b/c.parquet", is_dir=False)
    assert not rules.ignores("a/b/c.csv", is_dir=False)


def test_negation_reincludes_later() -> None:
    rules = IgnoreRules(["*.log", "!keep.log"])
    assert rules.ignores("other.log", is_dir=False)
    assert not rules.ignores("keep.log", is_dir=False)


def test_default_rules_ignore_reflake_and_git() -> None:
    rules = IgnoreRules.load("/tmp")
    assert rules.ignores(".reflake", is_dir=True)
    assert rules.ignores(".git", is_dir=True)
    assert rules.ignores("nested/.git", is_dir=True)


def test_commit_skips_git_and_ignored_paths(tmp_path: Path) -> None:
    (tmp_path / "data.csv").write_text("x")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("[core]")
    (tmp_path / ".reflakeignore").write_text("scratch/\n*.tmp\n")

    (tmp_path / "scratch").mkdir()
    (tmp_path / "scratch" / "notes.txt").write_text("nope")
    (tmp_path / "junk.tmp").write_text("nope")
    (tmp_path / "keep.tmp").write_text("explicit add wins")

    repo = create_repository(str(tmp_path))
    repo.commit("snapshot")

    head = repo.head_commit()
    assert head is not None
    tree = repo.read_commit(head).tree
    paths = sorted(entry.path for entry in repo.store.iter_all_entries(tree))
    # The ignore file is tracked; ignored paths and .git are not.
    assert paths == [".reflakeignore", "data.csv"]

    status = repo.status(working_tree=True)
    assert status.working_tree_added == []
    assert status.working_tree_modified == []

    # An explicitly named ignored file can still be staged (explicit wins).
    repo.add([str(tmp_path / "keep.tmp")])
    status = repo.status()
    assert status.added == ["keep.tmp"]


def test_anchored_rules_apply_when_adding_a_subdirectory(tmp_path: Path) -> None:
    (tmp_path / "seed.txt").write_text("seed")
    repo = create_repository(str(tmp_path))
    repo.commit("seed")

    (tmp_path / ".reflakeignore").write_text("data/raw/*.csv\n")
    (tmp_path / "data" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "raw" / "skip.csv").write_text("skip")
    (tmp_path / "data" / "keep.txt").write_text("keep")

    repo.add([str(tmp_path / "data")])
    status = repo.status()
    assert status.added == ["data/keep.txt"]
