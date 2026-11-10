"""Tags: immutable refs under ``refs/tags/*`` that pin a commit."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import head_commit

from reflake.cli import run_cli
from reflake.core import EmptyBranchError, ReflakeRepository, create_repository
from reflake.core.repository_sync import pull, push


def _repo(tmp_path: Path) -> tuple[ReflakeRepository, str]:
    (tmp_path / "a.txt").write_text("a")
    repo = create_repository(str(tmp_path))
    first = repo.commit("first")
    return repo, first


# ── Repository API ───────────────────────────────────────────────────────────


def test_tag_pins_commit_and_resolves(tmp_path: Path) -> None:
    repo, first = _repo(tmp_path)
    (tmp_path / "b.txt").write_text("b")
    second = repo.commit("second")

    assert repo.tag("v1.0") == second  # default target: current branch head
    assert repo.resolve_ref("v1.0") == second
    assert repo.tag("v1.0-old", first) == first  # explicit commit target
    assert repo.resolve_ref("v1.0-old") == first

    tags = {entry["tag"]: entry for entry in repo.tags()}
    assert tags["v1.0"] == {
        "tag": "v1.0",
        "commit_id": second,
        "message": "second",
    }


def test_tags_are_immutable_and_deletable(tmp_path: Path) -> None:
    repo, first = _repo(tmp_path)
    repo.tag("v1")
    (tmp_path / "b.txt").write_text("b")
    second = repo.commit("second")

    with pytest.raises(ValueError, match="Tag already exists"):
        repo.tag("v1")

    # Branch and tag namespaces are independent; a born branch wins resolution.
    repo.branch("v1")
    assert repo.resolve_ref("v1") == second

    # Deleting the branch leaves an unborn ref, which must not shadow the tag.
    repo.delete_branch("v1")
    assert repo.resolve_ref("v1") == first

    assert repo.delete_tag("v1") == "v1"
    # Only the unborn branch remains, so the name is merely "no commits"...
    with pytest.raises(EmptyBranchError):
        repo.resolve_ref("v1")
    # ...and a new tag is visible through it.
    repo.tag("v1", first)
    assert repo.resolve_ref("v1") == first


def test_tag_name_validation_and_namespace(tmp_path: Path) -> None:
    repo, first = _repo(tmp_path)
    for bad in ("", "/leading", "trailing/", "a..b", ".hidden", "x.lock"):
        with pytest.raises(ValueError):
            repo.tag(bad)

    repo.tag("release/v1", first)
    with pytest.raises(ValueError, match="conflicts with existing tag"):
        repo.tag("release", first)

    # Deleting the last tag below a namespace frees the parent name too.
    repo.delete_tag("release/v1")
    assert repo.tag("release", first) == first


def test_tag_requires_a_commit(tmp_path: Path) -> None:
    repo = create_repository(str(tmp_path))
    with pytest.raises(EmptyBranchError):
        repo.tag("v1")


def test_unknown_target_and_delete_errors(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    with pytest.raises(ValueError, match="Unknown branch or commit"):
        repo.tag("v1", "does-not-exist")
    with pytest.raises(ValueError, match="Unknown branch or commit"):
        repo.delete_tag("v1")


def test_tags_resolve_in_ref_commands(tmp_path: Path) -> None:
    repo, first = _repo(tmp_path)
    repo.tag("v1", first)
    assert [commit.id for commit in repo.log("v1")] == [first]


# ── GC: tags are reachability roots ──────────────────────────────────────────


def test_gc_keeps_tagged_commits(tmp_path: Path) -> None:
    repo, first = _repo(tmp_path)
    (tmp_path / "b.txt").write_text("b")
    second = repo.commit("second")
    repo.tag("v1", second)

    # Move the branch back: ``second`` is now reachable only through the tag.
    repo.reset(first)
    audit = repo.gc(dry_run=False, grace_seconds=0)
    assert audit.reachable_commits == 2
    assert repo.store.object_exists("commit", second)

    # ``reset`` also wrote a client reflog entry, and the reflog is an undo
    # root. Drop that client-local history to test tag reachability alone.
    for log in (tmp_path / ".reflake" / "reflog").glob("*.log"):
        log.unlink()
    repo.delete_tag("v1")
    pruned = repo.gc(dry_run=False, grace_seconds=0)
    assert pruned.orphan_commits == 1
    assert not repo.store.object_exists("commit", second)


def test_gc_keeps_reflog_only_commits(tmp_path: Path) -> None:
    """A reset-away commit stays available for redo until the reflog is gone."""
    repo, first = _repo(tmp_path)
    (tmp_path / "b.txt").write_text("b")
    second = repo.commit("second")
    repo.reset(first)

    audit = repo.gc(dry_run=False, grace_seconds=0)
    assert audit.reachable_commits == 2
    assert repo.store.object_exists("commit", second)


# ── CLI ──────────────────────────────────────────────────────────────────────


def test_cli_tag_create_list_delete(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("a")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "first"]) == 0
    commit_id = head_commit(capsys, tmp_path)

    assert run_cli(["--repo", str(tmp_path), "--json", "tag", "v1"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "tag": "v1",
        "commit_id": commit_id,
    }

    # No name lists (git-like), as does -l.
    assert run_cli(["--repo", str(tmp_path), "--json", "tag"]) == 0
    assert json.loads(capsys.readouterr().out) == [
        {"tag": "v1", "commit_id": commit_id, "message": "first"}
    ]
    assert run_cli(["--repo", str(tmp_path), "--json", "tag", "-l"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1

    # Any ref-accepting command resolves tags.
    assert run_cli(["--repo", str(tmp_path), "log", "v1"]) == 0
    capsys.readouterr()

    assert run_cli(["--repo", str(tmp_path), "--json", "tag", "-d", "v1"]) == 0
    assert json.loads(capsys.readouterr().out) == {"deleted": "v1"}

    assert run_cli(["--repo", str(tmp_path), "tag", "-d", "v1"]) == 3
    assert run_cli(["--repo", str(tmp_path), "tag", "--list", "v1"]) == 1


# ── Sync: explicit tag push/pull, immutable on the wire ─────────────────────


def test_push_and_pull_tag(tmp_path: Path, fake_s3_installer) -> None:
    fake_s3_installer({})
    repo, first = _repo(tmp_path)
    repo.tag("v1", first)

    remote = "s3://demo-bucket/repos/tagged"
    pushed = push(repo, remote, ref="v1")
    assert pushed.updated is True
    assert pushed.pushed_commits == 1

    clone_root = tmp_path / "clone"
    clone_root.mkdir()
    clone = create_repository(str(clone_root))
    pulled = pull(clone, remote, ref="v1")
    assert pulled.updated is True
    assert clone.resolve_ref("v1") == first

    # Idempotent both ways: the tag already points at the same commit.
    assert push(repo, remote, ref="v1").updated is False
    assert pull(clone, remote, ref="v1").updated is False


def test_push_rejects_moved_tag(tmp_path: Path, fake_s3_installer) -> None:
    fake_s3_installer({})
    repo, first = _repo(tmp_path)
    repo.tag("v1", first)
    remote = "s3://demo-bucket/repos/tagged"
    push(repo, remote, ref="v1")

    (tmp_path / "b.txt").write_text("b")
    second = repo.commit("second")
    repo.delete_tag("v1")
    repo.tag("v1", second)

    with pytest.raises(ValueError, match="tags are immutable"):
        push(repo, remote, ref="v1")


def test_pull_rejects_conflicting_local_tag(
    tmp_path: Path, fake_s3_installer
) -> None:
    fake_s3_installer({})
    repo, first = _repo(tmp_path)
    repo.tag("v1", first)
    remote = "s3://demo-bucket/repos/tagged"
    push(repo, remote, ref="v1")

    clone_root = tmp_path / "clone"
    clone_root.mkdir()
    clone = create_repository(str(clone_root))
    (clone_root / "b.txt").write_text("b")
    clone.tag("v1", clone.commit("local commit"))

    with pytest.raises(ValueError, match="tags are immutable"):
        pull(clone, remote, ref="v1")


def test_tag_parent_name_can_be_reused_after_child_delete(tmp_path: Path) -> None:
    repo, first = _repo(tmp_path)
    repo.tag("release/v1", first)
    repo.delete_tag("release/v1")
    repo.tag("release", first)
    assert repo.store.read_tag_ref("release") == first
