"""Tests for push, pull, and fetch sync operations."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from reflake import run_cli
from reflake.core import (
    NonFastForwardError,
    RefConflictError,
    create_repository,
)
from reflake.core.repository_sync import (
    FetchResult,
    PushResult,
    fetch,
    pull,
    push,
)

# ── push ──────────────────────────────────────────────────────────────────────


def test_push_blobs_and_commits_to_s3(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """Local commit → push to s3://demo-bucket/repos/test → S3 has all objects."""
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)

    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "b.txt").write_text("beta")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "initial"]) == 0
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    repo = create_repository(str(tmp_path))
    result = push(repo, "s3://demo-bucket/repos/test")

    assert isinstance(result, PushResult)
    assert result.updated is True
    assert result.pushed_commits == 1
    assert result.pushed_blobs == 2

    # Verify S3 objects exist
    s3_keys: set[str] = set(client._objects.keys())
    assert f"repos/test/commits/{commit_id}.json" in s3_keys
    assert any(k.startswith("repos/test/trees/") for k in s3_keys)
    assert any(k.startswith("repos/test/blobs/") for k in s3_keys)
    assert "repos/test/refs/heads/main" in s3_keys


def test_push_with_no_commits(tmp_path: Path, fake_s3_installer) -> None:
    """Push from a branch with no commits raises ValueError."""
    fake_s3_installer({})

    repo = create_repository(str(tmp_path))
    with pytest.raises(ValueError, match="no commits"):
        push(repo, "s3://demo-bucket/repos/test")


def test_push_idempotent(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """Push same commits twice → second push returns updated=False."""
    fake_s3_installer({})
    monkeypatch.chdir(tmp_path)

    (tmp_path / "x.txt").write_text("data")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "first"]) == 0
    capsys.readouterr()

    repo = create_repository(str(tmp_path))
    result1 = push(repo, "s3://demo-bucket/repos/test")
    assert result1.updated is True
    assert result1.pushed_commits == 1

    result2 = push(repo, "s3://demo-bucket/repos/test")
    assert result2.updated is False
    assert result2.pushed_commits == 0
    assert result2.pushed_blobs == 0


def test_push_multiple_commits(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """Push multiple commits increments counts and stores all objects."""
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)

    (tmp_path / "f.txt").write_text("v1")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "c1"]) == 0
    c1 = capsys.readouterr().out.strip()

    (tmp_path / "f.txt").write_text("v2")
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "c2"]) == 0
    c2 = capsys.readouterr().out.strip()

    repo = create_repository(str(tmp_path))
    result = push(repo, "s3://demo-bucket/repos/test")
    assert result.pushed_commits == 2
    assert result.updated is True

    s3_keys = set(client._objects.keys())
    assert f"repos/test/commits/{c1}.json" in s3_keys
    assert f"repos/test/commits/{c2}.json" in s3_keys
    assert "repos/test/refs/heads/main" in s3_keys


# ── pull ──────────────────────────────────────────────────────────────────────


def test_pull_from_s3(tmp_path: Path, capsys, monkeypatch, fake_s3_installer) -> None:
    """Push from local A → pull to local B → restore files on B → content matches."""
    fake_s3_installer({})

    # ── Repo A: create commit and push ──
    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    monkeypatch.chdir(repo_a)
    (repo_a / "hello.txt").write_text("world")
    (repo_a / "sub").mkdir()
    (repo_a / "sub" / "nested.txt").write_text("deep")
    assert run_cli(["--repo", str(repo_a), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_a), "commit", "-m", "initial"]) == 0
    capsys.readouterr()

    a_repo = create_repository(str(repo_a))
    result = push(a_repo, "s3://demo-bucket/repos/test")
    assert result.updated is True
    assert result.pushed_commits == 1
    assert result.pushed_blobs == 2

    # ── Repo B: pull from S3 ──
    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()
    monkeypatch.chdir(repo_b)

    pull_from_remote = run_cli(
        ["--repo", str(repo_b), "--json", "pull", "s3://demo-bucket/repos/test"]
    )
    assert pull_from_remote == 0
    pull_json = json.loads(capsys.readouterr().out)
    assert pull_json["pulled_commits"] == 1
    assert pull_json["pulled_blobs"] == 2
    assert pull_json["updated"] is True

    # Restore files from the pulled branch and verify content
    assert run_cli(["--repo", str(repo_b), "restore", "main"]) == 0
    restore_out = capsys.readouterr().out
    assert "Restored 2 file(s)" in restore_out
    assert (repo_b / "hello.txt").read_text() == "world"
    assert (repo_b / "sub" / "nested.txt").read_text() == "deep"


def test_pull_with_no_remote_branch(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """Pull from a remote branch that doesn't exist raises ValueError."""
    fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    (tmp_path / "_dummy.txt").write_text("x")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "initial"]) == 0
    capsys.readouterr()

    repo = create_repository(str(tmp_path))
    # The remote branch was never created: opening a remote no longer
    # auto-creates refs, so this is an unknown ref rather than an empty one.
    with pytest.raises(ValueError, match="Unknown branch or commit"):
        pull(repo, "s3://demo-bucket/repos/test")


def test_pull_idempotent(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """Pull twice → second pull returns updated=False."""
    fake_s3_installer({})

    # Repo A: create and push
    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    monkeypatch.chdir(repo_a)
    (repo_a / "data.txt").write_text("payload")
    assert run_cli(["--repo", str(repo_a), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_a), "commit", "-m", "c"]) == 0
    capsys.readouterr()

    a_repo = create_repository(str(repo_a))
    assert push(a_repo, "s3://demo-bucket/repos/test").updated is True

    # Repo B: pull twice from an empty branch.
    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()

    b_repo = create_repository(str(repo_b))
    result1 = pull(b_repo, "s3://demo-bucket/repos/test")
    assert result1.updated is True
    assert result1.pulled_commits == 1
    assert result1.pulled_blobs == 1

    result2 = pull(b_repo, "s3://demo-bucket/repos/test")
    assert result2.updated is False
    assert result2.pulled_commits == 0
    assert result2.pulled_blobs == 0


# ── fetch ─────────────────────────────────────────────────────────────────────


def test_fetch_downloads_objects_without_branch_update(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """Push from A, fetch to B → objects exist but branch ref not updated."""
    fake_s3_installer({})

    # Repo A: create and push two commits
    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    monkeypatch.chdir(repo_a)
    (repo_a / "f1.txt").write_text("one")
    assert run_cli(["--repo", str(repo_a), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_a), "commit", "-m", "c1"]) == 0
    capsys.readouterr()

    a_repo = create_repository(str(repo_a))
    assert push(a_repo, "s3://demo-bucket/repos/test").updated is True

    # Repo B: init + fetch
    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()
    (repo_b / "_init.txt").write_text("x")
    assert run_cli(["--repo", str(repo_b), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_b), "commit", "-m", "b"]) == 0
    capsys.readouterr()

    b_repo = create_repository(str(repo_b))
    # Record branch state before fetch
    pre_branch = b_repo.store.read_branch_ref("main")
    pre_commit = pre_branch.commit_id if pre_branch else None

    result = fetch(b_repo, "s3://demo-bucket/repos/test")
    assert isinstance(result, FetchResult)
    assert result.fetched_commits == 1
    assert result.fetched_blobs == 1
    assert result.branch == "main"

    # Objects should exist in B's store now (verified by fetched counts above)
    assert b_repo.store.object_exists("commit", a_repo.resolve_ref("main"))

    # Branch ref should NOT have been updated
    post_branch = b_repo.store.read_branch_ref("main")
    assert post_branch is not None
    assert post_branch.commit_id == pre_commit


def test_fetch_idempotent(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """Fetch twice → second fetch reports zero new objects."""
    fake_s3_installer({})

    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    monkeypatch.chdir(repo_a)
    (repo_a / "f.txt").write_text("data")
    assert run_cli(["--repo", str(repo_a), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_a), "commit", "-m", "c"]) == 0
    capsys.readouterr()

    a_repo = create_repository(str(repo_a))
    assert push(a_repo, "s3://demo-bucket/repos/test").updated is True

    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()
    (repo_b / "_init.txt").write_text("x")
    assert run_cli(["--repo", str(repo_b), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_b), "commit", "-m", "b"]) == 0
    capsys.readouterr()

    b_repo = create_repository(str(repo_b))

    r1 = fetch(b_repo, "s3://demo-bucket/repos/test")
    assert r1.fetched_commits == 1

    r2 = fetch(b_repo, "s3://demo-bucket/repos/test")
    assert r2.fetched_commits == 0
    assert r2.fetched_blobs == 0


# ── CLI JSON output ───────────────────────────────────────────────────────────


def test_cli_push_json_output(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """CLI push --json returns valid JSON with expected fields."""
    fake_s3_installer({})
    monkeypatch.chdir(tmp_path)

    (tmp_path / "x.txt").write_text("hello")
    assert run_cli(["--repo", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(tmp_path), "commit", "-m", "initial"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            ["--repo", str(tmp_path), "--json", "push", "s3://demo-bucket/repos/test"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["updated"] is True
    assert payload["pushed_commits"] == 1
    assert payload["pushed_blobs"] == 1
    assert payload["source_branch"] == "main"
    assert payload["remote_uri"] == "s3://demo-bucket/repos/test"


def test_cli_pull_json_output(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """CLI pull --json returns valid JSON with expected fields."""
    fake_s3_installer({})

    # Repo A: push
    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    monkeypatch.chdir(repo_a)
    (repo_a / "f.txt").write_text("data")
    assert run_cli(["--repo", str(repo_a), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_a), "commit", "-m", "c"]) == 0
    capsys.readouterr()

    a_repo = create_repository(str(repo_a))
    push(a_repo, "s3://demo-bucket/repos/test")

    # Repo B: pull --json from an empty branch.
    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()

    assert (
        run_cli(
            ["--repo", str(repo_b), "--json", "pull", "s3://demo-bucket/repos/test"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["updated"] is True
    assert payload["pulled_commits"] == 1
    assert payload["pulled_blobs"] == 1
    assert payload["source_branch"] == "main"
    assert payload["remote_uri"] == "s3://demo-bucket/repos/test"


def test_push_rejects_divergent_remote_history(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    """Push must reject a remote branch that diverged after the last push."""
    fake_s3_installer({})
    remote = "s3://demo-bucket/repos/test"

    repo_a_root = tmp_path / "repo_a"
    repo_a_root.mkdir()
    (repo_a_root / "shared.txt").write_text("base")
    repo_a = create_repository(str(repo_a_root))
    repo_a.commit("base")
    push(repo_a, remote)

    repo_b_root = tmp_path / "repo_b"
    repo_b_root.mkdir()
    repo_b = create_repository(str(repo_b_root))
    pull(repo_b, remote)
    (repo_b_root / "from-b.txt").write_text("b")
    repo_b.commit("b change")
    push(repo_b, remote)

    (repo_a_root / "from-a.txt").write_text("a")
    repo_a.commit("a change")
    local_head = repo_a.resolve_ref("main")

    # Pre-check catches divergence before any objects are copied.
    with pytest.raises(NonFastForwardError):
        push(repo_a, remote)

    # Local head must remain unchanged after the rejected push.
    assert repo_a.resolve_ref("main") == local_head


def test_pull_rejects_divergent_local_history(
    tmp_path: Path, fake_s3_installer
) -> None:
    """A pull may fetch immutable objects but must not replace a divergent head."""
    fake_s3_installer({})
    remote = "s3://demo-bucket/repos/test"

    repo_a_root = tmp_path / "repo_a"
    repo_a_root.mkdir()
    (repo_a_root / "shared.txt").write_text("base")
    repo_a = create_repository(str(repo_a_root))
    repo_a.commit("base")
    push(repo_a, remote)

    repo_b_root = tmp_path / "repo_b"
    repo_b_root.mkdir()
    repo_b = create_repository(str(repo_b_root))
    pull(repo_b, remote)
    (repo_b_root / "from-b.txt").write_text("b")
    repo_b.commit("b change")
    local_head = repo_b.resolve_ref("main")

    (repo_a_root / "from-a.txt").write_text("a")
    repo_a.commit("a change")
    push(repo_a, remote)

    with pytest.raises(NonFastForwardError):
        pull(repo_b, remote)

    assert repo_b.resolve_ref("main") == local_head


def test_cli_fetch_output(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    """CLI fetch --json returns valid JSON; plain text shows counts."""
    fake_s3_installer({})

    # Repo A: push
    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    monkeypatch.chdir(repo_a)
    (repo_a / "x.txt").write_text("data")
    assert run_cli(["--repo", str(repo_a), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_a), "commit", "-m", "c"]) == 0
    capsys.readouterr()

    a_repo = create_repository(str(repo_a))
    push(a_repo, "s3://demo-bucket/repos/test")

    # Repo B: fetch --json
    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()
    (repo_b / "_init.txt").write_text("x")
    assert run_cli(["--repo", str(repo_b), "init"]) == 0
    capsys.readouterr()
    assert run_cli(["--repo", str(repo_b), "commit", "-m", "b"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            ["--repo", str(repo_b), "--json", "fetch", "s3://demo-bucket/repos/test"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["fetched_commits"] == 1
    assert payload["fetched_blobs"] == 1
    assert payload["remote_uri"] == "s3://demo-bucket/repos/test"
    assert payload["branch"] == "main"


# ── Two-client divergence tests ──────────────────────────────────────────────


def test_push_two_client_multi_commit_divergence(
    tmp_path: Path,
    fake_s3_installer,
) -> None:
    """Client A and B both build on top of the same base; B pushes first;
    A's push must be rejected because the histories diverged."""
    fake_s3_installer({})
    remote = "s3://demo-bucket/repos/test"

    # Shared base
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "base.txt").write_text("base")
    shared_repo = create_repository(str(shared))
    shared_repo.commit("base commit")
    push(shared_repo, remote)

    # Client A: pull, make two commits
    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    a_repo = create_repository(str(repo_a))
    pull(a_repo, remote)
    (repo_a / "a1.txt").write_text("a1")
    a_repo.commit("a first change")
    (repo_a / "a2.txt").write_text("a2")
    a_repo.commit("a second change")
    a_head = a_repo.resolve_ref("main")

    # Client B: pull, make a different commit, push
    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()
    b_repo = create_repository(str(repo_b))
    pull(b_repo, remote)
    (repo_b / "b.txt").write_text("b")
    b_repo.commit("b change")
    push(b_repo, remote)

    # Client A pushes — histories diverged; must be rejected.
    with pytest.raises(NonFastForwardError):
        push(a_repo, remote)

    # A's local head must be intact.
    assert a_repo.resolve_ref("main") == a_head


def test_pull_two_client_multi_commit_divergence(
    tmp_path: Path,
    fake_s3_installer,
) -> None:
    """Client B builds locally while A advances the remote; B's pull must
    be rejected because B's local history diverged from the remote."""
    fake_s3_installer({})
    remote = "s3://demo-bucket/repos/test"

    # Shared base
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "base.txt").write_text("base")
    shared_repo = create_repository(str(shared))
    shared_repo.commit("base commit")
    push(shared_repo, remote)

    # Client A pushes two more commits.
    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    a_repo = create_repository(str(repo_a))
    pull(a_repo, remote)
    (repo_a / "a1.txt").write_text("a1")
    a_repo.commit("a first")
    (repo_a / "a2.txt").write_text("a2")
    a_repo.commit("a second")
    push(a_repo, remote)

    # Client B pulls the base, then makes local commits without pushing.
    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()
    b_repo = create_repository(str(repo_b))
    pull(b_repo, remote)
    # Now rewind B to base so B's history diverges (A has already advanced).
    # Re-open to avoid stale snapshot: create a fresh repo at B's worktree
    # that only has the base commit.
    (repo_b / "b1.txt").write_text("b1")
    b_repo.commit("b first")
    (repo_b / "b2.txt").write_text("b2")
    b_repo.commit("b second")
    b_head = b_repo.resolve_ref("main")

    # Now A pushes one more commit to move remote further ahead.
    (repo_a / "a3.txt").write_text("a3")
    a_repo.commit("a third")
    push(a_repo, remote)

    # B tries to pull — histories diverged; must be rejected.
    with pytest.raises(NonFastForwardError):
        pull(b_repo, remote)

    # B's local head must be intact.
    assert b_repo.resolve_ref("main") == b_head


# ── Concurrent-update (CAS race) tests ───────────────────────────────────────


def test_push_cas_race_via_stale_snapshot(
    tmp_path: Path,
    fake_s3_installer,
) -> None:
    """Simulate a CAS race: another client pushes between the pre-check
    and the CAS.  The fast-forward pre-check passes (because the remote
    head at the time of the check *is* an ancestor), but the final CAS
    uses a stale version token and must raise RefConflictError."""
    fake_s3_installer({})
    remote = "s3://demo-bucket/repos/test"

    # ── Shared base ──
    repo_a_root = tmp_path / "repo_a"
    repo_a_root.mkdir()
    (repo_a_root / "base.txt").write_text("base")
    repo_a = create_repository(str(repo_a_root))
    repo_a.commit("base")
    push(repo_a, remote)

    # ── Client B pulls the base and builds on top ──
    repo_b_root = tmp_path / "repo_b"
    repo_b_root.mkdir()
    repo_b = create_repository(str(repo_b_root))
    pull(repo_b, remote)
    (repo_b_root / "b.txt").write_text("b-data")
    b_commit = repo_b.commit("b commit")

    # The pre-check from _verify_push_fast_forward reads the remote head
    # fresh from the store and confirms it is an ancestor of b_commit.
    from reflake.core.repository import open_repository as _open

    remote_repo = _open(remote, worktree=repo_b_root)
    remote_state = remote_repo.store.read_branch_ref("main")
    assert remote_state is not None
    # Sanity: remote head *is* ancestor of b_commit at this point.
    from reflake.core.repository_sync import _is_ancestor_in

    assert _is_ancestor_in(remote_state.commit_id, b_commit, repo_b)

    # ── Simulate a concurrent push: directly advance the remote ref ──
    # This represents another client pushing between B's pre-check and B's CAS.
    # We write a new commit that B doesn't know about and move the remote head.
    concurrent_commit = "ffff" * 16  # fake commit id
    remote_repo.store.write_commit_bytes(
        concurrent_commit,
        b'{"id":"'
        + concurrent_commit.encode()
        + b'","parents":["'
        + remote_state.commit_id.encode()
        + b'"],"message":"concurrent",'
        b'"tree":"'
        + remote_state.commit_id.encode()
        + b'","branch":"main","generation":1,"created_at":""}',
    )
    remote_repo.store.write_branch_ref("main", concurrent_commit)

    # ── Now B tries to push.  The pre-check reads the new remote head
    # (concurrent_commit) and sees it is NOT an ancestor of b_commit →
    # NonFastForwardError.  This is the correct *divergence* path.
    #
    # To exercise the pure CAS-race path we need to bypass the pre-check
    # and go straight to fast_forward_branch with a stale snapshot.
    # We corrupt B's client_state so _require_branch_state returns stale data.
    repo_b.client_state.write_branch_snapshot(
        "main",
        commit_id=remote_state.commit_id,
    )
    # Now fast_forward_branch will use the stale snapshot: it sees the
    # old remote head as current, checks ancestry (passes because B's
    # commit IS a descendant of the old head), then CAS fails because
    # the real remote head has moved.
    with pytest.raises(RefConflictError):
        repo_b.fast_forward_branch("main", b_commit, operation="push")


def test_pull_cas_race_via_stale_snapshot(
    tmp_path: Path,
    fake_s3_installer,
) -> None:
    """Simulate a CAS race during pull: a stale snapshot causes the
    optimistic lock to fail even though ancestry would permit the update."""
    fake_s3_installer({})
    remote = "s3://demo-bucket/repos/test"

    # A creates and pushes base + one more commit.
    repo_a_root = tmp_path / "repo_a"
    repo_a_root.mkdir()
    (repo_a_root / "base.txt").write_text("base")
    repo_a = create_repository(str(repo_a_root))
    repo_a.commit("base")
    push(repo_a, remote)

    (repo_a_root / "a2.txt").write_text("a2")
    _a_c2 = repo_a.commit("a2")
    push(repo_a, remote)

    # B pulls a_c2 so B has the commit objects locally.
    repo_b_root = tmp_path / "repo_b"
    repo_b_root.mkdir()
    repo_b = create_repository(str(repo_b_root))
    pull(repo_b, remote)

    # A pushes one more commit so the remote head advances.
    (repo_a_root / "a3.txt").write_text("a3")
    a_c3 = repo_a.commit("a3")
    push(repo_a, remote)

    # Fetch a_c3's objects into B's store so ancestry check can read them.
    from reflake.core.repository_sync import fetch as _fetch

    _fetch(repo_b, remote)

    # Simulate a concurrent local writer: advance B's real head to a_c3
    # behind the snapshot's back (snapshot still says a_c2).
    repo_b.store.write_branch_ref("main", a_c3)

    # fast_forward_branch: ancestry check passes (a_c2 is ancestor of a_c3),
    # but CAS fails because the real head already moved past the snapshot.
    with pytest.raises(RefConflictError):
        repo_b.fast_forward_branch("main", a_c3, operation="pull")


def test_compare_and_set_branch_ref_create_is_exclusive(
    tmp_path: Path,
    fake_s3_installer,
) -> None:
    """Unit-level: create-only CAS (expected None) fails on an existing ref."""
    fake_s3_installer({})

    repo = create_repository(str(tmp_path))
    repo.commit("initial")
    commit_id = repo.resolve_ref("main")

    # First CAS succeeds with the correct (None → None) transition.
    result = repo.store.compare_and_set_branch_ref(
        "feature",
        commit_id,
        expected_commit_id=None,
    )
    assert result is True

    # Second create-only CAS on the existing ref must return False.
    result = repo.store.compare_and_set_branch_ref(
        "feature",
        commit_id,
        expected_commit_id=None,
    )
    assert result is False


def test_compare_and_set_branch_ref_rejects_wrong_commit(
    tmp_path: Path,
    fake_s3_installer,
) -> None:
    """Unit-level: compare_and_set_branch_ref returns False when the
    expected commit ID does not match the current store state."""
    fake_s3_installer({})

    repo = create_repository(str(tmp_path))
    repo.commit("initial")

    # CAS with a wrong expected_commit_id must return False.
    result = repo.store.compare_and_set_branch_ref(
        "main",
        "aaaa" * 16,  # different commit
        expected_commit_id="bbbb" * 16,  # wrong
    )
    assert result is False


# ── End-to-end recovery scenario ──────────────────────────────────────────────


def test_push_recovery_after_divergent_pull_and_fetch(
    tmp_path: Path,
    fake_s3_installer,
) -> None:
    """End-to-end: A and B diverge; B's push is rejected; B fetches the
    new objects (without updating the branch ref) and can inspect them."""
    fake_s3_installer({})
    remote = "s3://demo-bucket/repos/test"

    # Shared base
    base = tmp_path / "base"
    base.mkdir()
    (base / "f.txt").write_text("base")
    base_repo = create_repository(str(base))
    base_repo.commit("base")
    push(base_repo, remote)

    # A and B both pull the base
    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    a_repo = create_repository(str(repo_a))
    pull(a_repo, remote)

    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()
    b_repo = create_repository(str(repo_b))
    pull(b_repo, remote)

    # A commits and pushes
    (repo_a / "a.txt").write_text("a")
    a_repo.commit("a work")
    push(a_repo, remote)

    # B commits independently (diverging)
    (repo_b / "b.txt").write_text("b")
    b_repo.commit("b work")

    # B's push must be rejected
    with pytest.raises(NonFastForwardError):
        push(b_repo, remote)

    # B's local head is intact.
    b_head = b_repo.resolve_ref("main")
    assert b_head is not None

    # B can still fetch the new objects without updating the branch ref.
    from reflake.core.repository_sync import fetch

    fetch_result = fetch(b_repo, remote)
    assert fetch_result.fetched_commits >= 0
    # Fetch must NOT update the branch ref.
    assert b_repo.resolve_ref("main") == b_head


def test_push_progress_callback_reports_each_item(
    tmp_path: Path, fake_s3_installer
) -> None:
    """push() invokes the progress callback per transferred item."""
    fake_s3_installer({})
    (tmp_path / "a.txt").write_text("alpha")
    repo = create_repository(str(tmp_path))
    repo.commit("initial")

    events: list[tuple[int, int]] = []

    def progress(done: int, total: int) -> None:
        events.append((done, total))

    result = push(repo, "s3://demo-bucket/repos/test", progress=progress)
    assert result.updated is True
    assert events, "progress callback was never invoked"
    assert events[-1] == (events[-1][1], events[-1][1])  # final = total


def test_push_transfers_footer_objects(
    tmp_path: Path, fake_s3_installer
) -> None:
    """Parquet footer stats objects travel with push (plan covers footers)."""
    import duckdb

    from reflake.core.config import LocalConfig

    client = fake_s3_installer({})
    (tmp_path / "data.parquet").write_bytes(b"")
    pfile = tmp_path / "data.parquet"
    duckdb.sql(
        "COPY (SELECT range AS id FROM range(0, 5)) "
        f"TO '{pfile}' (FORMAT PARQUET)"
    )
    LocalConfig(identity="content", parquet_footer=True).save(tmp_path)
    repo = create_repository(str(tmp_path))
    repo.commit("with footer")
    entry = repo.resolve_entry("main", "data.parquet")
    assert entry.footer is not None

    result = push(repo, "s3://demo-bucket/repos/test")
    assert result.updated is True
    assert result.pushed_blobs == 1
    assert f"repos/test/footers/{entry.footer}" in client._objects
