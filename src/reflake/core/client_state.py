from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import quote

from .domain import BranchRefState

HEAD_FILE = "HEAD"


def encode_branch_name(branch: str) -> str:
    """Filesystem-safe key for a branch name (hierarchical names allowed).

    ``feature/x`` becomes ``feature%2Fx`` so per-branch staging, snapshots
    and reflogs stay single flat files with no directory collisions.
    """
    return quote(branch, safe="")


class LocalClientState:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.reflake_dir = self.root / ".reflake"
        self.refs_dir = self.reflake_dir / "refs"
        self.state_dir = self.reflake_dir / "state"
        self.branch_state_dir = self.state_dir / "branch-snapshots"
        self.partial_dir = self.state_dir / "partial"
        self.staging_dir = self.reflake_dir / "staging"
        self.cache_dir = self.reflake_dir / "cache"
        self.reflog_dir = self.reflake_dir / "reflog"
        self.head_path = self.refs_dir / HEAD_FILE

        for path in (
            self.reflake_dir,
            self.refs_dir,
            self.state_dir,
            self.branch_state_dir,
            self.partial_dir,
            self.staging_dir,
            self.cache_dir,
            self.reflog_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def append_reflog(
        self,
        branch: str,
        old_commit: str | None,
        new_commit: str | None,
        operation: str,
    ) -> None:
        """Record a ref update (client-side reflog, one line per entry)."""
        from datetime import datetime

        path = self.reflog_dir / f"{encode_branch_name(branch)}.log"
        timestamp = datetime.now(UTC).isoformat()
        old = old_commit or "0" * 64
        new = new_commit or "0" * 64
        line = f"{old} {new} {operation} {timestamp}\n"
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line)

    def iter_reflog(self, branch: str) -> Iterator[str]:
        path = self.reflog_dir / f"{encode_branch_name(branch)}.log"
        if not path.exists():
            return
        for raw in reversed(path.read_text(encoding="utf-8").splitlines()):
            if raw.strip():
                yield raw

    def iter_staged_changes(self) -> Iterator[dict[str, object]]:
        """Every staged change in client scope (all branches), best effort.

        GC must treat staged content as a root: ``add`` publishes a blob
        before the commit that makes it reachable, and a staged import can
        legitimately outlive the prune grace window. A corrupt staging file is
        ignored here (it cannot make a live ref unreachable; the ordinary
        command path reports the decode error).
        """
        if not self.staging_dir.is_dir():
            return
        for path in sorted(self.staging_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if isinstance(payload, list):
                for change in payload:
                    if isinstance(change, dict):
                        yield change

    def iter_reflog_commit_ids(self) -> Iterator[str]:
        """Commit ids mentioned by the client reflog (undo/redo roots).

        The reflog is client-local and never synced; a local ``gc`` on the
        same client must not prune a commit the user can still reset to.
        """
        if not self.reflog_dir.is_dir():
            return
        zero = "0" * 64
        for path in sorted(self.reflog_dir.glob("*.log")):
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in lines:
                parts = line.split(" ", 3)
                if len(parts) < 3:
                    continue
                for commit_id in parts[:2]:
                    if commit_id and commit_id != zero:
                        yield commit_id

    def derived_manifest_path(self, tree_hash: str) -> Path:
        """Path of the cached derived manifest for *tree_hash*.

        Content-addressed: the cache is safe because a tree hash always maps
        to identical bytes (see docs/architecture.md §3).
        """
        return self.cache_dir / "derived" / f"{tree_hash}.jsonl"

    def ensure_current_branch(self, default_branch: str) -> None:
        from .repository_support import validate_branch_name

        default_branch = validate_branch_name(default_branch)
        if not self.head_path.exists():
            self.set_current_branch(default_branch)

    def current_branch(self) -> str:
        content = self.head_path.read_text(encoding="utf-8").strip()
        if not content.startswith("refs/heads/"):
            raise ValueError("HEAD must be a symbolic ref under refs/heads/")
        branch = content.split("refs/heads/", maxsplit=1)[1]
        from .repository_support import validate_branch_name

        return validate_branch_name(branch)

    def set_current_branch(self, branch: str) -> None:
        from .repository_support import validate_branch_name

        branch = validate_branch_name(branch)
        self._atomic_write_text(self.head_path, f"refs/heads/{branch}\n")

    def read_staging_payload(self, branch: str) -> str | None:
        stage_path = self.stage_path(branch)
        if not stage_path.exists():
            return None
        return stage_path.read_text(encoding="utf-8")

    def write_staging_payload(self, branch: str, payload: str | None) -> None:
        stage_path = self.stage_path(branch)
        if payload is None:
            stage_path.unlink(missing_ok=True)
            return
        self._atomic_write_text(stage_path, payload)

    def read_branch_snapshot(self, branch: str) -> BranchRefState | None:
        snapshot_path = self.branch_snapshot_path(branch)
        if not snapshot_path.exists():
            return None
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        return BranchRefState(
            branch=branch,
            commit_id=payload.get("commit_id"),
        )

    def write_branch_snapshot(
        self,
        branch: str,
        *,
        commit_id: str | None,
    ) -> None:
        payload = json.dumps(
            {"commit_id": commit_id},
            sort_keys=True,
        )
        self._atomic_write_text(self.branch_snapshot_path(branch), f"{payload}\n")

    def branch_snapshot_path(self, branch: str) -> Path:
        """Client-local snapshot location, outside the shared refs namespace.

        Snapshots live under ``state/branch-snapshots/`` so they can never be
        mistaken for branch pointers by shared-store enumeration.
        """
        return self.branch_state_dir / f"{encode_branch_name(branch)}.json"

    def stage_path(self, branch: str) -> Path:
        return self.staging_dir / f"{encode_branch_name(branch)}.json"

    def partial_path(self, branch: str) -> Path:
        """Marker for a metadata-only pull/fetch waiting for blob hydration."""
        return self.partial_dir / f"{encode_branch_name(branch)}.json"

    def stale_path(self, branch: str) -> Path:
        return self.state_dir / "worktree-stale" / f"{encode_branch_name(branch)}.json"

    def mark_worktree_stale(
        self, branch: str, *, previous_commit_id: str | None = None
    ) -> None:
        payload = json.dumps({"previous_commit_id": previous_commit_id}) + "\n"
        self._atomic_write_text(self.stale_path(branch), payload)

    def clear_worktree_stale(self, branch: str) -> None:
        self.stale_path(branch).unlink(missing_ok=True)

    def is_worktree_stale(self, branch: str) -> bool:
        return self.stale_path(branch).exists()

    def stale_previous_commit(self, branch: str) -> str | None:
        path = self.stale_path(branch)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        value = payload.get("previous_commit_id")
        return value if isinstance(value, str) else None

    def mark_partial(self, branch: str) -> None:
        self._atomic_write_text(self.partial_path(branch), "{}\n")

    def clear_partial(self, branch: str) -> None:
        self.partial_path(branch).unlink(missing_ok=True)

    def is_partial(self, branch: str) -> bool:
        return self.partial_path(branch).exists()

    def _atomic_write_text(self, path: Path, payload: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp:
            temp_path = Path(temp.name)
            temp.write(payload)

        try:
            temp_path.replace(path)
        finally:
            temp_path.unlink(missing_ok=True)
