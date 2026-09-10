"""Branch-scoped staging state: load/save, source expansion, and status.

Staging is local client state keyed by branch name; this collaborator owns
the payload format, the source-path expansion rules, and the status report.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath

import msgspec

from ..client_state import LocalClientState
from ..domain import StageChange, StageStatus
from ..entry_codec import Entry
from ..hashing import blake3_digest_file
from ..manifest import FileEntry, walk_files
from ..objects import QueryRefStore, iter_s3_objects, parse_s3_uri
from ..repository_support import (
    normalize_repository_path,
    normalize_s3_import_path,
)
from .refs import RefManager


class StagingArea:
    def __init__(
        self,
        *,
        client_state: LocalClientState,
        root: Path,
        store: QueryRefStore,
        refs: RefManager,
        trust_mtime: bool = False,
    ) -> None:
        self.client_state = client_state
        self.root = root
        self.store = store
        self.refs = refs
        self.trust_mtime = trust_mtime

    def load(self, branch: str) -> dict[str, StageChange]:
        stage_payload = self.client_state.read_staging_payload(branch)
        if stage_payload is None:
            return {}
        changes: list[StageChange] = msgspec.json.decode(
            stage_payload, type=list[StageChange]
        )
        return {change.path: change for change in changes}

    def save(self, branch: str, staged: dict[str, StageChange]) -> None:
        if not staged:
            self.client_state.write_staging_payload(branch, None)
            return
        payload = msgspec.json.format(
            msgspec.json.encode(sorted(staged.values(), key=lambda item: item.path)),
            indent=2,
        )
        self.client_state.write_staging_payload(
            branch,
            payload.decode() + "\n",
        )

    def status(
        self, *, ref: str | None = None, working_tree: bool = False
    ) -> StageStatus:
        branch = ref or self.refs.current_branch()
        self.refs.ensure_branch_exists(branch)
        staged = self.load(branch)
        added = sorted(
            change.path for change in staged.values() if change.action == "add"
        )
        removed = sorted(
            change.path for change in staged.values() if change.action == "remove"
        )

        wt_added: list[str] = []
        wt_removed: list[str] = []
        wt_modified: list[str] = []
        if working_tree:
            wt_added, wt_removed, wt_modified = self._compare_working_tree(branch)

        return StageStatus(
            ref=branch,
            added=added,
            removed=removed,
            working_tree_added=wt_added,
            working_tree_removed=wt_removed,
            working_tree_modified=wt_modified,
        )

    def expand_stage_source(
        self,
        raw_source: str,
        *,
        destination_path: str | None,
    ) -> list[tuple[str, str]]:
        """Expand a raw stage source into ``(logical_path, source_uri)`` pairs."""
        if raw_source.startswith("s3://"):
            return self._expand_s3_stage_source(
                raw_source,
                destination_path=destination_path,
            )

        return self._expand_local_stage_source(
            raw_source,
            destination_path=destination_path,
        )

    def _expand_local_stage_source(
        self,
        raw_source: str,
        *,
        destination_path: str | None,
    ) -> list[tuple[str, str]]:
        raw_path = Path(raw_source).expanduser()
        source_path = (
            raw_path.resolve()
            if raw_path.is_absolute()
            else (self.root / raw_path).resolve()
        )
        if not source_path.exists():
            raise FileNotFoundError(f"Cannot stage missing path: {raw_source}")

        if source_path.is_file():
            logical_path = self._single_local_logical_path(
                source_path,
                destination_path=destination_path,
            )
            return [(logical_path, source_path.as_uri())]

        if not source_path.is_dir():
            raise FileNotFoundError(f"Cannot stage unsupported path: {raw_source}")

        repo_relative_dir: str | None
        try:
            repo_relative_dir = normalize_repository_path(
                source_path.relative_to(self.root).as_posix()
            )
        except ValueError:
            repo_relative_dir = None

        destination_prefix = (
            normalize_repository_path(destination_path)
            if destination_path is not None
            else repo_relative_dir or normalize_repository_path(source_path.name)
        )

        staged_entries: list[tuple[str, str]] = []
        for file_entry in walk_files(source_path):
            relative_suffix = file_entry.path.relative_to(source_path).as_posix()
            logical_path = normalize_repository_path(
                PurePosixPath(destination_prefix, relative_suffix).as_posix()
            )
            staged_entries.append((logical_path, file_entry.path.as_uri()))

        if not staged_entries:
            raise FileNotFoundError(f"Cannot stage empty directory: {raw_source}")
        return staged_entries

    def _single_local_logical_path(
        self,
        source_path: Path,
        *,
        destination_path: str | None,
    ) -> str:
        if destination_path is not None:
            return normalize_repository_path(destination_path)
        try:
            return normalize_repository_path(
                source_path.relative_to(self.root).as_posix()
            )
        except ValueError:
            return normalize_repository_path(source_path.name)

    def _expand_s3_stage_source(
        self,
        raw_source: str,
        *,
        destination_path: str | None,
    ) -> list[tuple[str, str]]:
        bucket, key = parse_s3_uri(raw_source)
        normalized_key = key.strip("/")
        if not normalized_key:
            raise ValueError(f"S3 source cannot be bucket root: {raw_source}")

        objects = list(iter_s3_objects(raw_source))
        if not objects:
            raise FileNotFoundError(f"Cannot stage missing S3 path: {raw_source}")

        exact_object_uri = f"s3://{bucket}/{normalized_key}"
        exact_object_matches = [
            obj for obj in objects if obj.source_uri == exact_object_uri
        ]
        is_single_object = len(exact_object_matches) == 1 and len(objects) == 1

        if is_single_object:
            logical_path = (
                normalize_repository_path(destination_path)
                if destination_path is not None
                else normalize_repository_path(PurePosixPath(normalized_key).name)
            )
            return [(logical_path, exact_object_uri)]

        prefix = normalized_key
        destination_prefix = (
            normalize_repository_path(destination_path)
            if destination_path is not None
            else normalize_repository_path(PurePosixPath(prefix).name)
        )

        staged_entries: list[tuple[str, str]] = []
        for obj in objects:
            relative_path = normalize_s3_import_path(
                key=obj.key,
                prefix=prefix,
                size=obj.size,
            )
            if relative_path is None:
                continue
            logical_path = normalize_repository_path(
                PurePosixPath(destination_prefix, relative_path).as_posix()
            )
            staged_entries.append((logical_path, obj.source_uri))

        if not staged_entries and exact_object_matches:
            logical_path = (
                normalize_repository_path(destination_path)
                if destination_path is not None
                else normalize_repository_path(PurePosixPath(normalized_key).name)
            )
            return [(logical_path, exact_object_uri)]
        if not staged_entries:
            raise FileNotFoundError(f"Cannot stage empty S3 prefix: {raw_source}")
        return staged_entries

    def _compare_working_tree(
        self, branch: str
    ) -> tuple[list[str], list[str], list[str]]:
        branch_ref = self.store.read_branch_ref(branch)
        head_commit = branch_ref.commit_id if branch_ref else None
        if head_commit is None:
            manifest_paths: set[str] = set()
            manifest_by_path: dict[str, Entry] = {}
        else:
            commit_obj = self.refs.read_commit(head_commit)
            manifest_by_path = {
                entry.path: entry
                for entry in self.store.iter_all_entries(commit_obj.tree)
            }
            manifest_paths = set(manifest_by_path)

        working_files: dict[str, FileEntry] = {}
        for file_entry in walk_files(self.root):
            working_files[file_entry.relative_path] = file_entry
        working_paths = set(working_files)

        added_paths: list[str] = []
        removed_paths: list[str] = []
        modified_paths: list[str] = []

        for path in sorted(working_paths - manifest_paths):
            added_paths.append(path)

        for path in sorted(manifest_paths - working_paths):
            removed_paths.append(path)

        for path in sorted(working_paths & manifest_paths):
            manifest_entry = manifest_by_path[path]
            working_file = working_files[path]
            if working_file.size != manifest_entry.size:
                modified_paths.append(path)
                continue
            if (
                self.trust_mtime
                and working_file.mtime_ns == manifest_entry.mtime_ns
            ):
                continue
            current_hash = blake3_digest_file(working_file.path)
            if current_hash != manifest_entry.hash:
                modified_paths.append(path)

        return added_paths, removed_paths, modified_paths
