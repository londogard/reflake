from __future__ import annotations

from pathlib import Path
from typing import Any, BinaryIO, Iterator, Literal

from blake3 import blake3

from .client_state import LocalClientState
from .config import BaseConfig, S3Config
from .hashing import blake3_digest_file
from .layout import initialize_reflake_layout
from .manifest import ManifestEntry
from .objects import (
    LocalObjectStore,
    ObjectStore,
    S3ObjectStore,
)
from .repository_support import merge_base_commit, matches_logical_path
from .services.entries import EntryFactory
from .services.refs import RefManager
from .services.staging import StagingArea
from .services.tree import TreeWriter
from .objects import (
    BlobTransferBackend,
    build_blob_transfer_backend,
    build_s3_client,
    parse_s3_uri,
)

from .domain import (
    CommitObject,
    DiffEntry,
    EmptyBranchError,
    GcResult,
    MergeResult,
    MoveResult,
    NotARepositoryError,
    RefConflictError,
    RemoveResult,
    StageChange,
    StageStatus,
    VerifyResult,
)


class ReflakeRepository:
    def __init__(
        self,
        root: str | Path,
        *,
        store: ObjectStore | None = None,
        client_state: LocalClientState | None = None,
        blob_transfer: BlobTransferBackend | None = None,
    ) -> None:
        is_remote_store = isinstance(store, S3ObjectStore)
        self.layout = initialize_reflake_layout(root, create_dirs=not is_remote_store)
        config = BaseConfig.load(root)
        if config is not None:
            config.validate()
        self.store = store or LocalObjectStore(self.layout.root)
        if isinstance(self.store, S3ObjectStore) and blob_transfer is not None:
            self.store = S3ObjectStore(
                self.store.bucket,
                self.store.prefix,
                client=self.store.client,
                branch_root=self.store.branch_root,
                blob_transfer=blob_transfer,
            )
        self.client_state = client_state or LocalClientState(self.layout.root)
        self.refs = RefManager(store=self.store, client_state=self.client_state)
        self.tree_writer = TreeWriter(
            store=self.store,
            refs=self.refs,
            client_state=self.client_state,
        )
        self.entries = EntryFactory(
            root=self.layout.root,
            store=self.store,
            capture_footers=bool(config.parquet_footer) if config else False,
        )
        self.staging = StagingArea(
            client_state=self.client_state,
            root=self.layout.root,
            store=self.store,
            refs=self.refs,
        )

    @property
    def root(self) -> Path:
        return self.layout.root

    def current_branch(self) -> str:
        return self.refs.current_branch()

    def set_current_branch(self, branch: str) -> None:
        self.refs.set_current_branch(branch)

    def head_commit(self) -> str | None:
        return self.refs.head_commit()

    def resolve_ref(self, branch_or_commit: str) -> str:
        return self.refs.resolve_ref(branch_or_commit)

    def branch(self, name: str) -> Path:
        return self.refs.branch(name)

    def merge(self, source_ref: str, target_ref: str) -> MergeResult:
        if not source_ref:
            raise ValueError("Source ref cannot be empty")
        if not target_ref:
            raise ValueError("Target ref cannot be empty")

        source_commit = self.refs.resolve_ref(source_ref)
        target_commit = self.refs.resolve_ref(target_ref)

        if source_commit == target_commit:
            return MergeResult(
                source_ref=source_ref,
                target_ref=target_ref,
                commit_id=target_commit,
                updated=False,
            )
        # Target already contains source: nothing to do.
        if self.refs.is_ancestor(
            ancestor_commit=source_commit,
            descendant_commit=target_commit,
        ):
            return MergeResult(
                source_ref=source_ref,
                target_ref=target_ref,
                commit_id=target_commit,
                updated=False,
            )
        # Source contains target: fast-forward.
        if self.refs.is_ancestor(
            ancestor_commit=target_commit,
            descendant_commit=source_commit,
        ):
            self.refs.fast_forward_branch(
                target_ref, source_commit, operation="merge"
            )
            return MergeResult(
                source_ref=source_ref,
                target_ref=target_ref,
                commit_id=source_commit,
                updated=True,
            )

        # Diverged: metadata-only 3-way merge (docs/architecture.md §9).
        base = self._merge_base(source_commit, target_commit)
        target_obj = self.refs.read_commit(target_commit)
        source_obj = self.refs.read_commit(source_commit)
        merged_tree = self.tree_writer.merge_trees(
            base.tree if base is not None else None,
            target_obj.tree,
            source_obj.tree,
        )
        branch_state = self.refs.require_branch_state(target_ref)
        commit_id = self.tree_writer.write_commit_object(
            branch=target_ref,
            message=f"merge {source_ref} into {target_ref}",
            parents=[target_commit, source_commit],
            tree_hash=merged_tree,
            expected_version_token=branch_state.version_token,
            operation="merge",
        )
        return MergeResult(
            source_ref=source_ref,
            target_ref=target_ref,
            commit_id=commit_id,
            updated=True,
        )

    def _merge_base(self, commit_a: str, commit_b: str) -> CommitObject | None:
        """Deepest common ancestor of two commits (merge-aware DAG walk)."""
        return merge_base_commit(commit_a, commit_b, read_commit=self.refs.read_commit)

    def fast_forward_branch(
        self,
        branch: str,
        target_commit: str,
        *,
        operation: str,
    ) -> bool:
        return self.refs.fast_forward_branch(branch, target_commit, operation=operation)

    # ── Core operations ─────────────────────────────────────────────────
    #: Retry budget for `commit --staged` when a peer advanced the branch
    #: (CAS conflict).  With trees, retry = re-apply the overlay onto the new
    #: parent, O(changes) — see docs/architecture.md §6.
    _STAGED_COMMIT_RETRIES = 3

    def commit(
        self,
        message: str,
        *,
        staged_only: bool = False,
    ) -> str:
        if not message.strip():
            raise ValueError("Commit message cannot be empty")

        config = BaseConfig.load(self.root)
        identity_mode = config.identity if config else "blake3"
        if identity_mode not in ("blake3", "meta"):
            identity_mode = "blake3"
        capture_footers = bool(config.parquet_footer) if config else False

        branch = self.current_branch()
        self.refs.ensure_branch_exists(branch)

        staged = self.staging.load(branch)
        staged_removed_paths: set[str] = {
            p for p, c in staged.items() if c.action == "remove"
        }
        staged_additions: dict[str, StageChange] = {
            p: c for p, c in staged.items() if c.action == "add"
        }

        # Materialize staged additions once (hashing + blob storage is
        # idempotent).  They participate in both modes: staged commits
        # overlay them onto the parent; full commits treat them as part of
        # the effective parent (v1 semantics — staged adds survive a full
        # commit even when the source file is gone).
        additions: list[ManifestEntry] = []
        if staged_additions:
            additions = [
                self.entries.entry_from_stage_change(
                    change,
                    change.identity_mode or identity_mode,
                    store_blob=True,
                )
                for change in staged_additions.values()
            ]
            additions.sort(key=lambda entry: entry.path)

        retries = self._STAGED_COMMIT_RETRIES if staged_only else 1
        commit_id: str | None = None
        for attempt in range(retries):
            # Ref advancement is a mutation boundary: use the client-side
            # snapshot as the expected state. If another client advanced
            # the branch since our last interaction, the stale snapshot
            # causes a RefConflictError, and staged commits re-apply onto
            # the new parent and retry.
            branch_state = self.refs.require_branch_state(branch)
            parent_commit = branch_state.commit_id

            parent_tree: str | None = None
            if parent_commit:
                parent = self.refs.read_commit(parent_commit)
                parent_tree = parent.tree

            if staged_only:
                if parent_tree is None:
                    root_tree = self.tree_writer.build_from_entries(iter(additions))
                else:
                    root_tree = self.tree_writer.overlay_staged(
                        parent_tree=parent_tree,
                        additions=additions,
                        removed_prefixes=staged_removed_paths,
                    )
            else:
                root_tree = self.tree_writer.build_worktree_tree(
                    worktree_root=self.layout.root,
                    parent_tree=parent_tree,
                    identity_mode=identity_mode,
                    removed_paths=staged_removed_paths,
                    staged_additions=additions or None,
                    capture_footers=capture_footers,
                )

            try:
                commit_id = self.tree_writer.write_commit_object(
                    branch=branch,
                    message=message,
                    parent_commit=parent_commit,
                    tree_hash=root_tree,
                    expected_version_token=branch_state.version_token,
                    operation="commit",
                )
                break
            except RefConflictError:
                if attempt == retries - 1:
                    raise
                continue

        if commit_id is None:
            raise RefConflictError(
                branch=branch,
                operation="commit",
                expected_commit_id=None,
                current_commit_id=None,
            )
        self.staging.save(branch, {})
        return commit_id

    def import_s3(
        self,
        source_uri: str,
        message: str,
        identity_mode: Literal["blake3", "meta"] = "blake3",
        *,
        path_patterns: list[str] | None = None,
        ref: str | None = None,
    ) -> str:
        from .repository_ops import repo_import_s3

        return repo_import_s3(
            self,
            source_uri,
            message,
            identity_mode,
            path_patterns=path_patterns,
            ref=ref,
        )

    def add(
        self,
        paths: list[str],
        *,
        ref: str | None = None,
        identity_mode: str = "blake3",
        destination_path: str | None = None,
    ) -> StageStatus:
        from .repository_ops import repo_add

        return repo_add(
            self,
            paths,
            ref=ref,
            identity_mode=identity_mode,
            destination_path=destination_path,
        )

    def rm(self, paths: list[str], *, ref: str | None = None) -> StageStatus:
        from .repository_ops import repo_rm

        return repo_rm(self, paths, ref=ref)

    def remove_paths(
        self,
        paths: list[str],
        message: str,
        *,
        ref: str | None = None,
    ) -> RemoveResult:
        from .repository_ops import repo_remove_paths

        return repo_remove_paths(self, paths, message, ref=ref)

    def move(
        self,
        source_path: str,
        destination_path: str,
        message: str,
        *,
        ref: str | None = None,
    ) -> MoveResult:
        from .repository_ops import repo_move

        return repo_move(self, source_path, destination_path, message, ref=ref)

    def move_staged(
        self,
        source_path: str,
        destination_path: str,
        *,
        ref: str | None = None,
    ) -> StageStatus:
        """Stage a move operation without committing."""
        from .repository_ops import repo_move_staged

        return repo_move_staged(self, source_path, destination_path, ref=ref)

    def status(
        self, *, ref: str | None = None, working_tree: bool = False
    ) -> StageStatus:
        return self.staging.status(ref=ref, working_tree=working_tree)

    def read_commit(self, commit_id: str) -> CommitObject:
        return self.refs.read_commit(commit_id)

    def log(self, ref: str) -> Iterator[CommitObject]:
        try:
            commit_id = self.refs.resolve_ref(ref)
        except EmptyBranchError:
            return

        while commit_id:
            commit = self.refs.read_commit(commit_id)
            yield commit
            commit_id = commit.first_parent

    def diff(self, from_ref: str, to_ref: str) -> list[DiffEntry]:
        from_commit = self.refs.read_commit(self.refs.resolve_ref(from_ref))
        to_commit = self.refs.read_commit(self.refs.resolve_ref(to_ref))

        from_iter = self.store.iter_all_entries(from_commit.tree)
        to_iter = self.store.iter_all_entries(to_commit.tree)

        changes: list[DiffEntry] = []
        from_entry = next(from_iter, None)
        to_entry = next(to_iter, None)

        while from_entry is not None or to_entry is not None:
            if from_entry is None:
                changes.append(
                    DiffEntry(
                        path=to_entry.path,  # type: ignore[union-attr]
                        change="added",
                        before_hash=None,
                        after_hash=to_entry.hash,  # type: ignore[union-attr]
                        before_size=None,
                        after_size=to_entry.size,  # type: ignore[union-attr]
                    )
                )
                to_entry = next(to_iter, None)
            elif to_entry is None:
                changes.append(
                    DiffEntry(
                        path=from_entry.path,
                        change="removed",
                        before_hash=from_entry.hash,
                        after_hash=None,
                        before_size=from_entry.size,
                        after_size=None,
                    )
                )
                from_entry = next(from_iter, None)
            elif from_entry.path == to_entry.path:
                if from_entry.hash != to_entry.hash or from_entry.size != to_entry.size:
                    changes.append(
                        DiffEntry(
                            path=from_entry.path,
                            change="modified",
                            before_hash=from_entry.hash,
                            after_hash=to_entry.hash,
                            before_size=from_entry.size,
                            after_size=to_entry.size,
                        )
                    )
                from_entry = next(from_iter, None)
                to_entry = next(to_iter, None)
            elif from_entry.path < to_entry.path:
                changes.append(
                    DiffEntry(
                        path=from_entry.path,
                        change="removed",
                        before_hash=from_entry.hash,
                        after_hash=None,
                        before_size=from_entry.size,
                        after_size=None,
                    )
                )
                from_entry = next(from_iter, None)
            else:
                changes.append(
                    DiffEntry(
                        path=to_entry.path,
                        change="added",
                        before_hash=None,
                        after_hash=to_entry.hash,
                        before_size=None,
                        after_size=to_entry.size,
                    )
                )
                to_entry = next(to_iter, None)

        return changes

    def verify(
        self,
        ref: str | None = None,
        path_prefixes: list[str] | None = None,
        *,
        dry_run: bool = False,
    ) -> VerifyResult:
        from .repository_ops import repo_verify

        return repo_verify(self, ref, path_prefixes, dry_run=dry_run)

    def _tree_entries(self, tree_hash: str) -> dict[str, ManifestEntry]:
        index: dict[str, ManifestEntry] = {}
        for entry in self.store.iter_all_entries(tree_hash):
            index[entry.path] = entry
        return index

    def resolve_entries(
        self, ref: str, *, include_staging: bool = False
    ) -> dict[str, ManifestEntry]:
        commit = self.refs.read_commit(self.refs.resolve_ref(ref))
        index = self._tree_entries(commit.tree)
        if include_staging:
            for change in self.staging.load(ref).values():
                if change.action == "remove":
                    index.pop(change.path, None)
                    continue
                if change.action == "add":
                    index[change.path] = self.entries.entry_from_stage_change(
                        change,
                        change.identity_mode or "blake3",
                        store_blob=False,
                    )
        return index

    def resolve_entries_for_prefix(
        self,
        ref: str,
        logical_prefix: str,
        *,
        include_staging: bool = False,
        commit_id: str | None = None,
    ) -> dict[str, ManifestEntry]:
        normalized_prefix = logical_prefix.strip("/")
        resolved_commit_id = commit_id or self.refs.resolve_ref(ref)
        commit = self.refs.read_commit(resolved_commit_id)

        if not normalized_prefix:
            index = self._tree_entries(commit.tree)
        else:
            index = {
                entry.path: entry
                for entry in self.store.iter_entries_for_prefix(
                    commit.tree,
                    normalized_prefix,
                )
            }

        if include_staging:
            for change in self.staging.load(ref).values():
                if normalized_prefix and not matches_logical_path(
                    change.path,
                    normalized_prefix,
                ):
                    continue
                if change.action == "remove":
                    index.pop(change.path, None)
                    continue
                if change.action == "add":
                    index[change.path] = self.entries.entry_from_stage_change(
                        change,
                        change.identity_mode or "blake3",
                        store_blob=False,
                    )
        return index

    def resolve_entry(
        self,
        ref: str,
        logical_path: str,
        *,
        include_staging: bool = False,
        commit_id: str | None = None,
    ) -> ManifestEntry | None:
        normalized_path = logical_path.strip("/")
        if not normalized_path:
            return None

        if include_staging:
            change = self.staging.load(ref).get(normalized_path)
            if change is not None:
                if change.action == "remove":
                    return None
                if change.action == "add":
                    return self.entries.entry_from_stage_change(
                        change,
                        change.identity_mode or "blake3",
                        store_blob=False,
                    )

        resolved_commit_id = commit_id or self.refs.resolve_ref(ref)
        commit = self.refs.read_commit(resolved_commit_id)
        return self.store.lookup_entry(commit.tree, normalized_path)

    def read_blob(self, blob_hash: str) -> bytes:
        return self.store.read_blob_bytes(blob_hash)

    def open_blob_stream(self, blob_hash: str) -> BinaryIO:
        return self.store.open_blob(blob_hash)

    def restore_files(
        self,
        ref: str,
        paths: list[str] | None = None,
        *,
        force: bool = False,
    ) -> list[str]:
        from .repository_ops import repo_restore_files

        return repo_restore_files(self, ref, paths, force=force)

    def generate_transfer_commands(
        self,
        ref: str | None = None,
        *,
        mode: str = "upload",
        include_metadata: bool = False,
    ) -> list[str]:
        from .repository_ops import repo_generate_transfer_commands

        return repo_generate_transfer_commands(
            self,
            ref,
            mode=mode,
            include_metadata=include_metadata,
        )

    def gc(self, *, dry_run: bool = True) -> GcResult:
        """Compute reachable objects from all refs and report (or prune) orphans.

        Audit-only by default (docs/architecture.md §11): reachable set = the
        commit DAG from every branch head, their tree DAGs, and the blob and
        footer objects those trees reference.  ``dry_run=False`` deletes the
        orphans.
        """
        reachable_commits: set[str] = set()
        reachable_trees: set[str] = set()
        reachable_blobs: set[str] = set()
        reachable_footers: set[str] = set()

        for branch in self.store.iter_branches():
            state = self.store.read_branch_ref(branch)
            commit_id = state.commit_id if state else None
            while commit_id and commit_id not in reachable_commits:
                reachable_commits.add(commit_id)
                commit = self.refs.read_commit(commit_id)
                for tree_hash in self.tree_writer.iter_tree_hashes(commit.tree):
                    reachable_trees.add(tree_hash)
                for blob_hash, footer_hash in self.tree_writer.iter_leaf_refs(
                    commit.tree
                ):
                    if blob_hash:
                        reachable_blobs.add(blob_hash)
                    if footer_hash:
                        reachable_footers.add(footer_hash)
                commit_id = commit.first_parent

        counts: dict[str, tuple[int, int]] = {}
        pruned_any = False
        for kind, reachable in (
            ("commit", reachable_commits),
            ("tree", reachable_trees),
            ("blob", reachable_blobs),
            ("footer", reachable_footers),
        ):
            stored = set(self.store.iter_object_ids(kind))
            orphans = stored - reachable
            counts[kind] = (len(stored), len(orphans))
            if not dry_run and orphans:
                pruned_any = True
                for object_id in orphans:
                    self.store.delete_object(kind, object_id)

        return GcResult(
            reachable_commits=len(reachable_commits),
            reachable_trees=len(reachable_trees),
            reachable_blobs=len(reachable_blobs),
            reachable_footers=len(reachable_footers),
            orphan_commits=counts["commit"][1],
            orphan_trees=counts["tree"][1],
            orphan_blobs=counts["blob"][1],
            orphan_footers=counts["footer"][1],
            pruned=pruned_any,
        )

    def cat(self, ref: str, path: str) -> bytes:
        entry = self.resolve_entry(ref, path)
        if entry is None:
            raise FileNotFoundError(f"Path not found in ref '{ref}': {path}")
        if entry.blob_hash:
            return self.read_blob(entry.blob_hash)
        if entry.source_uri:
            from .objects import open_source_uri

            with open_source_uri(entry.source_uri) as handle:
                return handle.read()
        raise FileNotFoundError(f"Entry has no readable content: {path}")

    def reflog(self, branch: str | None = None) -> Iterator[str]:
        branch_name = branch or self.current_branch()
        yield from self.client_state.iter_reflog(branch_name)

    def catalog(self) -> list[dict[str, Any]]:
        datasets: list[dict[str, Any]] = []
        for branch_name in sorted(self.store.iter_branches()):
            state = self.store.read_branch_ref(branch_name)
            commit_id = state.commit_id if state else None
            message = None
            if commit_id:
                message = self.read_commit(commit_id).message
            datasets.append(
                {"branch": branch_name, "commit_id": commit_id, "message": message}
            )
        return datasets


def _default_remote_client_root(worktree_root: Path, repo_uri: str) -> Path:
    repo_id = blake3(repo_uri.encode("utf-8")).hexdigest()[:16]
    return worktree_root / ".reflake" / "clients" / repo_id


def _find_repo_root(start: str | Path, *, must_exist: bool = False) -> Path:
    """Walk up from start looking for a .reflake directory."""
    current = Path(start).resolve()
    while True:
        if (current / ".reflake").is_dir():
            return current
        parent = current.parent
        if parent == current:
            if must_exist:
                raise NotARepositoryError(start)
            return Path(start).resolve()
        current = parent


def open_repository(
    root: str | Path,
    *,
    worktree: str | Path | None = None,
    client_root: str | Path | None = None,
    s3_client: object | None = None,
    s3_endpoint: str | None = None,
    blob_transfer: BlobTransferBackend | str | None = None,
    must_exist: bool = False,
) -> ReflakeRepository:
    if isinstance(root, str) and root.startswith("s3://"):
        bucket, prefix = parse_s3_uri(root)
        worktree_root = Path(worktree or ".").resolve()
        resolved_client_root = (
            Path(client_root).resolve()
            if client_root
            else _default_remote_client_root(worktree_root, root)
        )
        store_client = s3_client if s3_client is not None else build_s3_client(s3_endpoint)
        if isinstance(blob_transfer, str):
            blob_transfer = build_blob_transfer_backend(
                blob_transfer, endpoint_url=s3_endpoint
            )
        return ReflakeRepository(
            worktree_root,
            store=S3ObjectStore(
                bucket,
                prefix,
                client=store_client,
                branch_root=worktree_root / ".reflake" / "refs" / "heads",
                blob_transfer=blob_transfer,
            ),
            client_state=LocalClientState(resolved_client_root),
        )

    if root == "." or str(root) == ".":
        cwd = Path(".").resolve()
        if not (cwd / ".reflake").is_dir():
            root = _find_repo_root(".", must_exist=must_exist)
        else:
            root = str(cwd)
    repo_root = Path(root).resolve()
    if must_exist and not (repo_root / ".reflake").is_dir():
        raise NotARepositoryError(repo_root)

    config = BaseConfig.load(repo_root)

    endpoint = s3_endpoint
    if endpoint is None and isinstance(config, S3Config):
        endpoint = config.endpoint_url

    if blob_transfer is None and config is not None and config.transfer_backend:
        blob_transfer = build_blob_transfer_backend(
            config.transfer_backend, endpoint_url=endpoint
        )
    elif isinstance(blob_transfer, str):
        blob_transfer = build_blob_transfer_backend(blob_transfer, endpoint_url=endpoint)

    if isinstance(config, S3Config):
        bucket = config.bucket
        prefix = config.prefix
        worktree_root = Path(worktree or repo_root)
        resolved_client_root = (
            Path(client_root).resolve() if client_root else worktree_root
        )
        store_client = s3_client if s3_client is not None else build_s3_client(endpoint)
        return ReflakeRepository(
            worktree_root,
            store=S3ObjectStore(
                bucket,
                prefix,
                client=store_client,
                branch_root=worktree_root / ".reflake" / "refs" / "heads",
                blob_transfer=blob_transfer,
            ),
            client_state=LocalClientState(resolved_client_root),
        )

    worktree_root = Path(worktree).resolve() if worktree else repo_root
    resolved_client_root = Path(client_root).resolve() if client_root else repo_root
    return ReflakeRepository(
        worktree_root,
        store=LocalObjectStore(repo_root),
        client_state=LocalClientState(resolved_client_root),
    )
