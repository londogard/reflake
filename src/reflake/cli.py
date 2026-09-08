from __future__ import annotations

import json
import shlex
import sys
from dataclasses import asdict, dataclass
from importlib import metadata
from typing import Any, Literal

import msgspec
from simple_parsing import ArgumentParser
from simple_parsing.helpers import field, flag, subparsers

from .core import (
    EmptyBranchError,
    MergeConflictError,
    NonFastForwardError,
    NotARepositoryError,
    OptimisticLockError,
    PreconditionFailedError,
    RefConflictError,
    ReflakeError,
    ReflakeRepository,
    StageStatus,
    UnknownCommitError,
    UnknownRefError,
    build_analytical_index,
    open_repository,
    parse_where_clause,
    plan_pruned_scan,
)
from .core.config import (
    BaseConfig,
    LocalConfig,
    ReflakeConfig,
    S3Config,
    init_config,
)

IdentityMode = Literal["content", "pointer"]
HANDLED_CLI_ERRORS = (
    ReflakeError,
    FileNotFoundError,
    PermissionError,
    ValueError,
)

#: Retryable concurrency conflicts (safe to retry after pull/merge).
_RETRYABLE_ERRORS = (
    RefConflictError,
    NonFastForwardError,
    MergeConflictError,
    OptimisticLockError,
    PreconditionFailedError,
)

#: Missing objects or refs.
_MISSING_ERRORS = (
    UnknownRefError,
    UnknownCommitError,
    EmptyBranchError,
    FileNotFoundError,
)


def _error_exit_code(error: BaseException) -> int:
    """Exit-code contract: 0 ok · 1 usage/validation · 2 conflict · 3 missing."""
    if isinstance(error, _RETRYABLE_ERRORS):
        return 2
    if isinstance(error, _MISSING_ERRORS):
        return 3
    return 1


@dataclass
class CommitArgs:
    message: str = field(alias=["-m", "--message"], help="Commit message")
    staged_only: bool = flag(
        False,
        alias=["--staged", "--staged-only"],
        help="Commit only staged changes, ignoring the working tree",
    )


@dataclass
class AddArgs:
    paths: list[str] = field(
        positional=True, nargs="+", help="Files, directories, or S3 paths to stage"
    )
    identity: IdentityMode = "content"  # Identity strategy for staged additions
    destination_path: str | None = field(
        default=None,
        alias="--as",
        help="Logical destination path for a single staged source",
    )


@dataclass
class RmArgs:
    paths: list[str] = field(positional=True, nargs="+", help="Paths to remove")


@dataclass
class MoveArgs:
    source_path: str = field(positional=True, help="Path or prefix to rename")
    destination_path: str = field(
        positional=True,
        help="Destination path or prefix",
    )


@dataclass
class StatusArgs:
    pass


@dataclass
class BranchArgs:
    name: str = field(positional=True, help="Branch name")


@dataclass
class LogArgs:
    ref: str | None = field(
        default=None,
        positional=True,
        nargs="?",
        help="Ref (branch or commit) to show log for (defaults to current branch)",
    )


@dataclass
class ListArgs:
    path: str = field(
        default="",
        positional=True,
        nargs="?",
        help="Logical path or prefix to list (default: root)",
    )


@dataclass
class CatArgs:
    ref: str = field(
        positional=True,
        help="Ref (branch or commit) to read from",
    )
    path: str = field(
        positional=True,
        help="Logical file path to print",
    )


@dataclass
class ReflogArgs:
    branch: str | None = field(
        default=None,
        positional=True,
        nargs="?",
        help="Branch to inspect (default: current branch)",
    )


@dataclass
class BranchesArgs:
    pass


@dataclass
class CheckoutArgs:
    name: str = field(
        positional=True,
        help="Branch name to switch to",
    )


@dataclass
class RestoreArgs:
    ref: str = field(
        positional=True,
        help="Ref (branch or commit) to restore files from",
    )
    path: list[str] = field(
        default_factory=list,
        alias="--path",
        action="append",
        help="File paths to restore (repeatable, default: all files in ref)",
    )
    force: bool = flag(
        False,
        alias="--force",
        help="Allow overwriting existing files",
    )


@dataclass
class ConfigInitArgs:
    backend: str = field(
        default="local",
        alias="--backend",
        help="Backend type (local or s3)",
    )
    s3_bucket: str | None = field(
        default=None,
        alias="--s3-bucket",
        help="S3 bucket name (required for s3 backend)",
    )
    s3_prefix: str | None = field(
        default=None,
        alias="--s3-prefix",
        help="S3 key prefix",
    )
    s3_endpoint: str | None = field(
        default=None,
        alias="--s3-endpoint",
        help="S3 endpoint URL",
    )
    default_branch: str = field(
        default="main",
        alias="--default-branch",
        help="Default branch name",
    )


@dataclass
class ConfigSetArgs:
    key: str = field(
        positional=True, help="Config key (e.g. backend, dataset_root, s3.bucket)"
    )
    value: str = field(positional=True, help="Config value")


@dataclass
class ConfigListArgs:
    pass


@dataclass
class InitArgs:
    backend: str = field(
        default="local",
        alias="--backend",
        help="Backend type (local or s3)",
    )
    s3_bucket: str | None = field(
        default=None,
        alias="--s3-bucket",
        help="S3 bucket name (required for s3 backend)",
    )
    s3_prefix: str | None = field(
        default=None,
        alias="--s3-prefix",
        help="S3 key prefix",
    )
    s3_endpoint: str | None = field(
        default=None,
        alias="--s3-endpoint",
        help="S3 endpoint URL",
    )
    default_branch: str = field(
        default="main",
        alias="--default-branch",
        help="Default branch name",
    )


@dataclass
class ConfigArgs:
    command: ConfigInitArgs | ConfigSetArgs | ConfigListArgs = subparsers(
        {
            "init": ConfigInitArgs,
            "set": ConfigSetArgs,
            "list": ConfigListArgs,
        }
    )


@dataclass
class PushArgs:
    remote: str = field(positional=True, help="Remote S3 URI (s3://bucket/prefix)")
    ref: str | None = field(
        default=None,
        alias="--ref",
        help="Branch to push (default: current branch)",
    )
    transfer_backend: str = field(
        default="boto3",
        alias="--transfer-backend",
        help="Blob transfer backend: boto3 (default) or s5cmd",
    )


@dataclass
class PullArgs:
    remote: str = field(positional=True, help="Remote S3 URI (s3://bucket/prefix)")
    ref: str | None = field(
        default=None,
        alias="--ref",
        help="Branch to pull (default: current branch)",
    )
    transfer_backend: str = field(
        default="boto3",
        alias="--transfer-backend",
        help="Blob transfer backend: boto3 (default) or s5cmd",
    )


@dataclass
class FetchArgs:
    remote: str = field(positional=True, help="Remote S3 URI (s3://bucket/prefix)")
    ref: str | None = field(
        default=None,
        alias="--ref",
        help="Branch to fetch (default: current branch)",
    )
    transfer_backend: str = field(
        default="boto3",
        alias="--transfer-backend",
        help="Blob transfer backend: boto3 (default) or s5cmd",
    )


@dataclass
class TransferArgs:
    direction: str = field(
        default="upload",
        positional=True,
        help="Transfer direction: upload (local->S3) or download (S3->local)",
    )
    execute: bool = flag(
        False, alias="--execute", help="Execute generated s5cmd/aws commands"
    )
    ref: str | None = field(
        default=None, alias="--ref", help="Ref to transfer (default: current branch)"
    )


@dataclass
class DiffArgs:
    from_ref: str = field(positional=True, help="Source ref (branch or commit)")
    to_ref: str = field(positional=True, help="Target ref (branch or commit)")


@dataclass
class MergeArgs:
    source_ref: str = field(positional=True, help="Ref to merge from")
    target_ref: str = field(positional=True, help="Branch ref to fast-forward")


@dataclass
class VerifyArgs:
    path: list[str] = field(
        default_factory=list,
        alias="--path",
        action="append",
        help="Optional path/prefix filter (repeatable)",
    )


@dataclass
class PromoteArgs:
    path: list[str] = field(
        default_factory=list,
        alias="--path",
        action="append",
        help="Optional path/prefix filter (repeatable)",
    )


@dataclass
class GcArgs:
    prune: bool = flag(
        False,
        alias="--prune",
        help="Delete orphaned objects (default: audit-only dry run)",
    )


@dataclass
class QueryBuildArgs:
    output_dir: str | None = field(
        default=None,
        alias="--output-dir",
        help="Output directory for index",
    )
    parquet: bool = flag(
        False,
        help="Also export Parquet from the index table",
    )


@dataclass
class QueryPruneArgs:
    ref: str = field(positional=True, help="Ref (branch or commit) to scan")
    path: str = field(positional=True, help="Path or prefix within the ref")
    where: str = field(
        alias="--where",
        help='Pruning predicate, e.g. "id >= 100 AND active = true"',
    )


@dataclass
class QueryArgs:
    command: QueryBuildArgs | QueryPruneArgs = subparsers(
        {
            "build": QueryBuildArgs,
            "prune": QueryPruneArgs,
        }
    )


@dataclass
class ReflakeCLI:
    command: (
        CommitArgs
        | AddArgs
        | RmArgs
        | MoveArgs
        | StatusArgs
        | BranchArgs
        | DiffArgs
        | MergeArgs
        | VerifyArgs
        | PromoteArgs
        | GcArgs
        | QueryArgs
        | LogArgs
        | ListArgs
        | CatArgs
        | ReflogArgs
        | BranchesArgs
        | CheckoutArgs
        | RestoreArgs
        | InitArgs
        | ConfigArgs
        | PushArgs
        | PullArgs
        | FetchArgs
        | TransferArgs
    ) = subparsers(
        {
            "commit": CommitArgs,
            "add": AddArgs,
            "rm": RmArgs,
            "mv": MoveArgs,
            "status": StatusArgs,
            "branch": BranchArgs,
            "diff": DiffArgs,
            "merge": MergeArgs,
            "verify": VerifyArgs,
            "promote": PromoteArgs,
            "gc": GcArgs,
            "query": QueryArgs,
            "log": LogArgs,
            "list": ListArgs,
            "ls": ListArgs,
            "cat": CatArgs,
            "reflog": ReflogArgs,
            "branches": BranchesArgs,
            "checkout": CheckoutArgs,
            "restore": RestoreArgs,
            "init": InitArgs,
            "config": ConfigArgs,
            "push": PushArgs,
            "pull": PullArgs,
            "fetch": FetchArgs,
            "transfer": TransferArgs,
        }
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI (global, before the command)",
    )
    json: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias="--json",
        # store_true (not simple-parsing's bool-action): a global flag must
        # never consume the following command word as a value.
        action="store_true",
        help="Print result as structured JSON (global, before the command)",
    )


def build_parser() -> _CleanParser:
    parser = _CleanParser(
        prog="reflake",
        description="Serverless object-storage-first data versioning engine.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"reflake {metadata.version('reflake')}",
        help="Show version and exit",
    )
    parser.add_arguments(ReflakeCLI, dest="cli")
    return parser


class _CleanParser(ArgumentParser):
    """ArgumentParser that suppresses simple-parsing internal group headers."""

    def format_help(self):
        import re

        text = super().format_help()
        text = re.sub(
            r"ReflakeCLI \['cli'\]:\n  ReflakeCLI\(command: '[^)]+'\)\n?",
            "",
            text,
        )
        return text


def _print_status(stage: StageStatus) -> None:
    if (
        stage.working_tree_added
        or stage.working_tree_removed
        or stage.working_tree_modified
    ):
        if stage.working_tree_added:
            print("Working tree changes:")
            for path in stage.working_tree_added:
                print(f"  added:    {path}")
        if stage.working_tree_modified:
            for path in stage.working_tree_modified:
                print(f"  modified: {path}")
        if stage.working_tree_removed:
            for path in stage.working_tree_removed:
                print(f"  removed:  {path}")
        print()

    if stage.added or stage.removed:
        print("Staged changes:")
        for path in stage.added:
            print(f"  added:    {path}")
        for path in stage.removed:
            print(f"  removed:  {path}")
    elif (
        not stage.working_tree_added
        and not stage.working_tree_removed
        and not stage.working_tree_modified
    ):
        print("Nothing to commit, working tree clean")


def _stage_payload(stage: StageStatus) -> dict[str, object]:
    return {
        "ref": stage.ref,
        "added": stage.added,
        "removed": stage.removed,
        "modified": stage.modified,
        "working_tree_added": stage.working_tree_added,
        "working_tree_removed": stage.working_tree_removed,
        "working_tree_modified": stage.working_tree_modified,
    }


def _flatten_option_values(values: list[Any]) -> list[str]:
    flattened: list[str] = []
    for value in values:
        if isinstance(value, list):
            flattened.extend(str(item) for item in value)
            continue
        flattened.append(str(value))
    return flattened


def _config_set_value(config: ReflakeConfig, key: str, value: str) -> ReflakeConfig:
    if key == "backend":
        if value not in ("local", "s3"):
            raise ValueError(f"Backend must be 'local' or 's3', got: {value}")
        if value == "s3":
            bucket = config.bucket if isinstance(config, S3Config) else ""
            try:
                return S3Config(
                    dataset_root=config.dataset_root,
                    default_branch=config.default_branch,
                    bucket=bucket,
                )
            except ValueError as error:
                raise ValueError(
                    "Cannot switch to S3 backend without a bucket. Set s3.bucket first."
                ) from error
        return LocalConfig(
            dataset_root=config.dataset_root,
            default_branch=config.default_branch,
        )
    elif key == "dataset_root":
        config.dataset_root = value
    elif key == "default_branch":
        config.default_branch = value
    elif key == "identity":
        if value not in ("content", "pointer"):
            raise ValueError(f"identity must be 'content' or 'pointer', got: {value}")
        config.identity = value
    elif key == "transfer_backend":
        config.transfer_backend = value if value else None
    elif key == "parquet_footer":
        if value.strip().lower() in ("true", "1", "yes", "on"):
            config.parquet_footer = True
        elif value.strip().lower() in ("false", "0", "no", "off"):
            config.parquet_footer = False
        else:
            raise ValueError(f"parquet_footer must be a boolean, got: {value}")
    elif key == "s3.bucket":
        if isinstance(config, LocalConfig):
            return S3Config(
                dataset_root=config.dataset_root,
                default_branch=config.default_branch,
                bucket=value,
            )
        config.bucket = value
    elif key == "s3.prefix":
        if not isinstance(config, S3Config):
            raise ValueError("Cannot set s3.prefix on a local config")
        config.prefix = value
    elif key == "s3.endpoint_url":
        if not isinstance(config, S3Config):
            raise ValueError("Cannot set s3.endpoint_url on a local config")
        config.endpoint_url = value
    else:
        raise ValueError(f"Unknown config key: {key}")
    return config


def _command_name(command: object) -> str:
    if isinstance(command, CommitArgs):
        return "commit"
    if isinstance(command, AddArgs):
        return "add"
    if isinstance(command, RmArgs):
        return "rm"
    if isinstance(command, MoveArgs):
        return "mv"
    if isinstance(command, StatusArgs):
        return "status"
    if isinstance(command, BranchArgs):
        return "branch"
    if isinstance(command, DiffArgs):
        return "diff"
    if isinstance(command, MergeArgs):
        return "merge"
    if isinstance(command, VerifyArgs):
        return "verify"
    if isinstance(command, PromoteArgs):
        return "promote"
    if isinstance(command, LogArgs):
        return "log"
    if isinstance(command, ListArgs):
        return "list"
    if isinstance(command, CatArgs):
        return "cat"
    if isinstance(command, ReflogArgs):
        return "reflog"
    if isinstance(command, BranchesArgs):
        return "branches"
    if isinstance(command, CheckoutArgs):
        return "checkout"
    if isinstance(command, RestoreArgs):
        return "restore"
    if isinstance(command, InitArgs):
        return "init"
    if isinstance(command, QueryArgs):
        if isinstance(command.command, QueryPruneArgs):
            return "query prune"
        return "query build"
    if isinstance(command, PushArgs):
        return "push"
    if isinstance(command, PullArgs):
        return "pull"
    if isinstance(command, FetchArgs):
        return "fetch"
    if isinstance(command, TransferArgs):
        return "transfer"
    if isinstance(command, ConfigArgs):
        if isinstance(command.command, ConfigInitArgs):
            return "config init"
        if isinstance(command.command, ConfigSetArgs):
            return "config set"
        if isinstance(command.command, ConfigListArgs):
            return "config list"
        return "config"
    return "reflake"


def _open_or_init(root: str, **kwargs: Any) -> ReflakeRepository:
    """Open a local repo, initializing it first when missing (clone flow)."""
    from .core.repository import init_repository

    try:
        return open_repository(root, **kwargs)
    except NotARepositoryError:
        init_repository(root)
        return open_repository(root, **kwargs)


def run_cli(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    parser = build_parser()
    try:
        args = parser.parse_args(argv).cli
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    command = args.command
    command_name = _command_name(command)
    repo_root = args.root
    as_json = args.json

    try:
        if isinstance(command, CommitArgs):
            commit_id = open_repository(repo_root).commit(
                command.message,
                staged_only=command.staged_only,
            )
            if as_json:
                print(json.dumps({"commit_id": commit_id}, indent=2))
            else:
                print(commit_id)
            config = BaseConfig.load(repo_root)
            if config is not None and config.identity == "pointer":
                print(
                    "⚠  Metadata-only identity: this revision is unverifiable "
                    "until `reflake promote` is run. "
                    "You must retain source objects for future verification.",
                    file=sys.stderr,
                )
            return 0

        if isinstance(command, AddArgs):
            stage = open_repository(repo_root).add(
                paths=command.paths,
                identity_mode=command.identity,
                destination_path=command.destination_path,
            )
            if as_json:
                print(json.dumps(_stage_payload(stage), indent=2))
            else:
                _print_status(stage)
            if command.identity == "pointer":
                print(
                    "⚠  Metadata-only identity: revisions are unverifiable "
                    "until `reflake promote` is run. "
                    "You must retain source objects for future verification.",
                    file=sys.stderr,
                )
            return 0

        if isinstance(command, RmArgs):
            stage = open_repository(repo_root).rm(paths=command.paths)
            if as_json:
                print(json.dumps(_stage_payload(stage), indent=2))
            else:
                _print_status(stage)
            return 0

        if isinstance(command, MoveArgs):
            stage = open_repository(repo_root).move_staged(
                source_path=command.source_path,
                destination_path=command.destination_path,
            )
            if as_json:
                print(json.dumps(_stage_payload(stage), indent=2))
            else:
                _print_status(stage)
            return 0

        if isinstance(command, StatusArgs):
            stage = open_repository(repo_root, must_exist=True).status(
                working_tree=True
            )
            if as_json:
                print(json.dumps(_stage_payload(stage), indent=2))
            else:
                _print_status(stage)
            return 0

        if isinstance(command, BranchArgs):
            open_repository(repo_root).branch(command.name)
            print(f"Created branch '{command.name}'")
            return 0

        if isinstance(command, DiffArgs):
            changes = open_repository(repo_root).diff(
                command.from_ref,
                command.to_ref,
            )
            if as_json:
                payload = [
                    {
                        "path": change.path,
                        "change": change.change,
                        "before_hash": change.before_hash,
                        "after_hash": change.after_hash,
                        "before_size": change.before_size,
                        "after_size": change.after_size,
                    }
                    for change in changes
                ]
                print(json.dumps(payload, indent=2))
            else:
                for c in changes:
                    print(f"  {c.change}: {c.path}")
            return 0

        if isinstance(command, MergeArgs):
            result = open_repository(repo_root).merge(
                command.source_ref,
                command.target_ref,
            )
            if as_json:
                print(
                    json.dumps(
                        {
                            "source_ref": result.source_ref,
                            "target_ref": result.target_ref,
                            "commit_id": result.commit_id,
                            "updated": result.updated,
                        },
                        indent=2,
                    )
                )
            else:
                if result.updated:
                    print(
                        f"Merged '{result.source_ref}' into "
                        f"'{result.target_ref}' ({result.commit_id})"
                    )
                else:
                    print(
                        f"'{result.target_ref}' is already up to date "
                        f"with '{result.source_ref}'"
                    )
            return 0

        if isinstance(command, VerifyArgs):
            result = open_repository(repo_root).verify(
                path_prefixes=_flatten_option_values(command.path),
            )
            remaining = result.candidate_entries - result.verified_entries
            if as_json:
                print(
                    json.dumps(
                        {
                            "commit_id": result.commit_id,
                            "verified_entries": result.verified_entries,
                            "candidate_entries": result.candidate_entries,
                            "total_entries": result.total_entries,
                            "unverifiable_entries": remaining,
                        },
                        indent=2,
                    )
                )
            else:
                print(
                    f"Verified {result.verified_entries}/"
                    f"{result.candidate_entries} entries "
                    f"(total: {result.total_entries})"
                )
                if remaining > 0:
                    print(
                        f"⚠  {remaining} unverifiable entries remain "
                        f"(source objects must be retained; "
                        f"run `reflake promote` to materialize them).",
                        file=sys.stderr,
                    )
            # Read-only audit: non-zero while pointer entries remain.
            return 1 if remaining > 0 else 0

        if isinstance(command, PromoteArgs):
            result = open_repository(repo_root).promote(
                path_prefixes=_flatten_option_values(command.path),
            )
            if as_json:
                print(
                    json.dumps(
                        {
                            "commit_id": result.commit_id,
                            "verified_entries": result.verified_entries,
                            "candidate_entries": result.candidate_entries,
                            "total_entries": result.total_entries,
                            "created_commit": result.created_commit,
                        },
                        indent=2,
                    )
                )
            else:
                print(
                    f"Promoted {result.verified_entries}/"
                    f"{result.candidate_entries} entries "
                    f"(total: {result.total_entries})"
                )
                if result.created_commit:
                    print(f"Created commit: {result.commit_id}")
                remaining = result.candidate_entries - result.verified_entries
                if remaining > 0:
                    print(
                        f"⚠  {remaining} unverifiable entries remain "
                        f"(source objects must be retained for future promotion).",
                        file=sys.stderr,
                    )
            return 0

        if isinstance(command, GcArgs):
            result = open_repository(repo_root).gc(dry_run=not command.prune)
            if as_json:
                print(
                    json.dumps(
                        {
                            "reachable_commits": result.reachable_commits,
                            "reachable_trees": result.reachable_trees,
                            "reachable_blobs": result.reachable_blobs,
                            "reachable_footers": result.reachable_footers,
                            "orphan_commits": result.orphan_commits,
                            "orphan_trees": result.orphan_trees,
                            "orphan_blobs": result.orphan_blobs,
                            "orphan_footers": result.orphan_footers,
                            "pruned": result.pruned,
                        },
                        indent=2,
                    )
                )
            else:
                print(
                    f"Reachable: {result.reachable_commits} commits, "
                    f"{result.reachable_trees} trees, {result.reachable_blobs} blobs, "
                    f"{result.reachable_footers} footers"
                )
                print(
                    f"Orphans:   {result.orphan_commits} commits, "
                    f"{result.orphan_trees} trees, {result.orphan_blobs} blobs, "
                    f"{result.orphan_footers} footers"
                )
                if result.pruned:
                    print("Pruned orphaned objects.")
                else:
                    print("Dry run — nothing deleted. Re-run with --prune to delete.")
            return 0

        if isinstance(command, QueryArgs):
            cmd = command.command
            if isinstance(cmd, QueryPruneArgs):
                repo = open_repository(repo_root)
                commit_id = repo.resolve_ref(cmd.ref)
                commit_obj = repo.read_commit(commit_id)
                predicates = parse_where_clause(cmd.where)
                scans = list(
                    plan_pruned_scan(repo.store, commit_obj.tree, cmd.path, predicates)
                )
                if as_json:
                    print(
                        json.dumps(
                            [
                                {
                                    "path": scan.path,
                                    "footer_hash": scan.footer_hash,
                                    "total_row_groups": scan.total_row_groups,
                                    "kept_row_groups": list(scan.kept_row_groups),
                                    "pruned_row_groups": list(scan.pruned_row_groups),
                                }
                                for scan in scans
                            ],
                            indent=2,
                        )
                    )
                else:
                    for scan in scans:
                        print(
                            f"{scan.path}: kept {len(scan.kept_row_groups)}/"
                            f"{scan.total_row_groups} row groups "
                            f"({len(scan.pruned_row_groups)} pruned)"
                        )
                    if not scans:
                        print(
                            "No parquet files with captured footer stats "
                            "under this path."
                        )
                    else:
                        total_kept = sum(len(s.kept_row_groups) for s in scans)
                        total_pruned = sum(len(s.pruned_row_groups) for s in scans)
                        print(
                            f"Summary: {len(scans)} files, {total_kept} kept / "
                            f"{total_pruned} pruned row groups (metadata-only)."
                        )
                return 0
            paths = build_analytical_index(
                root=repo_root,
                output_dir=cmd.output_dir,
                export_parquet=cmd.parquet,
            )
            if as_json:
                print(
                    json.dumps(
                        {
                            "database_path": str(paths.database_path),
                            "parquet_path": (
                                str(paths.parquet_path)
                                if paths.parquet_path is not None
                                else None
                            ),
                        },
                        indent=2,
                    )
                )
            else:
                print(f"Database: {paths.database_path}")
                if paths.parquet_path:
                    print(f"Parquet:  {paths.parquet_path}")
            return 0

        if isinstance(command, LogArgs):
            repo = open_repository(repo_root)
            ref = command.ref or repo.current_branch()
            commits = list(repo.log(ref))
            if as_json:
                payload = [
                    {
                        "id": c.id,
                        "message": c.message,
                        "tree": c.tree,
                        "parents": list(c.parents),
                        "created_at": c.created_at,
                    }
                    for c in commits
                ]
                print(json.dumps(payload, indent=2))
            else:
                for i, c in enumerate(commits):
                    if i > 0:
                        print()
                    print(f"commit {c.id}")
                    print(f"Date:   {c.created_at}")
                    for p in c.parents:
                        print(f"Parent: {p}")
                    print()
                    msg_indented = "\n".join(
                        f"    {line}" for line in c.message.splitlines()
                    )
                    print(msg_indented)
            return 0

        if isinstance(command, ListArgs):
            repo = open_repository(repo_root)
            entries = repo.resolve_entries_for_prefix(
                repo.current_branch(),
                command.path,
            )
            if as_json:
                payload = [
                    {
                        "path": entry.path,
                        "hash": entry.hash,
                        "size": entry.size,
                        "identity_mode": entry.identity_mode,
                        "blob_hash": entry.blob_hash,
                        "source_uri": entry.source_uri,
                    }
                    for entry in entries.values()
                ]
                print(json.dumps(payload, indent=2))
            else:
                for path in sorted(entries):
                    print(path)
            return 0

        if isinstance(command, CatArgs):
            repo = open_repository(repo_root)
            sys.stdout.buffer.write(repo.cat(command.ref, command.path))
            return 0

        if isinstance(command, ReflogArgs):
            repo = open_repository(repo_root)
            branch_name = command.branch or repo.current_branch()
            entries = list(repo.reflog(branch_name))
            if as_json:
                payload = []
                for line in entries:
                    old_id, new_id, operation, timestamp = line.split(" ", 3)
                    payload.append(
                        {
                            "old": old_id,
                            "new": new_id,
                            "operation": operation,
                            "timestamp": timestamp,
                        }
                    )
                print(json.dumps(payload, indent=2))
            else:
                for line in entries:
                    print(line)
            return 0

        if isinstance(command, BranchesArgs):
            repo = open_repository(repo_root)
            datasets = repo.branches()
            if as_json:
                print(json.dumps(datasets, indent=2))
            else:
                for dataset in datasets:
                    head = dataset["commit_id"] or "<empty>"
                    print(
                        (
                            f"{dataset['branch']}: {head} "
                            f"{dataset['message'] or ''}"
                        ).rstrip()
                    )
            return 0

        if isinstance(command, ConfigArgs):
            config_command = command.command
            if isinstance(config_command, ConfigInitArgs):
                config = init_config(
                    repo_root,
                    backend=config_command.backend,  # type: ignore[arg-type]
                    default_branch=config_command.default_branch,
                    s3_bucket=config_command.s3_bucket,
                    s3_prefix=config_command.s3_prefix,
                    s3_endpoint_url=config_command.s3_endpoint,
                )
                path = config.save(repo_root)
                print(f"Config initialized: {path}")
                return 0
            if isinstance(config_command, ConfigSetArgs):
                config = BaseConfig.load(repo_root)
                if config is None:
                    print(
                        "No config found. Run 'reflake config init' first.",
                        file=sys.stderr,
                    )
                    return 1
                config = _config_set_value(
                    config, config_command.key, config_command.value
                )
                path = config.save(repo_root)
                print(f"Updated: {config_command.key}={config_command.value}")
                return 0
            if isinstance(config_command, ConfigListArgs):
                config = BaseConfig.load(repo_root)
                if config is None:
                    print(
                        "No config found. Run 'reflake config init' first.",
                        file=sys.stderr,
                    )
                    return 1
                raw = msgspec.json.format(msgspec.json.encode(config).decode("utf-8"))
                print(raw)
                return 0

        if isinstance(command, CheckoutArgs):
            open_repository(repo_root).set_current_branch(command.name)
            print(f"Switched to branch '{command.name}'")
            return 0

        if isinstance(command, RestoreArgs):
            restored = open_repository(repo_root).restore_files(
                command.ref,
                paths=_flatten_option_values(command.path) or None,
                force=command.force,
            )
            if not restored:
                print("Nothing to restore")
            else:
                rel_paths = "\n".join(f"  restored: {p}" for p in restored)
                print(
                    f"Restored {len(restored)} file(s) "
                    f"from '{command.ref}':\n{rel_paths}"
                )
            return 0

        if isinstance(command, InitArgs):
            if command.backend == "local":
                from .core.repository import init_repository

                repo_root = init_repository(
                    repo_root, default_branch=command.default_branch
                )
                print(f"Repository initialized: {repo_root / '.reflake'}")
                return 0
            config = init_config(
                repo_root,
                backend=command.backend,  # type: ignore[arg-type]
                default_branch=command.default_branch,
                s3_bucket=command.s3_bucket,
                s3_prefix=command.s3_prefix,
                s3_endpoint_url=command.s3_endpoint,
            )
            path = config.save(repo_root)
            print(f"Repository initialized: {path}")
            return 0

        if isinstance(command, PushArgs):
            from .core.repository_sync import push

            repo = open_repository(repo_root, blob_transfer=command.transfer_backend)
            result = push(
                repo,
                command.remote,
                ref=command.ref,
                blob_transfer=command.transfer_backend,
            )
            if as_json:
                print(json.dumps(asdict(result), indent=2))
            else:
                if result.updated:
                    print(
                        f"Pushed {result.pushed_commits} commit(s), "
                        f"{result.pushed_blobs} blob(s) to {command.remote}"
                    )
                else:
                    print("Everything up-to-date")
            return 0

        if isinstance(command, PullArgs):
            from .core.repository_sync import pull

            # Pull bootstraps the local repo (clone workflow).
            repo = _open_or_init(repo_root, blob_transfer=command.transfer_backend)
            result = pull(
                repo,
                command.remote,
                ref=command.ref,
                blob_transfer=command.transfer_backend,
            )
            if as_json:
                print(json.dumps(asdict(result), indent=2))
            else:
                if result.updated:
                    print(
                        f"Pulled {result.pulled_commits} commit(s), "
                        f"{result.pulled_blobs} blob(s) from {command.remote}"
                    )
                else:
                    print("Already up-to-date")
            return 0

        if isinstance(command, FetchArgs):
            from .core.repository_sync import fetch

            # Fetch bootstraps the local repo (clone workflow).
            repo = _open_or_init(repo_root, blob_transfer=command.transfer_backend)
            result = fetch(
                repo,
                command.remote,
                ref=command.ref,
                blob_transfer=command.transfer_backend,
            )
            if as_json:
                print(json.dumps(asdict(result), indent=2))
            else:
                print(
                    f"Fetched {result.fetched_commits} commit(s), "
                    f"{result.fetched_blobs} blob(s) from {command.remote}"
                )
            return 0

        if isinstance(command, TransferArgs):
            repo = open_repository(repo_root)
            cmds = repo.generate_transfer_commands(
                ref=command.ref,
                mode=command.direction,
                include_metadata=True,
            )
            if as_json:
                print(json.dumps(cmds, indent=2))
            elif command.execute:
                import subprocess

                for cmd in cmds:
                    subprocess.run(shlex.split(cmd), check=True)
                print(f"Executed {len(cmds)} transfer commands")
            else:
                for cmd in cmds:
                    print(cmd)
            return 0

    except HANDLED_CLI_ERRORS as error:
        code = _error_exit_code(error)
        if as_json:
            print(
                json.dumps(
                    {
                        "error": str(error),
                        "code": type(error).__name__,
                        "command": command_name,
                        "exit_code": code,
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
        elif isinstance(error, NotARepositoryError):
            print(f"fatal: {error}", file=sys.stderr)
        else:
            print(f"{command_name} error: {error}", file=sys.stderr)
        return code

    raise AssertionError(f"Unsupported command type: {type(command).__name__}")


def main(argv: list[str] | None = None) -> int:
    return run_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
