"""CLI argument definitions, parser construction, and command naming.

Help strings are extracted by simple-parsing from the field comments, so each
option documents itself in place instead of nesting long strings inside
``field(...)``.  A few fields keep explicit ``help=`` because they also carry
``# type: ignore`` or rationale comments that would otherwise be consumed as
help text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from importlib import metadata
from typing import Literal

from simple_parsing import ArgumentParser
from simple_parsing.helpers import field, flag, subparsers

IdentityMode = Literal["content", "pointer"]


@dataclass
class CommitArgs:
    message: str = field(alias=["-m", "--message"])  # Commit message
    # Commit only staged changes, ignoring the working tree.
    staged_only: bool = flag(False, alias=["--staged", "--staged-only"])


@dataclass
class AddArgs:
    # Files, directories, or S3 paths to stage.
    paths: list[str] = field(positional=True, nargs="+", metavar="paths")
    identity: IdentityMode = "content"  # Identity mode for staged additions
    # Logical destination path for a single staged source.
    destination_path: str | None = field(default=None, alias="--as")


@dataclass
class RmArgs:
    # Paths to remove.
    paths: list[str] = field(positional=True, nargs="+", metavar="paths")
    # Keep the files on disk (default: delete them, like `git rm`).
    keep_worktree: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias="--keep-worktree",
        action="store_true",
        help="Keep the files on disk (pure metadata removal)",
    )


@dataclass
class MoveArgs:
    # Path or prefix to rename.
    source_path: str = field(positional=True, metavar="source_path")
    # Destination path or prefix.
    destination_path: str = field(positional=True, metavar="destination_path")
    # Keep the files where they are (default: rename them on disk too).
    keep_worktree: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias="--keep-worktree",
        action="store_true",
        help="Keep the files in place (pure metadata move)",
    )


@dataclass
class StatusArgs:
    pass


@dataclass
class BranchArgs:
    name: str = field(positional=True)  # Branch name
    delete: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias=["-d", "--delete"],
        action="store_true",
        help="Delete the branch (CAS to unborn; re-create it with `branch`)",
    )


@dataclass
class LogArgs:
    # Ref (branch or commit) to show log for (default: current branch).
    ref: str | None = field(default=None, positional=True, nargs="?", metavar="ref")


@dataclass
class ListArgs:
    # Logical path or prefix to list (default: root).
    path: str = field(default="", positional=True, nargs="?", metavar="path")
    # Ref (branch or commit) to list (default: current branch).
    ref: str | None = field(default=None, alias="--ref")


@dataclass
class CatArgs:
    path: str = field(positional=True, metavar="path")  # Logical file path to print
    # Ref (branch, tag, or commit) to read from (default: current branch).
    ref: str | None = field(default=None, alias="--ref")


@dataclass
class ReflogArgs:
    # Branch to inspect (default: current branch).
    branch: str | None = field(
        default=None, positional=True, nargs="?", metavar="branch"
    )


@dataclass
class BranchesArgs:
    pass


@dataclass
class TagArgs:
    # Tag name to create, or to delete with --delete (omit to list tags).
    name: str | None = field(default=None, positional=True, nargs="?", metavar="name")
    # Commit or ref to tag (default: current branch head).
    ref: str | None = field(default=None, alias="--ref")
    delete: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias=["-d", "--delete"],
        action="store_true",
        help="Delete the tag instead of creating it",
    )
    list_tags: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias=["-l", "--list"],
        action="store_true",
        help="List tags instead of creating one",
    )


@dataclass
class CheckoutArgs:
    name: str = field(positional=True, metavar="name")  # Branch name to switch to


@dataclass
class RestoreArgs:
    # Paths to restore (default: every file in the ref).
    path: list[str] = field(
        default_factory=list, positional=True, nargs="*", metavar="path"
    )
    # Ref (branch, tag, or commit) to restore from (default: current branch).
    ref: str | None = field(default=None, alias="--ref")
    force: bool = flag(False, alias="--force")  # Allow overwriting existing files


@dataclass
class ResetArgs:
    ref: str = field(positional=True, metavar="ref")  # Commit or ref to point at
    # Branch to move (default: current branch).
    branch: str | None = field(default=None, alias="--branch")


@dataclass
class ConfigInitArgs:
    backend: str = field(default="local", alias="--backend")  # Backend: local or s3
    # S3 bucket name (required for s3 backend).
    s3_bucket: str | None = field(default=None, alias="--s3-bucket")
    s3_prefix: str | None = field(default=None, alias="--s3-prefix")  # S3 key prefix
    # S3 endpoint URL.
    s3_endpoint: str | None = field(default=None, alias="--s3-endpoint")
    # Default branch name.
    default_branch: str = field(default="main", alias="--default-branch")


@dataclass
class ConfigSetArgs:
    # Config key (e.g. backend, dataset_root, s3.bucket).
    key: str = field(positional=True, metavar="key")
    value: str = field(positional=True, metavar="value")  # Config value


@dataclass
class ConfigListArgs:
    pass


@dataclass
class InitArgs:
    backend: str = field(default="local", alias="--backend")  # Backend: local or s3
    # S3 bucket name (required for s3 backend).
    s3_bucket: str | None = field(default=None, alias="--s3-bucket")
    s3_prefix: str | None = field(default=None, alias="--s3-prefix")  # S3 key prefix
    # S3 endpoint URL.
    s3_endpoint: str | None = field(default=None, alias="--s3-endpoint")
    # Default branch name.
    default_branch: str = field(default="main", alias="--default-branch")


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
    remote: str = field(positional=True, metavar="remote")  # Remote S3 URI
    # Branch to push (default: current branch).
    ref: str | None = field(default=None, alias="--ref")
    # Blob transfer backend: boto3 (default) or s5cmd.
    transfer_backend: str = field(default="boto3", alias="--transfer-backend")


@dataclass
class PullArgs:
    remote: str = field(positional=True, metavar="remote")  # Remote S3 URI
    # Branch to pull (default: current branch).
    ref: str | None = field(default=None, alias="--ref")
    # Blob transfer backend: boto3 (default) or s5cmd.
    transfer_backend: str = field(default="boto3", alias="--transfer-backend")
    # Copy metadata only (commits, trees, refs); read blobs on demand.
    metadata_only: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias="--metadata-only",
        action="store_true",
        help="Copy metadata only (commits, trees, refs); read blobs on demand",
    )


@dataclass
class FetchArgs:
    remote: str = field(positional=True, metavar="remote")  # Remote S3 URI
    # Branch to fetch (default: current branch).
    ref: str | None = field(default=None, alias="--ref")
    # Blob transfer backend: boto3 (default) or s5cmd.
    transfer_backend: str = field(default="boto3", alias="--transfer-backend")
    # Copy metadata only (commits, trees, refs); read blobs on demand.
    metadata_only: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias="--metadata-only",
        action="store_true",
        help="Copy metadata only (commits, trees, refs); read blobs on demand",
    )


@dataclass
class CloneArgs:
    remote: str = field(positional=True, metavar="remote")  # Remote S3 URI
    # Local directory to clone into (default: current directory).
    directory: str = field(default=".", positional=True, nargs="?", metavar="directory")
    # Ref to check out (default: the remote default branch).
    ref: str | None = field(default=None, alias="--ref")
    # Also download every blob (default: metadata only, read on demand).
    with_data: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias="--with-data",
        action="store_true",
        help="Also download every blob into the worktree",
    )
    # Blob transfer backend: boto3 (default) or s5cmd.
    transfer_backend: str = field(default="boto3", alias="--transfer-backend")


@dataclass
class TransferArgs:
    # Transfer direction: upload (local->S3) or download (S3->local).
    direction: str = field(default="upload", positional=True, metavar="direction")
    execute: bool = flag(False, alias="--execute")  # Execute s5cmd/aws commands
    # Ref to transfer (default: current branch).
    ref: str | None = field(default=None, alias="--ref")


@dataclass
class DiffArgs:
    from_ref: str = field(positional=True, metavar="from_ref")  # Source ref
    to_ref: str = field(positional=True, metavar="to_ref")  # Target ref


@dataclass
class MergeArgs:
    source_ref: str = field(positional=True, metavar="source_ref")  # Merge from
    # Branch to merge into (default: current branch).
    target_ref: str | None = field(
        default=None, positional=True, nargs="?", metavar="target_ref"
    )


@dataclass
class IdentityVerifyArgs:
    # Ref (branch or commit) to audit (default: current branch).
    ref: str | None = field(default=None, alias="--ref")
    # Optional path/prefix filter (repeatable).
    path: list[str] = field(default_factory=list, alias="--path", action="append")
    # Also report pointer entries whose source metadata changed (metadata-only:
    # one HEAD per entry, no source byte reads).
    drift: bool = flag(False, alias="--drift")


@dataclass
class IdentityPromoteArgs:
    # Branch to promote (default: current branch).
    ref: str | None = field(default=None, alias="--ref")
    # Optional path/prefix filter (repeatable).
    path: list[str] = field(default_factory=list, alias="--path", action="append")


@dataclass
class IdentityArgs:
    """Entry-identity commands: audit unverifiable entries, then materialize."""

    command: IdentityVerifyArgs | IdentityPromoteArgs = subparsers(
        {
            "verify": IdentityVerifyArgs,
            "promote": IdentityPromoteArgs,
        }
    )


@dataclass
class GcArgs:
    # Delete orphaned objects (default: audit-only dry run).
    prune: bool = flag(False, alias="--prune")
    # Never prune objects younger than this many seconds (default: config
    # gc_grace_seconds, 86400).
    grace_seconds: int | None = field(default=None, alias="--grace-seconds")


@dataclass
class QueryBuildArgs:
    # Output directory for the index.
    output_dir: str | None = field(default=None, alias="--output-dir")
    parquet: bool = flag(False)  # Also export Parquet from the index table


@dataclass
class QueryScanArgs:
    # Filter predicate, e.g. "id >= 100 AND active = true".
    where: str = field(positional=True, metavar="where")
    # Ref (branch or commit) to scan (default: current branch).
    ref: str | None = field(default=None, alias="--ref")
    path: str = field(default="", alias="--path")  # Path or prefix (default: all)
    limit: int | None = field(default=None, alias="--limit")  # Stop after N rows
    explain: bool = flag(False, alias="--explain")  # Print the plan, no data bytes
    verify: bool = flag(False, alias="--verify")  # Full-hash-verify each content blob
    strict: bool = flag(False, alias="--strict")  # Fail on unreachable pointer sources


@dataclass
class QueryArgs:
    command: QueryBuildArgs | QueryScanArgs = subparsers(
        {
            "build": QueryBuildArgs,
            "scan": QueryScanArgs,
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
        | IdentityArgs
        | GcArgs
        | QueryArgs
        | LogArgs
        | ListArgs
        | CatArgs
        | ReflogArgs
        | BranchesArgs
        | TagArgs
        | CheckoutArgs
        | RestoreArgs
        | ResetArgs
        | InitArgs
        | ConfigArgs
        | PushArgs
        | PullArgs
        | FetchArgs
        | CloneArgs
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
            "identity": IdentityArgs,
            "gc": GcArgs,
            "query": QueryArgs,
            "log": LogArgs,
            "list": ListArgs,
            "ls": ListArgs,
            "cat": CatArgs,
            "reflog": ReflogArgs,
            "branches": BranchesArgs,
            "tag": TagArgs,
            "checkout": CheckoutArgs,
            "restore": RestoreArgs,
            "reset": ResetArgs,
            "init": InitArgs,
            "config": ConfigArgs,
            "push": PushArgs,
            "pull": PullArgs,
            "fetch": FetchArgs,
            "clone": CloneArgs,
            "transfer": TransferArgs,
        }
    )
    # Repository path or URI (global, before the command).
    repo: str = field(default=".")
    json: bool = flag(  # type: ignore[no-matching-overload]
        False,
        alias="--json",
        # store_true (not simple-parsing's bool-action): a global flag must
        # never consume the following command word as a value.
        action="store_true",
        help="Print result as structured JSON (global, before the command)",
    )


class _CleanParser(ArgumentParser):
    """ArgumentParser that suppresses simple-parsing internal group headers.

    simple-parsing renders each dataclass section as an internal header
    (``ReflakeCLI ['cli']:``) plus a dataclass signature line; only those two
    lines are noise — the option lines below them carry the real help text
    (extracted from field comments) and must stay.
    """

    _INTERNAL_GROUP = re.compile(r"^\w+ \['cli(?:\.\w+)*'\]:$")

    def format_help(self) -> str:
        cleaned: list[str] = []
        drop_signature = False
        for line in super().format_help().splitlines():
            if self._INTERNAL_GROUP.match(line):
                drop_signature = True
                continue
            if drop_signature:
                drop_signature = False
                if line.strip().endswith(")") and "(" in line:
                    continue  # the dataclass signature line under the header
            if not line.strip() and cleaned and not cleaned[-1].strip():
                continue  # collapse blank lines left by a removed header
            cleaned.append(line)
        return "\n".join(cleaned).rstrip("\n") + "\n"


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


#: Command name for every leaf command object (plus nested-wrapper fallbacks).
_COMMAND_NAMES: dict[type, str] = {
    CommitArgs: "commit",
    AddArgs: "add",
    RmArgs: "rm",
    MoveArgs: "mv",
    StatusArgs: "status",
    BranchArgs: "branch",
    DiffArgs: "diff",
    MergeArgs: "merge",
    LogArgs: "log",
    ListArgs: "list",
    CatArgs: "cat",
    ReflogArgs: "reflog",
    BranchesArgs: "branches",
    CheckoutArgs: "checkout",
    RestoreArgs: "restore",
    ResetArgs: "reset",
    InitArgs: "init",
    PushArgs: "push",
    PullArgs: "pull",
    TagArgs: "tag",
    FetchArgs: "fetch",
    CloneArgs: "clone",
    TransferArgs: "transfer",
    IdentityVerifyArgs: "identity verify",
    IdentityPromoteArgs: "identity promote",
    QueryBuildArgs: "query build",
    QueryScanArgs: "query scan",
    ConfigInitArgs: "config init",
    ConfigSetArgs: "config set",
    ConfigListArgs: "config list",
    # Nested-subcommand wrappers (reached only when no inner command exists).
    IdentityArgs: "identity verify",
    QueryArgs: "query build",
    ConfigArgs: "config",
}


def command_name(command: object) -> str:
    """Human-readable name of a parsed command, for error messages."""
    inner = getattr(command, "command", None)
    candidate = inner if inner is not None else command
    return _COMMAND_NAMES.get(type(candidate), "reflake")
