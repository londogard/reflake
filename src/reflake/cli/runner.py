"""CLI entry point: parse, dispatch to command handlers, map errors to exits.

Exit-code contract: ``0`` ok · ``1`` usage/validation · ``2`` retryable
conflict (CAS race, non-fast-forward, merge conflict) · ``3`` missing
repository ref/object.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable

from ..core import (
    EmptyBranchError,
    MergeConflictError,
    NonFastForwardError,
    NotARepositoryError,
    OptimisticLockError,
    PreconditionFailedError,
    RefConflictError,
    ReflakeError,
    UnknownCommitError,
    UnknownRefError,
)
from . import admin, history, identity, query, sync, worktree
from .args import (
    AddArgs,
    BranchArgs,
    BranchesArgs,
    CatArgs,
    CheckoutArgs,
    CloneArgs,
    CommitArgs,
    ConfigArgs,
    ConfigInitArgs,
    ConfigListArgs,
    ConfigSetArgs,
    DiffArgs,
    FetchArgs,
    FsckArgs,
    GcArgs,
    IdentityArgs,
    IdentityPromoteArgs,
    IdentityVerifyArgs,
    InitArgs,
    ListArgs,
    LogArgs,
    MergeArgs,
    MoveArgs,
    PullArgs,
    PushArgs,
    QueryArgs,
    QueryBuildArgs,
    QueryScanArgs,
    ReflogArgs,
    ResetArgs,
    RestoreArgs,
    RmArgs,
    StatusArgs,
    TagArgs,
    build_parser,
)

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

#: One registry: command display name + handler for each leaf command type.
_COMMANDS: dict[type, tuple[str, Callable[..., int]]] = {
    CommitArgs: ("commit", worktree.handle_commit),
    AddArgs: ("add", worktree.handle_add),
    RmArgs: ("rm", worktree.handle_rm),
    MoveArgs: ("mv", worktree.handle_mv),
    StatusArgs: ("status", worktree.handle_status),
    BranchArgs: ("branch", history.handle_branch),
    DiffArgs: ("diff", history.handle_diff),
    MergeArgs: ("merge", history.handle_merge),
    LogArgs: ("log", history.handle_log),
    ListArgs: ("list", history.handle_list),
    CatArgs: ("cat", history.handle_cat),
    ReflogArgs: ("reflog", history.handle_reflog),
    BranchesArgs: ("branches", history.handle_branches),
    TagArgs: ("tag", history.handle_tag),
    CheckoutArgs: ("checkout", history.handle_checkout),
    RestoreArgs: ("restore", history.handle_restore),
    ResetArgs: ("reset", history.handle_reset),
    IdentityVerifyArgs: ("identity verify", identity.handle_identity),
    IdentityPromoteArgs: ("identity promote", identity.handle_identity),
    QueryBuildArgs: ("query build", query.handle_query_build),
    QueryScanArgs: ("query scan", query.handle_query_scan),
    GcArgs: ("gc", admin.handle_gc),
    FsckArgs: ("fsck", admin.handle_fsck),
    InitArgs: ("init", admin.handle_init),
    ConfigInitArgs: ("config init", admin.handle_config),
    ConfigSetArgs: ("config set", admin.handle_config),
    ConfigListArgs: ("config list", admin.handle_config),
    PushArgs: ("push", sync.handle_push),
    PullArgs: ("pull", sync.handle_pull),
    FetchArgs: ("fetch", sync.handle_fetch),
    CloneArgs: ("clone", sync.handle_clone),
}

#: Fallbacks for nested wrappers (no inner command supplied).
_WRAPPER_NAMES: dict[type, str] = {
    IdentityArgs: "identity verify",
    QueryArgs: "query build",
    ConfigArgs: "config",
}


def _error_exit_code(error: BaseException) -> int:
    """Exit-code contract: 0 ok · 1 usage/validation · 2 conflict · 3 missing."""
    if isinstance(error, _RETRYABLE_ERRORS):
        return 2
    if isinstance(error, _MISSING_ERRORS):
        return 3
    return 1


#: Global options that may appear anywhere on the command line. Users write
#: ``reflake add data --repo s3://bucket/x`` far more often than they write the
#: documented ``reflake --repo ... add data``, so both work.
_GLOBAL_VALUE_OPTIONS = ("--repo",)


def _hoist_global_options(argv: list[str]) -> list[str]:
    hoisted: list[str] = []
    rest: list[str] = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument == "--":
            rest.extend(argv[index:])
            break
        if argument == "--json" or argument == "--json=true":
            hoisted.append("--json")
            index += 1
            continue
        if argument in _GLOBAL_VALUE_OPTIONS and index + 1 < len(argv):
            hoisted.extend(argv[index : index + 2])
            index += 2
            continue
        if any(argument.startswith(f"{option}=") for option in _GLOBAL_VALUE_OPTIONS):
            hoisted.append(argument)
            index += 1
            continue
        if argument == "-m" and index + 1 < len(argv):
            # A commit message may contain "--json" or "--repo".
            rest.extend(argv[index : index + 2])
            index += 2
            continue
        rest.append(argument)
        index += 1
    return hoisted + rest


def run_cli(argv: list[str] | None = None) -> int:
    argv = _hoist_global_options(argv if argv is not None else sys.argv[1:])
    parser = build_parser()
    try:
        args = parser.parse_args(argv).cli
    except SystemExit as exc:
        # argparse exits 2 for usage errors, but the CLI contract reserves
        # exit 2 for retryable conflicts — usage/validation is 1.
        return 0 if not exc.code else 1
    command = args.command
    inner = getattr(command, "command", None)
    leaf = inner if inner is not None else command
    display_name = _COMMANDS.get(type(leaf), (None, None))[0]
    command_display = display_name or _WRAPPER_NAMES.get(type(command), "reflake")

    try:
        spec = _COMMANDS.get(type(leaf))
        if spec is None:
            raise AssertionError(f"Unsupported command type: {type(leaf).__name__}")
        return spec[1](leaf, args.repo, args.json)
    except HANDLED_CLI_ERRORS as error:
        code = _error_exit_code(error)
        if args.json:
            print(
                json.dumps(
                    {
                        "error": str(error),
                        "code": type(error).__name__,
                        "command": command_display,
                        "exit_code": code,
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
        elif isinstance(error, NotARepositoryError):
            print(f"fatal: {error}", file=sys.stderr)
        else:
            print(f"{command_display} error: {error}", file=sys.stderr)
        return code


def main(argv: list[str] | None = None) -> int:
    return run_cli(argv)
