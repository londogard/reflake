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
    ConfigInitArgs,
    ConfigListArgs,
    ConfigSetArgs,
    DiffArgs,
    FetchArgs,
    GcArgs,
    IdentityPromoteArgs,
    IdentityVerifyArgs,
    InitArgs,
    ListArgs,
    LogArgs,
    MergeArgs,
    MoveArgs,
    PullArgs,
    PushArgs,
    QueryBuildArgs,
    QueryScanArgs,
    ReflogArgs,
    ResetArgs,
    RestoreArgs,
    RmArgs,
    StatusArgs,
    TagArgs,
    TransferArgs,
    build_parser,
    command_name,
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

#: One handler per leaf command type: ``(command, repo_root, as_json) -> exit``.
_HANDLERS: dict[type, Callable[..., int]] = {
    CommitArgs: worktree.handle_commit,
    AddArgs: worktree.handle_add,
    RmArgs: worktree.handle_rm,
    MoveArgs: worktree.handle_mv,
    StatusArgs: worktree.handle_status,
    BranchArgs: history.handle_branch,
    DiffArgs: history.handle_diff,
    MergeArgs: history.handle_merge,
    LogArgs: history.handle_log,
    ListArgs: history.handle_list,
    CatArgs: history.handle_cat,
    ReflogArgs: history.handle_reflog,
    BranchesArgs: history.handle_branches,
    TagArgs: history.handle_tag,
    CheckoutArgs: history.handle_checkout,
    RestoreArgs: history.handle_restore,
    ResetArgs: history.handle_reset,
    IdentityVerifyArgs: identity.handle_identity,
    IdentityPromoteArgs: identity.handle_identity,
    QueryBuildArgs: query.handle_query_build,
    QueryScanArgs: query.handle_query_scan,
    GcArgs: admin.handle_gc,
    InitArgs: admin.handle_init,
    ConfigInitArgs: admin.handle_config,
    ConfigSetArgs: admin.handle_config,
    ConfigListArgs: admin.handle_config,
    PushArgs: sync.handle_push,
    PullArgs: sync.handle_pull,
    FetchArgs: sync.handle_fetch,
    CloneArgs: sync.handle_clone,
    TransferArgs: sync.handle_transfer,
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
    command_display = command_name(command)
    inner = getattr(command, "command", None)
    leaf = inner if inner is not None else command

    try:
        handler = _HANDLERS.get(type(leaf))
        if handler is None:
            raise AssertionError(f"Unsupported command type: {type(leaf).__name__}")
        return handler(leaf, args.repo, args.json)
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
