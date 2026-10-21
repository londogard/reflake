"""Identity commands: audit unverifiable pointer entries, then materialize."""

from __future__ import annotations

import json
import sys

from ..core import open_repository
from .args import IdentityPromoteArgs, IdentityVerifyArgs
from .helpers import flatten_option_values


def handle_identity(
    command: IdentityVerifyArgs | IdentityPromoteArgs, repo_root: str, as_json: bool
) -> int:
    if isinstance(command, IdentityVerifyArgs):
        result = open_repository(repo_root).verify(
            ref=command.ref,
            path_prefixes=flatten_option_values(command.path),
            drift_check=command.drift,
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
                        "drifted_paths": result.drifted_paths,
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
            if result.drifted_paths:
                drift_lines = "\n".join(
                    f"  drifted: {path}" for path in result.drifted_paths[:20]
                )
                extra = (
                    f"\n  … ({len(result.drifted_paths)} total)"
                    if len(result.drifted_paths) > 20
                    else ""
                )
                print(
                    f"⚠  {len(result.drifted_paths)} pointer entry(ies) "
                    "no longer match their source metadata:"
                    f"\n{drift_lines}{extra}",
                    file=sys.stderr,
                )
            if remaining > 0:
                print(
                    f"⚠  {remaining} unverifiable entries remain "
                    f"(source objects must be retained; "
                    f"run `reflake identity promote` to materialize "
                    f"them).",
                    file=sys.stderr,
                )
        # Read-only audit: non-zero while pointer entries remain
        # or source drift was detected.
        return 1 if remaining > 0 or result.drifted_paths else 0

    result = open_repository(repo_root).promote(
        ref=command.ref,
        path_prefixes=flatten_option_values(command.path),
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
                f"(source objects must be retained for future "
                f"promotion).",
                file=sys.stderr,
            )
    return 0
