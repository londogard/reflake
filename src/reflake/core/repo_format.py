"""Repository-scoped format and ingest policy (``repo.json``).

Client config (``.reflake/config.json``) holds *client* behaviour: caches,
transfer backend, worktree preferences. It must not hold settings that change
the bytes a commit writes — ``identity`` and ``parquet_footer`` decide whether
an entry is ``b`` or ``bp``, so two clients disagreeing about them would
produce different tree hashes for identical data and flip entries back and
forth.

Those settings therefore live in the **shared scope** next to ``refs/``, are
written once by ``init``, and are read by every client that opens the
repository. Client config can still override them deliberately
(``reflake config set``), but the shared value is the default and the one a
fresh clone sees.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import msgspec

#: File name, relative to the shared scope root.
REPO_FORMAT_KEY = "repo.json"

CURRENT_FORMAT_VERSION = 2


@dataclass(frozen=True)
class RepoFormat:
    """Format + ingest policy shared by every client of a repository."""

    format_version: int = CURRENT_FORMAT_VERSION
    default_branch: str = "main"
    identity: str = "content"
    parquet_footer: bool = False

    def __post_init__(self) -> None:
        if self.format_version > CURRENT_FORMAT_VERSION:
            raise ValueError(
                f"Repository format version {self.format_version} is newer than "
                f"this reflake ({CURRENT_FORMAT_VERSION}). Upgrade reflake."
            )
        if self.format_version < CURRENT_FORMAT_VERSION:
            raise ValueError(
                f"Repository format version {self.format_version} predates "
                f"version {CURRENT_FORMAT_VERSION}: its trees use a different "
                "entry layout. Recreate the repository with this version "
                "(0.5.0 is a format break) or read it with Reflake 0.4.x."
            )
        if self.identity not in ("content", "pointer"):
            raise ValueError("identity must be one of: content, pointer")

    def to_json(self) -> bytes:
        return msgspec.json.encode(asdict(self)) + b"\n"

    @classmethod
    def parse(cls, payload: bytes) -> RepoFormat:
        data = msgspec.json.decode(payload)
        return cls(
            format_version=int(data.get("format_version", CURRENT_FORMAT_VERSION)),
            default_branch=str(data.get("default_branch", "main")),
            identity=str(data.get("identity", "content")),
            parquet_footer=bool(data.get("parquet_footer", False)),
        )


def format_for_config(config: object | None) -> RepoFormat:
    """Derive the shared format record from a client config (or defaults)."""
    if config is None:
        return RepoFormat()
    return RepoFormat(
        default_branch=getattr(config, "default_branch", "main") or "main",
        identity=getattr(config, "identity", "content") or "content",
        parquet_footer=bool(getattr(config, "parquet_footer", False)),
    )
