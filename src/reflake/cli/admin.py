"""Admin commands: init, config, gc."""

from __future__ import annotations

import json
import sys

import msgspec

from ..core import init_repository, open_repository
from ..core.config import (
    BaseConfig,
    LocalConfig,
    ReflakeConfig,
    S3Config,
    init_config,
)
from .args import (
    ConfigInitArgs,
    ConfigListArgs,
    ConfigSetArgs,
    GcArgs,
    InitArgs,
)


def handle_init(command: InitArgs, repo_root: str, as_json: bool) -> int:
    if command.backend == "local":
        repo_path = init_repository(repo_root, default_branch=command.default_branch)
        if as_json:
            print(
                json.dumps(
                    {
                        "repository": str(repo_path / ".reflake"),
                        "backend": "local",
                    },
                    indent=2,
                )
            )
        else:
            print(f"Repository initialized: {repo_path / '.reflake'}")
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
    if command.backend == "s3":
        _write_shared_format(config, command.s3_endpoint)
    if as_json:
        print(
            json.dumps(
                {"config_path": str(path), "backend": command.backend},
                indent=2,
            )
        )
    else:
        print(f"Repository initialized: {path}")
    return 0


def _write_shared_format(
    config: ReflakeConfig, endpoint_url: str | None = None
) -> None:
    """Create the shared ``repo.json`` so every client sees one ingest policy."""
    from ..core.objects import S3ObjectStore, build_s3_client
    from ..core.repo_format import RepoFormat

    if not isinstance(config, S3Config):
        return
    store = S3ObjectStore(
        config.bucket,
        config.prefix,
        client=build_s3_client(config.endpoint_url or endpoint_url),
    )
    if store.read_repo_format_bytes() is None:
        store.write_repo_format_bytes(
            RepoFormat(
                default_branch=config.default_branch,
                identity=config.identity,
                parquet_footer=config.parquet_footer,
            ).to_json()
        )


def handle_config(
    command: ConfigInitArgs | ConfigSetArgs | ConfigListArgs,
    repo_root: str,
    as_json: bool,
) -> int:
    if isinstance(command, ConfigInitArgs):
        config = init_config(
            repo_root,
            backend=command.backend,  # type: ignore[arg-type]
            default_branch=command.default_branch,
            s3_bucket=command.s3_bucket,
            s3_prefix=command.s3_prefix,
            s3_endpoint_url=command.s3_endpoint,
        )
        path = config.save(repo_root)
        if as_json:
            print(json.dumps({"config_path": str(path)}, indent=2))
        else:
            print(f"Config initialized: {path}")
        return 0
    if isinstance(command, ConfigSetArgs):
        config = BaseConfig.load(repo_root)
        if config is None:
            print(
                "No config found. Run 'reflake config init' first.",
                file=sys.stderr,
            )
            return 1
        config = _config_set_value(config, command.key, command.value)
        path = config.save(repo_root)
        _apply_shared_policy(repo_root, command.key, command.value)
        if as_json:
            print(
                json.dumps(
                    {
                        "key": command.key,
                        "value": command.value,
                        "config_path": str(path),
                    },
                    indent=2,
                )
            )
        else:
            print(f"Updated: {command.key}={command.value}")
        return 0
    if isinstance(command, ConfigListArgs):
        config = BaseConfig.load(repo_root)
        if config is None:
            print(
                "No config found. Run 'reflake config init' first.",
                file=sys.stderr,
            )
            return 1
        if as_json:
            print(
                json.dumps(
                    msgspec.json.decode(msgspec.json.encode(config)),
                    indent=2,
                )
            )
        else:
            raw = msgspec.json.format(msgspec.json.encode(config).decode("utf-8"))
            print(raw)
        return 0
    return 0


#: Keys that decide what a commit writes, so they live in the shared scope.
_REPOSITORY_WIDE_KEYS = frozenset({"identity", "parquet_footer", "default_branch"})


def _apply_shared_policy(repo_root: str, key: str, value: str) -> None:
    """Mirror a repository-wide setting into the shared ``repo.json``.

    Without this, `config set identity pointer` would change the client's
    config while the repository kept its old policy — and the shared policy
    wins, so the command would silently do nothing.
    """
    if key not in _REPOSITORY_WIDE_KEYS:
        return
    repo = open_repository(repo_root, must_exist=False)
    if key == "identity":
        repo.set_repository_policy(identity=value)
    elif key == "parquet_footer":
        repo.set_repository_policy(
            parquet_footer=value.strip().lower() in ("true", "1", "yes", "on")
        )
    else:
        repo.set_repository_policy(default_branch=value)


def handle_gc(command: GcArgs, repo_root: str, as_json: bool) -> int:
    result = open_repository(repo_root).gc(
        dry_run=not command.prune,
        grace_seconds=command.grace_seconds,
    )
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
                    "skipped_young": result.skipped_young,
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
        if result.skipped_young:
            print(
                f"Skipped:   {result.skipped_young} orphan(s) younger "
                "than the grace window (may belong to a concurrent writer)"
            )
        if result.pruned:
            print("Pruned orphaned objects.")
        elif command.prune:
            print("No orphaned objects to prune.")
        else:
            print("Dry run — nothing deleted. Re-run with --prune to delete.")
    return 0


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
                    "Cannot switch to S3 backend without a bucket. "
                    "Set s3.bucket first."
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
    elif key in ("stat_cache", "trust_mtime"):
        if value.strip().lower() in ("true", "1", "yes", "on"):
            config.stat_cache = True
        elif value.strip().lower() in ("false", "0", "no", "off"):
            config.stat_cache = False
        else:
            raise ValueError(f"{key} must be a boolean, got: {value}")
    elif key == "reflink_blobs":
        if value.strip().lower() in ("true", "1", "yes", "on"):
            config.reflink_blobs = True
        elif value.strip().lower() in ("false", "0", "no", "off"):
            config.reflink_blobs = False
        else:
            raise ValueError(f"reflink_blobs must be a boolean, got: {value}")
    elif key == "gc_grace_seconds":
        try:
            parsed = int(value)
        except ValueError as error:
            raise ValueError(
                f"gc_grace_seconds must be an integer, got: {value}"
            ) from error
        if parsed < 0:
            raise ValueError("gc_grace_seconds cannot be negative")
        config.gc_grace_seconds = parsed
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
