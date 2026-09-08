from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .domain import RepositoryObjectKind


@dataclass(frozen=True)
class ReflakeLayout:
    root: Path
    reflake_dir: Path
    blobs_dir: Path
    commits_dir: Path
    trees_dir: Path
    footers_dir: Path
    staging_dir: Path
    refs_dir: Path
    heads_dir: Path

    @classmethod
    def initialize(cls, root: str | Path, *, create_dirs: bool = True) -> ReflakeLayout:
        root_path = Path(root).resolve()
        reflake_dir = root_path / ".reflake"
        blobs_dir = reflake_dir / "blobs"
        commits_dir = reflake_dir / "commits"
        trees_dir = reflake_dir / "trees"
        footers_dir = reflake_dir / "footers"
        staging_dir = reflake_dir / "staging"
        refs_dir = reflake_dir / "refs"
        heads_dir = refs_dir / "heads"

        if create_dirs:
            for path in (
                blobs_dir,
                commits_dir,
                trees_dir,
                footers_dir,
                staging_dir,
                refs_dir,
                heads_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)

        return cls(
            root=root_path,
            reflake_dir=reflake_dir,
            blobs_dir=blobs_dir,
            commits_dir=commits_dir,
            trees_dir=trees_dir,
            footers_dir=footers_dir,
            staging_dir=staging_dir,
            refs_dir=refs_dir,
            heads_dir=heads_dir,
        )


def initialize_reflake_layout(
    root: str | Path, *, create_dirs: bool = True
) -> ReflakeLayout:
    # Layout creation never writes config: repository config is created
    # explicitly by init_repository() (or `reflake init`), so opening a
    # repository can never mutate it.
    return ReflakeLayout.initialize(root, create_dirs=create_dirs)


def blob_relpath(content_hash: str) -> Path:
    return Path(content_hash[:2]) / content_hash[2:]


def object_relative_key(kind: RepositoryObjectKind, object_id: str) -> str:
    """Single source of truth for an object's location relative to `.reflake/`.

    Both adapters (local filesystem and S3) derive their physical layout from
    this mapping, so the on-disk and on-bucket key spaces can never drift.
    """
    if kind == "blob":
        return f"blobs/{blob_relpath(object_id).as_posix()}"
    if kind == "commit":
        return f"commits/{object_id}.json"
    if kind == "tree":
        return f"trees/{object_id}"
    if kind == "footer":
        return f"footers/{object_id}"
    if kind == "ref":
        return f"refs/heads/{object_id}"
    raise ValueError(f"Unsupported object kind: {kind}")
