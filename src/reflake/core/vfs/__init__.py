from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from ..domain import EmptyBranchError, ReflakeError, UnknownRefError
from ..extras import EXTRAS

try:
    import fsspec
    from fsspec.spec import AbstractFileSystem
except ModuleNotFoundError as error:  # pragma: no cover – minimal installs
    raise ReflakeError(
        f"{EXTRAS['vfs']} needs the optional dependency 'fsspec'. "
        "Install it with: pip install 'reflake[vfs]'"
    ) from error

from ..entry_codec import Entry
from ..objects import RangeFile, open_source_range_file, source_client
from ..repository import ReflakeRepository, open_repository
from ..repository_support import _validate_no_binary, normalize_repository_path


@dataclass(frozen=True)
class ReflakeURI:
    dataset: str
    ref: str
    logical_path: str
    include_staging: bool = False


class ReflakeFileSystem(AbstractFileSystem):
    protocol = "reflake"

    def __init__(
        self,
        *,
        dataset_roots: dict[str, str | Path] | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_roots: dict[str, str | Path] = {
            name: (
                str(root)
                if isinstance(root, str) and root.startswith("s3://")
                else Path(root).resolve()
            )
            for name, root in (dataset_roots or {}).items()
        }
        self._repositories: dict[str, ReflakeRepository] = {}

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        # fsspec's glob/find machinery builds a pattern from the stripped
        # path and compares it against the paths returned by `ls`. Keeping
        # the protocol here makes both sides consistent (and `_parse_uri`
        # strips it again).
        return path

    def _open(  # type: ignore[bad-override]
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        **kwargs: object,
    ) -> Any:
        if mode != "rb":
            raise NotImplementedError(
                "ReflakeFileSystem currently supports read-only binary mode"
            )
        resolved = self._resolve_entry(path)
        repo = self._repository(resolved.root)
        entry = resolved.entry
        if entry.blob_hash is not None:
            blob_hash = entry.blob_hash
            return RangeFile(
                lambda offset, length: repo.store.read_blob_range(
                    blob_hash, offset, length
                ),
                entry.size,
            )
        if entry.source_uri is not None:
            return open_source_range_file(
                entry.source_uri,
                entry.size,
                client=source_client(repo.store),
            )
        raise FileNotFoundError(
            "Entry has no canonical blob hash and no readable source URI"
        )

    def modified(self, path: str, **kwargs: object) -> datetime:
        """Commit time, for fsspec consumers.

        Reflake records no per-file mtime in the tree (a Merkle node must not
        depend on filesystem facts), so the only meaningful timestamp for a
        virtual path is the commit that produced it.
        """
        scope = self._resolve_scope(path)
        commit = repo_commit(scope)
        try:
            return datetime.fromisoformat(commit.created_at)
        except ValueError:
            return datetime.fromtimestamp(0)

    def exists(self, path: str, **kwargs: object) -> bool:
        try:
            self.info(path)
            return True
        except (FileNotFoundError, ValueError):
            return False

    def isdir(self, path: str, **kwargs: object) -> bool:
        try:
            return self.info(path)["type"] == "directory"
        except (FileNotFoundError, ValueError):
            return False

    def info(self, path: str, **kwargs: object) -> dict[str, object]:
        scope = self._resolve_scope(path)
        entry = scope.entry
        if entry is not None:
            return {
                "name": path,
                "type": "file",
                "size": entry.size,
                "hash": entry.hash,
                "identity_mode": entry.identity_mode,
                "commit": scope.commit_id,
                "staged": scope.uri.include_staging,
                "dataset": scope.uri.dataset,
            }
        return {
            "name": path,
            "type": "directory",
            "size": 0,
            "commit": scope.commit_id,
            "staged": scope.uri.include_staging,
            "dataset": scope.uri.dataset,
        }

    def ls(
        self, path: str, detail: bool = True, **kwargs: object
    ) -> list[dict[str, object] | str]:
        uri = self._parse_uri(path, allow_empty_path=True)
        root = self._dataset_root(uri.dataset)
        repo = self._repository(root)
        prefix = uri.logical_path.strip("/")
        if any(char in prefix for char in "*?["):
            # fsspec's glob already handles patterns; direct `ls` calls with
            # a wildcard should behave the same instead of treating '*' as a
            # literal path component.
            ref_label = f"{uri.ref}+staged" if uri.include_staging else uri.ref
            return self.glob(
                f"reflake://{uri.dataset}@{ref_label}/{prefix}",
                detail=detail,
            )
        resolved = self._resolve_listing(uri, repo)
        entries = repo.resolve_entries_for_prefix(
            resolved.ref,
            resolved.path,
            include_staging=uri.include_staging,
            commit_id=resolved.commit_id,
        )
        children: dict[str, dict[str, object]] = {}
        base = resolved.path.strip("/")
        for entry in entries.values():
            remainder = entry.path[len(base) :].lstrip("/") if base else entry.path
            if not remainder:
                name = entry.path
                kind = "file"
                size = entry.size
                digest = entry.hash
                identity_mode = entry.identity_mode
            elif "/" in remainder:
                child = remainder.split("/", 1)[0]
                name = f"{base}/{child}" if base else child
                kind = "directory"
                size = 0
                digest = None
                identity_mode = None
            else:
                name = entry.path
                kind = "file"
                size = entry.size
                digest = entry.hash
                identity_mode = entry.identity_mode
            ref_label = (
                f"{resolved.ref}+staged" if uri.include_staging else resolved.ref
            )
            as_uri = f"reflake://{uri.dataset}@{ref_label}/{name}"
            children[as_uri] = {
                "name": as_uri,
                "type": kind,
                "size": size,
                "hash": digest,
                "identity_mode": identity_mode,
                "commit": resolved.commit_id,
            }
        ordered: list[dict[str, object] | str] = [
            children[key] for key in sorted(children)
        ]
        if detail:
            return ordered
        return [
            str(item["name"]) for item in ordered if isinstance(item, dict)
        ]

    def _resolve_entry(self, path: str) -> ResolvedEntry:
        scope = self._resolve_scope(path, require_file=True)
        assert scope.entry is not None
        return ResolvedEntry(
            uri=scope.uri,
            root=scope.root,
            commit_id=scope.commit_id,
            entry=scope.entry,
            ref=scope.ref,
        )

    def _resolve_scope(
        self, path: str, *, require_file: bool = False
    ) -> ResolvedScope:
        uri = self._parse_uri(path, allow_empty_path=True)
        root = self._dataset_root(uri.dataset)
        repo = self._repository(root)
        last_error: Exception | None = None
        valid_ref = False
        for ref, logical_path in _reference_candidates(uri):
            try:
                commit_id = repo.resolve_ref(ref)
            except (UnknownRefError, EmptyBranchError) as error:
                last_error = error
                continue
            valid_ref = True
            entry = repo.resolve_entry(
                ref,
                logical_path,
                include_staging=uri.include_staging,
                commit_id=commit_id,
            )
            if entry is not None:
                return ResolvedScope(
                    uri=uri,
                    root=root,
                    repo=repo,
                    ref=ref,
                    path=logical_path,
                    commit_id=commit_id,
                    entry=entry,
                )
            if not require_file:
                has_children = next(
                    repo.store.iter_entries_for_prefix(
                        commit_id_tree(repo, commit_id), logical_path
                    ),
                    None,
                )
                if has_children is not None:
                    return ResolvedScope(
                        uri=uri,
                        root=root,
                        repo=repo,
                        ref=ref,
                        path=logical_path,
                        commit_id=commit_id,
                        entry=None,
                    )
        if valid_ref:
            raise FileNotFoundError(path)
        if last_error is not None:
            raise last_error
        raise FileNotFoundError(path)

    def _resolve_listing(
        self, uri: ReflakeURI, repo: ReflakeRepository
    ) -> ResolvedScope:
        last_error: Exception | None = None
        valid_ref = False
        for ref, logical_path in _reference_candidates(uri):
            try:
                commit_id = repo.resolve_ref(ref)
            except (UnknownRefError, EmptyBranchError) as error:
                last_error = error
                continue
            valid_ref = True
            if logical_path:
                has_children = next(
                    repo.store.iter_entries_for_prefix(
                        commit_id_tree(repo, commit_id), logical_path
                    ),
                    None,
                )
                if has_children is None:
                    continue
            return ResolvedScope(
                uri=uri,
                root=self._dataset_root(uri.dataset),
                repo=repo,
                ref=ref,
                path=logical_path,
                commit_id=commit_id,
                entry=None,
            )
        if valid_ref:
            raise FileNotFoundError(uri.logical_path)
        if last_error is not None:
            raise last_error
        raise FileNotFoundError(uri.logical_path)

    def _repository(self, root: str | Path) -> ReflakeRepository:
        key = str(root)
        repo = self._repositories.get(key)
        if repo is None:
            repo = open_repository(root)
            self._repositories[key] = repo
        return repo

    def _dataset_root(self, dataset: str) -> str | Path:
        if dataset in self.dataset_roots:
            return self.dataset_roots[dataset]
        _validate_uri_component(dataset, "dataset")
        if "/" in dataset or dataset in (".", ".."):
            raise ValueError("Reflake URI dataset cannot contain path separators")
        raise FileNotFoundError(f"Unknown Reflake dataset: {dataset}")

    def _parse_uri(self, path: str, *, allow_empty_path: bool = False) -> ReflakeURI:
        stripped = path[len("reflake://") :] if path.startswith("reflake://") else path
        if not stripped:
            raise ValueError("Reflake URI cannot be empty")
        if "@" not in stripped:
            raise ValueError(
                "Reflake URI must include a ref: reflake://<dataset>@<ref>/<path>"
            )
        dataset, remainder = stripped.split("@", maxsplit=1)
        _validate_uri_component(dataset, "dataset")
        if not dataset:
            raise ValueError("Reflake URI dataset cannot be empty")
        if "/" in dataset:
            raise ValueError("Reflake URI dataset cannot contain path separators")
        if dataset in (".", ".."):
            raise ValueError("Reflake URI dataset cannot be '.' or '..'")
        if "/" in remainder:
            ref_raw, logical_path = remainder.split("/", maxsplit=1)
        else:
            ref_raw, logical_path = remainder, ""
        include_staging = False
        if ref_raw.endswith("+staged"):
            include_staging = True
            ref_raw = ref_raw[: -len("+staged")]
        ref = unquote(ref_raw)
        _validate_uri_component(ref, "ref")
        if not ref:
            raise ValueError("Reflake URI ref cannot be empty")
        if ref.startswith("/") or ref.endswith("/"):
            raise ValueError("Reflake URI ref cannot start or end with '/'")
        if any(part in ("", ".", "..") for part in ref.split("/")):
            raise ValueError("Reflake URI ref contains an invalid path segment")
        logical_path = unquote(logical_path).strip("/")
        if logical_path:
            logical_path = normalize_repository_path(logical_path)
        elif not allow_empty_path:
            raise ValueError("Reflake URI must include a logical file path")
        return ReflakeURI(
            dataset=dataset,
            ref=ref,
            logical_path=logical_path,
            include_staging=include_staging,
        )

def _reference_candidates(uri: ReflakeURI):
    """Yield ``(ref, path)`` resolutions, exact ref first, then hierarchical.

    A ref may contain ``/`` (``feature/x``), which the URI grammar encodes
    ambiguously against the logical path. Prefer the exact ref, then consume
    path components into the ref until a ref exists. This makes
    ``reflake://ds@feature/x/file.parquet`` work for branch ``feature/x``
    while preserving the historical ``ref=feature, path=x/file`` meaning.
    """
    parts = [part for part in uri.logical_path.split("/") if part]
    yield uri.ref, uri.logical_path
    for index in range(1, len(parts)):
        yield f"{uri.ref}/{'/'.join(parts[:index])}", "/".join(parts[index:])
    if parts:
        # Listing a hierarchical ref with no trailing path: the whole logical
        # path may be the ref (reflake://ds@feature/x).
        yield f"{uri.ref}/{'/'.join(parts)}", ""


def commit_id_tree(repo: ReflakeRepository, commit_id: str) -> str:
    return repo.read_commit(commit_id).tree


def repo_commit(scope: ResolvedScope):
    return scope.repo.read_commit(scope.commit_id)


@dataclass(frozen=True)
class ResolvedScope:
    uri: ReflakeURI
    root: str | Path
    repo: ReflakeRepository
    ref: str
    path: str
    commit_id: str
    entry: Entry | None


def _validate_uri_component(component: str, name: str) -> None:
    _validate_no_binary(component, context=f"Reflake URI {name}")


@dataclass(frozen=True)
class ResolvedEntry:
    uri: ReflakeURI
    root: str | Path
    commit_id: str
    entry: Entry
    ref: str = ""


fsspec.register_implementation("reflake", ReflakeFileSystem)
