from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, BinaryIO

import fsspec
from fsspec.spec import AbstractFileSystem

from ..entry_codec import Entry
from ..objects import open_source_uri
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
        return path[len("reflake://") :] if path.startswith("reflake://") else path

    def _open(  # type: ignore[bad-override]
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        **kwargs: object,
    ) -> IO[bytes]:
        if mode != "rb":
            raise NotImplementedError(
                "ReflakeFileSystem currently supports read-only binary mode"
            )
        resolved = self._resolve_entry(path)
        if resolved.entry.blob_hash:
            repo = self._repository(resolved.root)
            return _BlobReadFile(repo.open_blob_stream(resolved.entry.blob_hash))  # type: ignore[bad-return]
        if resolved.entry.source_uri:
            return _SourceURIFile(open_source_uri(resolved.entry.source_uri))  # type: ignore[bad-return]
        raise FileNotFoundError(
            "Entry has no canonical blob hash and no readable source URI"
        )

    def exists(self, path: str, **kwargs: object) -> bool:
        try:
            self._resolve_entry(path)
            return True
        except (FileNotFoundError, ValueError):
            return False

    def info(self, path: str, **kwargs: object) -> dict[str, object]:
        resolved = self._resolve_entry(path)
        return {
            "name": path,
            "type": "file",
            "size": resolved.entry.size,
            "hash": resolved.entry.hash,
            "identity_mode": resolved.entry.identity_mode,
            "mtime_ns": resolved.entry.mtime_ns,
            "commit": resolved.commit_id,
            "staged": resolved.uri.include_staging,
            "dataset": resolved.uri.dataset,
        }

    def ls(
        self, path: str, detail: bool = True, **kwargs: object
    ) -> list[dict[str, object] | str]:
        uri = self._parse_uri(path, allow_empty_path=True)
        root = self._dataset_root(uri.dataset)
        repo = self._repository(root)

        normalized_prefix = uri.logical_path.strip("/")
        if normalized_prefix == "*":
            normalized_prefix = ""
        entries = repo.resolve_entries_for_prefix(
            uri.ref,
            normalized_prefix,
            include_staging=uri.include_staging,
        )
        results: list[dict[str, object] | str] = []
        for entry in entries.values():
            as_uri = f"reflake://{uri.dataset}@{uri.ref}/{entry.path}"
            if detail:
                results.append(
                    {
                        "name": as_uri,
                        "type": "file",
                        "size": entry.size,
                        "hash": entry.hash,
                        "identity_mode": entry.identity_mode,
                    }
                )
            else:
                results.append(as_uri)
        return results

    def _resolve_entry(self, path: str) -> ResolvedEntry:
        uri = self._parse_uri(path)
        root = self._dataset_root(uri.dataset)
        repo = self._repository(root)
        commit_id = repo.resolve_ref(uri.ref)
        entry = repo.resolve_entry(
            uri.ref,
            uri.logical_path,
            include_staging=uri.include_staging,
            commit_id=commit_id,
        )
        if entry is None:
            raise FileNotFoundError(path)
        return ResolvedEntry(uri=uri, root=root, commit_id=commit_id, entry=entry)

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
        stripped = self._strip_protocol(path)
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
        ref = ref_raw
        if ref_raw.endswith("+staged"):
            include_staging = True
            ref = ref_raw[: -len("+staged")]
        _validate_uri_component(ref, "ref")
        if not ref:
            raise ValueError("Reflake URI ref cannot be empty")
        if "/" in ref:
            raise ValueError("Reflake URI ref cannot contain path separators")
        if ref in (".", ".."):
            raise ValueError("Reflake URI ref cannot be '.' or '..'")
        logical_path = logical_path.strip("/")
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


def _validate_uri_component(component: str, name: str) -> None:
    _validate_no_binary(component, context=f"Reflake URI {name}")


@dataclass(frozen=True)
class ResolvedEntry:
    uri: ReflakeURI
    root: str | Path
    commit_id: str
    entry: Entry


class _SourceURIFile(io.IOBase):
    def __init__(self, context_manager: Any) -> None:
        self._context_manager = context_manager
        self._handle = context_manager.__enter__()

    def read(self, size: int = -1) -> bytes:
        return self._handle.read(size)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return bool(getattr(self._handle, "seekable", lambda: False)())

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._handle.seek(offset, whence)

    def tell(self) -> int:
        return self._handle.tell()

    def close(self) -> None:
        if self.closed:
            return
        try:
            close = getattr(self._handle, "close", None)
            if callable(close):
                close()
        finally:
            self._context_manager.__exit__(None, None, None)
            super().close()


class _BlobReadFile(io.IOBase):
    def __init__(self, handle: BinaryIO) -> None:
        self._handle = handle

    def read(self, size: int = -1) -> bytes:
        return self._handle.read(size)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return bool(getattr(self._handle, "seekable", lambda: False)())

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._handle.seek(offset, whence)

    def tell(self) -> int:
        return self._handle.tell()

    def close(self) -> None:
        if self.closed:
            return
        try:
            close = getattr(self._handle, "close", None)
            if callable(close):
                close()
        finally:
            super().close()


fsspec.register_implementation("reflake", ReflakeFileSystem)
