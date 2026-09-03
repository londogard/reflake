"""Entry materialization: staged changes, S3 sources, and canonical blobs.

``EntryFactory`` turns staged changes and S3 sources into manifest entries
(computing identity and, when needed, storing canonical blobs).  Worktree
commits are built directly by ``TreeWriter`` (``services/tree.py``), which
streams serialized tree lines without constructing entry objects.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator

from blake3 import blake3

from ..domain import StageChange
from ..hashing import DEFAULT_CHUNK_SIZE, blake3_digest_file
from ..manifest import ManifestEntry
from ..repository_support import (
    matches_import_patterns,
    metadata_identity,
    normalize_import_patterns,
    normalize_s3_import_path,
)
from ..objects import (
    ObjectStore,
    describe_source_uri,
    iter_s3_objects,
    open_source_uri,
    parse_s3_uri,
)


class EntryFactory:
    def __init__(
        self,
        *,
        root: Path,
        store: ObjectStore,
        capture_footers: bool = False,
    ) -> None:
        self.root = root
        self.store = store
        self.capture_footers = capture_footers

    def _capture_footer(self, source_uri: str) -> str | None:
        if not self.capture_footers or not source_uri.lower().endswith(".parquet"):
            return None
        from ..objects.footer import capture_footer_stats
        from ..objects import open_source_uri

        try:
            with open_source_uri(source_uri) as handle:
                return capture_footer_stats(self.store, handle)
        except (OSError, ValueError):
            return None

    def materialize_s3_entries(
        self,
        *,
        source_uri: str,
        identity_mode: str,
        path_patterns: list[str] | None = None,
    ) -> Iterator[ManifestEntry]:
        _, prefix = parse_s3_uri(source_uri)
        normalized_prefix = prefix.strip("/")
        normalized_patterns = normalize_import_patterns(path_patterns)
        for obj in iter_s3_objects(source_uri):
            relative_path = normalize_s3_import_path(
                key=obj.key,
                prefix=normalized_prefix,
                size=obj.size,
            )
            if relative_path is None:
                continue
            if not matches_import_patterns(relative_path, normalized_patterns):
                continue
            if identity_mode == "blake3":
                identity_value = self.store_blob_from_source_uri(obj.source_uri)
            elif identity_mode == "meta":
                identity_value = metadata_identity(relative_path, obj.size)
            else:
                raise ValueError("identity_mode must be one of: blake3, meta")
            yield ManifestEntry(
                path=relative_path,
                hash=identity_value,
                size=obj.size,
                mtime_ns=obj.mtime_ns,
                identity_mode=identity_mode,
                source_uri=obj.source_uri,
            )

    def entry_from_working_path(
        self,
        relative_path: str,
        identity_mode: str,
        *,
        store_blob: bool,
    ) -> ManifestEntry:
        source_path = self.root / relative_path
        if not source_path.exists() or not source_path.is_file():
            raise FileNotFoundError(f"Cannot stage missing file: {relative_path}")
        stat = source_path.stat()
        source_uri = source_path.as_uri()
        footer = self._capture_footer(source_uri)
        if identity_mode == "blake3":
            identity_value = blake3_digest_file(source_path)
            if store_blob:
                self.store_blob(source_path, identity_value)
        elif identity_mode == "meta":
            identity_value = metadata_identity(relative_path, stat.st_size)
        else:
            raise ValueError("identity_mode must be one of: blake3, meta")
        return ManifestEntry(
            path=relative_path,
            hash=identity_value,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            identity_mode=identity_mode,
            source_uri=source_uri if identity_mode == "meta" else None,
            footer=footer,
        )

    def entry_from_stage_change(
        self,
        change: StageChange,
        identity_mode: str,
        *,
        store_blob: bool,
    ) -> ManifestEntry:
        if change.source_uri is not None:
            return self.entry_from_source_uri(
                logical_path=change.path,
                source_uri=change.source_uri,
                identity_mode=identity_mode,
                store_blob=store_blob,
            )
        source_path = self.root / change.path
        if source_path.exists():
            return self.entry_from_working_path(
                change.path,
                identity_mode,
                store_blob=store_blob,
            )
        if change.blob_hash and identity_mode == "blake3":
            return ManifestEntry(
                path=change.path,
                hash=change.blob_hash,
                size=change.size or 0,
                mtime_ns=0,
                identity_mode="blake3",
            )
        raise FileNotFoundError(f"Cannot stage missing file: {change.path}")

    def entry_from_source_uri(
        self,
        *,
        logical_path: str,
        source_uri: str,
        identity_mode: str,
        store_blob: bool,
    ) -> ManifestEntry:
        metadata = describe_source_uri(source_uri)
        footer = self._capture_footer(source_uri)
        if identity_mode == "blake3":
            identity_value = self.store_blob_from_source_uri(source_uri)
        elif identity_mode == "meta":
            identity_value = metadata_identity(logical_path, metadata.size)
        else:
            raise ValueError("identity_mode must be one of: blake3, meta")
        return ManifestEntry(
            path=logical_path,
            hash=identity_value,
            size=metadata.size,
            mtime_ns=metadata.mtime_ns,
            identity_mode=identity_mode,
            source_uri=metadata.source_uri if identity_mode == "meta" else None,
            footer=footer,
        )

    def store_blob(self, source_file: Path, content_hash: str) -> None:
        self.store.write_blob_file(content_hash, source_file, if_missing=True)

    def store_blob_from_source_uri(self, source_uri: str) -> str:
        temp_path: Path | None = None
        try:
            with NamedTemporaryFile(mode="wb", delete=False) as temp:
                temp_path = Path(temp.name)
                hasher = blake3()
                with open_source_uri(source_uri) as source:
                    while True:
                        chunk = source.read(DEFAULT_CHUNK_SIZE)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        temp.write(chunk)
            digest = hasher.hexdigest()
            if self.store.object_exists("blob", digest):
                return digest
            assert temp_path is not None
            self.store.write_blob_file(digest, temp_path, if_missing=True)
            return digest
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
