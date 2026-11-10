"""Entry materialization: staged changes, S3 sources, and canonical blobs.

``EntryFactory`` turns staged changes and S3 sources into manifest entries
(computing identity and, when needed, storing canonical blobs).  Worktree
commits are built directly by ``TreeWriter`` (``services/tree.py``), which
streams serialized tree lines without constructing entry objects.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile

from blake3 import blake3

from ..domain import ReflakeError, SourceNotFoundError, StageChange
from ..entry_codec import Entry
from ..hashing import DEFAULT_CHUNK_SIZE, blake3_digest_file
from ..objects import (
    ObjectIO,
    describe_source_uri,
    iter_s3_objects,
    open_source_range_file,
    open_source_uri,
    parse_s3_uri,
    source_client,
)
from ..repository_support import (
    matches_import_patterns,
    normalize_import_patterns,
    normalize_s3_import_path,
    pointer_identity,
)


class EntryFactory:
    def __init__(
        self,
        *,
        root: Path,
        store: ObjectIO,
        capture_footers: bool = False,
    ) -> None:
        self.root = root
        self.store = store
        self.capture_footers = capture_footers

    def _capture_footer(self, source_uri: str, size: int) -> str | None:
        if not self.capture_footers or not source_uri.lower().endswith(".parquet"):
            return None
        from ..objects.footer import capture_footer_stats

        try:
            # Ranged reads work for every source (local file or S3 object),
            # unlike an S3 streaming body, which cannot seek.
            with open_source_range_file(
                source_uri, size, client=source_client(self.store)
            ) as ranged:
                return capture_footer_stats(self.store, ranged)
        except (OSError, ValueError, ReflakeError):
            # Footer capture is best-effort metadata: never fail the ingest.
            return None

    def materialize_s3_entries(
        self,
        *,
        source_uri: str,
        identity_mode: str,
        path_patterns: list[str] | None = None,
    ) -> Iterator[Entry]:
        _, prefix = parse_s3_uri(source_uri)
        normalized_prefix = prefix.strip("/")
        normalized_patterns = normalize_import_patterns(path_patterns)
        for obj in iter_s3_objects(source_uri, client=source_client(self.store)):
            relative_path = normalize_s3_import_path(
                key=obj.key,
                prefix=normalized_prefix,
                size=obj.size,
            )
            if relative_path is None:
                continue
            if not matches_import_patterns(relative_path, normalized_patterns):
                continue
            marker: str | None = None
            if identity_mode == "content":
                identity_value = self.store_blob_from_source_uri(obj.source_uri)
            elif identity_mode == "pointer":
                marker = self.source_marker(obj.source_uri, etag=obj.etag)
                identity_value = pointer_identity(size=obj.size, marker=marker)
            else:
                raise ValueError("identity_mode must be one of: content, pointer")
            yield Entry.with_identity(
                relative_path,
                identity_value,
                obj.size,
                identity_mode,
                source_uri=obj.source_uri,
                footer=self._capture_footer(obj.source_uri, obj.size),
                source_etag=marker,
            )

    def entry_from_working_path(
        self,
        relative_path: str,
        identity_mode: str,
        *,
        store_blob: bool,
    ) -> Entry:
        source_path = self.root / relative_path
        if not source_path.exists() or not source_path.is_file():
            raise SourceNotFoundError(f"Cannot stage missing file: {relative_path}")
        stat = source_path.stat()
        source_uri = source_path.as_uri()
        footer = self._capture_footer(source_uri, stat.st_size)
        marker: str | None = None
        if identity_mode == "content":
            identity_value = blake3_digest_file(source_path)
            if store_blob:
                self.store_blob(source_path, identity_value)
        elif identity_mode == "pointer":
            digest = blake3_digest_file(source_path)
            marker = f"blake3:{digest}"
            identity_value = pointer_identity(size=stat.st_size, marker=marker)
        else:
            raise ValueError("identity_mode must be one of: content, pointer")
        return Entry.with_identity(
            relative_path,
            identity_value,
            stat.st_size,
            identity_mode,
            source_uri=source_uri if identity_mode == "pointer" else None,
            footer=footer,
            source_etag=marker if identity_mode == "pointer" else None,
        )

    def entry_from_stage_change(
        self,
        change: StageChange,
        identity_mode: str,
        *,
        store_blob: bool,
    ) -> Entry:
        """Turn a staged change into an entry without re-reading a snapshot.

        ``add`` already recorded everything a content commit needs (blob hash,
        size, footer) and everything a pointer commit needs (source URI,
        version metadata), so the common path performs no I/O at all.
        """
        mode = change.identity_mode or identity_mode
        if change.blob_hash and change.size is not None:
            # A change staged with a known identity (add, or a move that
            # preserved the committed entry) is applied verbatim: moving a
            # pointer must not silently change what it points at.
            return Entry.with_identity(
                change.path,
                change.blob_hash,
                change.size,
                mode,
                source_uri=change.source_uri if mode == "pointer" else None,
                footer=change.footer,
                source_etag=change.source_etag,
            )
        if mode == "pointer" and change.size is not None and change.source_uri:
            marker = change.source_etag
            if marker is None:
                marker = self.source_marker(change.source_uri)
            return Entry.with_identity(
                change.path,
                pointer_identity(size=change.size, marker=marker),
                change.size,
                "pointer",
                source_uri=change.source_uri,
                footer=change.footer,
                source_etag=marker,
            )
        # Fallback (hand-built staging, or a change staged by an older
        # client): materialize from the source.
        if change.source_uri is not None:
            return self.entry_from_source_uri(
                logical_path=change.path,
                source_uri=change.source_uri,
                identity_mode=mode,
                store_blob=store_blob,
            )
        source_path = self.root / change.path
        if source_path.exists():
            return self.entry_from_working_path(
                change.path,
                mode,
                store_blob=store_blob,
            )
        raise SourceNotFoundError(f"Cannot stage missing file: {change.path}")

    def entry_from_source_uri(
        self,
        *,
        logical_path: str,
        source_uri: str,
        identity_mode: str,
        store_blob: bool,
    ) -> Entry:
        metadata = describe_source_uri(source_uri, client=source_client(self.store))
        footer = self._capture_footer(source_uri, metadata.size)
        marker: str | None = None
        if identity_mode == "content":
            identity_value = self.store_blob_from_source_uri(source_uri)
        elif identity_mode == "pointer":
            marker = self.source_marker(
                source_uri, etag=metadata.etag, client=source_client(self.store)
            )
            identity_value = pointer_identity(size=metadata.size, marker=marker)
        else:
            raise ValueError("identity_mode must be one of: content, pointer")
        return Entry.with_identity(
            logical_path,
            identity_value,
            metadata.size,
            identity_mode,
            source_uri=metadata.source_uri if identity_mode == "pointer" else None,
            footer=footer,
            source_etag=marker,
        )

    def source_marker(
        self,
        source_uri: str,
        *,
        etag: str | None = None,
        client: object | None = None,
    ) -> str:
        """Immutable source-version marker for a pointer entry.

        S3 objects use their ETag (cheap metadata). Local and fsspec sources
        use a content digest: mtime is not an identity, because the same
        bytes on two machines, or after a `touch`, must not produce a
        different tree. The digest is computed once at ingest and stored on
        the entry; no canonical blob is written.
        """
        if source_uri.startswith("s3://") and etag:
            return etag
        hasher = blake3()
        with open_source_uri(source_uri, client=client) as source:
            while chunk := source.read(DEFAULT_CHUNK_SIZE):
                hasher.update(chunk)
        return f"blake3:{hasher.hexdigest()}"

    def store_blob(self, source_file: Path, content_hash: str) -> None:
        self.store.write_blob_file(content_hash, source_file, if_missing=True)

    def store_blob_from_source_uri(self, source_uri: str) -> str:
        """Materialize an external source as a canonical blob; return its hash.

        S3 -> S3 stays inside the bucket: the client reads the object once to
        compute the digest, then copies it server-side (`CopyObject`) into the
        blob key. The previous implementation downloaded to a temp file and
        re-uploaded, i.e. 2x the bytes plus temp disk equal to the object.
        """
        if source_uri.startswith("s3://"):
            digest = self._hash_source_uri(source_uri)
            if self.store.object_exists("blob", digest):
                return digest
            copy = getattr(self.store, "copy_blob_from", None)
            if copy is not None and copy(source_uri, digest):
                return digest

        temp_path: Path | None = None
        try:
            with NamedTemporaryFile(mode="wb", delete=False) as temp:
                temp_path = Path(temp.name)
                hasher = blake3()
                with open_source_uri(
                    source_uri, client=source_client(self.store)
                ) as source:
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

    def _hash_source_uri(self, source_uri: str) -> str:
        """Stream a source and return its digest, buffering nothing."""
        hasher = blake3()
        with open_source_uri(source_uri, client=source_client(self.store)) as source:
            while True:
                chunk = source.read(DEFAULT_CHUNK_SIZE)
                if not chunk:
                    return hasher.hexdigest()
                hasher.update(chunk)
