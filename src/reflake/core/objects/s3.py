from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, BinaryIO, Iterator

from botocore.exceptions import ClientError

from ..domain import (
    BranchRefState,
    ObjectMissingError,
    OptimisticLockError,
    PreconditionFailedError,
    RepositoryObjectKind,
    StorageUnavailableError,
)
from ..layout import object_relative_key
from ..manifest import ManifestEntry
from .backends import BlobTransferBackend
from .query import TreeCache, TreeWalker
from .source import _s3_is_404, _s3_is_precondition_failed, build_s3_client


class S3ObjectStore:
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        client: Any | None = None,
        branch_root: str | Path | None = None,
        blob_transfer: BlobTransferBackend | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client or build_s3_client()
        self.branch_root = Path(branch_root).resolve() if branch_root else None
        self.tree_cache = TreeCache()
        self._walker = TreeWalker(
            read_tree=self.read_tree_bytes,
            cache=self.tree_cache,
        )
        self._blob_transfer = blob_transfer

    @property
    def transfer_backend(self) -> BlobTransferBackend | None:
        return self._blob_transfer

    def object_uri(self, kind: RepositoryObjectKind, object_id: str) -> str:
        return f"s3://{self.bucket}/{self._key(kind, object_id)}"

    def object_path(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        raise NotImplementedError(
            "S3ObjectStore keeps objects in bucket storage; transfer plans "
            "require a local source or destination store"
        )

    def read_commit_bytes(self, commit_id: str) -> bytes | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("commit", commit_id),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise self._translate(error, "read_commit_bytes")
        return response["Body"].read()

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None:
        kwargs = {
            "Bucket": self.bucket,
            "Key": self._key("commit", commit_id),
            "Body": payload,
        }
        if if_missing:
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
        except ClientError as error:
            if if_missing and self._precondition_failed(error):
                raise OptimisticLockError(
                    f"Commit already exists: {commit_id}"
                ) from error
            raise self._translate(error, "write_commit_bytes")

    def read_tree_bytes(self, tree_hash: str) -> bytes | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("tree", tree_hash),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise self._translate(error, "read_tree_bytes")
        return response["Body"].read()

    def write_tree_bytes(
        self,
        tree_hash: str,
        payload: bytes,
        *,
        if_missing: bool = True,
    ) -> None:
        try:
            self._put_stream(
                key=self._key("tree", tree_hash),
                body=payload,
                if_missing=if_missing,
                error_message=f"Tree already exists: {tree_hash}",
            )
        except OptimisticLockError:
            if if_missing:
                return
            raise

    def write_tree_file(
        self,
        tree_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        with Path(source_path).open("rb") as handle:
            try:
                self._put_stream(
                    key=self._key("tree", tree_hash),
                    body=handle,
                    if_missing=if_missing,
                    error_message=f"Tree already exists: {tree_hash}",
                )
            except OptimisticLockError:
                if if_missing:
                    return
                raise

    def read_footer_bytes(self, footer_hash: str) -> bytes | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("footer", footer_hash),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise self._translate(error, "read_footer_bytes")
        return response["Body"].read()

    def write_footer_file(
        self,
        footer_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        with Path(source_path).open("rb") as handle:
            try:
                self._put_stream(
                    key=self._key("footer", footer_hash),
                    body=handle,
                    if_missing=if_missing,
                    error_message=f"Footer already exists: {footer_hash}",
                )
            except OptimisticLockError:
                if if_missing:
                    return
                raise

    def iter_all_entries(self, tree_hash: str) -> Iterator[ManifestEntry]:
        yield from self._walker.iter_all_entries(tree_hash)

    def lookup_entry(
        self, tree_hash: str, logical_path: str
    ) -> ManifestEntry | None:
        return self._walker.lookup_entry(tree_hash, logical_path)

    def iter_entries_for_prefix(
        self, tree_hash: str, logical_prefix: str
    ) -> Iterator[ManifestEntry]:
        yield from self._walker.iter_entries_for_prefix(tree_hash, logical_prefix)

    def read_branch_ref(self, branch: str) -> BranchRefState | None:
        key = self._key("ref", branch)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if self._missing(error):
                return None
            raise self._translate(error, "read_branch_ref")
        commit_id = response["Body"].read().decode("utf-8").strip() or None
        return BranchRefState(
            branch=branch,
            commit_id=commit_id,
            version_token=response.get("ETag", "").strip('"') or None,
        )

    def branch_path(self, branch: str) -> Path:
        if self.branch_root is not None:
            return self.branch_root / branch
        return Path(".reflake") / "refs" / "heads" / branch

    def write_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> None:
        payload = f"{commit_id}\n".encode("utf-8") if commit_id else b""
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": self._key("ref", branch),
            "Body": payload,
        }
        if if_match is not None:
            kwargs["IfMatch"] = (
                if_match if if_match.startswith('"') else f'"{if_match}"'
            )
        elif if_none_match is not None:
            kwargs["IfNoneMatch"] = if_none_match

        try:
            self.client.put_object(**kwargs)
        except ClientError as error:
            if self._precondition_failed(error):
                raise PreconditionFailedError(
                    f"Branch ref '{branch}' update failed precondition (version conflict)"
                ) from error
            raise self._translate(error, "write_branch_ref")

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_version_token: str | None,
        expected_commit_id: str | None = None,
    ) -> bool:
        # Atomic CAS: uses S3 conditional PutObject (IfMatch / IfNoneMatch)
        # to ensure no TOCTOU write races under concurrency.
        current = self.read_branch_ref(branch)
        current_version = current.version_token if current else None
        if current_version != expected_version_token:
            return False
        if expected_commit_id is not None:
            current_commit_id = current.commit_id if current else None
            if current_commit_id != expected_commit_id:
                return False

        try:
            if expected_version_token is None:
                self.write_branch_ref(branch, commit_id, if_none_match="*")
            else:
                self.write_branch_ref(
                    branch, commit_id, if_match=expected_version_token
                )
        except (PreconditionFailedError, OptimisticLockError):
            return False
        return True

    def _blob_uri(self, blob_hash: str) -> str:
        return f"s3://{self.bucket}/{self._key('blob', blob_hash)}"

    # ── Enumeration (GC) ────────────────────────────────────────────────

    def iter_branches(self) -> Iterator[str]:
        prefix = self._key("ref", "")
        paginator = self.client.get_paginator("list_objects_v2")
        try:
            pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for page in pages:
                for obj in page.get("Contents", []):
                    key = str(obj["Key"])
                    relative = key[len(prefix) :] if key.startswith(prefix) else key
                    yield relative
        except ClientError as error:
            raise self._translate(error, "list_objects")

    def iter_object_ids(self, kind: RepositoryObjectKind) -> Iterator[str]:
        if kind == "blob":
            prefix = self._key("blob", "")
            for key in self._iter_keys(prefix):
                relative = key[len(prefix) :] if key.startswith(prefix) else key
                yield relative.replace("/", "")
        elif kind in ("tree", "footer"):
            prefix = self._key(kind, "")
            for key in self._iter_keys(prefix):
                relative = key[len(prefix) :] if key.startswith(prefix) else key
                yield relative
        elif kind == "commit":
            prefix = self._key("commit", "")
            for key in self._iter_keys(prefix):
                name = key.rsplit("/", 1)[-1]
                if name.endswith(".json"):
                    yield name[: -len(".json")]

    def delete_object(self, kind: RepositoryObjectKind, object_id: str) -> None:
        try:
            self.client.delete_object(
                Bucket=self.bucket,
                Key=self._key(kind, object_id),
            )
        except ClientError as error:
            raise self._translate(error, "delete_object")

    def _iter_keys(self, prefix: str) -> Iterator[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                yield str(obj["Key"])

    # ── Blob operations ───────────────────────────────────────────────────

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        if self._blob_transfer is not None:
            with NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
            try:
                self._blob_transfer.download(self._blob_uri(blob_hash), tmp_path)
                return Path(tmp_path).read_bytes()
            finally:
                Path(tmp_path).unlink(missing_ok=True)
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("blob", blob_hash),
            )
        except ClientError as error:
            raise self._translate(error, "read_blob_bytes")
        return response["Body"].read()

    def open_blob(self, blob_hash: str) -> BinaryIO:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("blob", blob_hash),
            )
        except ClientError as error:
            raise self._translate(error, "open_blob")
        return response["Body"]

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        if self._blob_transfer is not None:
            if if_missing and self.object_exists("blob", blob_hash):
                return
            self._blob_transfer.upload(
                str(source_path),
                self._blob_uri(blob_hash),
                if_not_exists=if_missing,
            )
            return
        with Path(source_path).open("rb") as handle:
            try:
                self._put_stream(
                    key=self._key("blob", blob_hash),
                    body=handle,
                    if_missing=if_missing,
                    error_message=f"Blob already exists: {blob_hash}",
                )
            except OptimisticLockError:
                if if_missing:
                    return
                raise

    def write_blob_stream(
        self,
        blob_hash: str,
        source: BinaryIO,
        *,
        if_missing: bool = True,
    ) -> None:
        if self._blob_transfer is not None:
            temp_path: Path | None = None
            try:
                with NamedTemporaryFile(delete=False) as temp:
                    temp_path = Path(temp.name)
                    while chunk := source.read(1024 * 1024):
                        temp.write(chunk)
                if if_missing and self.object_exists("blob", blob_hash):
                    return
                assert temp_path is not None
                self._blob_transfer.upload(
                    str(temp_path),
                    self._blob_uri(blob_hash),
                    if_not_exists=if_missing,
                )
            finally:
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)
            return
        try:
            self._put_stream(
                key=self._key("blob", blob_hash),
                body=source,
                if_missing=if_missing,
                error_message=f"Blob already exists: {blob_hash}",
            )
        except OptimisticLockError:
            if if_missing:
                return
            raise

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool:
        if kind == "blob" and self._blob_transfer is not None:
            return self._blob_transfer.exists(self._blob_uri(object_id))
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(kind, object_id))
            return True
        except ClientError as error:
            if self._missing(error):
                return False
            raise self._translate(error, "object_exists")

    def version_token(self, kind: RepositoryObjectKind, object_id: str) -> str | None:
        try:
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=self._key(kind, object_id),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise self._translate(error, "version_token")
        return response.get("ETag", "").strip('"') or None

    def _key(self, kind: RepositoryObjectKind, object_id: str) -> str:
        relative_path = self._relative_path(kind, object_id)
        if self.prefix:
            return f"{self.prefix}/{relative_path}"
        return relative_path

    def _relative_path(self, kind: RepositoryObjectKind, object_id: str) -> str:
        return object_relative_key(kind, object_id)

    def _put_stream(
        self,
        *,
        key: str,
        body: BinaryIO,
        if_missing: bool,
        error_message: str,
    ) -> None:
        kwargs = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": body,
        }
        if if_missing:
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
        except ClientError as error:
            if if_missing and self._precondition_failed(error):
                raise OptimisticLockError(error_message) from error
            raise self._translate(error, "put_object")

    def _missing(self, error: ClientError) -> bool:
        return _s3_is_404(error)

    def _precondition_failed(self, error: ClientError) -> bool:
        return _s3_is_precondition_failed(error)

    def _translate(self, error: ClientError, operation: str) -> Exception:
        """Map a botocore error to a domain error (adapter boundary, §8)."""
        if self._missing(error):
            return ObjectMissingError(f"{operation}: object not found")
        if self._precondition_failed(error):
            return PreconditionFailedError(f"{operation}: precondition failed")
        return StorageUnavailableError(f"{operation}: {error}")
