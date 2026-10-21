"""Client-local worktree stat cache: the reason ``status`` and ``commit`` are
O(changes) rather than O(bytes).

Trees store content identity only (``hash`` + ``size``), deliberately: a
Merkle node must not depend on filesystem facts, or identical data ingested on
two machines would produce different tree hashes. That leaves the question
"which worktree files changed?" with no committed answer, so the client keeps
one: ``path -> (size, mtime_ns, digest)``.

A file whose ``size`` *and* ``mtime_ns`` match the cached record reuses its
recorded digest without opening the file — the same trade git's index makes
(a same-size, same-mtime rewrite is not detected; ``stat_cache false`` in the
repository config turns the optimization off and re-hashes everything).

The cache is client scope: never synced, never shared, safe to delete at any
time (it only makes the next command slower).
"""

from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile

import msgspec

#: A record is ``[size, mtime_ns, digest]``; the path is the map key.
Record = tuple[int, int, str]


class StatCache:
    """Mutable in-memory stat cache with atomic persistence."""

    def __init__(self, path: Path, *, enabled: bool = True) -> None:
        self.path = path
        self.enabled = enabled
        self._records: dict[str, Record] = {}
        self._dirty = False
        if enabled:
            self._load()

    def _load(self) -> None:
        try:
            payload = self.path.read_bytes()
        except OSError:
            return
        try:
            decoded = msgspec.json.decode(payload, type=dict[str, Record])
        except (msgspec.DecodeError, ValueError):
            # A corrupt cache is never fatal: it is a pure acceleration layer.
            return
        self._records = decoded

    def lookup(self, relative_path: str, *, size: int, mtime_ns: int) -> str | None:
        """Digest recorded for exactly this ``(size, mtime_ns)``, if any."""
        if not self.enabled:
            return None
        record = self._records.get(relative_path)
        if record is None or record[0] != size or record[1] != mtime_ns:
            return None
        return record[2]

    def record(
        self, relative_path: str, *, size: int, mtime_ns: int, digest: str
    ) -> None:
        if not self.enabled:
            return
        record = (size, mtime_ns, digest)
        if self._records.get(relative_path) != record:
            self._records[relative_path] = record
            self._dirty = True

    def forget(self, relative_path: str) -> None:
        if self._records.pop(relative_path, None) is not None:
            self._dirty = True

    def __len__(self) -> int:
        return len(self._records)

    def save(self) -> None:
        """Persist atomically; a failed write is not an error."""
        if not self.enabled or not self._dirty:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = msgspec.json.encode(self._records)
            with NamedTemporaryFile(dir=self.path.parent, delete=False) as temp:
                temp.write(payload)
                temp_path = Path(temp.name)
            temp_path.replace(self.path)
        except OSError:
            return
        self._dirty = False
