"""Integrity audit: refs -> commits -> trees -> leaves, plus object existence.

`fsck` answers two questions a content-addressed store cannot answer by itself:

1. *Is the metadata graph self-consistent and parseable?* Every ref resolves,
   every commit parses, every tree node parses (sorted, duplicate-free,
   single-component names), and every referenced object exists.
2. *Do the bytes still match their names?* Opt-in (`--verify-blobs`), because
   it reads every blob.

The existence check adapts like sync planning: per-object probes for small
reachable sets, one inventory listing per kind for large ones, so a 1M-file
dataset is a few thousand LIST pages rather than a million HEADs.
"""

from __future__ import annotations

from .concurrency import map_bounded
from .domain import FsckResult, RepositoryObjectKind, UnknownCommitError
from .entry_codec import CorruptEntryError
from .hashing import blake3_digest_stream
from .objects.base import ObjectIO, RepositoryStore
from .objects.tree import parse_tree_object

#: Above this many reachable objects of one kind, list the store once instead
#: of probing each id (same trade as sync planning).
_PROBE_THRESHOLD = 64


def _missing_objects(
    store: RepositoryStore, kind: RepositoryObjectKind, object_ids: set[str]
) -> list[str]:
    if not object_ids:
        return []
    if len(object_ids) <= _PROBE_THRESHOLD:
        return sorted(
            object_id
            for object_id in object_ids
            if not store.object_exists(kind, object_id)
        )
    present = {
        object_id
        for object_id in store.iter_object_ids(kind)
        if object_id in object_ids
    }
    return sorted(object_ids - present)


def _walk_trees(
    store: ObjectIO,
    roots: set[str],
    *,
    trees: set[str],
    blobs: set[str],
    footers: set[str],
    corrupt: list[str],
    leaves: list[int],
    pointers: list[int],
) -> None:
    stack = list(roots)
    while stack:
        tree_hash = stack.pop()
        if tree_hash in trees:
            continue
        trees.add(tree_hash)
        try:
            payload = store.read_tree_bytes(tree_hash)
        except ValueError:
            # Hash verification failure: the bytes are not what the key says.
            corrupt.append(f"tree:{tree_hash}")
            continue
        if payload is None:
            continue  # reported as missing by the existence pass
        try:
            entries = parse_tree_object(payload)
        except (CorruptEntryError, ValueError):
            corrupt.append(f"tree:{tree_hash}")
            continue
        for entry in entries:
            if entry.is_subtree:
                stack.append(entry.hash)
                continue
            leaves[0] += 1
            if entry.blob_hash is not None:
                blobs.add(entry.blob_hash)
            else:
                pointers[0] += 1
            if entry.footer is not None:
                footers.add(entry.footer)


def _verify_blob(store: ObjectIO, blob_hash: str) -> str | None:
    """Return the hash when the stored bytes do not match their name."""
    try:
        with store.open_blob(blob_hash) as handle:
            actual = blake3_digest_stream(handle)
    except (OSError, FileNotFoundError):
        return None  # missing objects are already reported by the existence pass
    return None if actual == blob_hash else blob_hash


def repo_fsck(
    repo: object,
    *,
    ref: str | None = None,
    verify_blobs: bool = False,
) -> FsckResult:
    """Audit the metadata graph (and optionally the blob bytes) of *repo*."""
    from .repository import ReflakeRepository

    assert isinstance(repo, ReflakeRepository)
    store = repo.store
    repo.refs.clear_commit_cache()

    corrupt: list[str] = []
    refs: list[str] = []
    if ref is not None:
        refs = [repo.resolve_ref(ref)]
    else:
        for branch in store.iter_branches():
            state = store.read_branch_ref(branch)
            if state is not None and state.commit_id:
                refs.append(state.commit_id)
        for tag in store.iter_tags():
            target = store.read_tag_ref(tag)
            if target:
                refs.append(target)

    commits: set[str] = set()
    tree_roots: set[str] = set()
    stack = list(refs)
    while stack:
        commit_id = stack.pop()
        if commit_id in commits:
            continue
        commits.add(commit_id)
        try:
            commit = repo.read_commit(commit_id)
        except UnknownCommitError:
            continue  # reported as a missing object below
        except ValueError:
            corrupt.append(f"commit:{commit_id}")
            continue
        tree_roots.add(commit.tree)
        stack.extend(commit.parents)

    trees: set[str] = set()
    blobs: set[str] = set()
    footers: set[str] = set()
    leaves = [0]
    pointers = [0]
    _walk_trees(
        store,
        tree_roots,
        trees=trees,
        blobs=blobs,
        footers=footers,
        corrupt=corrupt,
        leaves=leaves,
        pointers=pointers,
    )

    missing: list[str] = []
    missing += [f"commit:{cid}" for cid in _missing_objects(store, "commit", commits)]
    missing += [f"tree:{hid}" for hid in _missing_objects(store, "tree", trees)]
    missing += [f"blob:{bid}" for bid in _missing_objects(store, "blob", blobs)]
    missing += [f"footer:{fid}" for fid in _missing_objects(store, "footer", footers)]

    verified = 0
    corrupt_blobs: list[str] = []
    if verify_blobs and blobs:
        missing_blobs = {
            item.split(":", 1)[1] for item in missing if item.startswith("blob:")
        }
        candidates = sorted(blobs - missing_blobs)
        failures = map_bounded(
            lambda blob_hash: _verify_blob(store, blob_hash), candidates
        )
        corrupt_blobs = [failure for failure in failures if failure is not None]
        verified = len(candidates)

    return FsckResult(
        refs=len(refs),
        commits=len(commits),
        trees=len(trees),
        leaves=leaves[0],
        blobs=len(blobs),
        footers=len(footers),
        pointer_entries=pointers[0],
        missing_objects=sorted(missing),
        corrupt_objects=sorted(set(corrupt)),
        verified_blobs=verified,
        corrupt_blobs=sorted(corrupt_blobs),
    )
