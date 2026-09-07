# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and Reflake currently tracks changes before its first public alpha release.

## Unreleased

### Breaking pre-1.0 cleanup

No backcompat is kept for anything below — this is the last window for it.

- **Commit IDs are content-only**: `id = blake3(message, tree, parents)`.
  `branch`, `generation` (DAG-derivable perf hint, still stored), and
  `created_at` are recorded, not hashed — identical content yields identical
  ids across branches and retries are idempotent. Stored commit objects are
  compact canonical JSON instead of pretty-printed (parsed identically).
- **Identity modes renamed**: `blake3` → `content`, `meta` → `pointer`
  (persisted in every entry, CLI `--identity`, and config). The algorithm
  name no longer leaks into UX; `pointer` says what it is.
- **Single `Entry` record** (`core/entry_codec.py`): the `LeafRecord` /
  `ManifestEntry` / `TreeEntry` trio is gone — one frozen dataclass with
  `kind`-derived `identity_mode`/`blob_hash`, plus `CorruptEntryError` to
  distinguish unparseable lines from invalid entries. Validation is kept off
  the hot path: C-speed hex/path checks, and a plain `encode_leaf_parts()`
  serializer for producer-guaranteed values — every `Entry` instance stays
  valid, no exceptions. The 1M-file commit benchmark holds at ~19k files/sec.
- **CAS on commit ids, version tokens deleted**: `BranchRefState` is
  `(branch, commit_id)`; `compare_and_set_branch_ref` takes only
  `expected_commit_id` (`None` = must-not-exist). S3 keeps its ETag purely
  internal. Client snapshots, `RefManager` caches, and the ETag-quoting code
  collapse accordingly.
- **`branch_path()` removed** from the store protocol (`RefManager.branch()`
  returns the branch name); local locking is portable (`fcntl`/`msvcrt`).
- **`TransferPlan` replaces batch tuples**: `BlobTransferBackend` now has one
  `transfer(plan)` entry point with a `direction` enum; s5cmd rejects paths
  containing newlines (line-delimited manifest injection hardening).
- **Open never mutates**: `open_repository()` defaults to `must_exist=True`
  (`NotARepositoryError` otherwise); all creation goes through new
  `init_repository()` (idempotent, git-style) / `create_repository()`.
  The implicit default-config write on open is gone. `pull`/`fetch` bootstrap
  an empty local dir (clone flow); `push` requires a repo.
- **Global `--repo` / `--json`** (`reflake --repo <uri> [--json] <command>`);
  per-command copies removed.
- **`catalog` → `branches`** (plus `ls` alias for `list`).
- **`verify` is read-only** (exits non-zero while pointer entries remain);
  new **`promote`** command does the materializing commit.
- **Exit-code contract**: `0` ok · `1` usage/validation · `2` retryable
  conflict (`RefConflictError`, `NonFastForwardError`, `MergeConflictError`)
  · `3` missing ref/object; `--json` failures emit an error envelope on
  stderr.
- **Public Python API frozen** (see README): `init/create/open_repository`,
  `ReflakeRepository`, `ReflakeFileSystem`, `push`/`pull`/`fetch`, error
  types. `core.services` / `core.objects` internals carry no stability
  guarantees.

### Changed

- **Deterministic commit IDs**: the commit id now hashes only content
  — `created_at` is recorded in the payload but excluded from the hash, so
  identical content yields identical ids and retries are idempotent. Stored
  commit objects are now compact canonical JSON instead of pretty-printed
  (parsed identically). Old commits remain readable; only newly created ids
  use the new scheme.
- **Structural refactor — `TreeInspector` split from `TreeWriter`**:
  GC enumeration (`iter_tree_hashes`, `iter_leaf_refs`) and the
  derived-manifest cache live in `core/services/tree_inspect.py`;
  `TreeWriter` keeps thin delegating methods, so existing callers are
  unaffected and new code can depend on the read-only inspector.
- **Structural refactor — transfer endpoints out of `StoreInventory`**:
  `object_path` / `object_uri` moved to `HasLocalPath` / `HasRemoteURI`;
  `S3ObjectStore` no longer raises `NotImplementedError` for a method it
  cannot provide, and sync planning fails with a clear `TypeError` instead.
  Services now annotate narrow compositions (`RefObjectStore`,
  `ContentQueryStore`, `QueryRefStore`, `RepositoryStore`).
- **Batch sync is direction-aware**: `BlobTransferBackend` executes a
  `TransferPlan` (`direction` + items) through one `transfer()` entry point;
  `pull`/`fetch` through `s5cmd` no longer issue uploads for downloads, and
  batch paths are rejected if they contain newlines.
- Local blob writes are atomic (temp file + rename, verified hash) and
  hash-verified on write (`ValueError` on mismatch, no partial residue);
  the S3 buffered transfer path verifies too. Fixes torn-blob poisoning
  where a crashed write could stick forever behind `if_missing` guards.
- **Structural refactor — capability-split store protocols**: `ObjectStore` is
  now the composition of `ObjectIO`, `RefCas`, `TreeQuery`, and
  `StoreInventory`; services annotate against the narrow capability they use
  (e.g. sync transfers and footer capture depend on `ObjectIO` only).
- **Structural refactor — unified key-space**: `layout.object_relative_key()`
  is the single mapping from `(kind, id)` to a repository-relative location;
  the local filesystem layout and S3 key scheme both derive from it and can no
  longer drift.
- **Structural refactor — client state out of the shared refs namespace**:
  per-branch snapshots moved from `refs/heads/<branch>.json` to
  `state/branch-snapshots/<branch>.json`, so client-local files can never be
  mistaken for branch pointers (`iter_branches` no longer filters `.json`).
  Existing alpha clones with stale snapshot files can delete them safely.
- **`merge_sorted_streams` helper**: canonical k-way merge of path-sorted
  entry streams; staged overlays now build on it, keeping the tree sortedness
  invariant in one place.
- **Unified leaf-entry codec** (`core/entry_codec.py`): kind derivation, shape
  dispatch, field rules, encoding, and decoding now live in exactly one module
  shared by manifests and trees; adding a leaf attribute touches the codec and
  the record types instead of every serializer.
- **Slimmer entries, then a single `Entry`** *(breaking)*: `identity_value`
  and `blob_hash` were derived properties first (identity is always `hash`;
  the blob hash exists exactly when the entry is content-backed), then the
  whole `LeafRecord` / `ManifestEntry` / `TreeEntry` trio collapsed into one
  `Entry` record. Blob-backed entries can no longer carry `source_uri` —
  previously it was stored in memory but silently dropped on serialization.
- **Repository facade removal** *(breaking)*: the ~20 module-level convenience
  functions (`commit(root, ...)`, `add(root, ...)`, `cat`, `catalog`, …) are
  gone; the Python API is `open_repository()` plus `ReflakeRepository`
  methods, which now include `move_staged`, `cat`, `reflog`, and `branches`.
  `repository_ops` imports its types from `domain`, eliminating all runtime
  circular imports.
- New typed domain errors: `UnknownRefError`, `EmptyBranchError`,
  `UnknownCommitError` (all `ReflakeError` subclasses) replace bare
  `ValueError`s with magic strings.
- `BlobTransferBackend` gains `supports_batch()` / `upload_batch()` so sync no
  longer dispatches on backend class names.
- The three-way metadata merge streams merged entries into tree construction
  instead of materializing the full result in memory.
- `status --working-tree` skips content hashing when file sizes already differ.
- Removed dead code: unused `RepositoryObjectKind` members, duplicate
  `AnalyticalIndexPaths` definition, unreachable manifest validation branches,
  and the hidden `_leftover_additions_dirs` side channel.
- **CI quality gates**: `ruff check` + `pyrefly check` run on every push/PR,
  tests matrix across Python 3.11/3.12/3.13, clean-tree check ignores `dist/`,
  CI concurrency cancels superseded runs. `bench/bench.txt` moved out of
  `.github/workflows/`.
- **Operator runbook rewritten** for the v2 tree model and CAS-only
  concurrency: lock/`manifests/*.idx` procedures removed, `trees/` +
  `footers/` backup layout, `gc --prune` and footer-backfill operations
  documented.

### Fixed

- **`gc --prune` no longer deletes merged lineage**: reachability now walks
  *all* commit parents (BFS from every branch head) instead of the
  first-parent chain, so second parents of 3-way merge commits — and their
  trees/blobs — survive pruning. Tree walks are memoized across commits.
- **`commit --staged` no longer fails when a newly staged path sorts before an
  existing one**: the staged overlay now merges the parent tree and the
  additions as two sorted streams instead of appending leftovers at the end.
- **Merge-aware ancestry everywhere**: `is_ancestor`, fast-forward checks,
  merge-base computation, and push/pull commit collection now traverse *all*
  parent edges. Previously only first parents were walked, so after a 3-way
  merge a branch could not fast-forward to its own merge commit, and push
  silently omitted the merged-in lineage's commits.
- **`S3Config.endpoint_url` is now honored** when building boto3 clients and
  s5cmd invocations (previously stored but ignored; only ambient AWS env config
  worked).
- Source-URI reads for `file://` URIs decode percent-escaped paths, so files
  with quotes or spaces in their names can be staged, restored, and read back.
- Temp files are cleaned up when blob ingestion from a source URI or S3 stream
  is interrupted mid-copy.
- Derived-manifest index keys are extracted with real JSON parsing, so paths
  containing quote characters no longer corrupt block lookups.
- Local commits, branch refs, and repository config are written atomically
  (temp file + rename), preventing torn objects on crash.
- `transfer --execute` runs generated commands via argument lists instead of
  `shell=True`.
- `reflake gc`, sync planning, and tree-walk lookups are fully covered by the
  `ObjectStore` protocol; the type checker reports zero errors again.

## [0.1.0] - 2026-08-16

### Added

- **Footer-stats pruning engine (rev. 3, docs/architecture.md §4)**: `reflake query
  prune <ref> <path> --where "col >= x"` selects the parquet row groups that may
  match an AND-composed predicate (`= != < <= > >= IS NULL IS NOT NULL`) using only
  the compact `footers/<hash>` stats objects — no data bytes are read. Exposed as
  `parse_where_clause` / `prune_row_groups` / `plan_pruned_scan` in
  `core/query/pruning.py`; unknown columns and type mismatches keep row groups
  conservatively.
- **Store unification (rev. 3, §12)**: `core/repository_store/` and `core/storage/`
  are merged into `core/objects/` — one `ObjectStore` protocol
  (`objects/base.py`), two adapters (`LocalObjectStore`, `S3ObjectStore`), plus
  `backends.py` (storage/transfer protocols), `source.py` (source-URI access,
  `S3StorageBackend`), and `transfer.py` (boto3/s5cmd blob transfer).
- **Parquet footer capture (P1)**: with `config set parquet_footer true`, parquet
  files get a compact footer-stats object (schema hash + per-row-group column
  min/max/nulls, hand-decoded from the thrift-compact `FileMetaData`) stored
  under `footers/<hash>` and referenced from new `bp`/`mp` tree entries —
  enabling row-group pruning without reading object bytes. Unchanged files are
  backfilled on the next commit; `verify`-style re-scan can backfill later.
- `reflake cat <ref> <path>` prints a file's bytes from a ref; `list`, `cat`,
  `diff`, `log`, and `query` work on virtual refs (S3 URIs) without a local
  worktree.
- Streaming S3 import with metadata identity mode and repeatable path filters.
- Staged add support for arbitrary local files, directories, S3 objects, and S3 prefixes.
- Real S3 integration coverage for remote repository flows.
- AGPL-3.0-or-later licensing, attribution notice, and funding metadata for the first public release.
- **Tree-based object model (v2, P0 of `docs/architecture.md`)**: commits now
  build Merkle tree DAGs (`trees/`) instead of full JSONL manifests. Exact
  lookups descend the path chain through a client-side content-addressed
  tree cache (0–1 GETs warm); prefix/bulk listings stream from pruned
  subtree walks (O(matches + depth)); the `.idx` sidecar and embedded
  `commit.manifest_index` are gone.
- Derived manifests: a tree can be flattened to a JSONL manifest with block
  offsets, cached per-client keyed by the root-tree hash, for
  latency-critical point lookups (`export_derived_manifest`,
  `lookup_derived_entry`).
- Directory fanout is bounded (10k entries); oversized directories split
  into name-range shard subtrees, keeping tree depth ≤ ~4–5 at 100M entries.
- `commit --staged` re-applies the staged overlay onto a new parent and
  retries the CAS when a peer advanced the branch (P0 conflict-retry
  contract, `docs/architecture.md` §6).

### Changed

- **3-way metadata merge (P3, §9)**: diverged branches merge instead of
  erroring — `merge` computes the merge base (LCA), streams base/ours/theirs
  trees with standard merge rules (auto-keep identical additions, take the
  changed side, drop double-removals), and creates a merge commit with two
  parents when histories diverge; conflicting paths raise
  `MergeConflictError` with the offending paths.
- `reflake reflog` (P3): every successful ref update is recorded per-branch in
  client state (old → new commit, operation, timestamp); `reflake catalog`
  lists branches with their heads and messages.
- **Plan-then-batch sync (P2, §7)**: `push`/`pull`/`fetch` now compute the exact
  missing-object set (commits, trees, footers, blobs) as a transfer plan and
  execute it in one batch through the s5cmd backend (manifest file) or
  per-object through boto3, with an optional progress callback. Parquet
  footer stats objects now sync too.
- **Adapter error translation (P2, §8)**: the S3 adapters raise domain errors
  (`ObjectMissingError`, `PreconditionFailedError`, `StorageUnavailableError`,
  all `ReflakeError` subclasses) instead of raw botocore exceptions; the CLI no
  longer special-cases `BotoCoreError`/`ClientError`.
- `reflake gc` (P2, §11): audit-only by default — computes the reachable set
  from all refs (commit DAG → trees → blobs/footers) and reports orphans;
  `--prune` deletes them.
- `filesystem.py` moved to the `core/vfs/` package; `index.py` moved to the
  `core/query/` package (DuckDB catalog now carries a `footer` column); the
  CLI subcommand `index build` is now `query build`.
- `CommitObject` is now `{id, message, tree, parents: [...], created_at,
  branch, generation}`; the `manifest`/`parent`/`manifest_index` fields are
  gone. `SnapshotWriter` is replaced by `TreeWriter`
  (`services/tree.py`).
- Stores are `ObjectStore + RefStore + TreeQuerier`; S3 branch-lock
  machinery (locks, `lock list`, `lock cleanup`) is removed — version-token
  CAS is the only safety primitive.
- Full commits preserve parent-only entries and staged additions (v1 merge
  semantics); deletions require explicit staging, as before.
- Commit creation is faster than v1 in metadata mode (no per-file entry
  objects; ~48k files/sec on the 200k-file meta benchmark).
- CLI examples and tests now prefer `--repo` repository selection semantics.
- Client-local state writes now use atomic replace semantics for HEAD and staging payloads.
- Manifest index prefix iteration now streams rows instead of materializing full result sets.
- Manifest exact-path and prefix lookups now fall back to a full manifest scan when no index is available, so legacy or index-less repositories remain fully readable.
- `status(ref=...)` working-tree comparison now honors the requested branch instead of always comparing against the currently checked-out branch.
- The `RepositoryStore` contract is now split into focused `ObjectStore`, `RefStore`, `ManifestIndexStore`, and `ManifestQuerier` protocols, with `RepositoryStore` kept as the composed facade for compatibility.
- Manifest index lookups are deduplicated into a single shared implementation (`repository_store/manifest_query.py`) used by both local and S3 stores.
- Commit creation no longer relies on `_last_manifest_index` instance state; the manifest index is threaded explicitly through manifest writing and commit creation.
- S3 blob writes via streams (`push`/`pull`/`fetch`) now honor the configured blob transfer backend, so `s5cmd` accelerates sync transfers.
- The in-process commit cache is now bounded (LRU-style) instead of growing without limit.
- `ReflakeRepository` is now a facade over four focused collaborator services (`RefManager`, `SnapshotWriter`, `StagingArea`, `EntryFactory` in `core/services/`); the god-object class was split from ~1650 to ~790 lines and `repository_ops.py` no longer reaches into repository internals.

### Removed

- Removed `manifest_index.py`, `manifest_query.py`, `.idx` sidecars, the embedded manifest index, and the index/fallback decision tree.
- Removed the `reflake import` command; S3 ingress is now staged via `reflake add ... s3://... [--identity meta --as ...]` followed by `reflake commit --staged`.
- Removed `--identity` from `reflake commit`; the commit identity mode is now configured with `reflake config set identity meta`, or set per stage with `reflake add --identity ...`.
- Removed the direct-commit `-m/--message` flag from `reflake rm` and `reflake mv`; metadata-only mutations are staged and committed with `reflake commit --staged`.
- Removed `--ref` from `reflake verify` and `reflake index build`; both now target the current branch.
- Removed `reflake index query` and `reflake index drop`; `reflake index build` remains and the produced DuckDB database can be queried with the DuckDB CLI.

### Fixed

- Manifest parsing now validates entry shape, digests, and metadata-only invariants with line-aware errors.
- Commit creation is back to near-pre-refactor throughput: the commit hot loop now streams serialized manifest lines from the worktree instead of constructing and validating a `ManifestEntry` object per file (~2× faster for metadata-only commits; validated again on read).
- CLI commands now return clean `... error:` messages for common validation, filesystem, and object-storage failures instead of raw tracebacks.
- Corrupt S3-hosted manifest lines now surface actionable validation errors.
