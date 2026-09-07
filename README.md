# Reflake

**Reflake is a serverless, object-storage-first data versioning engine** — think Git semantics for datasets, where the only infrastructure you need is a folder or an S3 bucket.

Canonical data storage stays boring and immutable; all intelligence lives in metadata and access layers.

---

## Overview

```
                 ┌─────────────────────────────────────────────┐
                 │                reflake CLI                  │
                 │  commit · branch · merge · diff · push …    │
                 └──────────────────┬──────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                        core                             │
        │                                                         │
        │   services          objects            domain           │
        │   ┌─────────┐      ┌────────────┐     ┌───────────┐    │
        │   │ TreeWriter│────▶│ObjectStore │────▶│ types +   │    │
        │   │ RefManager│     │ ├ local    │     │ errors    │    │
        │   │ StagingArea      │ └ s3       │     └───────────┘    │
        │   │ EntryFactory     └────────────┘                      │
        │   └─────────┘      vfs + query (fsspec, DuckDB, pruning) │
        └─────────────────────────────────────────────────────────┘
```

Reflake separates data into three layers:

1. **Canonical layer (`blobs/`)**
   Content-addressed objects keyed by Blake3 digest, stored at `<hash[:2]>/<hash[2:]>`.
2. **Metadata layer (`trees/`, `commits/`, `refs/heads/`)**
   Merkle trees of JSONL entries map logical paths to content hashes; commits point at tree roots and form a DAG with full parent history; branches are CAS-updated pointers. Metadata operations (diff, log, status, rm, mv) never read blob bytes.
3. **Access layer**
   `reflake://<dataset>@<branch_or_commit>/<path>` resolves through the metadata layer and reads either canonical blobs or the original source URI (for metadata-only imports).

### Mental model

| Concept | What it is |
|---|---|
| **Repository** | A `.reflake/` directory locally, or an `s3://bucket/prefix` prefix remotely — same commands against both. |
| **Tree** | A content-addressed Merkle node: sorted JSONL lines addressing child trees or leaf files. Directories over 10k entries shard automatically. |
| **Commit** | `{tree, parents[], message}` — parents form a real DAG, so merges are first-class. The commit id hashes only content, so identical content yields identical ids across branches. |
| **Branch** | A pointer updated with compare-and-swap on the commit id (S3 conditional writes) — safe under concurrent clients without locks. |
| **Staging** | Per-client, per-branch overlay of adds/removes applied onto the parent tree by `commit --staged`. |
| **Identity modes** | `content` (content hash, default) or `pointer` (path+size reference, unverifiable until promoted). |

### Command map

`--repo` and `--json` are global flags and come before the command: `reflake --repo <uri> [--json] <command>`.

| Area | Commands |
|---|---|
| Ingest | `init`, `add`, `commit [--staged]`, `verify` (audit), `promote` |
| Inspect | `status`, `log`, `diff`, `list` (`ls`), `cat`, `branches`, `reflog` |
| Branch | `branch`, `checkout`, `merge` (fast-forward + 3-way metadata merge) |
| Mutate | `rm`, `mv`, `gc [--prune]`, `restore` |
| Sync | `push`, `pull`, `fetch`, `transfer` |
| Analyze | `query build` (DuckDB/Parquet), `query prune` (row-group pruning) |

Exit codes: `0` ok · `1` usage/validation (including `verify` with remaining pointer entries) · `2` conflict, retryable (CAS race, non-fast-forward, merge conflict) · `3` missing ref/object.

## Guardrails (Strict)

- Do not optimize canonical blob storage for ML throughput (no tarball/parquet/sharded blob layer).
- Do not read blob payloads for metadata-only operations (`diff`, `list`, `log`, `status`).
- Do not introduce a server/daemon/central database.
- Use Blake3 for all content hashing.
- Prefer JSONL manifests for stream-safe, O(1)-memory behavior.

## Technical Stack

- Python 3.11+
- `blake3` for hashing
- Merkle trees + JSONL commit/tree objects
- `fsspec` for URI access abstraction
- `duckdb` for disposable analytical indexing

## Install

```bash
uv pip install reflake
```

### Developer mode in repo

```bash
uv sync
uv run reflake --help
```

## Quickstart

```bash
# Initialize a new repository (idempotent, like git init)
mkdir -p /tmp/reflake-demo
uv run reflake --repo /tmp/reflake-demo init
# Or with S3 backend: uv run reflake --repo /tmp/reflake-demo init --backend s3 --s3-bucket my-bucket

echo "hello" > /tmp/reflake-demo/a.txt

uv run reflake --repo /tmp/reflake-demo commit -m "initial"

# Stage an S3 prefix as pointer entries, then commit the staged additions
uv run reflake --repo /tmp/reflake-demo add --identity pointer --as imports/bootstrap s3://my-bucket/bootstrap
uv run reflake --repo /tmp/reflake-demo commit --staged -m "metadata import"
uv run reflake --repo /tmp/reflake-demo verify
uv run reflake --repo /tmp/reflake-demo promote

# branch-scoped staged flow
uv run reflake --repo /tmp/reflake-demo branch feature
uv run reflake --repo /tmp/reflake-demo checkout feature
uv run reflake --repo /tmp/reflake-demo add data/new.csv
uv run reflake --repo /tmp/reflake-demo add --as imports/raw.csv /tmp/outside-repo/raw.csv
uv run reflake --repo /tmp/reflake-demo status
uv run reflake --repo /tmp/reflake-demo commit --staged -m "feature updates"
uv run reflake --repo /tmp/reflake-demo checkout main
uv run reflake --repo /tmp/reflake-demo merge feature main

# restore files from a ref
uv run reflake --repo /tmp/reflake-demo restore main
uv run reflake --repo /tmp/reflake-demo restore main --path data/new.csv
uv run reflake --repo /tmp/reflake-demo restore main --force

echo "hello v2" > /tmp/reflake-demo/a.txt
uv run reflake --repo /tmp/reflake-demo commit -m "update"

uv run reflake --repo /tmp/reflake-demo diff <from_ref> <to_ref>

# Stage and commit metadata mutations
uv run reflake --repo /tmp/reflake-demo rm old-prefix
uv run reflake --repo /tmp/reflake-demo mv raw/images curated/images
uv run reflake --repo /tmp/reflake-demo commit -m "clean up old files and rename image prefix"

# remote repo metadata operations from the current working tree
uv run reflake --repo s3://my-bucket/datasets/demo branch feature
uv run reflake --repo s3://my-bucket/datasets/demo commit -m "snapshot current working tree"
uv run reflake --repo s3://my-bucket/datasets/demo rm obsolete
uv run reflake --repo s3://my-bucket/datasets/demo mv bootstrap final
uv run reflake --repo s3://my-bucket/datasets/demo commit --staged -m "drop obsolete paths and rename imported prefix"

# JSON output for programmatic use (global --json flag)
uv run reflake --repo /tmp/reflake-demo --json status
uv run reflake --repo /tmp/reflake-demo --json diff main feature
```

## Identity Modes

Reflake supports two identity modes for entries:

- `content` (default)
	- Reads file bytes.
	- Stores canonical blob in `.reflake/blobs/`.
	- Entry includes `identity_mode=content`, `identity_value`, and `blob_hash`.

- `pointer`
	- Does not read file bytes.
	- Computes identity as `blake3("<relative_path>\n<size>")`.
	- Stores no canonical blob (`blob_hash=null`) and keeps `source_uri` for reads.

Set the mode per staged addition with `reflake add --identity pointer`, or set the
repository-wide default for `reflake commit` with `reflake config set identity pointer`.

This is useful for large bootstrap imports where strong content verification can be deferred.

### Durability contract for `pointer`

Pointer (`pointer`) revisions are **unverifiable**: the entry's
identity is derived from path and size, not from content bytes. Until you run
`reflake promote`, Reflake cannot prove that the content at `source_uri` matches
what was originally imported.

**Warnings.** The CLI emits a warning to stderr whenever you stage with
`--identity pointer` or commit a repository whose identity is configured to `pointer`,
and `verify` reports how many unverifiable entries remain (exiting non-zero).

**Source-retention policy.** Because pointer entries have no canonical
blob, you **must** retain the source objects at their original `source_uri`
until the entry has been promoted via `reflake promote`. If a source object is
deleted, overwritten, or moved before promotion, the corresponding
entry becomes irrecoverable — no content can be read and no hash can be
validated.

**Promotion to verifiable.** Run `reflake promote` to read every pointer
entry's source blob, compute a content hash, store the canonical blob,
and rewrite the entry in `content` mode. After promotion the source
retention requirement is lifted for those entries.

**Lifecycle summary:**

| State | `identity_mode` | `blob_hash` | Can read? | Can prove integrity? | Source required? |
|---|---|---|---|---|---|
| Pointer | `pointer` | `null` | ✅ (from `source_uri`) | ❌ | ✅ |
| Verified | `content` | hash | ✅ (from `blobs/`) | ✅ | ❌ |

## Verify And Promote Commands

`reflake verify` is a read-only audit: it reports how many pointer entries of the current branch would be promoted, and exits non-zero while any remain:

```bash
uv run reflake --repo /tmp/reflake-demo verify
uv run reflake --repo /tmp/reflake-demo verify --path images --path logs/2026
```

`reflake promote` materializes canonical blobs and writes a promotion commit (only when at least one entry is promoted):

```bash
uv run reflake --repo /tmp/reflake-demo promote
uv run reflake --repo /tmp/reflake-demo promote --path images --path logs/2026
```

- Both audit all entries by default (or selected path prefixes with `--path`).
- Promotion reads bytes from each entry's `source_uri`, computes the content hash, and stores canonical blob content.

## Incremental Ingress

```bash
uv run reflake --repo /tmp/reflake-demo add local/new.csv
uv run reflake --repo /tmp/reflake-demo add --as imports/new.csv /tmp/random/new.csv
uv run reflake --repo /tmp/reflake-demo add --as imports/new-batch /tmp/random/new-batch
uv run reflake --repo /tmp/reflake-demo add --identity pointer --as imports/bootstrap.csv s3://my-bucket/bootstrap.csv
uv run reflake --repo /tmp/reflake-demo add --identity pointer --as imports/bootstrap s3://my-bucket/bootstrap
uv run reflake --repo /tmp/reflake-demo commit --staged -m "add one file"
uv run reflake --repo /tmp/reflake-demo promote --path images --path root.txt
```

- `add` + `commit --staged` preserves the current branch manifest and reads bytes only for staged additions.
- `add` accepts repo-relative files, arbitrary local files, local directories, single S3 objects, and S3 prefixes; `--as` maps a single file/object to one logical path or remaps a directory/prefix under a destination prefix.
- `promote` reads bytes only for selected pointer entries that still need canonical blobs.
- Existing entries are preserved without re-uploading unchanged blob content.

## Merge Command

`reflake merge` updates a target branch from a source ref:

```bash
uv run reflake --repo /tmp/reflake-demo merge feature main
```

- The source ref can be a branch or commit; the target must be a branch.
- Fast-forward when the target head is an ancestor of the source.
- **Diverged histories get a metadata-only three-way merge** against the merge base: one-sided changes win, identical additions are kept, and conflicts raise `MergeConflictError` listing the paths. Merge commits record both parents.
- Ancestry checks walk the full parent DAG, so merged branches fast-forward, push, and pull correctly afterwards.

## Metadata-Only Remove And Move

`reflake rm` and `reflake mv` stage metadata-only mutations; `reflake commit --staged` writes a new commit:

```bash
uv run reflake --repo /tmp/reflake-demo rm logs/2025
uv run reflake --repo /tmp/reflake-demo mv incoming/images curated/images
uv run reflake --repo /tmp/reflake-demo commit --staged -m "remove old logs and rename prefix"
```

- These operations read tree metadata only; they do not download unchanged blob payloads.
- `rm` accepts file paths or path prefixes and removes all matching logical entries.
- `mv` accepts a file path or prefix and rewrites matching logical paths.
- `reflake status` shows staged removals and renames before `reflake commit --staged`.

## Sync (push/pull/fetch)

```bash
uv run reflake --repo . push s3://my-bucket/datasets/demo
uv run reflake --repo . pull s3://my-bucket/datasets/demo
uv run reflake --repo . fetch s3://my-bucket/datasets/demo
```

- Objects transfer plan-first: the exact missing set (commits, trees, footers, blobs) is computed, then executed via `boto3` per-object or batched through [`s5cmd`](https://github.com/peak/s5cmd) when configured (`config set transfer_backend s5cmd`).
- Divergent history is rejected before any bytes move (`NonFastForwardError`); the final ref update is a CAS, so concurrent pushes surface conflicts instead of overwriting.
- Push after a local merge transfers the entire merged lineage, including both parents' commits.
- S3-compatible endpoints (MinIO, Ministack, …) configured via `reflake init --backend s3 --s3-endpoint …` (or `config set s3.endpoint_url …`) are honored for repository operations; direct `s3://` remotes use the ambient AWS configuration chain.

## Analytical Index (Derived, Disposable)

```bash
uv run reflake --repo /tmp/reflake-demo query build --parquet
```

`reflake query build` writes a DuckDB database (and optional Parquet export) for the current branch's tree to `.reflake/index/<commit_id>.duckdb`. Query it with the DuckDB CLI:

```bash
duckdb /path/to/<commit>.duckdb "SELECT COUNT(*) FROM files"
```

If the index is deleted, Reflake remains fully functional from trees and commits.

### Row-group pruning

With `config set parquet_footer true`, parquet ingests also capture compact footer statistics (schema + per-row-group min/max/nulls) under `footers/<hash>`. `reflake query prune` then selects the row groups that may match a WHERE-style predicate — reading metadata only, never data pages:

```bash
uv run reflake --repo /tmp/reflake-demo query prune <ref> images/ --where "id >= 100 AND active = true"
```

## `fsspec` URI Example

```python
from reflake.core import ReflakeFileSystem

fs = ReflakeFileSystem(dataset_roots={"my_data": "/tmp/reflake-demo"})
with fs.open("reflake://my_data@main/a.txt", "rb") as handle:
    data = handle.read()

# include branch staged (not-yet-committed) changes
with fs.open("reflake://my_data@feature+staged/a.txt", "rb") as handle:
    staged_data = handle.read()
```

In `pointer` snapshots, Reflake reads from `source_uri` when no canonical `blobs/` object exists.

## Python API

```python
from reflake.core import create_repository, open_repository

repo = create_repository("/tmp/reflake-demo")  # init + open (idempotent)
repo = open_repository("/tmp/reflake-demo")  # fails unless initialized
repo = open_repository("s3://my-bucket/datasets/demo")  # remote (lazy)

commit_id = repo.commit("snapshot")
repo.verify()  # read-only audit of pointer entries
repo.promote()  # materialize blobs + commit
repo.merge("feature", "main")
```

Public, stable surface (everything else is internal and may change):

- construction: `init_repository`, `create_repository`, `open_repository`
- repository: `ReflakeRepository` methods
- reads: `ReflakeFileSystem`, `ReflakeURI`
- sync: `push`, `pull`, `fetch`
- errors: `ReflakeError` and subclasses (`RefConflictError`,
  `NonFastForwardError`, `UnknownRefError`, …)

`core.services`, `core.objects`, `core.query` internals and the store
protocols are not covered by stability guarantees. Read paths (`diff`,
`log`, `status`, VFS, query) depend only on narrow read capabilities, so
metadata operations never touch blob bytes by construction.

## Repository Layout

Reflake creates `.reflake/` under each dataset root:

- `blobs/` - canonical content-addressed object store
- `trees/` - Merkle tree nodes (sorted JSONL, content-addressed)
- `footers/` - parquet footer-stats objects (when enabled)
- `commits/` - commit metadata objects
- `refs/heads/` - branch pointers only (CAS-updated)
- `refs/HEAD` - symbolic active branch reference (default `main`)
- `state/` - client-local branch snapshots (never shared, never a ref)
- `staging/`, `cache/`, `index/`, `reflog/` - client-local state

Every object kind has exactly one physical location, defined by
`layout.object_relative_key()` — the local filesystem and the S3 key space are
guaranteed to stay in sync because both derive from that single mapping.

## Concurrency Model

There are no locks. Every shared mutation goes through compare-and-swap:

- Local repos use version tokens (mtime+size) checked before writing.
- S3 repos use conditional `PutObject` (`IfMatch`/`IfNoneMatch`) so the check-and-write is atomic server-side.
- `commit --staged` re-applies its overlay onto the new parent and retries on conflict; other mutations surface `RefConflictError` with both commit ids.

## Mandatory Validation Coverage

Current tests cover required invariants:

- Metadata-only diff reads no blob payloads.
- Manifest generation for 100k entries stays under RAM cap.
- `reflake://my_data@main/test.csv` resolves and returns expected bytes.
- Staged commits stay correct regardless of path sort order; merged lineages survive push/pull.

Run test suite:

```bash
uv run pytest tests
```

## S3 Integration Tests

Reflake includes `integration`-marked tests for real S3-compatible behavior. The preferred target is Ministack.

For the standard local workflow, run a single command from the repository root:

```bash
bash scripts/run_s3_integration.sh
```

That script starts a temporary Ministack container on `127.0.0.1:4566`, waits for the health endpoint, resets emulator state, runs `tests/test_s3_integration.py`, and cleans up the container when the test run finishes.

Start Ministack locally:

```bash
docker run --rm -p 4566:4566 nahuelnucera/ministack
```

Verify the emulator is ready:

```bash
curl http://127.0.0.1:4566/_ministack/health
```

Then set these environment variables before running the suite:

```bash
export REFLAKE_MINISTACK_ENDPOINT=http://127.0.0.1:4566
export REFLAKE_MINISTACK_ACCESS_KEY=test
export REFLAKE_MINISTACK_SECRET_KEY=test
export REFLAKE_MINISTACK_REGION=us-east-1
```

Reflake's integration fixture already uses path-style boto3 S3 addressing, so no extra S3 client flags are needed.

Then run:

```bash
uv run pytest tests/test_s3_integration.py -m integration
```

If `REFLAKE_MINISTACK_ENDPOINT` is unset or the endpoint is unreachable, the integration tests skip automatically.

To wipe the local emulator state between runs without restarting the container:

```bash
curl -X POST http://127.0.0.1:4566/_ministack/reset
```

## License And Support

Reflake is licensed under the GNU Affero General Public License v3.0 or later.

- The license keeps copyright and license notices attached to redistributed copies.
- Modified networked deployments must make their corresponding source available under the AGPL terms.
- That gives companies a practical reason to fund maintenance if they depend on Reflake while keeping the project genuinely open source.

If your company uses Reflake, sponsor ongoing maintenance at <https://github.com/sponsors/londogard>.
