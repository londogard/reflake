# Reflake Roadmap

**Status (2026-11-10).** The v2 Merkle-tree model is the design of record and
is shipped through **v0.6.0** (2026-11-10), the hardening release: verified
reads, GC/sync completeness, durable writes, worktree safety, directory-aware
VFS, streaming plans, and the KV store core. Version 0.5.0 made trees canonical
(no mtime in entries), moved to a client stat cache, parallelised all
latency-bound I/O, and made `rm`/`mv` worktree-safe. Through **v0.4.0**
(2026-10-03): tree objects with shard locality
and the write-skip cost model, virtual-first CLI + VFS + DuckDB query,
parquet footer capture, pruning, and pruned scans that execute (`query scan`
reads only the kept row groups — local, S3, or pointer sources), CAS-only
refs (local lock / S3 conditional writes), plan-then-batch sync, identity
audit/promote with metadata-only drift checks, tree-level 3-way merge,
reflog/branches, `reset`, ignore rules, GC grace window and streaming sweep,
multipart streaming, write-time tree validation, invariant fuzz tests, and
nightly benchmark CI.

See [`docs/architecture.md`](docs/architecture.md) for the model and
[`docs/vision.md`](docs/vision.md) for the vision and the full generation
spec. The v1 plan below the fold (repository-store abstraction,
manifest-first design) was completed and then **superseded** by the tree
model — the CHANGELOG has the history.

## Direction

Reflake stays client-driven with a shared object-storage repository:

- One repository concept; local path or `s3://bucket/prefix`.
- Metadata-first operations (branch/diff/merge/rm/mv/prune) that never read
  blob payloads.
- Content reads only when a command asks for bytes; the working tree is an
  optional cache, not the source of truth.
- CAS-only concurrency; per-client branch preference, staging, and caches.

## Now

0. **0.6 hardening follow-ups** — provenance, the catalog/discovery
   convention, and cross-client staging/reflog GC roots. The review blockers
   and streaming plans are fixed; see CHANGELOG "Unreleased".
1. **Format 2 follow-ups** — `reflake migrate` from 0.4.x (entry lines, tree
   fanout, pointer identity all changed); an S3 Inventory-backed existence
   source for `gc`/sync planning. **Landed in 0.5.1:** a format-2 marker with
   a clear pre-2 error, `reflake fsck` (including `--verify-blobs`), and the
   committed scale harness + nightly structural gate.
1. **Docs split** — architecture (current model), roadmap (this file), ADRs
   for past decisions; README stays user-facing.
2. **Tags/releases** — immutable named refs (`refs/tags/*`) so "dataset
   v1.2" can be pinned. **Landed (unreleased)**: `tag`/`tag -l`/`tag -d`,
   tag-aware resolution everywhere, explicit `push`/`pull --ref <tag>`, and
   tags as GC roots.
3. **Provenance** — record the committing client/actor (and optional tags) in
   commit metadata, so a shared repository answers "who committed this?".
4. **`export`** — first-class snapshot export (tree + blobs → directory or
   tar) instead of VFS loops, for handoff to non-Reflake consumers.
5. **Reproducible scale harness** — commit the 1M-file/S3 benchmark scripts
   and publish numbers alongside the cost model.

## v0.4 — Pruned scans that execute (implementation complete)

**Status (2026-10-03).** Landed: `query scan` (ranged execution with
`--ref/--path/--limit/--explain/--verify/--strict`, JSONL rows, `{stats, rows}`
JSON), the ranged accounting file object, pointer-source scanning, tests
(correctness vs DuckDB, byte bounds, fake-S3 kept ranges, benchmark scaling),
README/architecture/CHANGELOG updates, and the `0.4.0` version bump. Release
prep is in place; tag/push pending review. Follow-ups landed in the same pass:
`query prune` folded into `query scan --explain` (one plan path), the CLI split
into a `reflake.cli` package (one module per command group), option help
generated from field comments, footer capture rewritten onto pyarrow's
metadata reader (the hand-rolled thrift decoder is gone), and DuckDB-over-VFS
queries (`duckdb.register_filesystem` + seekable ranged `reflake://` reads).

**Goal.** Make footer statistics save real bytes: `reflake query scan
"<where>"` executes the prune plan — it reads only the row groups whose
stats may match, applies the exact predicate to those rows, returns them,
and reports bytes fetched vs bytes total.

**Definition of done**

- `query scan` returns correct rows locally and on S3, fetching only the
  kept row groups (measured).
- Tests: correctness vs DuckDB full scans, byte/request accounting,
  `--explain` reads zero data bytes, missing-column errors, no-footer
  fallback, fake-S3 range requests, plus one `benchmark`-marked scaling
  test in the nightly job.
- README documents the command with a real bytes-saved example; CHANGELOG,
  architecture §4, and this roadmap updated; `0.4.0` released.

**Work breakdown**

1. **Ranged blob file** — new `objects/rangefile.py`: a seekable file
   object over blob storage (local file handle, or S3 ranged GETs through
   the store's client) wrapped in a counting layer that tracks bytes and
   requests; pyarrow consumes it via `pa.PythonFile`.
2. **Scan execution** — new `query/execute.py::execute_scan(...)`: reuse
   `plan_pruned_scan` (`query/pruning.py`) for footer-backed pruning, then
   `pyarrow.parquet.ParquetFile.read_row_groups(kept)` and an exact
   `pyarrow.compute` filter mirroring the null semantics documented in
   `pruning.py` (comparisons involving NULL never match). Entries without
   footer stats are read fully and counted; non-parquet entries are
   skipped (counted). Rows stream lazily; `ScanStats` reports files, row
   groups kept/total, `bytes_fetched`, `bytes_total`, `requests`, and
   inapplicable columns.
3. **CLI** — `query scan "<where>" [--ref] [--path] [--limit] [--explain]
   [--json]` next to `query build`. stdout is JSONL rows; `--explain` prints
   the plan with zero data reads (the former `query prune`, folded in);
   `--json` emits a `{stats, rows}` envelope.
4. **Tests** — new `tests/test_query_scan.py`: duckdb-written parquet
   (`ROW_GROUP_SIZE` small) ingested with `identity="pointer"` +
   `parquet_footer` (pattern from `tests/test_promote_footer.py`);
   correctness vs DuckDB `WHERE`; bytes fetched bounded by kept groups'
   compressed size + footer and far below the file total; `--explain`
   fetches zero bytes; missing predicate column raises `ValueError` listing
   files; no-footer parquet counted as a full scan; fake S3 (its `Range`
   support already exists) asserts only kept-range requests.
5. **Docs & release** — README example with real numbers, architecture §4
   execution paragraph, CHANGELOG, version bump + annotated tag (the same
   flow as v0.3.0).

**Decisions**

- `pyarrow` becomes a hard dependency — scanning is a core product feature,
  not an optional extra.
- A predicate column missing from a file's schema raises `ValueError`
  listing the files (typo protection); union-schema semantics stays Later.
- `--verify` optionally full-hash-verifies a blob before scanning; default
  off (scan trusts content addressing + TLS, like `open_blob_stream`).

**Out of scope (this milestone)**

Tags/releases, provenance, `export`, shared repository config,
content-defined chunking, committed scale harness. Stretch if v0.4 lands
early: `refs/tags/*` ("Now" item 3).

**Risks**

- S3 read amplification: pyarrow issues many small reads (footer, then per
  column chunk). Measure request counts first; add read coalescing only if
  the numbers demand it.
- Byte-accounting tolerance in tests (kept groups' column-chunk compressed
  size + footer); keep the slack explicit.
- Exact-filter null semantics must match the pruning contract — the DuckDB
  comparison test is the referee.

## Later

- Shared repository config/description (schema notes, owners, landing paths)
  designed so ingest-shaping flags can travel with the repository instead of
  each client.
- A remote-native snapshot command for S3 prefixes (CLI surface over the
  library's `import_s3`).
- Optional content-defined chunking for large files/delta space (needs a
  design pass; blob storage is deliberately simple today).
- Point-in-time / row-group-aware reads over `mp` entries (remote stats).

## Non-Goals

- Server, daemon, coordination service, or central database.
- History rewriting (rebase/amend/cherry-pick); `reset` is the undo primitive.
- Write-capable fsspec; the VFS is read-only by design.
- ML-throughput blob reorg (tarballs/sharded blob layers).
- Backwards compatibility for on-disk formats before 1.0.


## Closed v1 phases (history)

These were completed before the tree model replaced them; kept here so old
references resolve:

- repository-store / storage-backend split (now one `objects/` package with
  capability-split protocols),
- local client state (`state/`, `staging/`, `reflog/`, caches),
- CAS-only ref updates with `RefConflictError`,
- unified `--repo` CLI + Python API,
- metadata-only `rm` / `mv` / `diff` / `merge`,
- content ingress (`add`, `import_s3`, `identity promote`),
- Ministack integration tests,
- path/URI ergonomics (no `cloudpathlib` dependency; URI parsing is internal
  and type-checked).

Still-open backlog items live under "Now" and "Later" above, not as phases.
