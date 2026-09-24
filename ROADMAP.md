# Reflake Roadmap

**Status (2026-09).** The v2 Merkle-tree model is the design of record and is
shipped: tree objects with shard locality and the write-skip cost model,
virtual-first CLI + VFS + DuckDB query, parquet footer capture and pruning,
CAS-only refs (local lock / S3 conditional writes), plan-then-batch sync,
identity audit/promote with metadata-only drift checks, tree-level 3-way
merge, reflog/branches, `reset`, ignore rules, GC grace window, multipart
streaming, and write-time tree validation.

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

## Now (v0.3 candidates)

1. **Docs split** — architecture (current model), roadmap (this file), ADRs
   for past decisions; README stays user-facing.
2. **Pruned scans that execute** — read only the row groups `query prune`
   keeps (Arrow table / DuckDB integration), so footer statistics produce
   bytes saved, not just a report.
3. **Tags/releases** — immutable named refs (`refs/tags/*`) so "dataset
   v1.2" can be pinned; the ref model already supports them.
4. **Provenance** — record the committing client/actor (and optional tags) in
   commit metadata, so a shared repository answers "who committed this?".
5. **`export`** — first-class snapshot export (tree + blobs → directory or
   tar) instead of VFS loops, for handoff to non-Reflake consumers.
6. **Reproducible scale harness** — commit the 1M-file/S3 benchmark scripts
   and publish numbers alongside the cost model.

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
