# Reflake — Vision & Generation Spec

> **One line:** Reflake is a serverless, object-storage-first data versioning
> engine: Git-like history for datasets where the only infrastructure you need
> is a folder or an S3 bucket, all intelligence lives in metadata, and data
> bytes move only when you explicitly ask for them.

This document has two parts:

- **Part I — Vision**: why Reflake exists, what it is and is not, who it is for.
- **Part II — Generation spec**: the normative model, invariants, formats,
  operations, CLI contract, and a milestone plan precise enough that an
  autonomous implementer could regenerate the project from it.

Version: written 2026-09-13, describing the intended design as of v0.2.1.
The spec is written as the *target* contract; a short status appendix notes
where the current implementation deviates.

---

# Part I — Vision

## 1. The problem

Datasets live in object storage. The workflows around them are still
filesystem workflows:

- "Which version of the data produced this model?" is answered by filenames
  and conventions, not by a version graph.
- Re-running a pipeline requires re-listing and re-reading whole prefixes.
- Branching a dataset (experiment with a subset, keep production intact) is
  either impossible or a full copy.
- Table formats (Iceberg/Delta) give snapshot semantics for *one table*, not
  for a directory tree of arbitrary files.
- Git-style tools assume a working tree is the source of truth; on S3 the
  bucket is the source of truth.

The result: metadata operations (branch, diff, merge, rename, audit) cost
full data transfers, or they need a server.

## 2. The bet

Reflake bets on five ideas:

1. **Canonical storage stays boring and immutable.** Content-addressed blobs
   (Blake3), written once, never mutated, never reorganized. No tarballs, no
   sharded blob layers, no ML-throughput format.
2. **All intelligence is metadata.** History is a DAG of commits pointing at
   Merkle trees; branches are CAS-updated pointers. Diff, log, status, rm, mv,
   merge, prune and promote touch only tree/commit/footer objects.
3. **The virtual dataset is the product.** A branch or commit *is* a dataset
   address (`reflake://<dataset>@<ref>/<path>`), readable without
   materializing a working tree. The working tree is an optional local cache
   for people who want one.
4. **Serverless, client-first, coordination-free.** No server, no daemon, no
   central database. Concurrency safety comes from compare-and-swap ref
   updates (S3 conditional writes), not locks. Every client holds its own
   branch preference, staging area, and caches.
5. **Parquet-aware metadata.** Footer statistics are captured at ingest as
   small content-addressed objects referenced from tree entries, so
   `WHERE`-style predicates can eliminate row groups without reading data
   pages — the one data-specific differentiator.

## 3. What Reflake is not (anti-goals)

- **Not a server or service.** No control plane, no catalog service, no locks,
  no coordination backend.
- **Not a table format.** It does not own schemas, partitions, or
  compaction; it versions *directories of files* and carries parquet
  metadata as a courtesy.
- **Not a git replacement for code.** No rebase, no history rewriting, no
  patches. Commit verbs are the UX vocabulary, not a git emulation target.
- **Not an ML data loader.** No sharding/tarring/caching layer for training
  throughput.
- **Not backwards compatible before 1.0.** On-disk formats may break between
  minors; the CHANGELOG must say so loudly.

## 4. Users and jobs-to-be-done

| User | Job | Reflake promise |
|---|---|---|
| Data engineer | Snapshot an S3 prefix into a versioned dataset without copying bytes | Pointer/import ingress + metadata commits; zero data movement to start |
| ML platform engineer | Branch a dataset for an experiment; keep production immutable | Branch = pointer; staged overlay commits; metadata-only merge |
| Analyst | Query "the dataset at `@main`" without downloading it | Virtual dataset view (VFS + DuckDB) over refs |
| Data steward | Prove what is stored, detect unverifiable imports, promote them | `identity verify` / `identity promote`; hash-verified reads |
| Platform operator | Share one S3 repository across many clients | CAS-only refs; no locks; per-client state |

## 5. Success criteria

A version of Reflake is "right" when:

- Adding one file to a 1M-file dataset writes O(path depth + changed
  directories) metadata objects, not O(files) — and a test asserts the object
  count.
- Exact-path lookup over a cached prefix costs 0–1 GETs; listings stream.
- A dataset can be branched/merged/diffed from any machine with only S3
  credentials, and two clients racing the same branch never lose an update.
- `git`, `dvc`, or a copy script cannot do the combination of (branch, diff,
  merge, prune, promote, virtual read) without either a server or a full
  copy.
- A reviewer can predict the cost of every command from the cost model.

---

# Part II — Generation spec

Normative language: **MUST**, **SHOULD**, **MAY** as in RFC 2119.

## 6. Objects and key layout

Two storage scopes exist. For a local repository both live under `<root>/.reflake`;
for a remote repository the *shared* scope lives under `s3://bucket/prefix/` and
the *client* scope lives next to the worktree in `.reflake/clients/<repo-id>/`.

### 6.1 Shared scope (the repository; only mutable part is refs)

```
blobs/<hh>/<rest>        # canonical content, Blake3 of bytes, immutable
trees/<hash>             # Merkle nodes: sorted JSONL, Blake3 of serialized bytes
footers/<hash>           # compact parquet footer statistics JSON, immutable
commits/<id>.json        # commit objects, immutable
refs/heads/<branch>      # newline-terminated commit id; absent/empty = unborn
```

Key mapping MUST come from a single function (`object_relative_key`) shared by
all adapters, so local and S3 layouts can never drift (spec invariant **I14**).

### 6.2 Client scope (never synced, never shared)

```
state/branch-snapshots/<branch>.json   # last observed head per branch (CAS expectations)
staging/<branch>.json                  # typed overlay delta (adds/removes)
reflog/<branch>.log                    # client-local ref-update log
cache/derived/<tree>.jsonl             # derived manifests (content-addressed, safe)
cache/footers/<hash>                   # parsed footer stats cache
```

### 6.3 Commit object

Stored:

```json
{"id":"<64 hex>","message":"...","tree":"<hash>","parents":["<id>", ...],
 "created_at":"<ISO-8601>","generation":<int>}
```

Identity: `id = BLAKE3(canonical_json({"message": m, "tree": t,
"parents": [...]}))` — sorted keys, compact separators, UTF-8, no trailing
newline. `created_at` and `generation` are recorded but **not hashed**, so
identical content yields identical ids and retries are idempotent.
`generation = 1 + max(parent generations)` (0 for unborn parents). Serialization
of the identity body MUST be frozen; any change to it is a format break.

### 6.4 Tree objects (Merkle nodes)

A tree node is a UTF-8 JSONL file, one entry per line, **strictly ascending by
name**; `name` is a single path component (no `/`, no `.`/`..`, non-empty).

```
["t",  name, hash]                              # subtree = directory
["s",  name, hash]                              # name-range shard of this directory
["b",  name, hash, size, mtime_ns]              # blob-backed file
["m",  name, hash, size, mtime_ns, source_uri]  # source reference, unverifiable
["bp", name, hash, size, mtime_ns, footer]      # parquet + footer stats
["mp", name, hash, size, mtime_ns, source_uri, footer]
```

- `hash` = Blake3 hex (64 lowercase chars). For `b`/`bp` it is the content
  hash; for `m`/`mp` it is `BLAKE3("<path>\n<size>")` — an *unverifiable
  reference*, explicitly not a content identity.
- Sharding rule (**I2**): a node has at most `MAX_TREE_ENTRIES` (10 000)
  children. A directory exceeding the bound is stored as contiguous
  name-range shards; each shard body is a **plain** node (shards never nest);
  a shard pointer's name is the first contained name, so ranges remain
  binary-searchable. An overflowing shard body splits into balanced sibling
  shards, never into nested shards.
- Writers MUST validate before publishing (**I11**): a serialized node that
  would fail re-parsing (duplicate names, unsorted names, bad components) must
  never be written, because a bad node is unrecoverable once a ref points at it.

### 6.5 Staging overlay (client scope)

`staging/<branch>.json` is a JSON array of typed changes, sorted by path:

```json
{"path":"data/x.parquet","action":"add","identity_mode":"content",
 "source_uri":"file:///...","blob_hash":"...","size":123}
```

A change is a *recipe*, not a snapshot: content is (re)materialized at commit
time. Implementations SHOULD document this, and MAY add an explicit
"freeze" mode (`add --freeze` records the digest but not the bytes).

## 7. Invariants (the testable core)

| # | Invariant |
|---|---|
| I1 | Tree nodes are strictly name-sorted, duplicate-free, single-component names |
| I2 | Fanout ≤ `MAX_TREE_ENTRIES`; ranges are contiguous; shard bodies are plain nodes |
| I3 | Objects are written before the ref that makes them reachable; ref update is last |
| I4 | `commit_id` hashes exactly `{message, tree, parents}` |
| I5 | Ref updates are CAS on the previously observed commit id (`None` = must not exist) |
| I6 | Metadata-only operations never read blob bytes (status/diff/list/log/rm/mv/merge/prune/promote-plan) |
| I7 | Blobs are immutable, content-addressed, and hash-verified on full reads |
| I8 | Derived artifacts (manifests, indexes, caches) never enter the shared scope |
| I9 | `checkout` never materializes; downloads happen only via `restore`/`pull`/`cat`/VFS |
| I10 | A worktree commit is an *overlay*: absence is not deletion; deletion is explicit |
| I11 | No object that would fail re-validation is ever published |
| I12 | Pointer (`m`/`mp`) entries are unverifiable references; no integrity claim may be made before `promote` |
| I13 | Path matching is component-aware (`a/b` matches `a/b` and `a/b/…`, never `a/bc`) |
| I14 | One key-space mapping function for every adapter |
| I15 | Ref concurrency is CAS-only; no locks, no stale-lock recovery |

## 8. Operations (semantics + cost model)

### 8.1 Ingest

- `commit` (full): walk the worktree (skip `.reflake` and symlinks), hash new
  or changed content-mode files in parallel, reuse parent leaves on
  size+mtime (`trust_mtime`, opt-in) or size+digest; merge parent-only entries
  in (overlay semantics, I10); prune staged removals; build bottom-up with
  `skip_hashes` so byte-identical nodes and untouched shard bodies are not
  rewritten. Cost: O(files walked) hashing + O(changed directories) writes.
- `commit --staged`: apply the overlay onto the parent tree via path splicing;
  on `RefConflictError`, re-apply onto the new parent and retry (3 attempts).
  Cost: O(changed paths) reads/writes, no blob reads for unchanged entries.
- Shape changes (file ↔ directory) MUST work in **both directions** through
  every splice path (`commit --staged`, `mv`, `promote`, import), producing a
  node where the new shape *replaces* the old — never two entries with one
  name.
- `add`: stage local files/dirs, arbitrary local files (`--as`), or S3
  objects/prefixes (pointer or content). Content-mode S3 import streams
  object → temp → hash → upload; it inherently costs 2× network for new
  content and MUST be documented as such.
- `rm` / `mv`: stage prefix-aware removals / rewrites. Metadata-only.

### 8.2 Read

- Exact lookup: descend the path chain, binary-searching each node; shard
  descent uses name ranges; 0–1 GET warm, ≤ depth GETs cold.
- Prefix/list: streamed subtree walk, skipping subtrees that cannot contain
  the prefix. O(matches + depth).
- `diff`: parallel tree walk; identical subtree hashes are skipped. O(changed).
- `log`: full parent DAG, newest-first by `generation` (tie-break id).
- `cat` / VFS: resolve entry, read blob (verified) or `source_uri`.
- `restore`: path- or prefix-scoped materialization; never overwrites unless
  `--force`; unknown paths are an error (exit 3).

### 8.3 History

- `merge`: fast-forward if target is an ancestor of source; otherwise 3-way
  merge over the nearest common ancestor (generation-ordered frontier walk).
  Rules: identical changes win; side equal to base yields the other side;
  delete/modify and shape changes conflict (paths reported). Shard-aware:
  pairwise shard-pointer comparison when the three sides share boundaries.
  Merge commit has both parents; CAS advances the target.
- `branch` / `checkout`: branch = pointer creation at current head;
  checkout = client-local branch preference only (I9).

### 8.4 Identity lifecycle

- States: content (`b`/`bp`) = verifiable; pointer (`m`/`mp`) = unverifiable
  until promoted. `identity verify` is read-only and exits non-zero while
  pointer entries remain; `identity promote` reads bytes, computes the content
  hash, stores the blob, splices promoted leaves, and commits.
- Pointer entries SHOULD record enough source metadata (ETag / version id /
  last-modified) to make *drift detection* possible without reading bytes;
  today only `size` is recorded, which cannot detect same-size overwrites.

### 8.5 Sync

- Plan first: compute reachable objects from the moved commits; find the
  missing set on the destination (per-object probes below 64 candidates, one
  inventory listing per kind above).
- Reject divergence before any bytes move (non-fast-forward).
- Execute in one batch (s5cmd `--if-not-exists`) or per object (boto3).
- Update the ref last via CAS. Crash/race leaves unreachable objects, never a
  dangling ref (I3).
- Blob transfer MUST stream: single PUTs cap at 5 GiB and reading whole blobs
  into memory is a product defect for dataset-sized files; multipart upload
  and streaming download are required for 1.0.

### 8.6 Retention

- `gc` (audit by default) marks from every branch head through commit DAG →
  tree DAG → leaf refs (blobs + footers), then reports orphans.
- `gc --prune` deletes orphans. **Safety rule required for 1.0:** pruning must
  not race object publication. Choose one: (a) grace period (never delete
  objects younger than N hours — S3 LastModified/local mtime), (b) a
  repository-wide maintenance marker that writers acknowledge, or
  (c) document that prune requires quiescence (current stance) — but then it
  must not be advertised as an ordinary command.

### 8.7 Analytical layer

- `query build`: flatten the tree to a CSV (benchmarked fast path) →
  DuckDB database (+ optional Parquet). Disposable, derived.
- `query prune`: parse an AND-only predicate, select row groups from
  `footers/<hash>` stats only, conservatively (unknown ⇒ keep). Never claim
  pruning for entries without stats.
- The pruning engine SHOULD grow an execution path (read only kept row groups,
  return Arrow) — planning without reading is only half the differentiator.
- Pointers stay out of the shared scope: caches are content-addressed and
  client-local (I8).

## 9. Concurrency & failure model

- **CAS-only (I15).** A ref's content is its commit id; the expectation is the
  commit id (or absence). Local: lock file + atomic replace. S3: read ref +
  ETag, conditional PUT (`IfMatch` / `IfNoneMatch`).
- **Write order (I3).** Publish immutable objects first, ref last. Any crash
  leaves garbage, never a ref to missing objects.
- **Retries.** `commit --staged` re-applies onto the new parent on conflict.
  Sync re-prechecks and surfaces `NonFastForwardError`. Optimistic flows never
  block.
- **Orphan policy.** Unreachable objects are legal; only `gc --prune` removes
  them (subject to §8.6).
- **Undo.** Refs currently only move forward. 1.0 MUST ship at least one of:
  `reset --soft <ref>` (client-side pointer move with CAS), `revert` via
  metadata commits, or an explicit "restore branch to commit" maintenance
  command. A versioning system without an undo mechanism is not credible.

## 10. CLI contract

```
reflake --repo <path|s3://bucket/prefix> [--json] <command> [args]
```

| Area | Commands | Notes |
|---|---|---|
| Ingest | `init`, `add`, `commit [--staged]` | `--repo`/`--json` are global, before the command |
| Identity | `identity verify`, `identity promote` | verify exits 1 while pointer entries remain |
| Inspect | `status`, `log`, `diff`, `list`/`ls`, `cat`, `branches`, `reflog` | worktree-optional |
| Branch | `branch`, `checkout`, `merge` | checkout is a pointer switch only |
| Mutate | `rm`, `mv`, `restore`, `gc [--prune]` | rm/mv are staged metadata ops |
| Sync | `push`, `pull`, `fetch` | one local + one S3 endpoint |
| Analyze | `query build`, `query prune` | derived, disposable |
| Config | `config init/set/list` | client-local; MUST be documented as such |

Exit codes: `0` ok · `1` usage/validation · `2` retryable conflict (CAS race,
non-fast-forward, merge conflict) · `3` missing ref/object.

Every command that returns structured data MUST honor `--json` (single
document on stdout; error envelope on stderr with `code`, `command`,
`exit_code`). Errors MUST name the *actionable* form of the command (e.g.
`reflake identity promote`, never a removed alias).

## 11. Public API (library)

Stable surface: `init_repository`, `create_repository`, `open_repository`,
`ReflakeRepository` methods, `ReflakeFileSystem`, `ReflakeURI`,
`push/pull/fetch`, and `ReflakeError` subclasses. Everything else — stores,
codecs, tree writers, pruning internals — is internal and MUST NOT be
re-exported from the package root. (Stability is a promise; a 70-symbol
`__all__` is not a promise, it is a leak.)

## 12. Generation plan (milestones an implementer can execute)

Each milestone is done when its acceptance tests pass; cost assertions are
counted in object operations, not wall-clock.

- **M0 — Skeleton.** Layout + key mapping, config (versioned, validated),
  hashing, entry codec (single `Entry`, validation on read), domain errors,
  CLI scaffold with the exit-code contract.
  *Tests:* codec round-trips and rejects corrupt payloads; config version
  guards; unknown kinds rejected.
- **M1 — Local history.** Tree build (bottom-up, `skip_hashes`, sharding),
  commit objects (content-only ids), refs CAS (file lock + atomic replace),
  `log`, `diff`, `status`.
  *Tests:* a no-op commit writes 0 tree nodes; one changed file in 50
  directories writes ≤ 4; commit ids are stable across branches and retries.
- **M2 — Overlay + metadata mutations.** Staging format, `add`/`rm`/`mv`,
  splice overlay with **shape-change replacement in both directions**,
  conflict retry loop.
  *Tests (property-style):* random sequences of add/rm/mv/full-commit keep
  every published tree parseable and duplicate-free; a directory staged over
  an existing leaf and a leaf staged over an existing directory both leave a
  single entry.
- **M3 — S3 adapter.** Key mapping, conditional writes, error translation,
  CAS with ETag; fake-S3 unit tests with PreconditionFailed semantics;
  Ministack integration tests for branch/commit/merge/race.
  *Tests:* two clients committing concurrently — exactly one wins, loser sees
  a retryable conflict; no lock objects exist.
- **M4 — Sync.** Reachable-set planning, adaptive probes/inventory,
  plan-then-batch through a transfer backend, divergence rejection, ref last.
  *Tests:* push then pull round-trip in a fresh clone; interrupted push leaves
  the ref untouched; s5cmd manifest injection rejected.
- **M5 — Identity lifecycle.** Pointer/`content` states, `verify` read-only,
  `promote` splice + commit; drift metadata stored on pointer entries.
  *Tests:* verify writes nothing; promote turns `m`→`b` and `mp`→`bp` keeping
  footers; missing source fails loudly.
- **M6 — Virtual layer.** VFS (`reflake://<dataset>@<ref>/<path>`), DuckDB
  catalog, footer capture, conservative pruning, **and one execution path
  that reads only kept row groups**.
  *Tests:* VFS reads without a worktree; pruned scan returns exactly the rows
  the predicate matches (compare against full scan); unknown columns never
  prune.
- **M7 — Collaboration.** Fast-forward + tree-level 3-way merge, reflog,
  branches, gc audit/prune with the §8.6 safety rule, reset/undo.
  *Tests:* merged lineage pushes/pulls; gc never deletes objects reachable
  from any branch; undo restores a branch pointer safely.

**Cross-cutting test strategy:** unit tests per module; cost-guarantee tests
that assert object op counts; invariant/property tests over random operation
sequences; fake-S3 tests with realistic conditional semantics; Ministack
integration tests; and a reproducible scale harness committed to the repo
(the current numbers in `bench/bench.txt` are not reproducible from source).

## 13. Open decisions (resolve before 1.0)

1. GC safety approach (§8.6) and whether `--prune` ships at all before it.
2. Undo: `reset`, `revert`, or both.
3. Pointer drift metadata: ETag/version-id storage and a cheap `verify` mode.
4. Exclusions: `.reflakeignore` (at minimum `.git`, `node_modules`, `*.tmp`)
   — committing `.git/**` today is a footgun, not a feature.
5. Large objects: multipart upload, streaming reads, concurrency limits.
6. Provenance: author/committer identity, tags/releases, optional signing.
7. Shared repository config: should ingest-shaping flags (`identity`,
   `parquet_footer`) travel with the commit or the repository instead of being
   client-local?
8. Hierarchical branch names (`feature/x`) and branch deletion.
9. Whether `export` is first-class or tree-walk + VFS is sufficient.
10. Materialized pruned scans (Arrow/DuckDB integration) as the headline
    feature of the next minor.

## 14. Glossary

- **blob** — content-addressed bytes (`blobs/<hh>/<rest>`).
- **tree** — Merkle node; names a directory's children.
- **shard** — contiguous name-range subtree of a large directory.
- **leaf** — a file entry (`b`/`m`/`bp`/`mp`).
- **commit** — `{tree, parents[]}` + recorded metadata; content-hashed.
- **ref** — branch pointer; only mutable shared state; CAS-updated.
- **pointer entry** — `m`/`mp`; unverifiable reference to `source_uri`.
- **promote** — materialize pointer entries into canonical blobs.
- **derived artifact** — manifests/indexes/caches; client-local, disposable.
- **virtual dataset** — `reflake://<dataset>@<ref>/<path>`.

---

## Appendix — Status vs. this spec (2026-09-19, after the 0.2.1 hardening)

Shipped and matching: tree model + sharding with locality, content-only commit
ids, CAS-only refs, worktree-optional CLI, plan-then-batch sync, identity
audit/promotion, footer capture + pruning *planning*, VFS, reflog/branches, gc
audit, **write-time tree validation (I11)**, **ignore rules**, **`reset`**,
**GC grace window**, **multipart streaming blobs**, **metadata-only drift checks
with source ETags**, **hierarchical branch names + branch deletion**, **full
`--json` coverage**, and a trimmed package-root export surface.

Still open (tracked in ROADMAP "Now"): pruning has no execution path yet;
provenance/author fields; tags/releases; `export`; shared repository config;
a committed reproducible scale harness; the docs split (architecture vs ADRs
vs roadmap).
