# Shared staging and undo leases

**Status:** design (0.6.0 follow-up; not implemented).
**Related:** [TODO.md](../TODO.md), [operator runbook](operator-runbook.md),
`docs/architecture.md` §6/§11.

## 1. The problem

0.6.0 fixed GC completeness for a **single client**:

- staged content blobs/footers are roots while they sit in the client's local
  `staging/` state;
- the client reflog is a root, so `reset`-away commits survive for undo.

Both roots are **client-local**. In a shared S3 repository, client B's
`gc --prune` cannot see client A's staging/reflog:

1. A runs `add` on a large S3 prefix in `content` mode. The blobs are uploaded
   immediately; the staging file is local.
2. A takes longer than `gc_grace_seconds` (default 24h) before committing.
3. B runs `gc --prune`. The blobs are unreachable from refs/tags, older than
   the grace window, and are deleted.
4. A commits. `commit` tries to re-materialize from `source_uri`; if the
   source was deleted/moved (the pointer retention contract was not written
   for content adds), the data is gone.

The same class applies to reflog-only commits after `reset`: B's GC can prune a
commit A could still undo to.

The fix cannot be a lock. Reflake is CAS-only and coordination-free. It must
be a **lease object in the shared scope** that any GC can read and that expires
without a daemon.

## 2. Goals and non-goals

Goals:

- A crashed writer's in-flight objects are protected for a bounded time.
- Any client's `gc` can see the protection.
- No locks, no central service, no per-object heartbeat at GC time.
- Bounded object count and bounded GC read cost.
- Works for both local and S3 stores (local is trivial but must stay
  consistent).
- No change to tree/commit formats; leases are disposable infrastructure.

Non-goals:

- A general distributed lock or transaction manager.
- Preventing a malicious writer from pinning garbage (TTL caps the damage).
- Cross-client staging *merge* semantics; leases only protect reachability.

## 3. Lease layout

Shared scope, next to `refs/`:

```
leases/
  stage/<writer-id>/<batch-id>.json     # blobs/footers published by `add`
  undo/<writer-id>/<batch-id>.json      # commits a client can undo to
```

`writer-id` is stable for a worktree/repository client (persisted in client
state, e.g. a random UUID generated on first open). `batch-id` is a random
identifier per `add`/reset batch (or a monotonic sequence), so a writer never
has to rewrite one growing JSON document.

A staging batch is small and bounded:

```json
{
  "version": 1,
  "writer": "0f3c...",
  "branch": "feature/x",
  "created_at": "2026-11-10T20:00:00Z",
  "expires_at": "2026-11-17T20:00:00Z",
  "blobs": ["<64 hex>", "..."],
  "footers": ["<64 hex>", "..."]
}
```

An undo batch carries `commits` instead of `blobs`/`footers`.

Bound each batch to, say, 1 000 entries; a million-file import writes ~1 000
lease objects, not one million and not one huge JSON. Each object is a single
PUT, written **after** the blobs it protects.

## 4. Write protocol

### `add` (content mode)

1. Upload/verify each blob as today (immutable, content-addressed).
2. Write one lease batch covering the newly published blobs/footers.
3. Save the local staging file (as today).

If the process crashes between 1 and 2, the blobs are protected only by the
ordinary grace window. That is the same window as today and is acceptable; the
lease narrows the unprotected window to one batch.

If the process crashes after 2, any GC keeps the blobs until the lease expires.

### `commit` / `promote` / `import`

1. Publish immutable objects.
2. CAS the ref.
3. Delete the writer's lease batches for that branch (best effort).

If deletion fails, the lease expires and GC collects later. Never delete a
lease before the ref CAS: the lease is the only thing protecting the objects.

### `reset` / `reflog`

1. CAS the branch pointer.
2. Write/refresh an `undo` lease batch containing the old and new commit ids
   (or the commit being left behind), with a longer TTL (e.g. 30 days).
3. Prune/ignore an undo lease when its commit is reachable from a branch/tag
   or the lease expires.

Undo leases can be per-writer and batched; `reflog` remains client-local, but
the lease makes the commits visible to any GC for the retention window.

### TTL and refresh

- Staging TTL: 7 days by default, configurable (`lease_ttl_seconds`).
- Undo TTL: 30 days by default, configurable.
- A new `add` from the same writer can either write a new batch (recommended)
  or refresh an existing one. Expiry is absolute; there is no GC heartbeat.

Clock skew: use the writer's `created_at`/`expires_at` for humans, but GC
compares `expires_at` against its own clock with a safety margin. A writer with
a badly skewed clock can under- or over-protect; document it.

## 5. GC algorithm

Mark phase additions, before sweeping:

1. List `leases/stage/` and `leases/undo/`.
2. For each lease object:
   - decode; skip malformed/unknown versions with a warning (never let a
     malformed lease make a live ref unreachable);
   - if `expires_at` is in the future, add its `blobs`, `footers`, and
     `commits` to the corresponding reachable sets (commits are walked through
     the normal commit/tree DAG).
3. Sweep as today, with the ordinary grace window as the final backstop.

Expired leases are candidates for deletion by `gc` (a separate sweep, after
the mark phase, or the same sweep). An expired lease protects nothing; leaving
it for one more cycle is harmless.

Read cost: one LIST per lease prefix and one GET per lease batch. A writer
staging 1M files produces ~1 000 batches; GC reads ~1 000 small JSON objects,
not 1M.

## 6. Concurrency and failure model

- **No locks.** Each writer owns its `writer-id`/`batch-id` objects; no
  read-modify-write of a shared lease.
- **CAS not required for leases.** They are advisory reachability hints, not
  branch state. A torn/partial lease is ignored (and expires), never applied.
  Writers should still write the lease in one PUT.
- **Crash before lease write:** grace window protects.
- **Crash after lease write, before commit:** lease protects until TTL; GC
  eventually collects.
- **Crash after ref CAS, before lease delete:** lease protects longer than
  necessary; GC collects after expiry.
- **Clock skew:** TTL margin; document.
- **Malicious writer:** can pin garbage until TTL; cap TTL and audit lease
  counts/sizes. No data loss.
- **Mixed versions:** older clients do not know `leases/`; their GC could
  prune protected objects. Before 1.0 this is acceptable but must be called
  out; after 1.0, `repo.json` should carry a `lease_format`/minimum-GC version
  and an older client should refuse `gc --prune` (or the server-side default
  should be audit-only).

## 7. Why not alternatives?

- **Per-blob lease object:** one PUT per blob, doubling the object count of a
  large import; worse than batched leases.
- **Branch lock / coordination service:** violates the no-server,
  CAS-only guardrail.
- **Infinite grace window:** unbounded garbage, no retention policy.
- **Require quiescence:** operationally brittle for a shared bucket; still
  needed as a documented fallback for mixed-version clients.
- **Store staging in the shared ref namespace:** pollutes ref listing and
  couples GC to client file formats.

## 8. Implementation plan

1. Client state: persist `writer_id`; add `LeaseManager` (encode/decode,
   batch ids, expiry).
2. `StagingArea`/`repo_add`: write staging lease batches after content-mode
   `add`; delete on successful commit (best effort).
3. `reset`: write undo lease batches (and optionally on `branch`/`tag` delete).
4. GC: list/read lease prefixes; add blobs/footers/commits to reachability;
   delete expired leases; report `leased_*` counts in `GcResult`.
5. Local store: implement the same via `_kv_list`/`_kv_put` (no special case).
6. CLI/config: `lease_ttl_seconds`, `undo_lease_ttl_seconds`; `gc` JSON fields.
7. Tests (fake S3):
   - two clients: A stages, B prunes with `grace_seconds=0`, blob survives;
   - after A commits and deletes leases, B prunes and blob disappears;
   - expired lease does not protect;
   - malformed lease is ignored and does not fail GC;
   - reset/undo lease protects a reset-away commit from another client;
   - 1M/1k-batch object-count assertions.
8. Docs: runbook retention section, README GC roots, `docs/architecture.md`
   §11, CHANGELOG.

## 9. Open decisions

1. Is `writer-id` per worktree or per client-state directory? Per worktree is
   simpler; moving a worktree should keep the same id.
2. Should staging leases be per branch or per writer? Per writer+branch makes
   deletion on commit precise; per writer is simpler.
3. Do we need a `lease_version` in `repo.json` now, or only at 1.0?
4. Undo TTL: fixed 30 days, or derived from reflog retention config?
5. Should `gc --prune` refuse to run if it finds leases from an unknown
   format/version (fail closed) or ignore them with a warning?
