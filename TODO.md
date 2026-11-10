# TODO — after 0.6.0

**Status:** 0.6.0 shipped the review hardening (verified reads, GC/sync
completeness, durable writes, worktree safety, directory-aware VFS, KV store
core, streaming plans). This file is the working backlog. It is deliberately
ordered by risk, not by feature appeal.

Priority legend: **P0** = data safety / shared-repo correctness, **P1** = 1.0
blocker, **P2** = ecosystem, performance, or developer experience.

---

## P0 — shared-repository data safety

- [ ] **Implement shared staging/undo leases.** Today GC roots for staged
      content and client reflogs are *client-local*: client B's
      `gc --prune` cannot see client A's staged blobs or undo commits. The
      design is in [docs/staging-lease.md](docs/staging-lease.md).
      *Acceptance:* A stages a blob on S3; B runs `gc --prune --grace-seconds 0`;
      the blob survives while A's lease is live. After A commits (or the lease
      expires), a later GC removes it. Same for a reset-away commit protected
      by an undo lease. Fake-S3 two-client tests; no locks.
- [ ] **Bounded-memory GC mark phase.** Reachability sets are still O(reachable
      objects) in RAM. Move the mark sets to a disk-backed/sharded set (or
      stream per commit/tree with a sorted on-disk index) and keep peak memory
      below a configurable budget at 1M+ objects. Add a benchmark that asserts
      peak RSS and object-op counts.
- [ ] **Crash-injection suite for every publish path.** Fail the Nth local/S3
      write during `add`, `commit`, `import`, `promote`, `push`, `pull`, and
      `gc`; after each failure, `fsck` must report no live ref pointing at a
      missing object. This is the executable form of I3/I16.
- [ ] **S3-compatible conditional-write capability probe.** Verify
      `IfMatch`/`IfNoneMatch` support once per repository (or at init) and
      fail closed with a clear error on stores that silently ignore them.
      Document the support matrix (AWS S3, MinIO, R2, GCS-S3, Azure-S3).
- [ ] **Retention policy / safe prune.** Add `gc --retain` semantics
      (tags + age + minimum commits) and a cooperative maintenance marker for
      long-running writers. A grace window alone is not a retention policy.
- [ ] **Cross-client reflog/undo policy.** Decide whether undo history is
      purely local (documented) or shared via undo leases; make the answer
      explicit in `reflog` output and the runbook.

## P1 — 1.0 blockers

- [ ] **Provenance.** Record author/actor, optional run/job ID, and a
      canonical commit metadata block. Decide what is hashed into the commit
      id (recommended: hash a `metadata` map; keep `created_at` unhashed) and
      add a format migration.
- [ ] **Hooks / data-quality plugins.** Add a client-side hook protocol, e.g.
      `pre_commit(staged, parent) -> Decision`, `post_commit(commit)`,
      `pre_merge`, `post_restore`. Fail closed; no server. Ship one example
      quality hook (row count / null budget) using `query scan`. Document that
      hooks are client policy, not a repository guarantee.
- [ ] **Catalog / discovery.** A well-known registry (e.g.
      `catalog.json` or `_reflake/catalog`) so a bucket can be browsed without
      a hand-maintained `dataset_roots` map; `reflake://dataset@ref` should
      resolve from that registry. Keep it optional and client-readable.
- [ ] **`export` / `import`.** First-class snapshot export to a directory or
      tar with a manifest and per-file hashes; import back into a repository.
      This is the handoff path to non-Reflake consumers.
- [ ] **Format migration.** `reflake migrate` from 0.4/0.5, plus a written
      compatibility policy. Freeze the format at 1.0 and support N-1 reads.
- [ ] **S3 integration in CI.** The MiniStack suite is skipped on machines
      without Docker; make it a required CI job (it already is in the workflow)
      and add nightly runs against at least one non-AWS S3-compatible store.
- [ ] **Security review / release hygiene.** Threat model (untrusted bucket
      writer, path traversal, malicious tree/commit, lease forgery), SBOM,
      dependency scanning, `SECURITY.md`, and a signed-release decision.
- [ ] **Licensing decision.** AGPL-3.0-or-later is a deliberate choice; if
      embedders are a target, decide on Apache-2.0 or dual licensing before
      1.0. Document the answer in `NOTICE`/README.
- [ ] **Performance claims at 1M.** Re-run the committed scale harness for
      commit/status/lookup/list/sync/GC with the new KV/streaming paths and
      publish reproducible numbers. Add sync/GC structural budgets.

## P2 — ecosystem and architecture

- [ ] **Spark / Trino / Arrow integration.** A real `FileIO`/catalog or
      Arrow-dataset bridge over `reflake://`; DuckDB and fsspec are done.
      This is the difference between “Python library” and “lakehouse piece”.
- [ ] **Lazy local clones / promisor reads.** `pull --metadata-only` hydrates
      on the next full pull, but a local repository cannot lazily read a blob
      from its configured remote. Either implement a promisor remote or
      document that virtual reads require an S3-backed clone.
- [ ] **Tag metadata-only transfers.** `pull --ref <tag> --metadata-only` is
      currently a full transfer; either implement the partial marker for tags
      or reject the combination explicitly.
- [ ] **Split the tree splicer** out of `TreeWriter` (merger and builder are
      already separate) and unify the three path validators in one
      `core/paths.py` module.
- [ ] **Remove `simple-parsing`?** The CLI is now a single registry, but the
      parser still depends on simple-parsing. Replacing it with `argparse`
      would drop a dependency and the remaining help-format hacks.
- [ ] **Shared repository policy refresh.** A long-lived client reads
      `repo.json` at open; policy changes by another client are not observed
      until reopen. Add a cheap version/ETag check or document the requirement.
- [ ] **Docs split / ADRs.** Architecture, ADRs for the KV/lease/format
      decisions, operator runbook, and a generated CLI reference.
- [ ] **Table-directory integration story.** Do not implement a table format.
      Document and test committing a Delta/Iceberg directory as an ordinary
      dataset, so table engines can consume the ref via a FileIO bridge.

---

## Explicit non-goals (keep them out)

- No server, daemon, lock service, or central catalog database.
- No table semantics (schemas, partitions, compaction, row-level deletes).
- No ML-throughput blob reorg (tarballs/sharded blob layers).
- No Windows support (POSIX-only; documented).
- No history rewriting (reset is the undo primitive).

## Known limits as of 0.6.0

- Cross-client GC visibility is the remaining P0 (see the lease design).
- `gc` mark memory is O(reachable); sync plans and query results stream.
- s5cmd batch mode must materialize its line manifest; the boto3 path streams.
- The S3 MiniStack suite cannot run where Docker is unavailable.
- `fsck --verify-blobs` reads the whole dataset; tree/commit verification is
  always on.
- `cat` verifies at end-of-stream, so a corrupt stream can emit partial bytes
  before the error; use `restore`/`query scan --verify` when pre-emission
  verification matters.
