# Reflake Operator Runbook

This document covers operational procedures for running Reflake against
S3-compatible object storage. It assumes you are already familiar with the
[Reflake README](../README.md) and [architecture](architecture.md).

> Concurrency safety is CAS-only (architecture §6): branch updates
> compare-and-set on the commit id (`IfMatch`/`IfNoneMatch` on S3, file
> locking for local repos). **There are no branch locks** — no `locks/`
> prefix, no `lock list/cleanup` commands.

---

## Repository layout (v2 tree model)

```
<PREFIX>/
  blobs/          # Content-addressed canonical objects (blake3, immutable)
  trees/          # Merkle tree nodes (sorted JSONL, content-addressed)
  footers/        # Parquet footer-stats objects (only when parquet_footer=true)
  commits/        # Commit objects {tree, parents[], message, generation}
  refs/heads/     # Branch pointers (the ONLY mutable state, CAS-updated)
```

Client-local state (`state/`, `staging/`, `cache/`, `index/`, `reflog/`)
lives next to the working repo and is never synced. There are no
`manifests/`, `manifests/*.idx`, or `locks/` objects — any runbook or
automation referencing them is stale.

---

## S3 IAM

Minimum policy for normal operations:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<BUCKET>",
                "arn:aws:s3:::<BUCKET>/<PREFIX>/*"
            ]
        }
    ]
}
```

Conditional writes (`IfMatch`/`IfNoneMatch`) require an S3-compatible
endpoint that honors them (AWS S3, MinIO, Ministack). **Credentials** come
from the standard AWS chain (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `~/.aws/credentials`, IAM instance profiles).
Reflake never stores credentials itself.

---

## Encryption

### In transit
All S3 API calls use HTTPS (TLS). Configure your endpoint to enforce TLS 1.2+.

### At rest

| Tier | Mechanism | Recommendation |
|------|-----------|----------------|
| S3 server-side | SSE-S3 (AES-256) | Enable as bucket default. Zero Reflake configuration needed. |
| S3 server-side | SSE-KMS | Set the default bucket encryption to KMS; grant the Reflake IAM role `kms:Decrypt` + `kms:GenerateDataKey`. |
| Client-side | Not yet supported | File an issue if this is a blocker. |

**Bucket policy snippet (enforce SSE-S3):**

```json
{
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::<BUCKET>/*",
    "Condition": {
        "StringNotEquals": {
            "s3:x-amz-server-side-encryption": "AES256"
        }
    }
}
```

---

## Bucket Versioning

**Enable bucket versioning.** All Reflake objects except `refs/heads/*`
are immutable by key, so versioning gives:

- **Accidental-deletion protection** (recover previous versions).
- **Audit trail** of every ref update.

```bash
aws s3api put-bucket-versioning \
    --bucket <BUCKET> \
    --versioning-configuration Status=Enabled
```

Not required to function, strongly recommended for production.

---

## Lifecycle / Retention

1. **Expire old object versions** after roll-over:
   ```json
   {
       "Rules": [
           {
               "Id": "expire-old-versions",
               "Status": "Enabled",
               "Filter": {},
               "NoncurrentVersionExpiration": { "NoncurrentDays": 90 }
           }
       ]
   }
   ```

2. **Transition blobs to cheaper storage** (optional):
   blobs are content-addressed and immutable — moving them to
   `STANDARD_IA` or Glacier Instant Retrieval after ~30 days is safe.
   Apply the rule to `<PREFIX>/blobs/` only.

**Do not** expire the *current version* of any object, and never
lifecycle-delete `refs/heads/*` — branch pointers are live mutable state.

---

## Backups

Protect against **bucket-level** loss (region failure, account compromise,
bucket deletion).

| Strategy | Coverage | Recovery time |
|----------|----------|---------------|
| **S3 Cross-Region Replication (CRR)** | All objects | Minutes |
| **AWS Backup for S3** | Point-in-time bucket restore | Hours |
| **Periodic `s3 sync` to another bucket** | blobs, trees, footers, commits, refs | Hours |

### What to back up

Everything under the prefix:

```
<PREFIX>/
  blobs/
  trees/
  footers/
  commits/
  refs/heads/
```

### Restore procedure

1. Restore the S3 prefix to the target bucket.
2. Clients resume normally — they re-read branch refs on the next command.
   If a client holds a stale snapshot it gets a `RefConflictError` and
   retries (staged commits re-apply automatically).

---

## Garbage collection

`reflake gc` is audit-only by default: it walks the commit DAG from every
branch head (following **all** parents of merge commits), then the tree
DAGs, and reports orphaned commits/trees/blobs/footers.

```bash
reflake --repo s3://my-bucket/my-prefix gc        # audit
reflake --repo s3://my-bucket/my-prefix gc --prune # delete orphans
```

- Run audit before every prune; prune only when no client is mid-push.
- With bucket versioning, pruned objects remain recoverable as
  noncurrent versions until your lifecycle rule expires them.
- GC reachability covers merged (second-parent) lineage — verify with
  `tests/test_gc.py` after upgrades.

## Parquet footer backfill

Footer-stats objects (`footers/`) exist only for repos with
`config set parquet_footer true` at ingest time. Older parquet entries
gain footers on the next commit that touches them, or via
`reflake promote`. `query prune` keeps row groups conservatively when
stats are absent — missing footers never cause wrong query results.

---

## Incident Recovery

### Symptom: `RefConflictError` on commit/push/pull

**Cause:** Another client advanced the branch between your read and your
write (optimistic-concurrency conflict). There is no lock to clear.

**Resolution:**
1. `reflake pull <URI>` (or `fetch`) to pick up the new head.
2. Retry — `commit --staged` re-applies the overlay onto the new parent
   and retries automatically (up to 3 attempts).
3. For `push` divergence (`NonFastForwardError`): pull, merge, push again.

### Symptom: `push` / `pull` reports "Everything up-to-date" but refs differ

**Cause:** Non-fast-forward — the remote moved since your last fetch.

**Resolution:**
1. `reflake pull <URI>` to fetch the latest state.
2. `reflake merge <source> <target>`; resolve conflicts if any.
3. Push again.

### Symptom: `promote` fails with `FileNotFoundError` (source_uri missing)

**Cause:** A pointer (`pointer`) entry's source object was deleted or
moved before promotion.

**Resolution:**
1. Restore the source object at its original `source_uri`.
2. Re-run `reflake promote`.
3. If unrestorable, the entry is unrecoverable: `reflake rm <path>` +
   `reflake commit --staged`.

### Symptom: Corrupted or missing blob

**Cause:** A blob was deleted/truncated (e.g. aggressive lifecycle rule).
Local writes are atomic (temp file + rename) and hash-verified, so local
corruption implies disk failure, not torn writes.

**Resolution:**
1. With bucket versioning: restore the previous version.
2. Without versioning/backup: entries referencing the blob fail to read —
   re-import the data.

### Symptom: Bucket or prefix accidentally deleted

**Resolution:**
1. Restore from backup (see [Backups](#backups)).
2. `reflake --repo <URI> verify` to confirm integrity.

---

## Quick Reference

| Task | Command |
|------|---------|
| Audit pointer entries | `reflake --repo <URI> verify` |
| Promote pointers to blobs | `reflake --repo <URI> promote` |
| Audit orphaned objects | `reflake --repo <URI> gc` (`--prune` deletes) |
| List branches with heads | `reflake --repo <URI> branches` |
| Branch history | `reflake --repo <URI> reflog` |
| Row-group pruning check | `reflake --repo <URI> query prune <ref> <path> --where "..."` |
