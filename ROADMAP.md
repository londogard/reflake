# Reflake Roadmap

## Product Direction

Reflake should remain client-driven while supporting a shared repository stored in object storage.

Target operating modes:

- Local repository backend for development, testing, and small datasets.
- S3 repository backend for shared collaborative datasets.

The CLI and repository model should stay unified across both modes. The user should interact with one repository concept, with the backend selected by repository URI or path.

Recommended long-term shape:

- Shared remote repository state in S3.
- Local client state for staging, current branch preference, caches, and temp files.
- Metadata-first operations for branching, diffing, merging, removing, and renaming.
- Content reads only when a command truly needs object bytes.

## Decisions

### Keep Both Local And S3

Keep both backends.

- Local remains useful for tests, development, offline workflows, and small datasets.
- S3 becomes the primary collaboration backend.
- The user-facing abstraction should be one repository concept, not two separate products.

### Keep Both Identity Modes

Keep both `blake3` and `meta`.

- `blake3` stays the default and durable canonical mode.
- `meta` stays the cheap bootstrap/import mode.
- `verify` remains the promotion path from metadata-only entries to canonical blob-backed entries.

To reduce complexity, `meta` should primarily be used for import/bootstrap flows rather than expanded into every workflow.

## Architectural Goal

Move Reflake from a local-working-tree-first design to a manifest-first repository design.

For large datasets, operations such as these must not require downloading or rewriting the full dataset:

- add 3 files
- remove 5 files
- rename `image.jpg` to `image.jpeg`
- merge a fast-forward branch
- bulk path rewrites

These should become metadata transformations over manifests and refs, plus selective uploads only for genuinely new content.

## Phase 1: Repository Store Abstraction

Goal: separate shared repository data from local client state.

Introduce a repository store abstraction that owns:

- blobs
- manifests
- commits
- refs

Operations needed:

- read commit object
- write commit object
- stream manifest reads
- stream manifest writes
- read branch ref
- compare-and-set branch ref
- read blob bytes
- write blob if missing
- check object existence
- retrieve backend version token or etag where available

Deliverables:

- `LocalRepositoryStore`
- `S3RepositoryStore`
- refactor repository code to stop reading and writing repository state directly with `Path`

Acceptance criteria:

- Existing local tests still pass.
- Repository mutations no longer depend on direct local `.reflake` path writes.

## Phase 2: Local Client State

Goal: keep user-specific mutable state local even when the repo is remote.

Keep these local only:

- active branch preference
- staging state
- temp files
- caches

Important decision:

- Do not keep shared remote `HEAD`.
- Shared truth is branch refs under `refs/heads/*`.
- Current branch selection is a client preference.

Deliverables:

- local client-state abstraction
- branch preference handling decoupled from shared repo refs
- local staging store independent of repository backend

Acceptance criteria:

- Multiple users can share the same S3-backed repo without clobbering each other's active branch preference.

## Phase 3: Safe Ref Updates On S3

Goal: make commits and merges safe for concurrent clients.

Implement optimistic concurrency for branch refs:

- read current ref value plus version token or etag
- update ref only if the token still matches
- fail clearly on write conflicts

Deliverables:

- compare-and-set branch updates in the repository store
- conflict errors surfaced clearly from commit and merge flows

Unimplemented note:

- stale-lock recovery is still missing if a client dies mid-update and leaves behind a branch lock object

Acceptance criteria:

- Concurrent updates do not silently overwrite each other.
- Fast-forward merge remains safe under concurrent clients.

## Phase 4: Repo URI Support

Goal: unify local and remote repositories behind one CLI and Python API.

Introduce a repository argument that can be either:

- local path
- `s3://bucket/prefix`

Examples:

```bash
uv run reflake --repo /tmp/demo commit -m "local commit"
uv run reflake --repo s3://my-bucket/datasets/demo branch feature
```

Deliverables:

- repository URI parsing
- backend selection based on repo location
- migration of existing commands to repository-aware `--repo` semantics

Acceptance criteria:

- Same command set works for both local and S3-backed repositories.

## Phase 5: Remote-Native Metadata Operations

Goal: support metadata changes without local checkout.

Prioritized commands:

- `branch`
- `diff`
- `merge`
- `rm`
- `mv` or rename

These operations should:

- load manifest metadata only
- transform logical paths or membership
- write a new manifest and commit
- update the target branch ref

They should not:

- download unchanged blobs
- reupload unchanged blobs
- depend on a full local working tree

Acceptance criteria:

- Removing and renaming paths in a large S3-backed repo can complete without reading full object payloads.

## Phase 6: Content Ingress Paths

Goal: support adding new content efficiently without forcing full dataset materialization.

Supported ingress modes:

- local file add
- S3 import
- verify selected metadata-only entries into canonical blobs

Rules:

- only read bytes for newly added or verified objects
- preserve existing manifest entries without rewriting blob content

Acceptance criteria:

- adding a small number of files to a large repo only uploads those new files and metadata

## Phase 7: S3-Native Integration Tests

Goal: validate shared-object-store behavior against a real S3-compatible service.

Preferred test target:

- Ministack

Coverage:

- branch creation
- commit updates
- fast-forward merge
- metadata import
- selective verify
- metadata-only remove and rename
- optimistic concurrency conflicts

Testing strategy:

- keep current fake-client tests as fast unit coverage
- add integration-marked tests for real object storage behavior using Ministack

Acceptance criteria:

- Core repo flows succeed against a real S3-compatible API, not just mocked boto calls.

## Phase 8: Optional Path Ergonomics

Goal: improve URI and path handling without weakening backend correctness.

`cloudpathlib.AnyPath` may be used as a convenience layer for path or URI ergonomics, but it should not replace the repository store abstraction.

Reason:

- Reflake still needs explicit backend semantics for optimistic locking, conditional writes, etags, and streaming control.

Recommendation:

- keep `StorageBackend` or `RepositoryStore` as the core contract
- optionally use `AnyPath` at the edges for parsing and convenience

## Initial Execution Order

Recommended implementation order:

1. Repository store abstraction
2. Local client state split
3. Safe S3 ref updates
4. Repo URI support
5. Remote-native `rm`
6. Remote-native `mv`
7. Ministack integration tests

## Explicit Non-Goals For Now

- write-capable `fsspec` interface
- non-fast-forward merge support
- server or daemon architecture
- central database
- remote sync commands as a substitute for repository-native S3 support