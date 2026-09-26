"""Profile GC: orphan-heavy stores and reachable-heavy repositories.

Measures the two costs that scale with object count — the mark phase
(reachability walk) and the sweep (existence listing + deletion) — so the
linear behaviour and the memory profile are visible and trackable.

Usage:
    uv run python scripts/profile_gc.py orphans 200000
    uv run python scripts/profile_gc.py reachable 1000000 [--memory]
"""

from __future__ import annotations

import argparse
import gc as gc_module
import resource
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

from reflake.core import Entry, create_repository


def _rss_mb() -> float:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kilobytes.
    return value / (1024 * 1024) if sys.platform == "darwin" else value / 1024


class _Stopwatch:
    def __init__(self, label: str) -> None:
        self.label = label

    def __enter__(self) -> _Stopwatch:
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        elapsed = time.perf_counter() - self.start
        print(f"  {self.label}: {elapsed:.2f}s   (rss_peak {_rss_mb():.0f} MB)")


def bench_orphans(count: int, memory: bool) -> None:
    """Fabricate *count* dangling blobs and measure audit + prune."""
    root = Path(tempfile.mkdtemp(prefix="gc-bench-orphans-"))
    repo = create_repository(root)
    (root / "seed.txt").write_text("seed")
    repo.commit("seed")
    store = repo.store

    with _Stopwatch(f"fabricate {count:,} orphan blobs"):
        for index in range(count):
            path = store.blob_path(f"{index:064x}")  # type: ignore[attr-defined]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x")

    if memory:
        tracemalloc.start()
    with _Stopwatch("store scan (ids + mtimes)"):
        scanned = sum(1 for _ in store.iter_object_ids_with_mtimes("blob"))
    if memory:
        live, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(f"    scan tracemalloc live={live / 1e6:.1f} MB peak={peak / 1e6:.1f} MB")

    with _Stopwatch("gc audit (dry run)"):
        audit = repo.gc(dry_run=True)
    print(f"    reachable={audit.reachable_blobs} orphans={audit.orphan_blobs}")

    with _Stopwatch("gc --prune (grace 0)"):
        pruned = repo.gc(dry_run=False, grace_seconds=0)
    print(f"    pruned={pruned.pruned} skipped_young={pruned.skipped_young}")
    remaining = sum(1 for _ in store.iter_object_ids("blob"))  # type: ignore[attr-defined]
    print(f"    blobs remaining: {remaining} (scanned {scanned})")


def bench_reachable(count: int, memory: bool) -> None:
    """Build a repository with *count* tracked files and measure GC."""
    root = Path(tempfile.mkdtemp(prefix="gc-bench-reach-"))
    repo = create_repository(root)

    def entries():
        for index in range(count):
            directory = index // 1000
            name = index % 1000
            yield Entry(
                path=f"d{directory:03d}/f{name:04d}.bin",
                kind="b",
                hash=f"{index:064x}",
                size=1,
                mtime_ns=1,
            )

    with _Stopwatch(f"build tree from {count:,} entries"):
        tree = repo.tree_writer.build_from_entries(entries())
    with _Stopwatch("write commit"):
        commit_id = repo.tree_writer.write_commit_object(
            branch="main",
            message="synthetic",
            tree_hash=tree,
            expected_commit_id=None,
            operation="commit",
        )
    gc_module.collect()

    if memory:
        tracemalloc.start()
        with _Stopwatch("gc audit (dry run) [tracemalloc on]"):
            audit = repo.gc(dry_run=True)
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(f"    tracemalloc peak={peak / 1e6:.0f} MB")
    else:
        with _Stopwatch("gc audit (dry run)"):
            audit = repo.gc(dry_run=True)
    print(
        f"    reachable commits={audit.reachable_commits} "
        f"trees={audit.reachable_trees} blobs={audit.reachable_blobs} "
        f"orphans={audit.orphan_blobs}"
    )
    print(f"    head {commit_id[:12]} rss_peak {_rss_mb():.0f} MB")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("orphans", "reachable"))
    parser.add_argument("count", type=int)
    parser.add_argument(
        "--memory",
        action="store_true",
        help="measure Python allocations with tracemalloc (slower)",
    )
    args = parser.parse_args()
    if args.mode == "orphans":
        bench_orphans(args.count, args.memory)
    else:
        bench_reachable(args.count, args.memory)


if __name__ == "__main__":
    main()
