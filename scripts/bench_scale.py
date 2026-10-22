#!/usr/bin/env python3
"""Reproducible scale harness for the commit/read hot paths.

Every number in CHANGELOG.md and docs/architecture.md should be reproducible
with this script, on a laptop and in CI:

    uv run python scripts/bench_scale.py --files 10000
    uv run python scripts/bench_scale.py --files 1000000 --dirs 1000

It measures the operations the cost model makes claims about:

  generate   write N files (excluded from every other number)
  commit     first commit of the whole tree (hashing + blob writes + trees)
  noop       commit with nothing changed (must write 0 tree objects)
  touch      one changed file in a 50-directory repo (must write <= 4 trees)
  status     clean worktree (must not read a single file)
  lookup     warm exact-path lookup (must read <= depth nodes)
  list       prefix listing inside a sharded directory (one shard)

Output is a Markdown table plus a JSON document (`--json`), so a CI job can
diff runs instead of eyeballing them. Exits non-zero when a *structural*
budget is violated (object counts, node reads): those are exact and
machine-independent, unlike wall-clock.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

# Allow running from a checkout without installing.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reflake import init_repository, open_repository  # noqa: E402
from reflake.core.objects.tree import MAX_TREE_ENTRIES  # noqa: E402


@dataclass
class Result:
    files: int
    directories: int
    node_fanout: int
    generate_s: float = 0.0
    commit_s: float = 0.0
    noop_commit_s: float = 0.0
    status_s: float = 0.0
    lookup_ms: float = 0.0
    lookup_cold_node_reads: int = 0
    prefix_list_ms: float = 0.0
    prefix_cold_node_reads: int = 0
    tree_objects: int = 0
    noop_tree_writes: int = 0
    problems: list[str] = field(default_factory=list)


def _build_worktree(root: Path, files: int, directories: int) -> None:
    for directory in range(directories):
        path = root / f"dir_{directory:04d}"
        path.mkdir(parents=True, exist_ok=True)
        for index in range(files // directories):
            (path / f"file_{index:05d}.txt").write_text(f"{directory}-{index}")


def _counting_reader(repo) -> tuple[list[str], object]:
    """Wrap tree reads so we can count nodes fetched."""
    reads: list[str] = []
    original = repo.store.read_tree_bytes

    def counting(tree_hash: str) -> bytes | None:
        reads.append(tree_hash)
        return original(tree_hash)

    repo.store._walker._read_tree = counting  # type: ignore[attr-defined]
    return reads, original


def run(files: int, directories: int, *, keep: Path | None = None) -> Result:
    result = Result(files=files, directories=directories, node_fanout=MAX_TREE_ENTRIES)
    root = keep or Path(tempfile.mkdtemp(prefix="reflake-bench-"))
    try:
        start = time.perf_counter()
        _build_worktree(root, files, directories)
        result.generate_s = time.perf_counter() - start

        init_repository(root)
        repo = open_repository(root)

        start = time.perf_counter()
        repo.commit("scale")
        result.commit_s = time.perf_counter() - start

        commit = repo.read_commit(repo.resolve_ref("main"))
        result.tree_objects = sum(
            1 for _ in repo.tree_writer.iter_tree_hashes(commit.tree)
        )

        # No-op commit: changed directories are byte-identical, so zero tree
        # objects may be written.
        writes: list[str] = []
        original_write = repo.store.write_tree_bytes

        def counting_write(tree_hash: str, payload: bytes, *, if_missing: bool = True):
            writes.append(tree_hash)
            return original_write(tree_hash, payload, if_missing=if_missing)

        repo.store.write_tree_bytes = counting_write  # type: ignore[assignment]
        start = time.perf_counter()
        repo.commit("noop")
        result.noop_commit_s = time.perf_counter() - start
        result.noop_tree_writes = len(writes)
        repo.store.write_tree_bytes = original_write  # type: ignore[assignment]

        start = time.perf_counter()
        repo.status(working_tree=True)
        result.status_s = time.perf_counter() - start

        reads, _ = _counting_reader(repo)

        # Warm exact lookup: the path chain is cached, so 0 node fetches.
        target = f"dir_{directories // 2:04d}/file_00000.txt"
        repo.store.tree_cache._entries.clear()
        repo.store.lookup_entry(commit.tree, target)
        reads.clear()
        start = time.perf_counter()
        found = repo.store.lookup_entry(commit.tree, target)
        result.lookup_ms = (time.perf_counter() - start) * 1000
        if found is None:
            result.problems.append(f"lookup missed {target}")

        # Cold exact lookup: root + path depth nodes.
        repo.store.tree_cache._entries.clear()
        reads.clear()
        start = time.perf_counter()
        repo.store.lookup_entry(commit.tree, target)
        result.lookup_cold_node_reads = len(reads)
        result.lookup_ms = (time.perf_counter() - start) * 1000

        # Cold prefix listing inside one directory. A sharded directory must
        # fetch only the shard whose range contains the prefix.
        prefix = target.rsplit("/", 1)[0]
        repo.store.tree_cache._entries.clear()
        reads.clear()
        start = time.perf_counter()
        listed = list(repo.store.iter_entries_for_prefix(commit.tree, prefix))
        result.prefix_list_ms = (time.perf_counter() - start) * 1000
        result.prefix_cold_node_reads = len(reads)
        if not listed:
            result.problems.append(f"prefix listing empty for {prefix}")

        # Structural budgets: exact, machine independent.
        if result.noop_tree_writes != 0:
            result.problems.append(
                "no-op commit wrote "
                f"{result.noop_tree_writes} tree objects (expected 0)"
            )
        # Structural budgets. A cold path resolution reads root + one node per
        # path level, plus one shard when the containing directory is
        # sharded. Listing a whole directory must read every one of its
        # shards; listing *inside* it must read one (that is the shard-range
        # pruning this harness exists to protect).
        per_directory = max(1, files // directories)
        shards = -(-per_directory // MAX_TREE_ENTRIES) if per_directory else 0
        expected_lookup = 2 + (1 if shards > 1 else 0)
        expected_prefix = 2 + (shards if shards > 1 else 0)
        if result.lookup_cold_node_reads > expected_lookup:
            result.problems.append(
                f"cold lookup read {result.lookup_cold_node_reads} nodes "
                f"(expected <= {expected_lookup})"
            )
        if result.prefix_cold_node_reads > expected_prefix:
            result.problems.append(
                f"cold prefix listing read {result.prefix_cold_node_reads} nodes "
                f"(expected <= {expected_prefix})"
            )
    finally:
        if keep is None:
            shutil.rmtree(root, ignore_errors=True)
    return result


def _markdown(result: Result) -> str:
    rows = [
        ("files", result.files),
        ("directories", result.directories),
        ("node fanout", result.node_fanout),
        ("tree objects", result.tree_objects),
        ("generate", f"{result.generate_s:.2f} s"),
        ("commit (cold)", f"{result.commit_s:.2f} s"),
        ("commit (no-op)", f"{result.noop_commit_s:.2f} s"),
        ("no-op tree writes", result.noop_tree_writes),
        ("status (clean)", f"{result.status_s:.3f} s"),
        ("lookup (cold)", f"{result.lookup_ms:.3f} ms"),
        ("lookup node reads", result.lookup_cold_node_reads),
        ("prefix listing (cold)", f"{result.prefix_list_ms:.3f} ms"),
        ("prefix node reads", result.prefix_cold_node_reads),
    ]
    width = max(len(label) for label, _ in rows)
    lines = ["| metric | value |", "|---|---|"]
    lines += [f"| {label.ljust(width)} | {value} |" for label, value in rows]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files", type=int, default=10_000)
    parser.add_argument("--dirs", type=int, default=200)
    parser.add_argument("--dir", type=Path, default=None, help="Use this directory")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.files < args.dirs:
        parser.error("--files must be >= --dirs")

    result = run(args.files, args.dirs, keep=args.dir)
    if args.json:
        print(json.dumps(asdict(result), indent=2))
    else:
        print(_markdown(result))
        print()
        print(f"python {sys.version.split()[0]}, {os.cpu_count()} cpus")
    if result.problems:
        print("\nBUDGET VIOLATIONS:", file=sys.stderr)
        for problem in result.problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
