"""Nightly performance gate (`-m benchmark`).

The structural budgets are the real gate: object counts and node reads are
exact and machine-independent, so a change that starts rewriting unchanged
directories or fetching every shard fails here regardless of CI hardware.
The wall-clock caps are deliberately loose (2-3x a laptop) and only catch
gross regressions.

Run explicitly with:

    uv run pytest tests/test_scale_budgets.py -m benchmark -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.benchmark

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import bench_scale  # noqa: E402


def test_structural_budgets_hold(tmp_path: Path) -> None:
    """A clean 10k-file repo: no wasted tree writes, no wasted node reads."""
    result = bench_scale.run(10_000, 200, keep=tmp_path)

    assert result.problems == [], result.problems
    assert result.noop_tree_writes == 0
    assert result.lookup_cold_node_reads <= 2
    assert result.prefix_cold_node_reads <= 2
    assert result.tree_objects == 201  # root + one node per directory


def test_flat_directory_reads_one_shard(tmp_path: Path) -> None:
    """A 10k-entry directory shards into 5 nodes; a lookup reads one."""
    result = bench_scale.run(10_000, 1, keep=tmp_path)

    assert result.problems == [], result.problems
    assert result.lookup_cold_node_reads == 3  # root + directory + shard
    # Listing the directory must read all five shard bodies.
    assert result.prefix_cold_node_reads == 7


def test_wall_clock_caps(tmp_path: Path) -> None:
    result = bench_scale.run(10_000, 200, keep=tmp_path)

    assert result.commit_s < 60, f"cold commit took {result.commit_s:.1f}s"
    assert result.noop_commit_s < 10, f"no-op commit took {result.noop_commit_s:.1f}s"
    assert result.status_s < 5, f"status took {result.status_s:.1f}s"
    assert result.lookup_ms < 250, f"cold lookup took {result.lookup_ms:.1f}ms"
