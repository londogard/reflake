"""Profile the 1M-file commit path to identify bottlenecks."""
from __future__ import annotations

import cProfile
import io
import pstats
import tempfile
import time
from pathlib import Path

from reflake.core import ReflakeRepository


def _touch_1m_files(worktree: Path) -> None:
    print("Generating 1M empty files...")
    (worktree / "images" / "cats").mkdir(parents=True, exist_ok=True)
    (worktree / "images" / "dogs").mkdir(parents=True, exist_ok=True)
    (worktree / "logs").mkdir(parents=True, exist_ok=True)
    (worktree / "other").mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    for i in range(1_000_000):
        if i < 200:
            path = worktree / "images" / "cats" / f"cat_{i:06d}.jpg"
        elif i < 500:
            path = worktree / "images" / "dogs" / f"dog_{i:06d}.jpg"
        else:
            category = "logs" if (i % 2) == 0 else "other"
            path = worktree / category / f"file_{i:07d}.bin"
        path.touch()
    print(f"  Done in {time.perf_counter()-t0:.2f}s")


def run() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        worktree = Path(tmpdir) / "worktree"
        worktree.mkdir()
        _touch_1m_files(worktree)

        repo = ReflakeRepository(worktree)
        from reflake.core.config import LocalConfig
        LocalConfig(dataset_root=str(worktree), identity="pointer").save(worktree)

        pr = cProfile.Profile()
        print("\nProfiling commit...")
        t0 = time.perf_counter()
        pr.enable()
        repo.commit("1M snapshot")
        pr.disable()
        print(f"Commit took: {time.perf_counter()-t0:.2f}s")

        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
        ps.print_stats(30)
        print(s.getvalue())


if __name__ == "__main__":
    run()
