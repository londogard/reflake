"""Bounded concurrency for object I/O.

Every network-bound loop in Reflake (blob uploads during a commit, transfer
plans, restores, multipart parts) is latency-bound, not CPU-bound: a single
S3 PUT costs ~20-60 ms of waiting. Running a handful in flight is the
difference between a 1 000-file commit taking 30 s and 2 s.

One helper, used everywhere, so every path gets the same behaviour:

- a small thread pool (boto3 and blake3 both release the GIL);
- deterministic result order, like ``map``;
- the first exception propagates and cancels the remaining work.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")

#: Default in-flight object operations. High enough to hide RTT, low enough to
#: stay well inside S3 request-rate and socket-pool limits.
DEFAULT_IO_WORKERS = min(16, max(4, (os.cpu_count() or 4) * 4))


def map_bounded(
    function: Callable[[T], R],
    items: Iterable[T],
    *,
    workers: int | None = None,
) -> list[R]:
    """``list(map(function, items))`` with up to *workers* calls in flight."""
    materialized = list(items)
    if not materialized:
        return []

    def run(item: T) -> R:
        return function(item)

    resolved_workers = workers or DEFAULT_IO_WORKERS
    if len(materialized) == 1 or resolved_workers < 2:
        return [run(item) for item in materialized]
    with ThreadPoolExecutor(max_workers=resolved_workers) as pool:
        return list(pool.map(run, materialized))


def run_bounded(
    function: Callable[[T], None],
    items: Iterable[T],
    *,
    workers: int | None = None,
) -> int:
    """Run *function* over *items* with bounded concurrency; return the count."""
    return len(map_bounded(function, items, workers=workers))
