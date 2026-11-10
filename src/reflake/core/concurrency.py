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
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")

#: Default in-flight object operations. High enough to hide RTT, low enough to
#: stay well inside S3 request-rate and socket-pool limits.
DEFAULT_IO_WORKERS = min(16, max(4, (os.cpu_count() or 4) * 4))


def _iter_bounded(
    function: Callable[[T], R],
    items: Iterable[T],
    *,
    workers: int,
) -> Iterator[R]:
    """Yield results in input order with at most *workers* calls in flight.

    The input is consumed lazily, so a million-item transfer plan does not
    need to be materialized to start moving bytes.
    """
    iterator = iter(items)
    try:
        first = next(iterator)
    except StopIteration:
        return
    if workers < 2:
        yield function(first)
        for item in iterator:
            yield function(item)
        return
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending: deque[Future[R]] = deque([pool.submit(function, first)])
        for item in iterator:
            if len(pending) >= workers:
                yield pending.popleft().result()
            pending.append(pool.submit(function, item))
        while pending:
            yield pending.popleft().result()


def map_bounded(
    function: Callable[[T], R],
    items: Iterable[T],
    *,
    workers: int | None = None,
) -> list[R]:
    """``list(map(function, items))`` with up to *workers* calls in flight."""
    return list(
        _iter_bounded(
            function,
            items,
            workers=workers or DEFAULT_IO_WORKERS,
        )
    )


def run_bounded(
    function: Callable[[T], None],
    items: Iterable[T],
    *,
    workers: int | None = None,
) -> int:
    """Run *function* over *items* with bounded concurrency; return the count."""
    return sum(
        1
        for _ in _iter_bounded(
            function,
            items,
            workers=workers or DEFAULT_IO_WORKERS,
        )
    )
