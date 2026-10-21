"""Small helpers shared across CLI command handlers."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any


def flatten_option_values(values: list[Any]) -> list[str]:
    """Flatten simple-parsing ``action="append"`` values to plain strings."""
    flattened: list[str] = []
    for value in values:
        if isinstance(value, list):
            flattened.extend(str(item) for item in value)
            continue
        flattened.append(str(value))
    return flattened


def progress_printer(label: str, *, enabled: bool = True) -> Callable[[int, int], None]:
    """A quiet progress line on stderr for latency-bound CLI work.

    Every 64 items (and on completion) prints ``label done/total`` with a
    carriage return so a long push/pull shows life without spamming logs.
    Disabled for ``--json`` runs, where stderr is reserved for the error
    envelope.
    """

    def report(done: int, total: int) -> None:
        if not enabled:
            return
        if done not in (total,) and done % 64 != 0:
            return
        end = "\n" if done >= total else ""
        print(f"\r{label} {done}/{total}", end=end, file=sys.stderr, flush=True)

    return report


class Counter:
    """Progress for work whose total is unknown (staging a directory walk)."""

    def __init__(self, label: str, *, enabled: bool = True) -> None:
        self.label = label
        self.enabled = enabled
        self._shown = False

    def __call__(self, count: int) -> None:
        if not self.enabled or (count % 250 != 0 and count != 1):
            return
        print(f"\r{self.label} {count}", end="", file=sys.stderr, flush=True)
        self._shown = True

    def finish(self) -> None:
        if self._shown:
            print(file=sys.stderr, flush=True)
