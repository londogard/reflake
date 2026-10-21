"""Reflake command-line interface.

The ``reflake.cli:main`` entry point lives here; implementation is split by
command group (``worktree``, ``history``, ``identity``, ``query``, ``sync``,
``admin``) with shared argument definitions in ``args`` and dispatch/error
handling in ``runner``.
"""

from __future__ import annotations

from .args import build_parser
from .runner import main, run_cli

__all__ = ["build_parser", "main", "run_cli"]
