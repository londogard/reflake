"""Collaborator services for ReflakeRepository.

These objects own cohesive slices of repository behavior:

- ``RefManager`` — branch refs, commit reads, and their caches.
- ``TreeWriter`` — bottom-up tree building, overlays, and commit writing.
- ``TreeInspector`` — read-only tree walks (GC/sync) and derived-manifest export.
- ``StagingArea`` — branch-scoped staging state, source expansion, and status.
- ``EntryFactory`` — entry materialization and canonical blob storage.
"""

from __future__ import annotations

from .entries import EntryFactory
from .refs import RefManager, _BoundedCache
from .staging import StagingArea
from .tree import TreeWriter
from .tree_inspect import TreeInspector

__all__ = [
    "EntryFactory",
    "RefManager",
    "StagingArea",
    "TreeInspector",
    "TreeWriter",
    "_BoundedCache",
]
