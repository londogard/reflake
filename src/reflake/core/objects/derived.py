"""Derived-manifest index: block offsets for the optional client-side cache.

The v1 ``.idx`` sidecar and the embedded ``commit.manifest_index`` are gone
from the repository (§3 of docs/architecture.md).  What survives is the *block
offset model* used by the derived-manifest materialization: a flattened tree
cached per-client (keyed by root-tree hash) with block offsets so
latency-critical point lookups can binary-search blocks and range-read one
slice — never touching the shared store.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

#: Derived-manifest block size (entries per index block).
DEFAULT_DERIVED_BLOCK_ENTRY_COUNT = 4096


@dataclass(frozen=True)
class DerivedIndexBlock:
    first_path: str
    offset: int


@dataclass(frozen=True)
class DerivedIndex:
    manifest_size: int
    block_entry_count: int
    blocks: tuple[DerivedIndexBlock, ...]

    @property
    def is_empty(self) -> bool:
        return not self.blocks

    def serialize(self) -> str:
        payload = {
            "manifest_size": self.manifest_size,
            "block_entry_count": self.block_entry_count,
            "blocks": [[block.first_path, block.offset] for block in self.blocks],
        }
        return json.dumps(payload, separators=(",", ":"))


def load_derived_index(data: dict[str, Any]) -> DerivedIndex:
    blocks_list = data.get("blocks", [])
    blocks: tuple[DerivedIndexBlock, ...]
    if blocks_list:
        if isinstance(blocks_list[0], dict):
            blocks = tuple(
                DerivedIndexBlock(
                    first_path=str(block["first_path"]),
                    offset=int(block["offset"]),
                )
                for block in blocks_list
            )
        else:
            blocks = tuple(
                DerivedIndexBlock(first_path=str(first_path), offset=int(offset))
                for first_path, offset in blocks_list
            )
    else:
        blocks = ()
    return DerivedIndex(
        manifest_size=int(data.get("manifest_size", 0)),
        block_entry_count=int(
            data.get("block_entry_count", DEFAULT_DERIVED_BLOCK_ENTRY_COUNT)
        ),
        blocks=blocks,
    )
