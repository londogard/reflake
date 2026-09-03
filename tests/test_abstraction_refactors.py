"""Tests for the structural refactors: key-space unification, protocol split,
sorted-stream merging, and branch-snapshot relocation."""

from __future__ import annotations

import json
from pathlib import Path

from reflake.core import open_repository
from reflake.core.entry_codec import (
    LeafRecord,
    decode_leaf,
    encode_leaf,
    leaf_kind_for,
)
from reflake.core.layout import object_relative_key
from reflake.core.manifest import ManifestEntry
from reflake.core.objects.tree import TreeEntry, parse_tree_object, serialize_tree_object
from reflake.core.repository_support import merge_sorted_streams


DIGEST = "a" * 64
FOOTER = "b" * 64


def _entry(path: str) -> ManifestEntry:
    return ManifestEntry(path=path, hash="0" * 64, size=1, mtime_ns=2)


def _make_commit(repo, name: str, message: str) -> str:
    path = repo.root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"content-{name}")
    return repo.commit(message)


# ── Unified leaf codec: one codec, two record types, identical bytes ───────


def test_leaf_codec_round_trips_every_shape() -> None:
    cases = [
        LeafRecord(kind="b", name="f.txt", hash=DIGEST, size=1, mtime_ns=2),
        LeafRecord(
            kind="m", name="f.txt", hash=DIGEST, size=1, mtime_ns=2,
            source_uri="s3://bucket/key",
        ),
        LeafRecord(
            kind="bp", name="f.parquet", hash=DIGEST, size=1, mtime_ns=2,
            footer=FOOTER,
        ),
        LeafRecord(
            kind="mp", name="f.parquet", hash=DIGEST, size=1, mtime_ns=2,
            source_uri="file:///f.parquet", footer=FOOTER,
        ),
    ]
    for record in cases:
        line = encode_leaf(record)
        assert decode_leaf(line) == record


def test_manifest_and_tree_serializations_agree() -> None:
    entry = ManifestEntry(
        path="dir/f.parquet",
        hash=DIGEST,
        size=7,
        mtime_ns=8,
        identity_mode="meta",
        source_uri="file:///dir/f.parquet",
        footer=FOOTER,
    )
    tree_entry = TreeEntry(
        name="f.parquet",
        kind="mp",
        hash=DIGEST,
        size=7,
        mtime_ns=8,
        source_uri="file:///dir/f.parquet",
        footer=FOOTER,
    )
    # Same leaf payload modulo the name field: bodies match exactly.
    assert entry.serialize().split(",", 2)[2] == tree_entry.serialize().split(",", 2)[2]
    parsed = TreeEntry.parse(tree_entry.serialize())
    assert parsed == tree_entry
    manifest_round_trip = ManifestEntry.deserialize(entry.serialize())
    assert manifest_round_trip == entry


def test_subtree_lines_reject_leaf_fields() -> None:
    import pytest

    with pytest.raises(ValueError, match="Subtree entries only carry"):
        TreeEntry(name="d", kind="t", hash=DIGEST, size=5)
    with pytest.raises(ValueError, match="Blob-backed entries cannot carry source_uri"):
        ManifestEntry(path="f.txt", hash=DIGEST, size=1, mtime_ns=1,
                      identity_mode="blake3", source_uri="file:///f.txt")


def test_leaf_kind_derivation_is_single_sourced() -> None:
    assert leaf_kind_for("blake3", has_footer=False) == "b"
    assert leaf_kind_for("blake3", has_footer=True) == "bp"
    assert leaf_kind_for("meta", has_footer=False) == "m"
    assert leaf_kind_for("meta", has_footer=True) == "mp"


def test_tree_object_round_trip_with_mixed_entries() -> None:
    entries = [
        TreeEntry(name="a.bin", kind="b", hash=DIGEST, size=1, mtime_ns=2),
        TreeEntry(name="sub", kind="t", hash=DIGEST),
        TreeEntry(name="z.meta", kind="m", hash=DIGEST, size=1, mtime_ns=2,
                  source_uri="s3://bucket/z"),
    ]
    payload = serialize_tree_object(entries)
    assert parse_tree_object(payload) == entries


# ── Key-space: one mapping drives both adapters' layouts ────────────────────


def test_object_relative_key_matches_physical_layout() -> None:
    digest = "ab" + "c" * 62
    assert object_relative_key("blob", digest) == f"blobs/ab/{'c' * 62}"
    assert object_relative_key("commit", "f" * 64) == f"commits/{'f' * 64}.json"
    assert object_relative_key("tree", "e" * 64) == f"trees/{'e' * 64}"
    assert object_relative_key("footer", "d" * 64) == f"footers/{'d' * 64}"
    assert object_relative_key("ref", "main") == "refs/heads/main"


def test_local_store_paths_derive_from_key_space(tmp_path: Path) -> None:
    repo = open_repository(tmp_path)
    commit_id = _make_commit(repo, "x.txt", "base")

    assert repo.store.blob_path("0" * 64) == tmp_path / ".reflake/blobs/00" / ("0" * 62)
    assert repo.store.commit_path(commit_id) == (
        tmp_path / ".reflake" / f"commits/{commit_id}.json"
    )
    tree_hash = repo.read_commit(commit_id).tree
    assert repo.store.tree_path(tree_hash) == tmp_path / ".reflake" / "trees" / tree_hash


# ── Sorted-stream merging ────────────────────────────────────────────────────


def test_merge_sorted_streams_interleaves_and_stabilizes_ties() -> None:
    left = [_entry("a"), _entry("m"), _entry("z")]
    right = [_entry("b"), _entry("m"), _entry("n")]

    merged = list(merge_sorted_streams(iter(left), iter(right)))
    assert [entry.path for entry in merged] == ["a", "b", "m", "m", "n", "z"]
    # Stable: the tie keeps the earlier stream's entry first.
    assert merged[2] is left[1]


def test_merge_sorted_streams_handles_empty_streams() -> None:
    only_left = list(merge_sorted_streams([_entry("a")], iter(())))
    assert [entry.path for entry in only_left] == ["a"]
    only_right = list(merge_sorted_streams(iter(()), [_entry("z")]))
    assert [entry.path for entry in only_right] == ["z"]


def test_overlay_staged_uses_stream_merge_for_replacement(tmp_path: Path) -> None:
    repo = open_repository(tmp_path)
    (tmp_path / "b.txt").write_text("old")
    repo.commit("base")
    head_tree = repo.read_commit(repo.head_commit()).tree

    from reflake.core.services.tree import TreeWriter

    writer = TreeWriter(store=repo.store, refs=repo.refs, client_state=repo.client_state)
    replacement = ManifestEntry(
        path="b.txt",
        hash="1" * 64,
        size=3,
        mtime_ns=4,
        identity_mode="blake3",
    )
    new_tree = writer.overlay_staged(
        parent_tree=head_tree,
        additions=[replacement],
        removed_prefixes=set(),
    )
    entries = {entry.path: entry for entry in repo.store.iter_all_entries(new_tree)}
    assert set(entries) == {"b.txt"}
    assert entries["b.txt"].hash == "1" * 64


# ── Branch snapshots live outside the shared refs namespace ────────────────


def test_branch_snapshots_relocated_out_of_refs_heads(tmp_path: Path) -> None:
    repo = open_repository(tmp_path)
    _make_commit(repo, "a.txt", "base")
    repo.refs.client_state.write_branch_snapshot(
        "main", commit_id=repo.head_commit(), version_token=None
    )

    snapshot = tmp_path / ".reflake/state/branch-snapshots/main.json"
    assert snapshot.exists()
    heads_dir = tmp_path / ".reflake/refs/heads"
    assert [path.name for path in heads_dir.iterdir()] == ["main"]

    assert sorted(repo.store.iter_branches()) == ["main"]


def test_staged_flow_end_to_end_after_refactors(tmp_path: Path, capsys) -> None:
    from reflake import run_cli

    (tmp_path / "z.txt").write_text("z")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "base"]) == 0
    capsys.readouterr()

    (tmp_path / "a.txt").write_text("a")
    assert run_cli(["add", "--repo", str(tmp_path), "a.txt", "--json"]) == 0
    capsys.readouterr()
    assert run_cli(["commit", "--repo", str(tmp_path), "--staged", "-m", "a"]) == 0
    capsys.readouterr()

    assert run_cli(["catalog", "--repo", str(tmp_path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["branch"] for item in payload] == ["main"]
