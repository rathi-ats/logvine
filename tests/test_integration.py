"""Integration tests for storage engine end-to-end behavior."""

from __future__ import annotations

import tempfile
from pathlib import Path

from src.storage.engine import LSMStorageEngine


def _flush_current_memtable(engine: LSMStorageEngine) -> None:
    """Force current active memtable to disk via frozen memtable."""
    engine.memtable.rotate()
    engine.flush()


def test_put_get_delete_batch_put_and_range_read() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = LSMStorageEngine(tmpdir)

        engine.put(b"user:1", b"Alice")
        assert engine.get(b"user:1") == b"Alice"

        engine.batch_put(
            [b"user:2", b"user:3", b"user:4"],
            [b"Bob", b"Charlie", b"David"],
        )

        range_items = list(engine.read_key_range(b"user:1", b"user:5"))
        range_map = dict(range_items)
        assert range_map[b"user:1"] == b"Alice"
        assert range_map[b"user:2"] == b"Bob"
        assert range_map[b"user:3"] == b"Charlie"
        assert range_map[b"user:4"] == b"David"

        engine.delete(b"user:3")
        try:
            engine.get(b"user:3")
            assert False, "Expected KeyError for deleted key"
        except KeyError:
            pass


def test_flush_persists_to_sstable_and_manifest() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = LSMStorageEngine(tmpdir)
        engine.put(b"a", b"1")
        engine.put(b"b", b"2")

        _flush_current_memtable(engine)

        level0 = engine.manifest.get_level_sstables(0)
        assert len(level0) == 1
        sstable_meta = level0[0]
        sstable_path = Path(sstable_meta["path"])
        assert sstable_path.exists()
        assert sstable_meta["entry_count"] == 2
        assert sstable_meta["min_key_hex"] == b"a".hex()
        assert sstable_meta["max_key_hex"] == b"b".hex()

        # Read should still work after data moved out of active memtable.
        assert engine.get(b"a") == b"1"
        assert engine.get(b"b") == b"2"


def test_wal_replay_recovers_state_after_restart() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = LSMStorageEngine(tmpdir)
        engine.put(b"k1", b"v1")
        engine.put(b"k2", b"v2")
        engine.delete(b"k1")

        recovered = LSMStorageEngine(tmpdir)
        assert recovered.get(b"k2") == b"v2"
        try:
            recovered.get(b"k1")
            assert False, "Expected KeyError for deleted key after WAL replay"
        except KeyError:
            pass


def test_compaction_merges_l0_to_l1_and_keeps_latest_value() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = LSMStorageEngine(tmpdir)

        # Disable background compaction for deterministic setup.
        engine.compaction.may_start_compaction = lambda _manifest, _path: None

        # Flush 3 L0 SSTables (threshold is > 2) with overlapping key b.
        engine.put(b"a", b"v1")
        engine.put(b"b", b"old")
        _flush_current_memtable(engine)

        engine.put(b"b", b"new")
        _flush_current_memtable(engine)

        engine.put(b"c", b"v3")
        _flush_current_memtable(engine)

        assert len(engine.manifest.get_level_sstables(0)) == 3

        engine.compaction.compact(engine.manifest, engine.storage_path)

        level0 = engine.manifest.get_level_sstables(0)
        level1 = engine.manifest.get_level_sstables(1)
        assert len(level0) == 0
        assert len(level1) == 1
        assert Path(level1[0]["path"]).exists()

        # Validate reads through a fresh engine (manifest + SSTable path).
        recovered = LSMStorageEngine(tmpdir)
        assert recovered.get(b"a") == b"v1"
        assert recovered.get(b"b") == b"new"
        assert recovered.get(b"c") == b"v3"
