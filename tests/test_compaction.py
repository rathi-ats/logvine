import asyncio
from pathlib import Path

from src.storage.compaction import CompactionManager
from src.storage.manifest import Manifest
from src.storage.sstable import SSTable
from src.storage.sstable_manager import SSTableManager


def _metadata_from_sstable(path: Path, level: int, sstable: SSTable) -> dict:
    keys = sorted(sstable.index.keys())
    return {
        "path": str(path),
        "level": level,
        "created_at_ms": 0,
        "size_bytes": path.stat().st_size,
        "entry_count": len(keys),
        "min_key_hex": keys[0].hex(),
        "max_key_hex": keys[-1].hex(),
    }


def test_compaction_merges_level0_sstables_and_keeps_newest_value(tmp_path):
    storage_path = Path(tmp_path)
    manifest = Manifest(storage_path / "manifest.json")
    compaction = CompactionManager()

    older_path = storage_path / "sstable_100.sst"
    newer_path = storage_path / "sstable_200.sst"

    older = SSTable(older_path, level=0)
    newer = SSTable(newer_path, level=0)

    asyncio.run(older.write(iter([(b"a", b"1"), (b"b", b"old")])))
    asyncio.run(newer.write(iter([(b"b", b"new"), (b"c", b"3")])))

    manifest.add_sstable(0, _metadata_from_sstable(older_path, 0, older))
    manifest.add_sstable(0, _metadata_from_sstable(newer_path, 0, newer))

    compaction.compact(manifest, storage_path)

    assert manifest.get_level_sstables(0) == []
    level1 = manifest.get_level_sstables(1)
    assert len(level1) == 1

    compacted_path = Path(level1[0]["path"])
    assert compacted_path.exists()
    assert not older_path.exists()
    assert not newer_path.exists()

    compacted = SSTable(compacted_path, level=1)
    assert compacted.get(b"a") == b"1"
    assert compacted.get(b"b") == b"new"
    assert compacted.get(b"c") == b"3"


def test_repeated_compactions_do_not_overwrite_existing_level1_files(tmp_path):
    storage_path = Path(tmp_path)
    manifest = Manifest(storage_path / "manifest.json")
    compaction = CompactionManager()
    manager = SSTableManager(manifest)

    first_a = SSTable(storage_path / "sstable_100.sst", level=0)
    first_b = SSTable(storage_path / "sstable_200.sst", level=0)
    asyncio.run(first_a.write(iter([(b"a", b"1")])))
    asyncio.run(first_b.write(iter([(b"b", b"2")])))
    manifest.add_sstable(0, manager.build_metadata(first_a, level=0))
    manifest.add_sstable(0, manager.build_metadata(first_b, level=0))

    compaction.compact(manifest, storage_path)
    first_level1 = manifest.get_level_sstables(1)
    assert len(first_level1) == 1

    second_a = SSTable(storage_path / "sstable_300.sst", level=0)
    second_b = SSTable(storage_path / "sstable_400.sst", level=0)
    asyncio.run(second_a.write(iter([(b"c", b"3")])))
    asyncio.run(second_b.write(iter([(b"d", b"4")])))
    manifest.add_sstable(0, manager.build_metadata(second_a, level=0))
    manifest.add_sstable(0, manager.build_metadata(second_b, level=0))

    compaction.compact(manifest, storage_path)
    level1 = manifest.get_level_sstables(1)
    level1_paths = {entry["path"] for entry in level1}

    assert len(level1) == 2
    assert len(level1_paths) == 2
    for entry in level1:
        assert Path(entry["path"]).exists()
