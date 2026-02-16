import asyncio
from pathlib import Path

from src.storage.compaction import CompactionManager
from src.storage.manifest import Manifest
from src.storage.sstable import SSTable


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
