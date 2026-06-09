import asyncio
from pathlib import Path

from src.storage.manifest import Manifest
from src.storage.sstable import SSTable
from src.storage.sstable_manager import SSTableManager


def _write_sstable(path: Path, level: int, pairs: list[tuple[bytes, bytes]]) -> SSTable:
    sstable = SSTable(path, level=level)
    asyncio.run(sstable.write(iter(pairs)))
    return sstable


def test_iter_sstable_metadata_orders_newest_first_within_level(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    manager = SSTableManager(manifest)

    s1 = _write_sstable(tmp_path / "sstable_100.sst", 0, [(b"a", b"1")])
    s2 = _write_sstable(tmp_path / "sstable_200.sst", 0, [(b"b", b"2")])
    s3 = _write_sstable(tmp_path / "sstable_050.sst", 1, [(b"c", b"3")])

    manifest.add_sstable(0, manager.build_metadata(s1, level=0))
    manifest.add_sstable(0, manager.build_metadata(s2, level=0))
    manifest.add_sstable(1, manager.build_metadata(s3, level=1))

    ordered = [(level, meta["path"]) for level, meta in manager.iter_sstable_metadata()]
    assert ordered == [
        (0, str(s2.path)),
        (0, str(s1.path)),
        (1, str(s3.path)),
    ]


def test_get_prefers_newer_sstable_entry(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    manager = SSTableManager(manifest)

    old = _write_sstable(tmp_path / "sstable_100.sst", 0, [(b"k", b"old")])
    new = _write_sstable(tmp_path / "sstable_200.sst", 0, [(b"k", b"new")])

    manifest.add_sstable(0, manager.build_metadata(old, level=0))
    manifest.add_sstable(0, manager.build_metadata(new, level=0))

    assert manager.get(b"k") == b"new"
    assert manager.get(b"missing") is None


def test_get_checks_older_overlapping_sstable_after_newer_miss(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    manager = SSTableManager(manifest)

    older = _write_sstable(
        tmp_path / "sstable_100.sst",
        0,
        [(b"a", b"1"), (b"z", b"26")],
    )
    newer = _write_sstable(
        tmp_path / "sstable_200.sst",
        0,
        [(b"m", b"13"), (b"z", b"27")],
    )

    manifest.add_sstable(0, manager.build_metadata(older, level=0))
    manifest.add_sstable(0, manager.build_metadata(newer, level=0))

    assert manager.get(b"a") == b"1"


def test_get_overlapping_sstables_returns_only_overlaps(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    manager = SSTableManager(manifest)

    a = _write_sstable(tmp_path / "sstable_100.sst", 0, [(b"a", b"1"), (b"c", b"3")])
    b = _write_sstable(tmp_path / "sstable_200.sst", 0, [(b"d", b"4"), (b"f", b"6")])
    c = _write_sstable(tmp_path / "sstable_300.sst", 1, [(b"x", b"24"), (b"z", b"26")])

    manifest.add_sstable(0, manager.build_metadata(a, level=0))
    manifest.add_sstable(0, manager.build_metadata(b, level=0))
    manifest.add_sstable(1, manager.build_metadata(c, level=1))

    overlaps = manager.get_overlapping_sstables(b"b", b"e")
    overlap_paths = sorted(str(s.path) for s in overlaps)
    assert overlap_paths == sorted([str(a.path), str(b.path)])


def test_build_metadata_and_materialize_round_trip(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    manager = SSTableManager(manifest)

    sstable = _write_sstable(tmp_path / "sstable_123.sst", 0, [(b"a", b"1"), (b"b", b"2")])
    metadata = manager.build_metadata(sstable, level=2)

    assert metadata["path"] == str(sstable.path)
    assert metadata["level"] == 2
    assert metadata["entry_count"] == 2
    assert metadata["min_key_hex"] == b"a".hex()
    assert metadata["max_key_hex"] == b"b".hex()

    rehydrated = manager.metadata_to_sstable(metadata)
    assert rehydrated.path == sstable.path
    assert rehydrated.level == 2


def test_build_metadata_raises_for_empty_index(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    manager = SSTableManager(manifest)
    sstable = SSTable(tmp_path / "empty.sst", level=0)

    try:
        manager.build_metadata(sstable, level=0)
        assert False, "Expected ValueError for empty SSTable index"
    except ValueError:
        pass
