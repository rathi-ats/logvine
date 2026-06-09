import asyncio

from src.storage.sstable import SSTable


def test_write_and_get_round_trip(tmp_path):
    path = tmp_path / "test.sst"
    sstable = SSTable(path)
    pairs = [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

    asyncio.run(sstable.write(iter(pairs)))

    assert path.exists()
    assert sstable.get(b"a") == b"1"
    assert sstable.get(b"b") == b"2"
    assert sstable.get(b"c") == b"3"
    assert sstable.get(b"missing") is None


def test_range_scan_uses_exclusive_end_and_is_sorted(tmp_path):
    path = tmp_path / "range.sst"
    sstable = SSTable(path)
    pairs = [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")]

    asyncio.run(sstable.write(iter(pairs)))

    scanned = list(sstable.range_scan(b"b", b"d"))
    assert scanned == [(b"b", b"2"), (b"c", b"3")]


def test_iter_items_returns_all_items_sorted(tmp_path):
    path = tmp_path / "iter.sst"
    sstable = SSTable(path)
    pairs = [(b"z", b"26"), (b"m", b"13"), (b"a", b"1")]

    asyncio.run(sstable.write(iter(sorted(pairs, key=lambda x: x[0]))))

    items = list(sstable.iter_items())
    assert items == [(b"a", b"1"), (b"m", b"13"), (b"z", b"26")]


def test_new_instance_loads_index_from_disk(tmp_path):
    path = tmp_path / "reload.sst"
    writer = SSTable(path)
    pairs = [(b"k1", b"v1"), (b"k2", b"v2")]
    asyncio.run(writer.write(iter(pairs)))

    reader = SSTable(path)
    assert reader.index == {}
    assert reader.get(b"k1") == b"v1"
    assert reader.get(b"k2") == b"v2"
    assert b"k1" in reader.index
    assert b"k2" in reader.index


def test_get_on_missing_file_returns_none(tmp_path):
    sstable = SSTable(tmp_path / "missing.sst")
    assert sstable.get(b"any") is None
