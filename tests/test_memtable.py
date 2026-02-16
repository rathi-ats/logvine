from src.storage.memtable import MemTable


def test_put_get_and_size_tracking():
    memtable = MemTable(max_size=100)

    memtable.put(b"k1", b"abc")
    assert memtable.get(b"k1") == b"abc"
    assert memtable._current_size == 3

    memtable.put(b"k1", b"abcd")
    assert memtable.get(b"k1") == b"abcd"
    assert memtable._current_size == 4


def test_delete_writes_tombstone():
    memtable = MemTable(max_size=100)
    memtable.put(b"k1", b"v1")
    memtable.delete(b"k1")
    assert memtable.get(b"k1") == b"__TOMBSTONE__"


def test_is_full_respects_max_size():
    memtable = MemTable(max_size=4)
    memtable.put(b"k1", b"abcd")
    assert memtable.is_full() is True


def test_rotate_moves_data_to_frozen_and_resets_active_state():
    memtable = MemTable(max_size=100)
    memtable.put(b"a", b"1")
    memtable.put(b"b", b"2")
    memtable.set_max_wal_offset(123)

    memtable.rotate()

    assert memtable._data == {}
    assert memtable._frozen == {b"a": b"1", b"b": b"2"}
    assert memtable._current_size == 0
    assert memtable._max_wal_offset == 0
    assert memtable._max_wal_offset_frozen == 123
    assert memtable.get(b"a") == b"1"


def test_clear_frozen_removes_frozen_data():
    memtable = MemTable(max_size=100)
    memtable.put(b"a", b"1")
    memtable.rotate()

    memtable.clearFrozen()

    assert memtable._frozen == {}
    assert memtable.get(b"a") is None


def test_get_range_returns_inclusive_sorted_results_with_active_overrides():
    memtable = MemTable(max_size=100)
    memtable.put(b"a", b"old-a")
    memtable.put(b"b", b"old-b")
    memtable.put(b"c", b"old-c")
    memtable.rotate()

    memtable.put(b"b", b"new-b")
    memtable.put(b"d", b"new-d")

    result = memtable.get_range(b"a", b"c")
    assert list(result.keys()) == [b"a", b"b", b"c"]
    assert result[b"a"] == b"old-a"
    assert result[b"b"] == b"new-b"
    assert result[b"c"] == b"old-c"


def test_iter_sorted_returns_all_entries_in_key_order():
    memtable = MemTable(max_size=100)
    memtable.put(b"c", b"3")
    memtable.put(b"a", b"1")
    memtable.put(b"b", b"2")
    memtable.rotate()
    memtable.put(b"d", b"4")

    items = list(memtable.iter_sorted())
    keys = [k for k, _ in items]
    assert keys == sorted(keys)
    assert (b"a", b"1") in items
    assert (b"b", b"2") in items
    assert (b"c", b"3") in items
    assert (b"d", b"4") in items


def test_set_max_wal_offset_is_monotonic():
    memtable = MemTable(max_size=100)
    memtable.set_max_wal_offset(10)
    memtable.set_max_wal_offset(7)
    memtable.set_max_wal_offset(42)
    assert memtable._max_wal_offset == 42
