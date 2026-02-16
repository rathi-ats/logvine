import json
import threading

from src.storage.manifest import Manifest
from src.storage.memtable import MemTable
from src.storage.wal import OperationType, WAL


def test_memtable_concurrent_single_key_writes_preserve_consistency():
    memtable = MemTable(max_size=10_000_000)
    key = b"shared"
    writers = 12
    writes_per_writer = 300
    barrier = threading.Barrier(writers)
    written_values: set[bytes] = set()
    values_lock = threading.Lock()

    def writer(worker_id: int) -> None:
        barrier.wait()
        for i in range(writes_per_writer):
            value = f"w{worker_id}:{i}".encode("utf-8")
            memtable.put(key, value)
            with values_lock:
                written_values.add(value)

    threads = [
        threading.Thread(target=writer, args=(worker_id,)) for worker_id in range(writers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final_value = memtable.get(key)
    assert final_value is not None
    assert final_value in written_values
    # With exactly one key in active map, current_size must match that value's length.
    assert memtable._current_size == len(final_value)


def test_memtable_concurrent_multi_key_writes_size_matches_data():
    memtable = MemTable(max_size=10_000_000)
    threads_count = 8
    entries_per_thread = 150
    barrier = threading.Barrier(threads_count)

    def writer(worker_id: int) -> None:
        barrier.wait()
        for i in range(entries_per_thread):
            key = f"k:{worker_id}:{i}".encode("utf-8")
            value = f"v:{worker_id}:{i}".encode("utf-8")
            memtable.put(key, value)

    threads = [
        threading.Thread(target=writer, args=(worker_id,))
        for worker_id in range(threads_count)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(memtable._data) == threads_count * entries_per_thread
    assert memtable._current_size == sum(len(v) for v in memtable._data.values())


def test_wal_concurrent_appends_produce_replayable_complete_log(tmp_path):
    wal = WAL(tmp_path / "wal.log")
    threads_count = 10
    appends_per_thread = 120
    barrier = threading.Barrier(threads_count)

    expected_records = {
        (OperationType.PUT.value, f"k:{t}:{i}".encode(), f"v:{t}:{i}".encode())
        for t in range(threads_count)
        for i in range(appends_per_thread)
    }

    def writer(worker_id: int) -> None:
        barrier.wait()
        for i in range(appends_per_thread):
            key = f"k:{worker_id}:{i}".encode()
            value = f"v:{worker_id}:{i}".encode()
            wal.append(OperationType.PUT.value, key, value)

    threads = [
        threading.Thread(target=writer, args=(worker_id,))
        for worker_id in range(threads_count)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    replayed = list(wal.replay())
    replayed_records = {(op, key, value) for op, key, value, _ in replayed}
    offsets = [offset for _, _, _, offset in replayed]

    assert len(replayed) == threads_count * appends_per_thread
    assert replayed_records == expected_records
    assert offsets == sorted(offsets)
    assert len(set(offsets)) == len(offsets)


def test_manifest_concurrent_adds_are_not_lost(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    threads_count = 16
    adds_per_thread = 40
    barrier = threading.Barrier(threads_count)

    def worker(worker_id: int) -> None:
        barrier.wait()
        for i in range(adds_per_thread):
            path = str(tmp_path / f"sst_{worker_id}_{i}.sst")
            manifest.add_sstable(
                0,
                {
                    "path": path,
                    "level": 0,
                    "created_at_ms": 0,
                    "size_bytes": 1,
                    "entry_count": 1,
                    "min_key_hex": "61",
                    "max_key_hex": "61",
                },
            )

    threads = [
        threading.Thread(target=worker, args=(worker_id,))
        for worker_id in range(threads_count)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total = threads_count * adds_per_thread
    assert len(manifest.get_level_sstables(0)) == total
    assert manifest.get_version() == total

    persisted = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert len(persisted["levels"]["0"]) == total
    assert persisted["version"] == total
