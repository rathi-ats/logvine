"""Debug test to trace where writes are being lost."""

import tempfile, threading, time, logging
from src.storage.engine import LSMStorageEngine

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Global tracking
keys_written_to_wal = set()
keys_in_memtable = set()
keys_in_frozen = set()
keys_in_sstable = set()
lock = threading.Lock()

# Patch WAL append
original_wal_append = None

def patched_wal_append(self, operation, key, value):
    global keys_written_to_wal
    result = original_wal_append(self, operation, key, value)
    with lock:
        keys_written_to_wal.add(key)
    return result

# Patch MemTable.put
original_memtable_put = None

def patched_memtable_put(self, key, value):
    global keys_in_memtable
    result = original_memtable_put(self, key, value)
    with lock:
        keys_in_memtable.add(key)
    return result

# Patch MemTable.rotate
original_rotate = None

def patched_rotate(self):
    global keys_in_frozen
    result = original_rotate(self)
    # After rotate, get all keys from frozen queue
    with lock:
        keys_in_frozen.clear()
        for frozen_data, _ in self._frozen_queue:
            keys_in_frozen.update(frozen_data.keys())
    logger.info(f"After rotate: {len(keys_in_frozen)} keys in frozen_queue total")
    return result

with tempfile.TemporaryDirectory() as tmp:
    engine = LSMStorageEngine(tmp)
    engine.memtable.max_size = 500
    
    # Apply patches
    from src.storage.wal import WAL
    from src.storage.memtable import MemTable
    
    original_wal_append = WAL.append
    WAL.append = patched_wal_append
    
    original_memtable_put = MemTable.put
    MemTable.put = patched_memtable_put
    
    original_rotate = MemTable.rotate
    MemTable.rotate = patched_rotate
    
    written_keys = set()
    barrier = threading.Barrier(4)
    
    def writer(tid):
        barrier.wait()
        for i in range(100):
            key = f"thread_{tid}_key_{i}".encode()
            value = f"value_{tid}_{i}_{time.time_ns()}".encode()
            engine.put(key, value)
            with lock:
                written_keys.add(key)
    
    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    # Wait for background flushes
    for _ in range(50):
        if engine.memtable.get_frozen_queue_depth() == 0 and engine.memtable._current_size == 0:
            break
        time.sleep(0.1)
    
    if engine.memtable._current_size > 0:
        engine.memtable.rotate()
    engine.flush_all()
    
    print(f"\n=== KEY TRACKING ===")
    print(f"Written to written_keys set: {len(written_keys)}")
    print(f"Written to WAL: {len(keys_written_to_wal)}")
    print(f"Put in MemTable: {len(keys_in_memtable)}")
    print(f"In frozen queue: {len(keys_in_frozen)}")
    
    # Check what's retrievable
    retrievable = 0
    for key in written_keys:
        try:
            engine.get(key)
            retrievable += 1
        except KeyError:
            pass
    
    print(f"Retrievable: {retrievable}/{len(written_keys)}")
    
    # Find gaps
    lost_before_wal = written_keys - keys_written_to_wal
    lost_before_memtable = keys_written_to_wal - keys_in_memtable
    lost_before_frozen = keys_in_memtable - keys_in_frozen
    lost_in_flush = keys_in_frozen - set()  # Check after flush
    
    print(f"\n=== WHERE ARE KEYS LOST? ===")
    print(f"Lost before WAL: {len(lost_before_wal)}")
    print(f"Lost between WAL and MemTable: {len(lost_before_memtable)}")
    print(f"Lost between MemTable and Frozen: {len(lost_before_frozen)}")
    if lost_before_frozen:
        print(f"  Sample: {list(lost_before_frozen)[:5]}")
    
    # Try to find missing keys in memtable state
    print(f"\n=== FINAL STATE ===")
    print(f"Active data size: {len(engine.memtable._data)}")
    print(f"Frozen queue depth: {engine.memtable.get_frozen_queue_depth()}")
    
    # Check if any are still in active data (shouldn't be)
    still_in_active = written_keys & set(engine.memtable._data.keys())
    print(f"Still in active memtable: {len(still_in_active)}")
    if still_in_active:
        print(f"  Sample: {list(still_in_active)[:3]}")
