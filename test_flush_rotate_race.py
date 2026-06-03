"""Test to reproduce the flush/rotate race condition bug.

This test demonstrates the critical concurrency bug where overlapping async
flush/rotate operations can overwrite frozen state and drop writes under load.
"""

import threading
import time
import tempfile
import logging

from src.storage.engine import LSMStorageEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_concurrent_writes_with_overlapping_flushes_drop_writes():
    """
    Reproduce the race condition where concurrent puts with async flushes
    can lose writes.

    The bug occurs when:
    1. Thread A fills memtable and calls rotate()
    2. Thread B spawns async flush in background
    3. Thread C calls put() while flush is running, fills memtable again
    4. Thread D calls rotate() again before Thread B's flush completes
    5. rotate() overwrites _frozen, losing writes from Thread C
    """

    with tempfile.TemporaryDirectory() as tmp_path:
        engine = LSMStorageEngine(tmp_path)
        # Keep this test focused on flush/rotate correctness. Background
        # compaction has a separate read-path/data-loss bug under investigation.
        engine.compaction.may_start_compaction = lambda _manifest, _path: None

        # Small memtable to trigger flushes quickly
        engine.memtable.max_size = 500

        written_keys = set()
        written_keys_lock = threading.Lock()

        num_threads = 4
        writes_per_thread = 100
        barrier = threading.Barrier(num_threads)

        def concurrent_writer(thread_id: int):
            """Write many values concurrently to trigger race condition."""
            barrier.wait()  # Synchronize start

            for i in range(writes_per_thread):
                key = f"thread_{thread_id}_key_{i}".encode()
                value = f"value_{thread_id}_{i}_{time.time_ns()}".encode()

                try:
                    engine.put(key, value)
                    with written_keys_lock:
                        written_keys.add(key)
                except Exception as e:
                    logger.error(f"Put failed for {key}: {e}")

        # Spawn concurrent writers
        threads = [
            threading.Thread(target=concurrent_writer, args=(tid,))
            for tid in range(num_threads)
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        logger.info(f"All writer threads joined. Total keys written: {len(written_keys)}")
        logger.info(f"  Active memtable: {len(engine.memtable._data)} keys, size={engine.memtable._current_size}")
        logger.info(f"  Frozen queue depth: {engine.memtable.get_frozen_queue_depth()}")

        # Wait for background flush threads to complete
        # Give them up to 5 seconds
        for attempt in range(50):
            if engine.memtable.get_frozen_queue_depth() == 0 and engine.memtable._current_size == 0:
                logger.info(f"  All data flushed after {attempt*0.1:.1f}s")
                break
            time.sleep(0.1)

        logger.info(f"Before final flush:")
        logger.info(f"  Active data: {len(engine.memtable._data)} keys")
        logger.info(f"  Frozen queue: {engine.memtable.get_frozen_queue_depth()} buffers")

        # Final flush to ensure ALL data is written:
        # 1. Rotate active data to frozen
        # 2. Flush all frozen buffers
        if engine.memtable._current_size > 0:
            logger.info(f"  Rotating {len(engine.memtable._data)} keys from active data")
            engine.memtable.rotate()
        engine.flush_all()

        logger.info(f"After final flush: queue depth={engine.memtable.get_frozen_queue_depth()}")
        # Verify all written keys are readable
        missing_keys = []
        for key in written_keys:
            try:
                value = engine.get(key)
                assert value is not None
                logger.debug(f"Key {key} retrieved successfully")
            except KeyError:
                missing_keys.append(key)
                logger.warning(f"Key {key} is MISSING (dropped by race condition)")

        if missing_keys:
            logger.error(f"\nBUG REPRODUCED: {len(missing_keys)} / {len(written_keys)} keys lost")
            logger.error(f"Missing keys: {missing_keys[:10]}...")  # Show first 10
            assert False, f"Lost {len(missing_keys)} writes due to flush/rotate race condition"
        else:
            logger.info(f"\nAll {len(written_keys)} writes persisted")


def test_rapid_rotate_with_concurrent_flush_corrupts_frozen():
    """
    Test scenario where rapid rotates while flush is in progress
    corrupt the _frozen state.

    Demonstrates:
    - rotate() overwrites _frozen while flush is reading it
    - clearFrozen() called at wrong time
    - Data loss in active memtable
    """

    with tempfile.TemporaryDirectory() as tmp_path:
        engine = LSMStorageEngine(tmp_path)
        # Keep this test focused on flush/rotate correctness. Background
        # compaction has a separate read-path/data-loss bug under investigation.
        engine.compaction.may_start_compaction = lambda _manifest, _path: None
        engine.memtable.max_size = 1000

        written_data = {}

        # Phase 1: Write initial data
        for i in range(20):
            key = f"initial_{i}".encode()
            value = f"value_{i}".encode()
            engine.put(key, value)
            written_data[key] = value

        # Phase 2: Trigger first rotate (will spawn async flush)
        logger.info("Triggering first rotate...")
        engine.memtable.rotate()
        first_frozen = engine.memtable.get_frozen_items()
        logger.info(f"First frozen state: {len(first_frozen)} keys")

        flush_started = threading.Event()
        flush_delay = threading.Event()

        # Patch flush to delay in the middle
        original_flush = engine.flush

        def delayed_flush():
            flush_started.set()
            time.sleep(0.5)  # Simulate slow I/O
            flush_delay.set()
            original_flush()

        # Phase 3: Spawn flush and immediately hammer with new writes
        flush_thread = threading.Thread(target=delayed_flush)
        flush_thread.start()

        # Wait for flush to start
        flush_started.wait()

        # Immediately write more data while flush is running
        logger.info("Writing new data while flush in progress...")
        for i in range(20, 40):
            key = f"concurrent_{i}".encode()
            value = f"value_{i}".encode()
            engine.put(key, value)
            written_data[key] = value

        # Phase 4: Trigger another rotate before first flush completes
        logger.info("Forcing second rotate while first flush still running...")
        if engine.memtable._current_size > 0:
            engine.memtable.rotate()
            second_frozen = engine.memtable.get_frozen_items()
            logger.info(f"Second frozen state: {len(second_frozen)} keys")

        flush_thread.join()

        # Phase 5: Verify data integrity
        logger.info("Verifying data integrity...")
        lost_keys = []
        for key, expected_value in written_data.items():
            try:
                actual_value = engine.get(key)
                if actual_value != expected_value:
                    logger.warning(f"Value mismatch for {key}")
            except KeyError:
                lost_keys.append(key)
                logger.warning(f"Key {key} is MISSING")

        if lost_keys:
            logger.error(f"\nBUG REPRODUCED: Lost {len(lost_keys)} keys")
            assert False, f"Lost {len(lost_keys)} keys: {lost_keys}"
        else:
            logger.info(f"All {len(written_data)} keys present")


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Test 1: Concurrent writes with overlapping flushes")
    logger.info("=" * 80)
    try:
        test_concurrent_writes_with_overlapping_flushes_drop_writes()
    except AssertionError as e:
        logger.error(f"Test failed: {e}")

    logger.info("\n" + "=" * 80)
    logger.info("Test 2: Rapid rotate with concurrent flush")
    logger.info("=" * 80)
    try:
        test_rapid_rotate_with_concurrent_flush_corrupts_frozen()
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
