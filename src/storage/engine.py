"""StorageEngine: Coordinates all storage components.

Manages interactions between MemTable, WAL, SSTables, and Manifest
to provide a unified storage interface.
"""

import heapq
import logging
from abc import ABC, abstractmethod
from pathlib import Path
import threading
from time import time
from typing import Dict, Iterator
import asyncio

from src.config import SETTINGS
from src.storage.compaction import CompactionManager
from src.storage.exceptions import BatchTooLargeException
from src.storage.manifest import Manifest
from src.storage.memtable import MemTable
from src.storage.sstable import SSTable
from src.storage.sstable_manager import SSTableManager
from src.storage.wal import WAL, OperationType

logger = logging.getLogger(__name__)


class StorageEngine(ABC):
    """Abstract base class for storage engine implementations."""

    @abstractmethod
    def put(self, key: bytes, value: bytes) -> None:
        """Write a key-value pair to storage.

        Args:
            key: The key to write.
            value: The value to write.
        """
        pass

    @abstractmethod
    def get(self, key: bytes) -> bytes:
        """Read a value by key from storage.

        Args:
            key: The key to read.

        Returns:
            The value associated with the key.

        Raises:
            KeyError: If the key is not found.
        """
        pass

    @abstractmethod
    def delete(self, key: bytes) -> None:
        """Mark a key as deleted (tombstone).

        Args:
            key: The key to delete.
        """
        pass

    @abstractmethod
    def read_key_range(self, start_key: bytes, end_key: bytes) -> Dict[bytes, bytes]:
        """Read all key-value pairs in a range.

        Args:
            start_key: Inclusive start of the range.
            end_key: Exclusive end of the range.

        Returns:
            Dictionary of key-value pairs in the range.
        """
        pass

    @abstractmethod
    def batch_put(self, keys: list[bytes], values: list[bytes]) -> None:
        """Write multiple key-value pairs atomically.

        Args:
            keys: List of keys to write.
            values: List of values to write.

        Raises:
            ValueError: If keys and values have different lengths.
        """
        pass

    @abstractmethod
    def flush(self) -> None:
        """Flush MemTable to SSTable on disk."""
        pass


class LSMStorageEngine(StorageEngine):
    """Simple in-memory storage engine using only MemTable.

    This is a basic implementation suitable for testing and development.
    In production, use LSMStorageEngine which includes WAL, SSTables, and compaction.
    """

    def __init__(self, storage_path: str):
        """Initialize the storage engine.

        Args:
            storage_path: Path to the storage directory.
        """
        self.storage_path = Path(storage_path)
        self.wal = WAL(self.storage_path / "wal.log")
        self.manifest = Manifest(self.storage_path / "manifest.json")
        self.manifest.load()
        self.memtable = MemTable(max_size=SETTINGS.memtable_max_size)
        self.sstable_manager = SSTableManager(self.manifest)
        self.compaction = CompactionManager()
        self._flush_lock = threading.Lock()
        self.replay_wal()
        logger.info(f"Initialized LSMStorageEngine at {self.storage_path}")


    def replay_wal(self) -> None:
        """Replay WAL to restore MemTable state on startup."""
        logger.info("Replaying WAL...")

        for operation, key, value, offset in self.wal.replay():
            logger.debug(f"Replaying operation {operation} for key {key}")
            if operation == OperationType.PUT.value:
                self.memtable.put(key, value)
            elif operation == OperationType.DELETE.value:
                self.memtable.delete(key)
            self.memtable.set_max_wal_offset(offset)
        logger.info("WAL replay complete.")

    def put(self, key: bytes, value: bytes) -> None:
        """Write a key-value pair.

        Args:
            key: The key to write.
            value: The value to write.
        """
        # Append to WAL first (outside memtable lock to avoid unnecessary blocking)
        offset = self.wal.append(OperationType.PUT.value, key, value)

        # Acquire write lock for the entire memtable operation
        # This ensures no other thread can rotate between put() and our is_full() check
        self.memtable._acquire_write()
        try:
            before_size = self.memtable._current_size
            # Use unlocked version since we're already holding the lock
            self.memtable._put_unlocked(key, value)
            self.memtable._max_wal_offset = max(self.memtable._max_wal_offset, offset)
            after_size = self.memtable._current_size

            # Check if we need to rotate - STILL holding the lock
            # This ensures that between our is_full() check and rotate() call, no other
            # thread can interleave and also call rotate()
            should_rotate = self.memtable.is_full()
            if should_rotate:
                logger.debug(f"Rotating: {before_size} -> {after_size}, full check returned True")
                self.memtable._rotate_unlocked()
        finally:
            self.memtable._release_write()

        # Spawn flush thread AFTER releasing the lock
        if should_rotate:
            threading.Thread(target=self.flush, daemon=True).start()

        logger.debug(f"PUT {key} -> {len(value)} bytes")

    def get(self, key: bytes) -> bytes:
        """Read a value by key.

        Args:
            key: The key to read.

        Returns:
            The value associated with the key.

        Raises:
            KeyError: If the key is not found.
        """
        value = self.memtable.get(key)
        if value is None:
            # If not in MemTable, try to find it in SSTables
            value = self.sstable_manager.get(key)
        if not value or value == b"__TOMBSTONE__":
            raise KeyError(key)
        logger.debug(f"GET {key} -> {len(value)} bytes")
        return value

    def delete(self, key: bytes) -> None:
        """Mark a key as deleted.

        Args:
            key: The key to delete.
        """
        offset = self.wal.append(OperationType.DELETE.value, key, b"")
        self.memtable.delete(key)
        self.memtable.set_max_wal_offset(offset)
        logger.debug(f"DELETE {key}")

    def read_key_range(self, start_key: bytes, end_key: bytes) -> Iterator[tuple[bytes, bytes]]:
        """Read all key-value pairs in a range.

        Args:
            start_key: Inclusive start of the range.
            end_key: Exclusive end of the range.

        Returns:
            Iterator of key-value pairs in the range.
        """

        def _advance(iterator, index):
            try:
                while True:
                    k, v = next(iterator)
                    if index != -1 and k in memtable_keys:
                        continue
                    heapq.heappush(heap, (k, index, v, iterator))
                    return
            except StopIteration:
                pass

        memtable_result = self.memtable.get_range(start_key, end_key)
        memtable_keys = set(memtable_result.keys())
        memtable_items = sorted(memtable_result.items())
        last_key = None


        # Check overlapping SSTables (if any)
        overlapping_sstables = self.sstable_manager.get_overlapping_sstables(
            start_key, end_key
        )

        heap: list[tuple[bytes, int, bytes, Iterator[tuple[bytes, bytes]]]] = []  # (key, sstable_index, value, iterator)

        memtable_iter = iter(memtable_items)
        memtable_record = next(memtable_iter, (None, None))  # Get the first item from the memtable result, or (None, None) if it's empty

        if memtable_record[0] is not None:
         # Get the first item from the memtable result, or (None, None) if it's empty
            heapq.heappush(heap, (memtable_record[0], -1, memtable_record[1], memtable_iter))  # Use index -1 for memtable to prioritize it over SSTables

        for i, sstable in enumerate(sorted(overlapping_sstables, key=lambda s: s.path, reverse=True)):  # Sort SSTables by path in reverse order to prioritize newer SSTables
            it = sstable.range_scan(start_key, end_key)
            key, value = next(it, (None, None))  # Get the first item from each iterator, or (None, None) if it's empty

            if key is not None and key not in memtable_keys:
                heapq.heappush(heap, (key, i, value, it))

        while heap:
            key, source_index, value, iterator = heapq.heappop(heap)

            # Safety
            if key < start_key:
                _advance(iterator, source_index)
                continue

            # Safety
            if key >= end_key:
                break

            if last_key is not None and key == last_key:
                logger.debug(f"Skipping duplicate key: {key} from source index {source_index}")
                _advance(iterator, source_index)
                continue

            if value != b"__TOMBSTONE__":
                yield key, value

            last_key = key
            _advance(iterator, source_index)





    def batch_put(self, keys: list[bytes], values: list[bytes]) -> None:
        """Write multiple key-value pairs atomically.

        Args:
            keys: List of keys to write.
            values: List of values to write.

        Raises:
            ValueError: If keys and values have different lengths.
        """
        if len(keys) != len(values):
            raise ValueError("keys and values must have the same length")

        # Compute size of incoming data
        input_size = 0
        ENTRY_OVERHEAD = 64

        for key, value in zip(keys, values):
            prev = self.memtable.get(key)
            if prev:
                input_size = input_size + len(value) + ENTRY_OVERHEAD - len(prev)
            else:
                input_size = input_size + len(value) + ENTRY_OVERHEAD + len(key)

        if input_size > SETTINGS.memtable_max_size:
            raise BatchTooLargeException(input_size, SETTINGS.memtable_max_size)

        last_record_offset = self.wal.append_batch(OperationType.PUT.value, keys, values)

        # Hold write lock for entire batch operation
        self.memtable._acquire_write()
        try:
            should_rotate = self.memtable.batch_would_exceed(input_size)
            if should_rotate:
                self.memtable._rotate_unlocked()

            for key, value in zip(keys, values):
                self.memtable._put_unlocked(key, value)

            self.memtable._max_wal_offset = last_record_offset
        finally:
            self.memtable._release_write()

        if should_rotate:
            threading.Thread(target=self.flush, daemon=True).start()

        logger.info(f"BATCH_PUT {len(keys)} items")


    def flush(self) -> None:
        """Flush the oldest frozen MemTable buffer to disk.

        With the frozen queue approach, multiple frozen buffers can exist.
        This method processes the oldest one. Flushes are serialized so two
        background flush threads cannot both write the same queue head and
        then clear different frozen buffers.
        """
        with self._flush_lock:
            logger.info("Flushing frozen MemTable buffer to disk...")

            try:
                frozen_items = sorted(self.memtable.get_frozen_items().items())

                if not frozen_items:
                    logger.debug("No frozen items to flush")
                    return

                # Get the WAL offset BEFORE clearing frozen
                wal_offset = self.memtable.get_max_wal_offset_frozen()

                sstable_path = self.storage_path / f"sstable_{int(time()*1000)}.sst"
                sstable = SSTable(sstable_path)
                asyncio.run(sstable.write(iter(frozen_items)))

                sstable_metadata = self.sstable_manager.build_metadata(sstable, level=0)
                self.manifest.add_sstable(0, sstable_metadata)
                self.manifest.save()

                # Clear frozen AFTER we've got the offset and saved manifest
                self.memtable.clear_frozen()

                # Truncate WAL up to this buffer's offset
                self.wal.truncate_upto(wal_offset)
                logger.info(f"Flushed frozen buffer ({len(frozen_items)} keys) to {sstable_path}")

                self.compaction.may_start_compaction(self.manifest, self.storage_path)

            except Exception as e:
                logger.error(f"Error flushing MemTable: {e}")

    def flush_all(self) -> None:
        """Flush ALL frozen buffers in the queue to disk.

        This is typically called during shutdown or in tests to ensure
        all buffered data is persisted.
        """
        logger.info(f"Flushing all {self.memtable.get_frozen_queue_depth()} frozen buffers...")
        while self.memtable.get_frozen_queue_depth() > 0:
            self.flush()
