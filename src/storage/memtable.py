"""MemTable: In-memory write buffer for recent writes.

Acts as the first stage of the LSM tree, holding recent writes in memory
before they are flushed to disk as SSTables.
"""

import logging
import threading
from typing import Dict, Optional


logger = logging.getLogger(__name__)


class MemTable:
    """In-memory sorted map for buffering recent writes.

    Thread-safe with read-write locking:
    - Multiple reads can occur concurrently
    - Writes are exclusive (blocks reads and other writes)
    """

    def __init__(self, max_size: int):
        """Initialize the MemTable.
        Args:
            max_size: Maximum size in bytes before triggering a flush.
        """
        self.max_size = max_size
        self._data: Dict[bytes, bytes] = {}
        # Queue of (frozen_data, wal_offset) tuples to handle concurrent flushes
        self._frozen_queue: list[tuple[Dict[bytes, bytes], int]] = []
        self._current_size = 0
        self._max_wal_offset = 0

        # Read-write lock for concurrency control
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._read_count = 0
        logger.info(f"Initialized MemTable with max_size={max_size}")

    def _acquire_read(self) -> None:
        """Acquire read lock (multiple readers allowed)."""
        self._read_lock.acquire()
        self._read_count += 1
        if self._read_count == 1:
            self._write_lock.acquire()
        self._read_lock.release()

    def _release_read(self) -> None:
        """Release read lock."""
        self._read_lock.acquire()
        self._read_count -= 1
        if self._read_count == 0:
            self._write_lock.release()
        self._read_lock.release()

    def _acquire_write(self) -> None:
        """Acquire write lock (exclusive access)."""
        self._write_lock.acquire()

    def _release_write(self) -> None:
        """Release write lock."""
        self._write_lock.release()

    def set_max_wal_offset(self, offset: int) -> None:
        """Set the maximum WAL offset that has been fsynced to wal"""
        self._acquire_write()
        prev = self._max_wal_offset
        self._max_wal_offset = max(self._max_wal_offset, offset)
        self._release_write()
        logger.debug(
            f"Updated MemTable WAL offset: previous={prev}, current={self._max_wal_offset}"
        )

    def put(self, key: bytes, value: bytes) -> None:
        """Insert or update a key-value pair.

        Args:
            key: The key to insert (bytes).
            value: The value to insert (bytes).
        """
        self._acquire_write()
        try:
            self._put_unlocked(key, value)
        finally:
            self._release_write()
        logger.debug(
            f"MemTable put key={key!r}, value_size={len(value)}, current_size={self._current_size}"
        )

    def _put_unlocked(self, key: bytes, value: bytes) -> None:
        """Insert or update a key-value pair (caller must hold write lock).

        Args:
            key: The key to insert (bytes).
            value: The value to insert (bytes).
        """
        if key in self._data:
            self._current_size -= len(self._data[key])

        self._data[key] = value
        self._current_size += len(value)

    def get(self, key: bytes) -> Optional[bytes]:
        """Retrieve a value by key.

        Args:
            key: The key to look up (bytes).

        Returns:
            The value if found, None otherwise.
        """
        self._acquire_read()
        try:
            # Check active data first (most recent)
            if key in self._data:
                return self._data.get(key)
            # Check frozen queue from newest to oldest
            for frozen_data, _ in reversed(self._frozen_queue):
                if key in frozen_data:
                    return frozen_data.get(key)
        finally:
            self._release_read()

    def delete(self, key: bytes) -> None:
        """Mark a key as deleted (tombstone).

        Args:
            key: The key to delete (bytes).
        """
        self.put(key, b"__TOMBSTONE__")  # Use a special value to indicate deletion
        logger.debug(f"MemTable delete key={key!r}")

    def is_full(self) -> bool:
        """Check if the MemTable has reached its size limit.

        Returns:
            True if the MemTable should be flushed, False otherwise.
        """
        logger.debug(f"MemTable size: {self._current_size} bytes, max size: {self.max_size} bytes")
        return self._current_size >= self.max_size

    def iter_sorted(self):
        """Iterate over key-value pairs in sorted order.

        Yields:
            Tuple of (key, value) in sorted key order.
        """
        self._acquire_read()
        try:
            # Create a snapshot to iterate safely
            items = [(key, self._data[key]) for key in sorted(self._data.keys())]
            # Add items from frozen queue (oldest to newest)
            for frozen_data, _ in self._frozen_queue:
                items += [(key, frozen_data[key]) for key in sorted(frozen_data.keys())]
            items.sort(key=lambda x: x[0])  # Sort by key
        finally:
            self._release_read()

        for key, value in items:
            yield key, value

    def rotate(self):
        """Copy the current data to frozen queue and clear the active data for new writes.

        The frozen data is added to a queue so that multiple concurrent rotates
        don't overwrite each other. Each flush will process the oldest frozen entry.
        """
        self._acquire_write()
        try:
            self._rotate_unlocked()
        finally:
            self._release_write()

    def _rotate_unlocked(self):
        """Rotate without acquiring lock (caller must hold write lock)."""
        frozen_count = len(self._data)
        # Append (data, wal_offset) to queue instead of overwriting single _frozen
        self._frozen_queue.append((self._data.copy(), self._max_wal_offset))
        self._data.clear()
        self._current_size = 0
        self._max_wal_offset = 0
        logger.info(
            f"Rotated MemTable: frozen_count={frozen_count}, "
            f"queue_depth={len(self._frozen_queue)}"
        )

    def clear_frozen(self):
        """Remove the oldest frozen buffer from the queue after it has been flushed to disk.

        This is called after a flush completes successfully.
        """
        self._acquire_write()
        try:
            if self._frozen_queue:
                frozen_data, _ = self._frozen_queue.pop(0)
                frozen_count = len(frozen_data)
            else:
                frozen_count = 0
        finally:
            self._release_write()
        logger.info(f"Cleared oldest frozen MemTable buffer: count={frozen_count}, remaining_in_queue={len(self._frozen_queue)}")

    def get_range(self, start_key: bytes, end_key: bytes) -> Dict[bytes, bytes]:
        """Retrieve all key-value pairs in a key range.

        Args:
            start_key: Inclusive start of the key range (bytes).
            end_key: Exclusive end of the key range (bytes).
        Returns:
            Dictionary of (key, value) for keys in the specified range.
        """
        logger.info(f"Getting range from MemTable: start_key={start_key}, end_key={end_key}")
        self._acquire_read()
        try:
            result = {}
            # Check frozen queue (oldest to newest, but override with newer)
            for frozen_data, _ in self._frozen_queue:
                for key in sorted(frozen_data.keys()):
                    if key >= end_key:
                        break
                    if start_key <= key < end_key:
                        result[key] = frozen_data[key]
            # Active data (most recent) overrides frozen
            for key in sorted(self._data.keys()):
                if key >= end_key:
                    break
                if start_key <= key < end_key:
                    result[key] = self._data[key]
            logger.info(f"Found {len(result)} keys in range from MemTable")
            return result
        except Exception as e:
            logger.exception("Error in MemTable get_range")
            return {}
        finally:
            self._release_read()


    def batch_would_exceed(self, batch_size: int) -> bool:
        would_exceed = self._current_size + batch_size >= self.max_size
        logger.debug(
            f"MemTable batch check: current_size={self._current_size}, "
            f"batch_size={batch_size}, max_size={self.max_size}, would_exceed={would_exceed}"
        )
        return would_exceed

    def get_frozen_items(self) -> Dict[bytes, bytes]:
        """Get the oldest frozen buffer for flushing.

        Returns:
            Dictionary of frozen key-value pairs, or empty dict if no frozen data.
        """
        if self._frozen_queue:
            return self._frozen_queue[0][0]
        return {}

    def get_max_wal_offset_frozen(self) -> int:
        """Get the WAL offset of the oldest frozen buffer.

        Returns:
            The WAL offset, or 0 if no frozen data.
        """
        if self._frozen_queue:
            return self._frozen_queue[0][1]
        return 0

    def get_frozen_queue_depth(self) -> int:
        """Get the number of frozen buffers waiting to be flushed.

        Returns:
            The depth of the frozen queue.
        """
        return len(self._frozen_queue)
