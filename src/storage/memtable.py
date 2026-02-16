"""MemTable: In-memory write buffer for recent writes.

Acts as the first stage of the LSM tree, holding recent writes in memory
before they are flushed to disk as SSTables.
"""

import logging
import threading
from typing import Optional


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
        self._data: dict[bytes, bytes] = {}
        self._frozen: dict[bytes, bytes] = {}
        self._current_size = 0
        self._max_wal_offset = 0
        self._max_wal_offset_frozen = 0
        
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
            if key in self._data:
                self._current_size -= len(self._data[key])

            self._data[key] = value
            self._current_size += len(value)
        finally:
            self._release_write()
        logger.debug(
            f"MemTable put key={key!r}, value_size={len(value)}, current_size={self._current_size}"
        )

    def get(self, key: bytes) -> Optional[bytes]:
        """Retrieve a value by key.

        Args:
            key: The key to look up (bytes).

        Returns:
            The value if found, None otherwise.
        """
        self._acquire_read()
        try:
            if key in self._data:
                return self._data.get(key)
            elif key in self._frozen:
                return self._frozen.get(key)
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
        # """
        self._acquire_read()
        try:
            # Create a snapshot to iterate safely
            items = [(key, self._data[key]) for key in sorted(self._data.keys())]
            items += [(key, self._frozen[key]) for key in sorted(self._frozen.keys())]
            items.sort(key=lambda x: x[0])  # Sort by key
        finally:
            self._release_read()
        
        for key, value in items:
            yield key, value

    def rotate(self):
        """Copy the current data to frozen and clear the active data for new writes.

        Returns:
            A dictionary of key-value pairs to be flushed.
        """
        self._acquire_write()
        try:
            frozen_count = len(self._data)
            self._frozen = self._data.copy()
            self._data.clear()
            self._current_size = 0
            self._max_wal_offset_frozen = self._max_wal_offset
            self._max_wal_offset = 0
        finally:
            self._release_write()
        logger.info(
            f"Rotated MemTable: frozen_count={frozen_count}, "
            f"max_wal_offset_frozen={self._max_wal_offset_frozen}"
        )
    
    def clearFrozen(self):
        """Clear the frozen data after it has been flushed to disk."""
        self._acquire_read()
        try:
            frozen_count = len(self._frozen)
            self._frozen.clear()
        finally:
            self._release_read()
        logger.info(f"Cleared frozen MemTable entries: count={frozen_count}")
    
    def get_range(self, start_key: bytes, end_key: bytes) -> dict[bytes, bytes]:
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
            for key in sorted(self._frozen.keys()):
                if key > end_key:
                    break
                if start_key <= key <= end_key:
                    result[key] = self._frozen[key]
            for key in sorted(self._data.keys()):
                if key > end_key:
                    break
                if start_key <= key <= end_key:
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

    def get_frozen_items(self) -> dict[bytes, bytes]:
        return self._frozen
    
    def get_max_wal_offset_frozen(self):
        return self._max_wal_offset_frozen
