"""MemTable: In-memory write buffer for recent writes.

Acts as the first stage of the LSM tree, holding recent writes in memory
before they are flushed to disk as SSTables.
"""

from asyncio.log import logger
import threading
import time
from typing import Generator, Optional


class MemTable:
    """In-memory sorted map for buffering recent writes.
    
    Thread-safe with read-write locking:
    - Multiple reads can occur concurrently
    - Writes are exclusive (blocks reads and other writes)
    """

    def __init__(self, max_size: int = 1_0):
        """Initialize the MemTable.
        Args:
            max_size: Maximum size in bytes before triggering a flush.
        """
        self.max_size = max_size
        self.data: dict[bytes, bytes] = {}
        self.frozen: dict[bytes, bytes] = {}
        self.current_size = 0
        self.max_wal_offset = 0
        self.max_wal_offset_frozen = 0
        
        # Read-write lock for concurrency control
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._read_count = 0

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
        """Set the maximum WAL offset that has been flushed to disk."""
        self._acquire_write()
        self.max_wal_offset = max(self.max_wal_offset, offset)
        self._release_write()

    def put(self, key: bytes, value: bytes) -> None:
        """Insert or update a key-value pair.

        Args:
            key: The key to insert (bytes).
            value: The value to insert (bytes).
        """
        self._acquire_write()
        try:
            if key in self.data:
                self.current_size -= len(self.data[key])

            self.data[key] = value
            self.current_size += len(value)
        finally:
            self._release_write()

    def get(self, key: bytes) -> Optional[bytes]:
        """Retrieve a value by key.

        Args:
            key: The key to look up (bytes).

        Returns:
            The value if found, None otherwise.
        """
        self._acquire_read()
        try:
            if key in self.data:
                return self.data.get(key)
            elif key in self.frozen:
                return self.frozen.get(key)
        finally:
            self._release_read()

    def delete(self, key: bytes) -> None:
        """Mark a key as deleted (tombstone).

        Args:
            key: The key to delete (bytes).
        """
        self.put(key, b"__TOMBSTONE__")  # Use a special value to indicate deletion

    def is_full(self) -> bool:
        """Check if the MemTable has reached its size limit.

        Returns:
            True if the MemTable should be flushed, False otherwise.
        """
        logger.debug(f"MemTable size: {self.current_size} bytes, max size: {self.max_size} bytes")
        return self.current_size >= self.max_size

    def iter_sorted(self):
        """Iterate over key-value pairs in sorted order.

        Yields:
            Tuple of (key, value) in sorted key order.
        # """
        self._acquire_read()
        try:
            # Create a snapshot to iterate safely
            items = [(key, self.data[key]) for key in sorted(self.data.keys())]
            items += [(key, self.frozen[key]) for key in sorted(self.frozen.keys())]
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
            self.frozen = self.data.copy()
            self.data.clear()
            self.current_size = 0
            self.max_wal_offset_frozen = self.max_wal_offset
            self.max_wal_offset = 0
        finally:
            self._release_write()
    
    def clearFrozen(self):
        """Clear the frozen data after it has been flushed to disk."""
        self._acquire_read()
        try:
            self.frozen.clear()
        finally:
            self._release_read()
    
    def get_range(self, start_key: bytes, end_key: bytes) -> dict:
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
            for key in sorted(self.frozen.keys()):
                if key > end_key:
                    break
                if start_key <= key <= end_key:
                    result[key] = self.frozen[key]
            for key in sorted(self.data.keys()):
                if key > end_key:
                    break
                if start_key <= key <= end_key:
                    result[key] = self.data[key]
            logger.info(f"Found {len(result)} keys in range from MemTable")
            return result
        except Exception as e:
            logger.error(f"Error in get_range: {e}")
            return {}    
        finally:
            self._release_read()

