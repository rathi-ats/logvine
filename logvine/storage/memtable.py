"""MemTable: In-memory write buffer for recent writes.

Acts as the first stage of the LSM tree, holding recent writes in memory
before they are flushed to disk as SSTables.
"""

import threading
import time
from typing import Optional


class MemTable:
    """In-memory sorted map for buffering recent writes.
    
    Thread-safe with read-write locking:
    - Multiple reads can occur concurrently
    - Writes are exclusive (blocks reads and other writes)
    """

    def __init__(self, max_size: int = 1_000_000):
        """Initialize the MemTable.

        Args:
            max_size: Maximum size in bytes before triggering a flush.
        """
        self.max_size = max_size
        self.data: dict[bytes, bytes] = {}
        self.current_size = 0
        
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

        # if key in self.data:
        #     self.current_size -= len(self.data[key])

        # self.data[key] = value
        # self.current_size += len(value)

    def get(self, key: bytes) -> Optional[bytes]:
        """Retrieve a value by key.

        Args:
            key: The key to look up (bytes).

        Returns:
            The value if found, None otherwise.
        """
        # self._acquire_read()
        # try:
        return self.data.get(key)
        # finally:
        #     self._release_read()

    def delete(self, key: bytes) -> None:
        """Mark a key as deleted (tombstone).

        Args:
            key: The key to delete (bytes).
        """
        self._acquire_write()
        try:
            self.data[key] = b"__TOMBSTONE__"
        finally:
            self._release_write()

    def is_full(self) -> bool:
        """Check if the MemTable has reached its size limit.

        Returns:
            True if the MemTable should be flushed, False otherwise.
        """
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
        finally:
            self._release_read()

        # items = [(key, self.data[key]) for key in sorted(self.data.keys())]
        
        for key, value in items:
            yield key, value
