"""StorageEngine: Coordinates all storage components.

Manages interactions between MemTable, WAL, SSTables, and Manifest
to provide a unified storage interface.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

from logvine.storage.memtable import MemTable
from logvine.storage.wal import WAL, OperationType

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
            end_key: Inclusive end of the range.

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
        self.memtable = MemTable()
        self.replay_wal()
        logger.info(f"Initialized LSMStorageEngine at {self.storage_path}")

    
    def replay_wal(self) -> None:
        """Replay WAL to restore MemTable state on startup."""
        logger.info("Replaying WAL...")
        for operation, key, value in self.wal.replay():
            logger.info(f"Replaying operation {operation} for key {key}")
            if operation == OperationType.PUT.value:
                self.memtable.put(key, value)
            elif operation == OperationType.DELETE.value:
                self.memtable.delete(key)
        logger.info("WAL replay complete.")

    def put(self, key: bytes, value: bytes) -> None:
        """Write a key-value pair.

        Args:
            key: The key to write.
            value: The value to write.
        """
        self.wal.append(OperationType.PUT.value, key, value)
        self.memtable.put(key, value)
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
            raise KeyError(key)
        if value == b"__TOMBSTONE__":
            raise KeyError(key)
        logger.debug(f"GET {key} -> {len(value)} bytes")
        return value

    def delete(self, key: bytes) -> None:
        """Mark a key as deleted.

        Args:
            key: The key to delete.
        """
        self.wal.append(OperationType.DELETE.value, key, b"")
        self.memtable.delete(key)
        logger.debug(f"DELETE {key}")

    def read_key_range(self, start_key: bytes, end_key: bytes) -> Dict[bytes, bytes]:
        """Read all key-value pairs in a range.

        Args:
            start_key: Inclusive start of the range.
            end_key: Exclusive end of the range.

        Returns:
            Dictionary of key-value pairs in the range.
        """
        result = {}
        for key, value in self.memtable.iter_sorted():
            if key < start_key:
                continue
            # Treat end_key as inclusive: stop when key > end_key
            if key > end_key:
                break
            if value != b"__TOMBSTONE__":
                result[key] = value
        logger.debug(f"RANGE_READ {start_key} to {end_key} -> {len(result)} items")
        return result

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

        for key, value in zip(keys, values):
            self.wal.append(OperationType.PUT.value, key, value)
            self.memtable.put(key, value)
        logger.debug(f"BATCH_PUT {len(keys)} items")

    def flush(self) -> None:
        """Flush MemTable to disk.

        In SimpleStorageEngine, this is a no-op since data is only in memory.
        In production LSMStorageEngine, this would write to SSTable and clear MemTable.
        """
        logger.info("FLUSH requested (no-op for SimpleStorageEngine)")

