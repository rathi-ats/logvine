"""StorageEngine: Coordinates all storage components.

Manages interactions between MemTable, WAL, SSTables, and Manifest
to provide a unified storage interface.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
import threading
from time import time
from typing import Dict, Optional
import asyncio

from logvine.storage import sstable
from logvine.storage.compaction import CompactionManager
from logvine.storage.manifest import Manifest
from logvine.storage.memtable import MemTable
from logvine.storage.sstable import SSTable
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
        self.manifest = Manifest(self.storage_path / "manifest.json")
        self.manifest.load()
        self.memtable = MemTable()
        self.compaction = CompactionManager()
        self.replay_wal()
        logger.info(f"Initialized LSMStorageEngine at {self.storage_path}")

        # For debugging purposes, log the current state of the manifest and memtable after initialization
        logger.debug(f"MemTable state after initialization: {self.memtable.data}")  
        logger.debug(f"WAL state after initialization: {self.wal.path} (exists: {self.wal.path.exists()}, size: {self.wal.path.stat().st_size if self.wal.path.exists() else 'N/A'} bytes)")
        # Log key ranges in bytes of all SSTables in the manifest for debugging
        for level, sstables in self.manifest.levels.items():
            for sstable_meta in sstables:
                logger.debug(f"SSTable {sstable_meta['path']} (level {level}) has key range: {bytes.fromhex(sstable_meta['min_key_hex'])} to {bytes.fromhex(sstable_meta['max_key_hex'])}")
        
    
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
        offset = self.wal.append(OperationType.PUT.value, key, value)
       
        self.memtable.put(key, value)
        self.memtable.set_max_wal_offset(offset)

        if self.memtable.is_full():
            self.memtable.rotate()

            threading.Thread(target=self.flush).start()  # Flush asynchronously
        
        logger.debug(f"PUT {key} -> {len(value)} bytes")
    
    def _get_from_sstables(self, key: bytes) -> Optional[bytes]:
        """Helper method to search for a key in SSTables."""
        sstable = None
        for level in sorted(self.manifest.levels.keys()):
            # traverse SSTables in descending order of creation time (newest first) to find the most recent value
            for sstable_meta in sorted(self.manifest.levels[level], key=lambda x: x["path"], reverse=True):
                min_key = bytes.fromhex(sstable_meta["min_key_hex"])
                max_key = bytes.fromhex(sstable_meta["max_key_hex"])
                if key < min_key or key > max_key:
                    logger.info(f"Key {key} is out of range for SSTable {sstable_meta['path']} (level {level}), skipping")
                    continue  # Key is out of range for this SSTable, skip it 
                else:  
                    sstable = SSTable(Path(sstable_meta["path"]))
                    logger.info(f"Searching for key {key} in SSTable {sstable_meta['path']} (level {level})")
                    logger.debug(f"min_key: {min_key}, max_key: {max_key}")
                    return sstable.get(key) if sstable else None
        
    

    def _get_overlapping_sstables_for_range(self, start_key: bytes, end_key: bytes) -> list[SSTable]:
        """Helper method to find all SSTables that overlap with a given key range."""
        overlapping_sstables = []
        for level in sorted(self.manifest.levels.keys()):
            for sstable_meta in self.manifest.levels[level]:
                min_key = bytes.fromhex(sstable_meta["min_key_hex"])
                max_key = bytes.fromhex(sstable_meta["max_key_hex"])
                if end_key < min_key or start_key > max_key:
                    logger.info(f"Range {start_key}-{end_key} is out of range for SSTable {sstable_meta['path']} (level {level}), skipping")
                    continue  # Range is out of range for this SSTable, skip it
                else:
                    logger.info(f"Range {start_key}-{end_key} overlaps with SSTable {sstable_meta['path']} (level {level}), adding to list")
                    overlapping_sstables.append(SSTable(Path(sstable_meta["path"])))
        return overlapping_sstables



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
            value = self._get_from_sstables(key)
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

    def read_key_range(self, start_key: bytes, end_key: bytes) -> Dict[bytes, bytes]:
        """Read all key-value pairs in a range.

        Args:
            start_key: Inclusive start of the range.
            end_key: Exclusive end of the range.

        Returns:
            Dictionary of key-value pairs in the range.
        """
        result = {}

        # First check SSTables since they contain the oldest data, 
        # then frozen MemTable, and finally the active MemTable to 
        # ensure we return the most recent value for each key in the range


        # Check overlapping SSTables (if any) since they contain older data than the memtables
        overlapping_sstables = self._get_overlapping_sstables_for_range(start_key, end_key)
        for sstable in sorted(overlapping_sstables, key=lambda s: s.path):
            for key, value in sstable.range_scan(start_key, end_key).items():
                if value != b"__TOMBSTONE__":
                    result[key] = value

        # Next check frozen and active memtable since they contain the most recent data that 
        # hasn't been flushed to SSTable yet
        memtable_result = self.memtable.get_range(start_key, end_key)
        for key, value in memtable_result.items():
            if value != b"__TOMBSTONE__":
                result[key] = value 

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
        
        try:
            for key, value in zip(keys, values):
                offset = self.wal.append(OperationType.PUT.value, key, value)
                self.memtable.put(key, value)
                self.memtable.set_max_wal_offset(offset)
            logger.info(f"BATCH_PUT {len(keys)} items")
        except Exception as e:
            logger.error(f"Error in batch_put: {e}")
        
        if self.memtable.is_full():
            self.memtable.rotate()
            threading.Thread(target=self.flush).start()  # Flush asynchronously

    def flush(self) -> None:
        """Flush MemTable to disk."""
        logger.info("Flushing MemTable to disk...")
        
        try:
            sstable_path = self.storage_path / f"sstable_{int(time()*1000)}.sst"
            sstable = SSTable(sstable_path)
            asyncio.run(sstable.write(self.memtable.frozen.items()))
            frozen_keys = sorted(self.memtable.frozen.keys())
            
            if not frozen_keys:
                logger.warning("No keys to flush")
                return
            
            self.manifest.add_sstable(
                0,
                {
                    "path": str(sstable_path),
                    "level": 0,
                    "created_at_ms": int(time() * 1000),
                    "size_bytes": sstable_path.stat().st_size if sstable_path.exists() else 0,
                    "entry_count": len(self.memtable.frozen),
                    "min_key_hex": frozen_keys[0].hex(),
                    "max_key_hex": frozen_keys[-1].hex(),
                }
            )
            self.memtable.clearFrozen()
            self.manifest.save()
            
            self.wal.truncate_upto(self.memtable.max_wal_offset_frozen)        
            logger.info(f"Flushed MemTable to {sstable_path}")

            self.compaction.may_start_compaction(self.manifest, self.storage_path)

        except Exception as e:
            logger.error(f"Error flushing MemTable: {e}")
