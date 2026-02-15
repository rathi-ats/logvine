"""Manifest: Metadata about SSTable levels and structure.

Tracks which SSTables exist at each level and coordinates compaction
decisions based on the LSM tree structure.
"""

import json
import os
from pathlib import Path
import threading
from typing import Any, Dict, List
from venv import logger


class Manifest:
    """Manages SSTable metadata across all levels."""

    def __init__(self, path: Path):
        """Initialize the Manifest.

        Args:
            path: Path to the manifest file.
        """
        self.path = path
        self.levels: Dict[int, List[Dict[str, Any]]] = {}
        self.version = 0
        self._lock = threading.Lock()

    def load(self) -> None:
        """Load manifest from disk."""
        with self._lock:
            if not self.path.exists():
                self.levels = {}
                self.version = 0
                return

            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            raw_levels = data.get("levels", {})
            levels: Dict[int, List[Dict[str, Any]]] = {}
            for level, entries in raw_levels.items():
                levels[int(level)] = list(entries)
            self.levels = levels
            self.version = int(data.get("version", 0))

    def save(self) -> None:
        """Save manifest to disk."""
        with self._lock:
            serialized_levels = {str(level): entries for level, entries in self.levels.items()}
            data = {"version": self.version, "levels": serialized_levels}
            tmp_path = self.path.with_name(f"{self.path.name}.tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, separators=(",", ":"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)
            dir_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

    def add_sstable(self, level: int, metadata: Dict[str, Any]) -> None:
        """Record a new SSTable at a given level.

        Args:
            level: The LSM level where the SSTable is added.
            metadata: SSTable metadata dictionary.
        """
        logger.info(f"Adding SSTable to manifest: level={level}, path={metadata.get('path')}")
        with self._lock:
            self.levels.setdefault(level, []).append(metadata)
            self.version += 1
            self._save_locked()

    def remove_sstable(self, level: int, sstable_path: str) -> None:
        """Remove an SSTable record (after compaction).

        Args:
            level: The LSM level containing the SSTable.
            sstable_path: Path to the SSTable file.
        """
        logger.info(f"Removing SSTable from manifest: level={level}, path={sstable_path}")  
        with self._lock:
            entries = self.levels.get(level, [])
            self.levels[level] = [
                entry for entry in entries if entry.get("path") != sstable_path
            ]
            self.version += 1
            self._save_locked()

    def _save_locked(self) -> None:
        """Persist manifest atomically. Caller must hold self._lock."""
        serialized_levels = {str(level): entries for level, entries in self.levels.items()}
        data = {"version": self.version, "levels": serialized_levels}
        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self.path)
        dir_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def get_level_sstables(self, level: int) -> List[Dict[str, Any]]:
        """Get all SSTables at a specific level.

        Args:
            level: The LSM level to query.

        Returns:
            List of SSTable metadata entries at this level.
        """
        return self.levels.get(level, [])

    def should_compact(self, level: int, max_size: int) -> bool:
        """Determine if a level should be compacted.

        Args:
            level: The LSM level to check.
            max_size: Maximum number of SSTables before compaction.

        Returns:
            True if compaction is needed, False otherwise.
        """
        return len(self.get_level_sstables(level)) >= max_size
