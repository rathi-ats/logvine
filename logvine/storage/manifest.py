"""Manifest: Metadata about SSTable levels and structure.

Tracks which SSTables exist at each level and coordinates compaction
decisions based on the LSM tree structure.
"""

from pathlib import Path
from typing import Dict, List


class Manifest:
    """Manages SSTable metadata across all levels."""

    def __init__(self, path: Path):
        """Initialize the Manifest.

        Args:
            path: Path to the manifest file.
        """
        self.path = path
        self.levels: Dict[int, List[str]] = {}  # level -> list of SSTable paths
        self.version = 0

    def load(self) -> None:
        """Load manifest from disk."""
        raise NotImplementedError

    def save(self) -> None:
        """Save manifest to disk."""
        raise NotImplementedError

    def add_sstable(self, level: int, sstable_path: str) -> None:
        """Record a new SSTable at a given level.

        Args:
            level: The LSM level where the SSTable is added.
            sstable_path: Path to the SSTable file.
        """
        raise NotImplementedError

    def remove_sstable(self, level: int, sstable_path: str) -> None:
        """Remove an SSTable record (after compaction).

        Args:
            level: The LSM level containing the SSTable.
            sstable_path: Path to the SSTable file.
        """
        raise NotImplementedError

    def get_level_sstables(self, level: int) -> List[str]:
        """Get all SSTables at a specific level.

        Args:
            level: The LSM level to query.

        Returns:
            List of SSTable paths at this level.
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
