"""SSTable: Sorted String Table on disk.

Immutable, sorted key-value store written to disk. Multiple SSTables
form the levels of the LSM tree.
"""

from pathlib import Path
from typing import Iterator, Optional, Tuple


class SSTable:
    """Immutable sorted key-value store on disk."""

    def __init__(self, path: Path, level: int = 0):
        """Initialize an SSTable.

        Args:
            path: Path to the SSTable file.
            level: The level in the LSM tree this SSTable belongs to.
        """
        self.path = path
        self.level = level
        self.index = {}  # key -> file offset for binary search

    def write(self, key_value_pairs: Iterator[Tuple[str, bytes]]) -> None:
        """Write sorted key-value pairs to disk.

        Args:
            key_value_pairs: Iterator of (key, value) tuples in sorted order.
        """
        raise NotImplementedError

    def get(self, key: str) -> Optional[bytes]:
        """Retrieve a value by key using the index.

        Args:
            key: The key to look up.

        Returns:
            The value if found, None otherwise.
        """
        raise NotImplementedError

    def range_scan(
        self, start_key: str, end_key: str
    ) -> Iterator[Tuple[str, bytes]]:
        """Scan for all keys in a range.

        Args:
            start_key: Inclusive start of the range.
            end_key: Exclusive end of the range.

        Yields:
            (key, value) tuples in sorted order.
        """
        raise NotImplementedError

    def overlaps(self, other: "SSTable") -> bool:
        """Check if key ranges overlap with another SSTable.

        Args:
            other: The other SSTable to compare with.

        Returns:
            True if key ranges overlap, False otherwise.
        """
        raise NotImplementedError
