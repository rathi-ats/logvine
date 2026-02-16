"""SSTable: Sorted String Table on disk.

Immutable, sorted key-value store written to disk. Multiple SSTables
form the levels of the LSM tree.
"""

from asyncio.log import logger
import os
from pathlib import Path
import struct
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
    

    async def write(self, key_value_pairs: Iterator[Tuple[bytes, bytes]]) -> None:
        """Write sorted key-value pairs to disk.

        Args:
            key_value_pairs: Iterator of (key, value) tuples in sorted order.
        """
        logger.info(f"Writing SSTable to {self.path}")

        index = {}

        try:
            if not self.path.parent.exists():
                self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("wb") as f:
                for key, value in key_value_pairs:
                    offset = f.tell()
                    index[key] = offset

                    f.write(struct.pack(">I", len(key)))
                    f.write(key)
                    f.write(struct.pack(">I", len(value)))
                    f.write(value)

                index_offset = f.tell()

                # write index
                for key, offset in index.items():
                    f.write(struct.pack(">I", len(key)))
                    f.write(key)
                    f.write(struct.pack(">Q", offset))  # 8-byte offset

                # footer: where index starts
                f.write(struct.pack(">Q", index_offset))

                f.flush()
                os.fsync(f.fileno())
                self.index = index

        except Exception as e:
            logger.error(f"Error writing SSTable: {e}")
            if self.path.exists():
                self.path.unlink()  # Remove incomplete SSTable file
            raise e

        logger.info(f"Finished writing SSTable to {self.path}, size: {self.path.stat().st_size} bytes")


    def _load_index(self) -> None:
        """Load the index from disk into memory."""
        if not self.path.exists():
            self.index = {}
            return

        try:
            with self.path.open("rb") as f:
                file_size = f.seek(0, os.SEEK_END)

                f.seek(-8, os.SEEK_END)
                index_offset = struct.unpack(">Q", f.read(8))[0]

                f.seek(index_offset)

                self.index = {}

                index_end = file_size - 8  # footer starts here
                while f.tell() < index_end:  # Read until we reach the footer
                    key_len = struct.unpack(">I", f.read(4))[0]
                    key = f.read(key_len)
                    offset = struct.unpack(">Q", f.read(8))[0]
                    self.index[key] = offset
        except Exception as e:
            logger.error(f"Error loading SSTable index: {e}")
            self.index = {}


    def get(self, key: bytes) -> Optional[bytes]:
        """Retrieve a value by key using the index.

        Args:
            key: The key to look up.

        Returns:
            The value if found, None otherwise.
        """
        if not self.index:
            self._load_index()
            
        offset = self.index.get(key)
        if offset is None:
            logger.debug(f"Key not found in SSTable index: {key}")  
            logger.debug(f"SSTable index keys: {[k for k in self.index.keys()]}")
            return None
        with self.path.open("rb") as f:
            f.seek(offset)
            key_len = struct.unpack(">I", f.read(4))[0]
            f.read(key_len)  # skip key
            value_len = struct.unpack(">I", f.read(4))[0]
            value = f.read(value_len)
            return value
        

    def range_scan(
        self, start_key: bytes, end_key: bytes
    ) -> Iterator[Tuple[bytes, bytes]]:
        """Scan for all keys in a range.

        Args:
            start_key: Inclusive start of the range.
            end_key: Inclusive end of the range.

        Yields:
            Dictionary(key, value) in sorted order.
        """
        if not self.index:
            self._load_index()
    
        for key in sorted(self.index.keys()):
            if start_key <= key <= end_key:
                value = self.get(key)
                if value is not None:
                    yield key, value
            elif key > end_key:
                break  # Since keys are sorted, we can stop once we pass end_key
    
    def iter_items(self) -> Iterator[tuple[bytes, bytes]]:
        """Get an iterator of all key-value pairs in the SSTable."""
        if not self.index:
            self._load_index()
        for key in sorted(self.index.keys()):
            value = self.get(key)
            if value is not None:
                yield (key, value)

    # def overlaps(self, other: "SSTable") -> bool:
    #     """Check if key ranges overlap with another SSTable.

    #     Args:
    #         other: The other SSTable to compare with.

    #     Returns:
    #         True if key ranges overlap, False otherwise.
    #     """
    #     if not self.index:
    #         self._load_index()
    #     if not other.index:
    #         other._load_index()

    #     if not self.index or not other.index:
    #         return False  # No keys means no overlap

    #     self_min = min(self.index.keys())
    #     self_max = max(self.index.keys())
    #     other_min = min(other.index.keys())
    #     other_max = max(other.index.keys())

    #     return not (self_max < other_min or self_min > other_max)
    

    # def compact_with(self, other: "SSTable", output_path: Path) -> "SSTable":
    #     """Compact this SSTable with another into a new SSTable.

    #     Args:
    #         other: The other SSTable to compact with.
    #         output_path: Path for the new compacted SSTable.

    #     Returns:
    #         A new SSTable instance representing the compacted result.
    #     """
    #     if not self.index:
    #         self._load_index()
    #     if not other.index:
    #         other._load_index()

    #     merged_data = {}
    #     for key in sorted(set(self.index.keys()) | set(other.index.keys())):
    #         value_self = self.get(key)
    #         value_other = other.get(key)

    #         if value_self is not None and value_other is not None:
    #             merged_data[key] = value_other  # Prefer newer value
    #         elif value_self is not None:
    #             merged_data[key] = value_self
    #         elif value_other is not None:
    #             merged_data[key] = value_other

    #     new_sstable = SSTable(output_path, level=max(self.level, other.level) + 1)
    #     new_sstable.write(iter(merged_data.items()))
    #     return new_sstable
