"""SSTableManager: Encapsulates SSTable lookup and selection logic."""

import logging
from pathlib import Path
from time import time
from typing import Any, Optional

from src.storage.manifest import Manifest
from src.storage.sstable import SSTable

logger = logging.getLogger(__name__)


class SSTableManager:
    """Coordinates SSTable reads based on manifest metadata."""

    def __init__(self, manifest: Manifest):
        self.manifest = manifest

    def iter_sstable_metadata(
        self, level: Optional[int] = None, newest_first: bool = True
    ):
        """Yield SSTable metadata with configurable level and ordering."""
        if level is None:
            level_entries = self.manifest.iter_levels()
        else:
            level_entries = [(level, self.manifest.get_level_sstables(level))]

        for level_id, entries in level_entries:
            for sstable_meta in sorted(
                entries,
                key=lambda item: item["path"],
                reverse=newest_first,
            ):
                yield level_id, sstable_meta

    def metadata_to_sstable(self, sstable_meta: dict[str, Any]) -> SSTable:
        """Materialize an SSTable from manifest metadata."""
        return SSTable(Path(sstable_meta["path"]), level=sstable_meta["level"])

    def build_metadata(self, sstable: SSTable, level: int) -> dict[str, Any]:
        """Build manifest metadata for a written SSTable."""
        if not sstable.index:
            sstable._load_index()
        if not sstable.index:
            raise ValueError(f"SSTable index is empty for {sstable.path}")

        sorted_keys = sorted(sstable.index.keys())
        return {
            "path": str(sstable.path),
            "level": level,
            "created_at_ms": int(time() * 1000),
            "size_bytes": sstable.path.stat().st_size if sstable.path.exists() else 0,
            "entry_count": len(sorted_keys),
            "min_key_hex": sorted_keys[0].hex(),
            "max_key_hex": sorted_keys[-1].hex(),
        }

    def get(self, key: bytes) -> Optional[bytes]:
        """Find a key in SSTables and return its value if present."""
        for level, sstable_meta in self.iter_sstable_metadata():
            min_key = bytes.fromhex(sstable_meta["min_key_hex"])
            max_key = bytes.fromhex(sstable_meta["max_key_hex"])
            if key < min_key or key > max_key:
                logger.info(
                    f"Key {key} is out of range for SSTable {sstable_meta['path']} "
                    f"(level {level}), skipping"
                )
                continue

            sstable = SSTable(Path(sstable_meta["path"]))
            logger.info(
                f"Searching for key {key} in SSTable {sstable_meta['path']} (level {level})"
            )
            logger.debug(f"min_key: {min_key}, max_key: {max_key}")
            return sstable.get(key)
        return None

    def get_overlapping_sstables(self, start_key: bytes, end_key: bytes) -> list[SSTable]:
        """Return SSTables whose key-ranges overlap the requested range."""
        overlapping = []
        for level, sstable_meta in self.iter_sstable_metadata():
            min_key = bytes.fromhex(sstable_meta["min_key_hex"])
            max_key = bytes.fromhex(sstable_meta["max_key_hex"])
            if end_key < min_key or start_key > max_key:
                logger.info(
                    f"Range {start_key}-{end_key} is out of range for SSTable "
                    f"{sstable_meta['path']} (level {level}), skipping"
                )
                continue
            logger.info(
                f"Range {start_key}-{end_key} overlaps with SSTable "
                f"{sstable_meta['path']} (level {level}), adding to list"
            )
            overlapping.append(self.metadata_to_sstable(sstable_meta))
        return overlapping
