import asyncio
import heapq
import logging
from pathlib import Path
import threading
from time import time
from typing import Iterator

from src.storage.manifest import Manifest
from src.storage.sstable import SSTable
from src.storage.sstable_manager import SSTableManager


L0_THRESHOLD = 2  # Example threshold value

logger = logging.getLogger(__name__)

class CompactionManager:
    
    def __init__(self):
        self._compaction_in_progress = False
        self._state_lock = threading.Lock()

    def _needs_compaction(self, manifest):
        level0_count = len(manifest.get_level_sstables(0))
        needs = level0_count > L0_THRESHOLD
        logger.debug(
            f"Compaction check: level0_count={level0_count}, threshold={L0_THRESHOLD}, needs={needs}"
        )
        return needs
    
    def is_compaction_in_progress(self) -> bool:
        """Check if compaction is currently in progress."""
        with self._state_lock:
            return self._compaction_in_progress
    
    def may_start_compaction(self, manifest, storage_path: Path):
        """Determine if compaction can be started."""
        with self._state_lock:
            if self._compaction_in_progress:
                logger.info("Compaction is already in progress. Cannot start another.")
                return
            if not self._needs_compaction(manifest):
                logger.info("Compaction is not needed at this time.")
                return
            self._compaction_in_progress = True

        logger.info(f"Compaction can be started for storage_path={storage_path}.")
        thread = threading.Thread(target=self.compact, args=(manifest, storage_path))
        thread.start()

    def merge_sstables(
        self, sstable_infos, sstable_manager: SSTableManager
    ) -> Iterator[tuple[bytes, bytes]]:
        """Merge multiple SSTables into a single SSTable."""
        # Read from all input SSTables,
        # merge their key-value pairs while handling duplicates and tombstones,
        # and return the merged output

        logger.info(f"Merging {len(sstable_infos)} SSTables")
        
        # Implement merge sort logic here, reading from each SSTable 

        heap: list[tuple[bytes, int, bytes, Iterator[tuple[bytes, bytes]]]] = []  # (key, sstable_index, value, iterator)

        def _advance(iterator, index):
            try:
                k, v = next(iterator)
                heapq.heappush(heap, (k, index, v, iterator))
            except StopIteration:
                pass


        for index, sstable_info in enumerate(sstable_infos):
            logger.info(f"Reading SSTable for merge: {sstable_info['path']}")
            sstable = sstable_manager.metadata_to_sstable(sstable_info)

            # Temp: log the keys in the SSTable index for debugging
            if not sstable.index:
                sstable._load_index()
            logger.debug(f"SSTable {sstable_info['path']} has keys: {list(sstable.index.keys())}")



            iterator = sstable.iter_items()  # Get an iterator of (key, value) pairs

            try:                
                first_item = next(iterator)
                heapq.heappush(heap, (first_item[0], index, first_item[1], iterator))
            except StopIteration:
                logger.info(f"SSTable {sstable_info['path']} is empty, skipping.")

        last_key = None
        while heap:
            key, sstable_index, value, iterator = heapq.heappop(heap)

            if last_key is not None and key == last_key:
                logger.debug(f"Skipping duplicate key: {key} from SSTable index {sstable_index}")
                _advance(iterator, sstable_index)
                continue  # Skip duplicate keys, keep the one from the most recent SSTable

            yield (key, value)

            last_key = key

            _advance(iterator, sstable_index)
        


    def compact(self, manifest: Manifest, storage_path: Path):
        """Perform compaction of SSTables."""
        try:
            logger.info("Starting compaction process...")
            with self._state_lock:
                self._compaction_in_progress = True
            sstable_manager = SSTableManager(manifest)
            level0_sstables = [
                meta for _, meta in sstable_manager.iter_sstable_metadata(level=0)
            ]
            if not level0_sstables:
                logger.info("No SSTables to compact in level 0.")
                return
            logger.info(f"Compacting {len(level0_sstables)} SSTable(s) from level 0")
            
            # For simplicity, we will just merge all level 0 SSTables into one new SSTable
            new_sstable_path = Path(storage_path / f"sstable_compacted_{int(time())}.sst")
            merged_data = self.merge_sstables(level0_sstables, sstable_manager)

            # Write merged data to new SSTable
            new_sstable = SSTable(new_sstable_path, level=1)
            asyncio.run(new_sstable.write(merged_data))
            logger.info(f"Wrote compacted SSTable: {new_sstable_path}")

            # Update manifest: remove old SSTables and add new one to level 1
            new_sstable_metadata = sstable_manager.build_metadata(new_sstable, level=1)
            manifest.add_sstable(level=1, metadata=new_sstable_metadata)

            for sstable_info in level0_sstables:
                manifest.remove_sstable(level=0, sstable_path=sstable_info["path"])
            
            # Remove old SSTable files from disk
            self._remove_old_sstable_files(level0_sstables)

            logger.info(f"Compaction completed. Created new SSTable at {new_sstable_path}")

        except Exception as e:
            logger.exception("Error during compaction")
        finally:
            with self._state_lock:
                self._compaction_in_progress = False
            logger.info("Compaction process finished")
    

    def _remove_old_sstable_files(self, sstable_infos):
        """Remove old SSTable files from disk after compaction."""
        deleted = 0
        for sstable_info in sstable_infos:
            try:
                Path(sstable_info["path"]).unlink()
                logger.info(f"Deleted old SSTable file: {sstable_info['path']}")
                deleted += 1
            except Exception as e:
                logger.error(f"Error deleting old SSTable file {sstable_info['path']}: {e}")
        logger.info(f"Deleted {deleted}/{len(sstable_infos)} old SSTable file(s)")
