import asyncio
import heapq
import logging
from multiprocessing import heap
from pathlib import Path
import threading
from time import time
from typing import Iterator, List

from logvine.storage.manifest import Manifest
from logvine.storage.sstable import SSTable


L0_THRESHOLD = 2  # Example threshold value

logger = logging.getLogger(__name__)

class CompactionManager:
    
    def __init__(self):
        self.compaction_in_progress = False

    def _needs_compaction(self, manifest):

        return len(manifest.levels[0]) > L0_THRESHOLD
    
    def is_compaction_in_progress(self) -> bool:
        """Check if compaction is currently in progress."""
        return self.compaction_in_progress
    
    def may_start_compaction(self, manifest, storage_path: Path):
        """Determine if compaction can be started."""
        if self.is_compaction_in_progress():
            logger.info("Compaction is already in progress. Cannot start another.")
            return
        if not self._needs_compaction(manifest):
            logger.info("Compaction is not needed at this time.")
            return
        logger.info("Compaction can be started.")
        thread = threading.Thread(target=self.compact, args=(manifest, storage_path))
        thread.start()

    def _advance(self, heap, iterator, index):
        try:
            k, v = next(iterator)
            heapq.heappush(heap, (k, index, v, iterator))
        except StopIteration:
            pass

    def merge_sstables(self, sstable_infos) -> Iterator[tuple[bytes, bytes]]:
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


        for index, sstable_info in enumerate(sorted(sstable_infos, key=lambda x: x["path"], reverse=True)):
            logger.info(f"Reading SSTable for merge: {sstable_info['path']}")
            sstable = SSTable(Path(sstable_info["path"]), level=sstable_info["level"])

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
            # Get SSTables from level 0
            self.compaction_in_progress = True
            level0_sstables = manifest.levels[0]
            if not level0_sstables:
                logger.info("No SSTables to compact in level 0.")
                return
            
            # For simplicity, we will just merge all level 0 SSTables into one new SSTable
            new_sstable_path = Path(storage_path / f"sstable_compacted_{int(time())}.sst")
            merged_data = self.merge_sstables(level0_sstables)

            # Write merged data to new SSTable
            new_sstable = SSTable(new_sstable_path, level=1)
            asyncio.run(new_sstable.write(merged_data))

            # Update manifest: remove old SSTables and add new one to level 1
            new_sstable_index = new_sstable.index 

            new_sstable_metadata = {            
                    "path": str(new_sstable_path),
                    "level": 1,
                    "created_at_ms": int(time() * 1000),
                    "size_bytes": new_sstable_path.stat().st_size if new_sstable_path.exists() else 0,
                    "entry_count": len(new_sstable_index.keys()),
                    "min_key_hex": list(new_sstable_index.keys())[0].hex(),
                    "max_key_hex": list(new_sstable_index.keys())[-1].hex(),
                }
            manifest.add_sstable(level=1, metadata=new_sstable_metadata)

            for sstable_info in level0_sstables:
                manifest.remove_sstable(level=0, sstable_path=sstable_info["path"])
            
            # Remove old SSTable files from disk
            self._remove_old_sstable_files(level0_sstables)

            logger.info(f"Compaction completed. Created new SSTable at {new_sstable_path}")

        except Exception as e:
            logger.error(f"Error during compaction: {e}")
        finally:
            self.compaction_in_progress = False
    

    def _remove_old_sstable_files(self, sstable_infos):
        """Remove old SSTable files from disk after compaction."""
        for sstable_info in sstable_infos:
            try:
                Path(sstable_info["path"]).unlink()
                logger.info(f"Deleted old SSTable file: {sstable_info['path']}")
            except Exception as e:
                logger.error(f"Error deleting old SSTable file {sstable_info['path']}: {e}")