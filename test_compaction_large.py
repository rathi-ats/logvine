"""Test compaction with many keys like in test_flush_rotate_race.py."""

import tempfile
import threading
from pathlib import Path
from src.storage.engine import LSMStorageEngine


def test_compaction_large_dataset():
    """Test compaction with more keys to trigger the bug."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage_path = Path(tmpdir)
        engine = LSMStorageEngine(storage_path)
        
        # Write 400 keys (like in test_flush_rotate_race.py Test 1)
        print("Writing 400 keys...")
        written_keys = {}
        for i in range(400):
            key = f"key_{i:03d}".encode()
            value = f"value_{i:03d}".encode()
            engine.put(key, value)
            written_keys[key] = value
            if (i + 1) % 100 == 0:
                print(f"  {i + 1} keys written")
                engine.flush_all()
        
        # Final flush to ensure all data is persisted
        engine.flush_all()
        
        # Wait for any pending compaction
        import time
        time.sleep(2)
        
        # Check manifest before retrieval
        print("\nManifest before retrieval:")
        level0_count = len(engine.manifest.get_level_sstables(0))
        level1_count = len(engine.manifest.get_level_sstables(1))
        print(f"  Level 0: {level0_count} SSTables")
        print(f"  Level 1: {level1_count} SSTables")
        
        # Now check all keys are retrievable
        print(f"\nVerifying all {len(written_keys)} keys...")
        missing_keys = []
        found_keys = []
        for key, expected_value in written_keys.items():
            actual_value = engine.get(key)
            if actual_value != expected_value:
                missing_keys.append((key, expected_value, actual_value))
            else:
                found_keys.append(key)
        
        print(f"Found: {len(found_keys)}/{len(written_keys)} keys")
        
        if missing_keys:
            print(f"\n❌ Lost {len(missing_keys)} keys:")
            for key, expected, actual in missing_keys[:20]:  # Show first 20
                print(f"  - {key}: expected {expected}, got {actual}")
                # Try to find in which level this key should be
                for level in range(2):
                    sstables = engine.manifest.get_level_sstables(level)
                    for sstable_meta in sstables:
                        min_key = bytes.fromhex(sstable_meta["min_key_hex"])
                        max_key = bytes.fromhex(sstable_meta["max_key_hex"])
                        if min_key <= key <= max_key:
                            print(f"      -> Range indicates Level {level} SSTable: {sstable_meta['path']}")
        else:
            print(f"\n✅ All {len(written_keys)} keys present!")


if __name__ == "__main__":
    test_compaction_large_dataset()
