"""Debug compaction merge logic."""

import tempfile
from pathlib import Path
from src.storage.engine import LSMStorageEngine
import time


def test_compaction_merge_debug():
    """Test compaction merge to see what's happening."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage_path = Path(tmpdir)
        engine = LSMStorageEngine(storage_path)
        
        # Write batch 1
        print("\n=== Batch 1: Writing keys ===")
        batch1_keys = {}
        for i in range(10):
            key = f"key_0{i}".encode()
            value = f"batch1_value_{i}".encode()
            engine.put(key, value)
            batch1_keys[key] = value
            print(f"  Put {key} = {value}")
        
        engine.flush_all()
        print(f"After batch 1 flush:")
        print(f"  Level 0: {len(engine.manifest.get_level_sstables(0))} SSTables")
        print(f"  Level 1: {len(engine.manifest.get_level_sstables(1))} SSTables")
        
        # Verify batch 1 is retrievable
        print("\nVerifying batch 1 after flush:")
        for key, expected in batch1_keys.items():
            actual = engine.get(key)
            print(f"  {key} = {actual} {'✓' if actual == expected else '✗'}")
        
        # Write batch 2
        print("\n=== Batch 2: Writing keys ===")
        batch2_keys = {}
        for i in range(10):
            key = f"key_1{i}".encode()
            value = f"batch2_value_{i}".encode()
            engine.put(key, value)
            batch2_keys[key] = value
            print(f"  Put {key} = {value}")
        
        engine.flush_all()
        print(f"After batch 2 flush:")
        print(f"  Level 0: {len(engine.manifest.get_level_sstables(0))} SSTables")
        print(f"  Level 1: {len(engine.manifest.get_level_sstables(1))} SSTables")
        
        # Verify both batches are retrievable before compaction
        print("\nVerifying batch 1 and 2 before compaction:")
        for key, expected in {**batch1_keys, **batch2_keys}.items():
            actual = engine.get(key)
            print(f"  {key} = {actual} {'✓' if actual == expected else '✗'}")
        
        # Write batch 3 to trigger compaction (L0 has 2 now)
        print("\n=== Batch 3: Writing keys (will trigger compaction) ===")
        batch3_keys = {}
        for i in range(10):
            key = f"key_2{i}".encode()
            value = f"batch3_value_{i}".encode()
            engine.put(key, value)
            batch3_keys[key] = value
            print(f"  Put {key} = {value}")
        
        engine.flush_all()
        print(f"After batch 3 flush (before compaction):")
        print(f"  Level 0: {len(engine.manifest.get_level_sstables(0))} SSTables")
        print(f"  Level 1: {len(engine.manifest.get_level_sstables(1))} SSTables")
        
        # Wait for compaction
        print("\nWaiting for compaction...")
        time.sleep(2)
        
        print(f"After compaction:")
        level0 = engine.manifest.get_level_sstables(0)
        level1 = engine.manifest.get_level_sstables(1)
        print(f"  Level 0: {len(level0)} SSTables")
        for meta in level0:
            print(f"    - {Path(meta['path']).name}: {meta['entry_count']} entries")
        print(f"  Level 1: {len(level1)} SSTables")
        for meta in level1:
            print(f"    - {Path(meta['path']).name}: {meta['entry_count']} entries")
        
        # Now verify all three batches
        print("\nVerifying all batches after compaction:")
        all_keys = {**batch1_keys, **batch2_keys, **batch3_keys}
        missing = []
        for key, expected in all_keys.items():
            try:
                actual = engine.get(key)
                if actual == expected:
                    print(f"  ✓ {key}")
                else:
                    print(f"  ✗ {key}: expected {expected}, got {actual}")
                    missing.append(key)
            except KeyError:
                print(f"  ✗ {key}: NOT FOUND (KeyError)")
                missing.append(key)
        
        if missing:
            print(f"\n❌ Missing {len(missing)} keys after compaction!")
            print(f"Missing keys by batch:")
            batch1_missing = [k for k in missing if b'key_0' in k]
            batch2_missing = [k for k in missing if b'key_1' in k]
            batch3_missing = [k for k in missing if b'key_2' in k]
            print(f"  Batch 1: {len(batch1_missing)}/{len(batch1_keys)}")
            print(f"  Batch 2: {len(batch2_missing)}/{len(batch2_keys)}")
            print(f"  Batch 3: {len(batch3_missing)}/{len(batch3_keys)}")
        else:
            print(f"\n✅ All {len(all_keys)} keys present after compaction!")


if __name__ == "__main__":
    test_compaction_merge_debug()
