"""Test to reproduce and debug the compaction data loss bug."""

import tempfile
from pathlib import Path
from src.storage.engine import LSMStorageEngine


def test_compaction_data_loss():
    """Test that compaction doesn't lose data when merging L0 to L1."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage_path = Path(tmpdir)
        engine = LSMStorageEngine(storage_path)
        
        # Write 3 batches of keys to trigger multiple L0 SSTables
        batch_size = 20
        written_keys = {}
        
        # Batch 1: Write and flush
        print("\n=== Batch 1: Writing keys 0-19 ===")
        for i in range(batch_size):
            key = f"batch1_key{i:02d}".encode()
            value = f"batch1_value{i:02d}".encode()
            engine.put(key, value)
            written_keys[key] = value
        engine.flush_all()
        
        # Check batch 1 is retrievable before any other writes
        print("Checking batch 1 retrieval after flush...")
        for key in list(written_keys.keys())[:5]:  # Check first 5
            value = engine.get(key)
            assert value == written_keys[key], f"Lost {key} after batch 1 flush"
            print(f"  ✓ {key} = {value}")
        
        # Batch 2: Write and flush
        print("\n=== Batch 2: Writing keys 20-39 ===")
        for i in range(batch_size):
            key = f"batch2_key{i:02d}".encode()
            value = f"batch2_value{i:02d}".encode()
            engine.put(key, value)
            written_keys[key] = value
        engine.flush_all()
        
        # Check both batch 1 and 2 are retrievable
        print("Checking batch 1 and 2 retrieval after batch 2 flush...")
        for key in list(written_keys.keys())[:10]:
            value = engine.get(key)
            assert value == written_keys[key], f"Lost {key} after batch 2 flush"
            print(f"  ✓ {key} = {value}")
        
        # Batch 3: Write to trigger compaction (L0 has 2 SSTables already)
        print("\n=== Batch 3: Writing keys 40-59 (will trigger compaction) ===")
        for i in range(batch_size):
            key = f"batch3_key{i:02d}".encode()
            value = f"batch3_value{i:02d}".encode()
            engine.put(key, value)
            written_keys[key] = value
        engine.flush_all()
        
        # Wait a bit for compaction to complete
        import time
        time.sleep(1)
        
        # Now check all keys are still retrievable after compaction
        print(f"\n=== Verifying all {len(written_keys)} keys after compaction ===")
        missing_keys = []
        for key, expected_value in written_keys.items():
            actual_value = engine.get(key)
            if actual_value != expected_value:
                missing_keys.append((key, expected_value, actual_value))
            else:
                print(f"  ✓ {key}")
        
        if missing_keys:
            print(f"\n❌ Lost {len(missing_keys)} keys during compaction:")
            for key, expected, actual in missing_keys[:10]:  # Show first 10
                print(f"  - {key}: expected {expected}, got {actual}")
            
            # Print manifest to debug
            print("\nManifest state:")
            for level in range(2):
                sstables = engine.manifest.get_level_sstables(level)
                print(f"  Level {level}: {len(sstables)} SSTables")
                for sstable in sstables:
                    print(f"    - {sstable['path']}: {sstable['entry_count']} entries")
        else:
            print(f"\n✅ All {len(written_keys)} keys present after compaction!")


if __name__ == "__main__":
    test_compaction_data_loss()
