## Concurrency Bug Fix Summary

### Original Issue
**Critical concurrency bug**: Overlapping async flush/rotate operations could overwrite frozen state and drop writes under load.

### Root Cause Analysis

We identified and fixed **TWO separate issues**:

#### 1. **Fixed: Flush/Rotate Race Condition**
**Problem**:
- Single `_frozen` dict could be overwritten while flush was reading it
- Multiple concurrent rotates would lose intermediate data

**Solution**:
- Implemented **frozen queue**: list of (frozen_data, wal_offset) tuples
- Each rotate appends to queue instead of overwriting
- Each flush processes oldest buffer in queue
- Flushes are serialized with an engine-level lock so two flush threads cannot
  read the same queue head and then clear different buffers

#### 2. **Fixed: Interleaved Put/Rotate Race**
**Problem**:
- `put()` would:
  1. Call `memtable.put()` (acquire lock, modify _data, release lock)
  2. Call `set_max_wal_offset()` (acquire lock again, release)
  3. Call `is_full()` (NO LOCK)
  4. Call `rotate()` (acquire lock)
- Between step 3 and 4, another thread could rotate, leaving new puts in active _data that never get frozen

**Solution**:
- Hold write lock for ENTIRE put() operation
- New internal methods: `_put_unlocked()`, `_rotate_unlocked()`
- Engine.put() acquires write lock once and holds it for:
  - put_unlocked()
  - offset update
  - is_full() check
  - rotate_unlocked() if needed
- Same pattern for batch_put()

### Changes Made

**MemTable (src/storage/memtable.py)**:
- Replaced single `_frozen` dict with `_frozen_queue` list
- Added `_put_unlocked()` - put without acquiring lock
- Added `_rotate_unlocked()` - rotate without acquiring lock
- Added `get_frozen_queue_depth()` - return queue length
- Updated `clear_frozen()` to pop from queue
- Updated all methods accessing frozen state to handle queue

**Engine (src/storage/engine.py)**:
- Updated `put()` to hold write lock for entire operation
- Updated `batch_put()` similarly
- Updated `flush()` to work with frozen queue under a flush lock
- Added `flush_all()` to flush all queued buffers

### Testing
- Focused race and MemTable/concurrency tests pass
- Created `test_flush_rotate_race.py` with 2 test scenarios demonstrating the fix
- Frozen queue prevents data overwrite race
- Serialized flushes prevent queue-head races between concurrent flush threads
- Proper locking prevents interleaved rotate race

### Remaining Issues Found (Separate Bug)
During investigation, we discovered a **separate compaction bug**:
- When compaction merges L0 SSTables, some data is lost
- Example: thread_0 keys (first batch written) disappear after compaction
- This is NOT part of the flush/rotate race - it's a different issue in the compaction logic
- All written data reaches SSTable, but compaction incorrectly deletes some entries

### Conclusion
- **Fixed**: Critical flush/rotate race that could drop writes
- **Fixed**: Interleaved put/rotate race with locked coordination
- **Found**: Separate compaction deletion bug (out of scope for this fix)

The frozen queue + proper locking approach successfully prevents the original concurrency issues and allows safe concurrent access to the memtable.
