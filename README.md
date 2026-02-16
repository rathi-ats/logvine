# logvine

A log-structured persistent key-value store in Python.

## Overview

`logvine` implements a simplified LSM-style storage engine with these components:

- `MemTable`: in-memory write buffer with read/write locking
- `WAL`: write-ahead log with replay and truncation after writing to SSTable
- `SSTable`: immutable sorted on-disk tables
- `Manifest`: persisted metadata for SSTables by level
- `SSTableManager`: SSTable selection, lookup, and metadata helpers
- `CompactionManager`: level-0 compaction into higher levels

Core operations:

- `put`
- `get`
- `delete`
- `batch_put`
- `read_key_range` (streaming from server)

## Repository Layout

```text
src/
  client.py
  controller.py
  demo.py
  server.py
  operations.py
  config.py
  storage/
    engine.py
    memtable.py
    wal.py
    sstable.py
    sstable_manager.py
    manifest.py
    compaction.py
    exceptions.py
tests/
```

## Requirements

- Python `>=3.9`

## Configuration

Central runtime settings live in `src/config.py` and are controlled by env vars:

- `LOGVINE_MEMTABLE_MAX_SIZE`
  - Max MemTable size in bytes before flush/rotation logic
  - Default: `32000000`
- `LOGVINE_MAX_WORKERS`
  - Controller request concurrency semaphore size
  - Default: `16`

Example:

```bash
export LOGVINE_MEMTABLE_MAX_SIZE=32000000
export LOGVINE_MAX_WORKERS=16
```

## Running the Server

```bash
python3 src/server.py /tmp/logvine
```

Defaults:

- Host: `127.0.0.1`
- Port: `9000`

## Client and Demo

`src/client.py` provides `LogvineClient`, an async TCP client with support for streaming `read_key_range` responses.
Note: client/demo default port is `9999`, so pass `--port 9000` when using server defaults.

Run demo mode:

```bash
python3 src/demo.py --mode demo --host 127.0.0.1 --port 9000
```

Run concurrent write-then-read simulation:

```bash
python3 src/demo.py --mode concurrent --host 127.0.0.1 --port 9000 \
  --writers 8 --readers 8 --write-ops 200 --read-ops 200 --keyspace 500
```

## JSON Protocol (line-delimited over TCP)

Examples:

```json
{"operation":"put","key":"k1","value":"v1"}
{"operation":"get","key":"k1"}
{"operation":"delete","key":"k1"}
{"operation":"batch_put","keys":["k1","k2"],"values":["v1","v2"]}
{"operation":"read_key_range","start_key":"k1","end_key":"k9"}
```

`read_key_range` responses are streamed in chunks and terminated by a done message.

## Running Tests

If `pytest` is installed:

```bash
python3 -m pytest -q tests
```

Or run specific files:

```bash
python3 -m pytest -q tests/test_wal.py
python3 -m pytest -q tests/test_integration.py
python3 -m pytest -q tests/test_sstable_manager.py
```

## Benchmarks

Current benchmark scripts live under `tests/benchmarks/`:

- `benchmark_runner.py`: shared reporting ops/s, MB/s, and p50/p95/p99 latency.
- `write_benchmark.py`: batch write throughput/latency benchmark on `LSMStorageEngine.batch_put`.
- `mixed_rw_benchmark.py`: mixed workload benchmark (80% reads / 20% writes).
- `point_get_benchmark.py`: point `get` benchmark across MemTable hit, SSTable hit, and miss paths.
- `range_benchmark.py`: range-scan throughput/latency benchmark across growing datasets.

Run examples:

```bash
python3 tests/benchmarks/write_benchmark.py
python3 tests/benchmarks/mixed_rw_benchmark.py
python3 tests/benchmarks/point_get_benchmark.py
cd tests/benchmarks && python3 range_benchmark.py
```

These scripts are exploratory and intended for local performance investigation (not CI pass/fail gates).

## Notes

- Keys are handled as byte sequences internally and compared lexicographically.
- WAL appends are fsynced for durability.
- Manifest updates are atomic (temp file + replace + fsync).
- Current compaction strategy only supports level-0 merges.

## Future Work
- Support compaction at higher levels 
- Extend for a multi-node distributed setup for better fault tolerance (through data replication on at least one other node) and horizontal scaling (keys are distributed based on range-based partitioning for efficient range scans).
