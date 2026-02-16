import random
from tests.benchmarks.benchmark_runner import BenchmarkRunner
from src.storage.engine import StorageEngine, LSMStorageEngine


NUM_KEYS = 400_000
VALUE_SIZE = 100  # bytes
RANGE_SIZE = 10000  # keys per scan


def make_key(i: int) -> bytes:
    return f"{i:010d}".encode()


def make_value() -> bytes:
    return b"x" * VALUE_SIZE


def preload(engine: StorageEngine):
    print("Preloading dataset...")
    keys = []
    values = []
    for i in range(NUM_KEYS):
        keys.append(make_key(i))
        values.append(make_value())
        if len(keys) == 1000:
            engine.batch_put(keys, values)
            keys.clear()
            values.clear()

    if keys:
        engine.batch_put(keys, values)
    
    # Calculate total bytes written for reporting
    total_bytes = NUM_KEYS * (10 + VALUE_SIZE)  # 10 bytes for key + value size
    print(f"Preloaded {NUM_KEYS} keys, total bytes: {total_bytes / (1024 * 1024):.2f} MB")

    print("Preload complete.")


def range_op():
    start = random.randint(0, NUM_KEYS - RANGE_SIZE - 1)
    end = start + RANGE_SIZE

    start_key = make_key(start)
    end_key = make_key(end)

    total_bytes = 0

    for k, v in engine.read_key_range(start_key, end_key):
        total_bytes += len(k) + len(v)
    
    # print(f"Scanned range {start_key} to {end_key}, total bytes: {total_bytes / (1024 * 1024):.2f} MB")
    return total_bytes
        


storagePath = "/tmp/benchmark/range/4"
engine = LSMStorageEngine(storagePath)

preload(engine)

runner = BenchmarkRunner(
    name=f"RangeScan_{RANGE_SIZE}",
    duration_seconds=10,
    warmup_seconds=3,
)

print(storagePath)
runner.run(range_op)



# 1. Memtable only (200K keys, ~20 MB, Memtable_max_size = 32MB) 1000 key_range
# /tmp/benchmark/range/1
# === Benchmark: RangeScan_1000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 730
# Bytes processed (MB): 76.58
# Throughput: 73.00 ops/s
# Data rate: 7.66 MB/s
# Latency (ms):
#   p50: 13.514
#   p95: 19.193
#   p99: 22.120

# 2. Memtable only (200K keys, ~20 MB, Memtable_max_size = 32MB) 10000 key_range
# === Benchmark: RangeScan_10000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 344
# Bytes processed (MB): 360.87
# Throughput: 34.40 ops/s
# Data rate: 36.09 MB/s
# Latency (ms):
#   p50: 28.548
#   p95: 36.784
#   p99: 42.062

# 3. Memtable + SSTable (400K keys, ~42 MB, Memtable_max_size = 32MB) 1000 key_range
# /tmp/benchmark/range/3
# === Benchmark: RangeScan_1000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 332
# Bytes processed (MB): 34.83
# Throughput: 33.20 ops/s
# Data rate: 3.48 MB/s
# Latency (ms):
#   p50: 29.336
#   p95: 44.316
#   p99: 53.721

# 4. Memtable + 1 SSTable (400K keys, ~42 MB, Memtable_max_size = 32MB) 10000 key_range
# /tmp/benchmark/range/4
# === Benchmark: RangeScan_10000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 248
# Bytes processed (MB): 260.16
# Throughput: 24.80 ops/s
# Data rate: 26.02 MB/s
# Latency (ms):
#   p50: 40.357
#   p95: 52.544
#   p99: 58.624

# 5. Memtable + 2 SSTable (800K keys, ~84 MB, Memtable_max_size = 32MB) 10000 key_range
# /tmp/benchmark/range/4
# === Benchmark: RangeScan_10000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 7
# Bytes processed (MB): 7.34
# Throughput: 0.70 ops/s
# Data rate: 0.73 MB/s
# Latency (ms):
#   p50: 1899.297
#   p95: 2283.369
#   p99: 2283.369

# 6. Memtable + 3rd SSTable triggers compactions (1200K keys, ~126 MB, Memtable_max_size = 32MB) 10000 key_range
# /tmp/benchmark/range/4
# === Benchmark: RangeScan_10000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 2
# Bytes processed (MB): 2.10
# Throughput: 0.20 ops/s
# Data rate: 0.21 MB/s
# Latency (ms):
#   p50: 6982.623
#   p95: 6982.623
#   p99: 6982.623

# 6. Memtable + L1 compacted SSTable (1600K keys, ~168 MB, Memtable_max_size = 32MB) 10000 key_range
# /tmp/benchmark/range/4
# === Benchmark: RangeScan_10000 ===
# Warmup: 3.00s, Run: 10.00s
# Error writing SSTable: dictionary changed size during iteration
# Error flushing MemTable: dictionary changed size during iteration
# Operations: 6
# Bytes processed (MB): 6.29
# Throughput: 0.60 ops/s
# Data rate: 0.63 MB/s
# Latency (ms):
#   p50: 1862.754
#   p95: 1893.613
#   p99: 1893.613


# 7. Memtable + L1 compacted SSTable + L0 SSTables (2000K keys, ~210 MB, Memtable_max_size = 32MB) 10000 key_range
# /tmp/benchmark/range/4
# === Benchmark: RangeScan_10000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 4
# Bytes processed (MB): 4.20
# Throughput: 0.40 ops/s
# Data rate: 0.42 MB/s
# Latency (ms):
#   p50: 2824.810
#   p95: 3188.239
#   p99: 3188.239


# 8. Memtable + L1 compacted SSTable + compaction triggered (2400K keys, ~252 MB, Memtable_max_size = 32MB) 10000 key_range
# /tmp/benchmark/range/4
# === Benchmark: RangeScan_10000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 1
# Bytes processed (MB): 1.05
# Throughput: 0.10 ops/s
# Data rate: 0.10 MB/s
# Latency (ms):
#   p50: 11887.413
#   p95: 11887.413
#   p99: 11887.413

# 9. Memtable + 2 L1 compacted SSTables (2800K keys, ~294 MB, Memtable_max_size = 32MB) 10000 key_range
# /tmp/benchmark/range/4
# === Benchmark: RangeScan_10000 ===
# Warmup: 3.00s, Run: 10.00s
# Operations: 5
# Bytes processed (MB): 5.25
# Throughput: 0.50 ops/s
# Data rate: 0.52 MB/s
# Latency (ms):
#   p50: 2096.217
#   p95: 2936.972
#   p99: 2936.972
