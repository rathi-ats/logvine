import os
from src.storage import engine
from tests.benchmarks.benchmark_runner import BenchmarkRunner


runner = BenchmarkRunner("WriteThroughput", duration_seconds=10)

storagePath = "/tmp/benchmark/batch_write/1"

storage_engine = engine.LSMStorageEngine(storagePath)

def generate_batch(n):
    """Generate a batch of n key-value pairs."""
    return ([f"key_{i}".encode() for i in range(n)], [f"value_{i}".encode() for i in range(n)])

def write_op():
    batch = generate_batch(1000)  # 100 KV pairs
    bytes_written = sum(len(k) + len(v) for k, v in zip(batch[0], batch[1]))
    storage_engine.batch_put(batch[0], batch[1])
    return bytes_written

print(storagePath)



runner.run(write_op)

# Output: 100 KV pairs written per operation, ~98 ops/s, ~0.13 MB/s throughput, with p50 latency around 10ms.
# === Benchmark: WriteThroughput ===
# Warmup: 2.00s, Run: 10.00s
# Operations: 980
# Throughput: 98.00 ops/s
# Data rate: 0.13 MB/s
# Latency (ms):
#   p50: 9.887
#   p95: 11.674
#   p99: 13.284


# Output: 1000 KV pairs written per operation, ~9 ops/s, ~0.14 MB/s throughput, with p50 latency around 10ms.
# === Benchmark: WriteThroughput ===
# Warmup: 2.00s, Run: 10.00s
# Operations: 90
# Throughput: 9.00 ops/s
# Data rate: 0.14 MB/s
# Latency (ms):
#   p50: 105.995
#   p95: 141.353
#   p99: 157.366


# ******************* After batching WAL writes together *******************

# Output: 100KV pairs written per operation, ~1999 ops/s, ~2.63 MB/s throughput, with p95 latency around 0.57ms.
# === Benchmark: WriteThroughput ===
# Warmup: 2.00s, Run: 10.00s
# Operations: 19991
# Bytes processed (MB): 26.31
# Throughput: 1999.10 ops/s
# Data rate: 2.63 MB/s
# Latency (ms):
#   p50: 0.481
#   p95: 0.571
#   p99: 0.695


# Output: 1000 KV pairs written per operation, ~264 ops/s, ~3.98 MB/s throughput, with p95 latency around 4.8ms.
# === Benchmark: WriteThroughput ===
# Warmup: 2.00s, Run: 10.00s
# Operations: 2646
# Bytes processed (MB): 39.82
# Throughput: 264.60 ops/s
# Data rate: 3.98 MB/s
# Latency (ms):
#   p50: 3.537
#   p95: 4.807
#   p99: 6.048

