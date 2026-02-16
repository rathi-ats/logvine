import random

from src.storage.engine import LSMStorageEngine
from tests.benchmarks.benchmark_runner import BenchmarkRunner


runner = BenchmarkRunner("MixedReadWrite_20_80", duration_seconds=10, warmup_seconds=2)

storage_path = "/tmp/benchmark/mixed_rw/3"
engine = LSMStorageEngine(storage_path)

READ_PERCENT = 20
WRITE_PERCENT = 80
KEYSPACE = 50_000
VALUE_SIZE = 100


def make_key(i: int) -> bytes:
    return f"{i:010d}".encode()


def make_value(i: int) -> bytes:
    return f"v{i:010d}".encode().ljust(VALUE_SIZE, b"x")


def preload():
    print("Preloading dataset for mixed R/W benchmark...")
    keys = []
    values = []
    for i in range(KEYSPACE):
        keys.append(make_key(i))
        values.append(make_value(i))
        if len(keys) == 1000:
            engine.batch_put(keys, values)
            keys.clear()
            values.clear()
    if keys:
        engine.batch_put(keys, values)
    print(f"Preloaded {KEYSPACE} keys")


write_counter = KEYSPACE


def mixed_op() -> int:
    """Execute one read or write operation and return bytes processed."""
    global write_counter

    if random.randint(1, 100) <= READ_PERCENT:
        key = make_key(random.randint(0, KEYSPACE - 1))
        value = engine.get(key)
        return len(key) + len(value)

    key = make_key(write_counter)
    value = make_value(write_counter)
    engine.put(key, value)
    write_counter += 1
    return len(key) + len(value)


print(storage_path)
preload()
runner.run(mixed_op)

# --------- Results ---------


# === Benchmark: MixedReadWrite_80_20 ===
# /tmp/benchmark/mixed_rw/1
# Preloading dataset for mixed R/W benchmark...
# Preloaded 50000 keys

# Warmup: 2.00s, Run: 10.00s
# Operations: 456966
# Bytes processed (MB): 47.94
# Throughput: 45696.60 ops/s
# Data rate: 4.79 MB/s
# Latency (ms):
#   p50: 0.006
#   p95: 0.078
#   p99: 0.114

# ---------

# === Benchmark: MixedReadWrite_50_50 ===

# /tmp/benchmark/mixed_rw/2
# Preloading dataset for mixed R/W benchmark...
# Preloaded 50000 keys

# Warmup: 2.00s, Run: 10.00s
# Operations: 221941
# Bytes processed (MB): 23.28
# Throughput: 22194.10 ops/s
# Data rate: 2.33 MB/s
# Latency (ms):
#   p50: 0.044
#   p95: 0.098
#   p99: 0.143


# ---------

# === Benchmark: MixedReadWrite_20_80 ===

# /tmp/benchmark/mixed_rw/3
# Preloading dataset for mixed R/W benchmark...
# Preloaded 50000 keys

# Warmup: 2.00s, Run: 10.00s
# Operations: 140430
# Bytes processed (MB): 14.73
# Throughput: 14043.00 ops/s
# Data rate: 1.47 MB/s
# Latency (ms):
#   p50: 0.074
#   p95: 0.120
#   p99: 0.161