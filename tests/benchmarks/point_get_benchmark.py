from src.storage.engine import LSMStorageEngine
from tests.benchmarks.benchmark_runner import BenchmarkRunner


storage_path = "/tmp/benchmark/point_get/1"
engine = LSMStorageEngine(storage_path)

VALUE_SIZE = 100


def make_value(label: str) -> bytes:
    return label.encode().ljust(VALUE_SIZE, b"x")


def setup_memtable_key() -> bytes:
    key = b"memtable_hit_key"
    engine.put(key, make_value("memtable"))
    return key


def setup_sstable_key() -> bytes:
    key = b"sstable_hit_key"
    engine.put(key, make_value("sstable"))
    engine.memtable.rotate()
    engine.flush()
    return key


sstable_key = setup_sstable_key()
memtable_key = setup_memtable_key()
miss_key = b"definitely_missing_key"


def memtable_hit_op() -> int:
    value = engine.get(memtable_key)
    return len(memtable_key) + len(value)


def sstable_hit_op() -> int:
    value = engine.get(sstable_key)
    return len(sstable_key) + len(value)


def miss_op() -> int:
    try:
        engine.get(miss_key)
    except KeyError:
        pass
    return len(miss_key)


print(storage_path)

BenchmarkRunner("PointGet_MemTableHit", duration_seconds=10, warmup_seconds=2).run(
    memtable_hit_op
)
BenchmarkRunner("PointGet_SSTableHit", duration_seconds=10, warmup_seconds=2).run(
    sstable_hit_op
)
BenchmarkRunner("PointGet_Miss", duration_seconds=10, warmup_seconds=2).run(miss_op)

# -------- Results ------

# === Benchmark: PointGet_MemTableHit ===
# Warmup: 2.00s, Run: 10.00s
# Operations: 2895911
# Bytes processed (MB): 320.36
# Throughput: 289591.10 ops/s
# Data rate: 32.04 MB/s
# Latency (ms):
#   p50: 0.003
#   p95: 0.003
#   p99: 0.004

# === Benchmark: PointGet_SSTableHit ===
# Warmup: 2.00s, Run: 10.00s
# Operations: 89795
# Bytes processed (MB): 9.85
# Throughput: 8979.50 ops/s
# Data rate: 0.98 MB/s
# Latency (ms):
#   p50: 0.108
#   p95: 0.114
#   p99: 0.176

# === Benchmark: PointGet_Miss ===
# Warmup: 2.00s, Run: 10.00s
# Operations: 1516754
# Bytes processed (MB): 31.82
# Throughput: 151675.40 ops/s
# Data rate: 3.18 MB/s
# Latency (ms):
#   p50: 0.006
#   p95: 0.006
#   p99: 0.008

