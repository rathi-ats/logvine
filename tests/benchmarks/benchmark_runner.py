import time
from typing import Callable, List


class BenchmarkRunner:
    def __init__(self, name: str, duration_seconds: float = 10.0, warmup_seconds: float = 2.0):
        self.name = name
        self.duration_seconds = duration_seconds
        self.warmup_seconds = warmup_seconds

        self.latencies_ns: List[int] = []
        self.total_ops = 0
        self.total_bytes = 0

    def _percentile(self, data: List[int], p: float) -> float:
        if not data:
            return 0.0
        data_sorted = sorted(data)
        k = int(len(data_sorted) * p / 100)
        k = min(k, len(data_sorted) - 1)
        return data_sorted[k] / 1e6  # ns → ms

    def run(self, operation: Callable[[], int]) -> None:
        print(f"=== Benchmark: {self.name} ===")
        print(f"Warmup: {self.warmup_seconds:.2f}s, Run: {self.duration_seconds:.2f}s")

        # Warmup phase
        end = time.perf_counter() + self.warmup_seconds
        while time.perf_counter() < end:
            operation()

        # Reset metrics
        self.latencies_ns.clear()
        self.total_ops = 0
        self.total_bytes = 0

        # Measurement phase
        end = time.perf_counter() + self.duration_seconds

        while time.perf_counter() < end:
            start = time.perf_counter_ns()
            bytes_processed = operation()
            duration = time.perf_counter_ns() - start

            self.latencies_ns.append(duration)
            self.total_ops += 1
            self.total_bytes += bytes_processed

        self._report()

    def _report(self) -> None:
        duration = self.duration_seconds
        ops_per_sec = self.total_ops / duration
        mb_per_sec = (self.total_bytes / (1024 * 1024)) / duration

        print(f"Operations: {self.total_ops}")
        print(f"Bytes processed (MB): {self.total_bytes / (1024 * 1024):.2f}")
        print(f"Throughput: {ops_per_sec:.2f} ops/s")
        print(f"Data rate: {mb_per_sec:.2f} MB/s")

        print("Latency (ms):")
        print(f"  p50: {self._percentile(self.latencies_ns, 50):.3f}")
        print(f"  p95: {self._percentile(self.latencies_ns, 95):.3f}")
        print(f"  p99: {self._percentile(self.latencies_ns, 99):.3f}")
