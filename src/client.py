"""Client for logvine key-value store.

Connects to a logvine server and sends requests in JSON format.
"""

import asyncio
import argparse
import json
import logging
import random
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LogvineClient:
    """Async client for communicating with logvine server."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9999):
        """Initialize the client.

        Args:
            host: Server host address.
            port: Server port.
        """
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self._io_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to the server."""
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port
        )
        logger.info(f"Connected to logvine server at {self.host}:{self.port}")

    async def disconnect(self) -> None:
        """Disconnect from the server."""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        logger.info("Disconnected from server")

    async def _send_request(self, request_dict: dict) -> dict:
        """Send a request and receive a response.

        Args:
            request_dict: Request as a dictionary.

        Returns:
            Response as a dictionary.

        Raises:
            RuntimeError: If not connected.
        """
        if not self.writer:
            raise RuntimeError("Not connected to server")

        async with self._io_lock:
            request_json = json.dumps(request_dict)
            self.writer.write(request_json.encode() + b"\n")
            await self.writer.drain()

            response_line = await self.reader.readline()
            response_dict = json.loads(response_line.decode())

        return response_dict

    async def put(self, key: str, value: str) -> bool:
        """Write a key-value pair.

        Args:
            key: The key to write.
            value: The value to write.

        Returns:
            True if successful.

        Raises:
            RuntimeError: If the operation fails.
        """
        response = await self._send_request(
            {"operation": "put", "key": key, "value": value}
        )
        if "error" in response:
            raise RuntimeError(response["error"])
        return response.get("success", False)

    async def get(self, key: str) -> str:
        """Read a value by key.

        Args:
            key: The key to read.

        Returns:
            The value associated with the key.

        Raises:
            RuntimeError: If the key is not found or an error occurs.
        """
        response = await self._send_request(
            {"operation": "get", "key": key}
        )
        if "error" in response:
            raise RuntimeError(response["error"])
        return response.get("value")

    async def delete(self, key: str) -> bool:
        """Delete a key.

        Args:
            key: The key to delete.

        Returns:
            True if successful.

        Raises:
            RuntimeError: If the operation fails.
        """
        response = await self._send_request(
            {"operation": "delete", "key": key}
        )
        if "error" in response:
            raise RuntimeError(response["error"])
        return response.get("success", False)

    async def read_key_range(self, start_key: str, end_key: str) -> dict:
        """Read all key-value pairs in a range.

        Args:
            start_key: Inclusive start of the range.
            end_key: Exclusive end of the range.

        Returns:
            Dictionary of key-value pairs in the range.

        Raises:
            RuntimeError: If the operation fails.
        """
        response = await self._send_request(
            {"operation": "read_key_range", "start_key": start_key, "end_key": end_key}
        )
        if "error" in response:
            raise RuntimeError(response["error"])
        return response.get("results", {})

    async def batch_put(self, keys: list[str], values: list[str]) -> bool:
        """Write multiple key-value pairs atomically.

        Args:
            keys: List of keys to write.
            values: List of values to write (must match keys length).

        Returns:
            True if successful.

        Raises:
            RuntimeError: If the operation fails.
        """
        if len(keys) != len(values):
            raise ValueError("keys and values must have the same length")
        response = await self._send_request(
            {"operation": "batch_put", "keys": keys, "values": values}
        )
        if "error" in response:
            raise RuntimeError(response["error"])
        return response.get("success", False)


async def _writer_worker(
    worker_id: int,
    host: str,
    port: int,
    ops: int,
    keyspace: int,
) -> tuple[int, int, list[str]]:
    """Run writer workload on a dedicated connection."""
    client = LogvineClient(host=host, port=port)
    ok = 0
    errors = 0
    written_keys: list[str] = []
    await client.connect()
    try:
        for op_idx in range(ops):
            key_idx = (worker_id * ops + op_idx) % keyspace
            key = f"bench:key:{key_idx}"
            value = f"writer={worker_id}:op={op_idx}:ts={time.time_ns()}"
            try:
                await client.put(key, value)
                ok += 1
                written_keys.append(key)
            except Exception:
                errors += 1
    finally:
        await client.disconnect()
    return ok, errors, written_keys


async def _reader_worker(
    worker_id: int,
    host: str,
    port: int,
    ops: int,
    written_keys: list[str],
) -> tuple[int, int, int]:
    """Run reader workload on a dedicated connection."""
    client = LogvineClient(host=host, port=port)
    ok = 0
    misses = 0
    errors = 0
    rng = random.Random(worker_id)
    await client.connect()
    try:
        for _ in range(ops):
            key = written_keys[rng.randrange(len(written_keys))]
            try:
                await client.get(key)
                ok += 1
            except RuntimeError as exc:
                if "Key not found" in str(exc):
                    misses += 1
                else:
                    errors += 1
            except Exception:
                errors += 1
    finally:
        await client.disconnect()
    return ok, misses, errors


async def simulate_concurrent_read_writes(
    host: str,
    port: int,
    writers: int,
    readers: int,
    write_ops: int,
    read_ops: int,
    keyspace: int,
) -> None:
    """Simulate write-then-read validation with multiple connections."""
    start = time.perf_counter()
    writer_tasks = [
        asyncio.create_task(_writer_worker(i, host, port, write_ops, keyspace))
        for i in range(writers)
    ]
    writer_results = await asyncio.gather(*writer_tasks)

    all_written_keys: list[str] = []
    for _, _, keys in writer_results:
        all_written_keys.extend(keys)
    unique_written_keys = sorted(set(all_written_keys))

    if not unique_written_keys:
        elapsed = max(time.perf_counter() - start, 1e-9)
        print("Write-then-read simulation complete")
        print(
            f"writers={writers}, readers={readers}, keyspace={keyspace}, "
            f"write_ops_per_writer={write_ops}, read_ops_per_reader={read_ops}"
        )
        print(f"elapsed_seconds={elapsed:.3f}, throughput_ops_per_sec=0.0")
        print("writes_ok=0, write_errors=0, reads_ok=0, read_misses=0, read_errors=0")
        return

    reader_tasks = [
        asyncio.create_task(
            _reader_worker(i, host, port, read_ops, unique_written_keys)
        )
        for i in range(readers)
    ]
    reader_results = await asyncio.gather(*reader_tasks)
    elapsed = max(time.perf_counter() - start, 1e-9)

    writes_ok = sum(ok for ok, _, _ in writer_results)
    write_errors = sum(err for _, err, _ in writer_results)
    reads_ok = sum(ok for ok, _, _ in reader_results)
    read_misses = sum(m for _, m, _ in reader_results)
    read_errors = sum(err for _, _, err in reader_results)
    total_ops = writes_ok + write_errors + reads_ok + read_misses + read_errors
    throughput = total_ops / elapsed

    print("Write-then-read simulation complete")
    print(
        f"writers={writers}, readers={readers}, keyspace={keyspace}, "
        f"write_ops_per_writer={write_ops}, read_ops_per_reader={read_ops}"
    )
    print(f"elapsed_seconds={elapsed:.3f}, throughput_ops_per_sec={throughput:.1f}")
    print(
        f"writes_ok={writes_ok}, write_errors={write_errors}, "
        f"reads_ok={reads_ok}, read_misses={read_misses}, read_errors={read_errors}"
    )


async def main():
    """Run client demo or concurrent workload simulation."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="logvine client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--mode", choices=["demo", "concurrent"], default="concurrent")
    parser.add_argument("--writers", type=int, default=8)
    parser.add_argument("--readers", type=int, default=8)
    parser.add_argument("--write-ops", type=int, default=200)
    parser.add_argument("--read-ops", type=int, default=200)
    parser.add_argument("--keyspace", type=int, default=500)
    args = parser.parse_args()

    try:
        if args.mode == "concurrent":
            await simulate_concurrent_read_writes(
                host=args.host,
                port=args.port,
                writers=args.writers,
                readers=args.readers,
                write_ops=args.write_ops,
                read_ops=args.read_ops,
                keyspace=args.keyspace,
            )
            return

        client = LogvineClient(host=args.host, port=args.port)
        await client.connect()
        try:
            await client.put("user:1", "Alice")
            print("Wrote user:1 = Alice")

            value = await client.get("user:1")
            print(f"Read user:1 = {value}")

            await client.batch_put(
                ["user:2", "user:3", "user:4"],
                ["Bob", "Charlie", "David"]
            )
            print("Batch wrote 3 users")

            results = await client.read_key_range("user:1", "user:5")
            print(f"Range query results: {results}")

            await client.delete("user:1")
            print("Deleted user:1")

            value = await client.get("user:1")
            print(f"Read user:1 after deleting: {value}")
        finally:
            await client.disconnect()

    except Exception as e:
        logger.error(f"Client error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
