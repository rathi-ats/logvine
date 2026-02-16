#  Setup
import argparse
import asyncio
import logging
import random
import time

from src.client import LogvineClient


async def demo(client: LogvineClient) -> None:
    await client.put("user:1", "Alice")
    print("Wrote user:1 = Alice")

    value = await client.get("user:1")
    print(f"Read user:1 = {value}")

    await client.batch_put(
        ["user:2", "user:3", "user:4"],
        ["Bob", "Charlie", "David"],
    )
    print("Batch wrote 3 users")

    results = await client.read_key_range_dict("user:1", "user:5")
    print(f"Range query results: {results}")

    await client.delete("user:1")
    print("Deleted user:1")

    try:
        value = await client.get("user:1")
        print(f"Read user:1 after delete = {value}")
    except RuntimeError as e:
        print(f"Expected error after delete: {e}")


async def writer_worker(worker_id: int, host: str, port: int, ops: int, keyspace: int):
    ok = errors = 0
    written_keys: list[str] = []

    async with LogvineClient(host, port) as client:
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

    return ok, errors, written_keys


async def reader_worker(worker_id: int, host: str, port: int, ops: int, keys: list[str]):
    ok = misses = errors = 0
    rng = random.Random(worker_id)

    async with LogvineClient(host, port) as client:
        for _ in range(ops):
            key = keys[rng.randrange(len(keys))]
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
    start = time.perf_counter()

    writer_tasks = [
        asyncio.create_task(writer_worker(i, host, port, write_ops, keyspace))
        for i in range(writers)
    ]
    writer_results = await asyncio.gather(*writer_tasks)

    all_written_keys: list[str] = []
    for _, _, keys in writer_results:
        all_written_keys.extend(keys)

    unique_keys = sorted(set(all_written_keys))

    if not unique_keys:
        print("No keys written; aborting read phase.")
        return

    reader_tasks = [
        asyncio.create_task(reader_worker(i, host, port, read_ops, unique_keys))
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="logvine client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--mode", choices=["demo", "concurrent"], default="demo")
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
        else:
            async with LogvineClient(args.host, args.port) as client:
                await demo(client)

    except Exception as e:
        logger.error(f"Client error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
