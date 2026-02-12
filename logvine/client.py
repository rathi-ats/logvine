"""Client for logvine key-value store.

Connects to a logvine server and sends requests in JSON format.
"""

import asyncio
import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LogvineClient:
    """Async client for communicating with logvine server."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9000):
        """Initialize the client.

        Args:
            host: Server host address.
            port: Server port.
        """
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None

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

    async def flush(self) -> bool:
        """Flush the MemTable to SSTable.

        Returns:
            True if successful.

        Raises:
            RuntimeError: If the operation fails.
        """
        response = await self._send_request({"operation": "flush"})
        if "error" in response:
            raise RuntimeError(response["error"])
        return response.get("success", False)


async def main():
    """Example usage of the client."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    client = LogvineClient()

    try:
        await client.connect()

        # Example: Single put/get
        await client.put("user:1", "Alice")
        print("Wrote user:1 = Alice")

        value = await client.get("user:1")
        print(f"Read user:1 = {value}")

        # Example: Batch put
        await client.batch_put(
            ["user:2", "user:3", "user:4"],
            ["Bob", "Charlie", "David"]
        )
        print("Batch wrote 3 users")

        # Example: Range query
        results = await client.read_key_range("user:1", "user:5")
        print(f"Range query results: {results}")

        # Example: Delete
        await client.delete("user:1")
        print("Deleted user:1")

    except Exception as e:
        logger.error(f"Client error: {e}")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
