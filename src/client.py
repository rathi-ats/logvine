import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

logger = logging.getLogger(__name__)


class LogvineClient:
    """Async client for communicating with logvine server."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9999):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._io_lock = asyncio.Lock()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *_):
        await self.disconnect()

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        logger.info(f"Connected to logvine server at {self.host}:{self.port}")

    async def disconnect(self) -> None:
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.writer = None
            self.reader = None
        logger.info("Disconnected from server")

    async def _read_json_line(self) -> dict[str, Any]:
        if not self.reader:
            raise RuntimeError("Not connected to server")

        line = await self.reader.readline()
        if not line:
            raise RuntimeError("Server closed connection")
        return json.loads(line.decode())

    async def _send_request(self, request_dict: dict) -> dict[str, Any]:
        if not self.writer:
            raise RuntimeError("Not connected to server")

        async with self._io_lock:
            self.writer.write(json.dumps(request_dict).encode() + b"\n")
            await self.writer.drain()
            response = await self._read_json_line()

        if response.get("status") == "error" or "error" in response:
            raise RuntimeError(response.get("error", "Unknown error"))

        return response

    async def put(self, key: str, value: str) -> bool:
        resp = await self._send_request({"operation": "put", "key": key, "value": value})
        return resp.get("success", False)

    async def get(self, key: str) -> str:
        resp = await self._send_request({"operation": "get", "key": key})
        return resp.get("value")

    async def delete(self, key: str) -> bool:
        resp = await self._send_request({"operation": "delete", "key": key})
        return resp.get("success", False)

    async def batch_put(self, keys: list[str], values: list[str]) -> bool:
        if len(keys) != len(values):
            raise ValueError("keys and values must have the same length")

        resp = await self._send_request(
            {"operation": "batch_put", "keys": keys, "values": values}
        )
        return resp.get("success", False)


    async def read_key_range(
        self, start_key: str, end_key: str
    ) -> AsyncIterator[tuple[str, str]]:
        """Stream key-value pairs in range."""
        if not self.writer:
            raise RuntimeError("Not connected to server")

        async with self._io_lock:
            self.writer.write(
                json.dumps(
                    {
                        "operation": "read_key_range",
                        "start_key": start_key,
                        "end_key": end_key,
                    }
                ).encode()
                + b"\n"
            )
            await self.writer.drain()

            while True:
                msg = await self._read_json_line()

                if msg.get("stream_status") == "done":
                    break

                if msg.get("stream_status") == "terminated" or msg.get("status") == "error":
                    raise RuntimeError(msg.get("error", "Stream error"))

                for k, v in msg.get("items", []):
                    yield k, v


    async def read_key_range_dict(self, start_key: str, end_key: str) -> dict[str, str]:
        """Materialize range into dict (for demos/tests)."""
        result = {}
        async for k, v in self.read_key_range(start_key, end_key):
            result[k] = v
        return result




