"""Controller module for logvine key-value store.
Handles network requests, request parsing,
routing in distributed scenarios, and concurrency control.
"""

import asyncio
import json
import logging
import threading
from pathlib import Path
from typing import Iterator, Optional, Dict, Any

from src.operations import OperationFactory, RequestOperation
from src.storage.engine import LSMStorageEngine
from src.storage.exceptions import BatchTooLargeException

logger = logging.getLogger(__name__)


class Controller:
    """Main controller for the log-structured key-value store.

    Handles network requests, request parsing, routing for distributed
    scenarios, and concurrency control at the API level.
    """

    def __init__(
        self,
        storage_path: str,
        node_id: Optional[int] = None,
        num_nodes: int = 1,
        max_workers: int = 16,
        is_leader: bool = False,
    ):
        """Initialize the controller.

        Args:
            storage_path: Path to the storage directory.
            node_id: This node's ID in a distributed setup (None for single-node).
            num_nodes: Total number of nodes in the cluster.
            max_workers: Maximum number of concurrent request handlers.
        """
        self.storage_path = storage_path
        self.node_id = node_id
        self.num_nodes = num_nodes
        self.max_workers = max_workers

        # Create storage engine instance (SimpleStorageEngine for now, full LSM later)
        self.storage_engine = LSMStorageEngine(storage_path)

        self._semaphore = asyncio.Semaphore(max_workers)  # Limits concurrent requests
        logger.info(f"Controller initialized with storage at {storage_path}, "
                    f"node_id={node_id}, num_nodes={num_nodes}, max_workers={max_workers}")

    async def handle_request(self, data: str, writer: asyncio.StreamWriter):
        """Handle an incoming network request.

        Implements concurrency control via semaphore and routes to appropriate handler.

        Args:
            data: Raw request data (JSON format).

        Returns:
            Response data (JSON format).
        """
        async with self._semaphore:
            try:
                operation = OperationFactory.from_json(data)

                result = operation.execute(self.storage_engine)
                if isinstance(result, Iterator):
                    await self._stream_response(operation, result, writer)

                else:
                    response = operation.to_response(result=result)
                    writer.write(response.encode() + b"\n")
                    await writer.drain()
            except KeyError:
                writer.write(operation.to_response(error="Key not found").encode() + b"\n")
                await writer.drain()
            except ValueError as e:
                error_resp = {"error": str(e)}
                writer.write(error_resp.encode() + b"\n")
                await writer.drain()
            except BatchTooLargeException as e:
                logger.warning(f"Very large batch encountered in request: {e}")
                error_resp = {
                    "status": "error",
                    "error": str(e),
                    "error_type": "batch_too_large"
                }
                writer.write(json.dumps(error_resp).encode() + b"\n")
                await writer.drain()
            except Exception as e:
                logger.error(f"Unexpected error handling request: {e}")
                error_resp = {"error": str(e)}
                writer.write(error_resp.encode() + b"\n")
                await writer.drain()
    

    async def _stream_response(self, operation, iterator, writer, chunk_size=100):
        try:
            chunk = []
            chunk_id = 1

            for key, value in iterator:
                chunk.append([key.decode(), value.decode()])

                if len(chunk) >= chunk_size:
                    writer.write(operation.to_stream_response(items=chunk, chunk_id=chunk_id).encode() + b"\n")
                    await writer.drain()
                    chunk = []
                    chunk_id += 1

            if chunk:
                writer.write(operation.to_stream_response(items=chunk, chunk_id=chunk_id).encode() + b"\n")
                await writer.drain()

            writer.write(operation.to_stream_done().encode() + b"\n")
            await writer.drain()

        except Exception as e:
            writer.write(operation.to_stream_error(str(e)).encode() + b"\n")
            await writer.drain()

