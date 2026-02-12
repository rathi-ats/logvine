"""Controller module for logvine key-value store.

Manages the overall write and read operations, coordinating between
MemTable, WAL, and SSTables. Handles network requests, request parsing,
routing in distributed scenarios, and concurrency control.
"""

import asyncio
import json
import logging
import threading
from pathlib import Path
from typing import Optional, Dict, Any

from logvine.operations import OperationFactory, RequestOperation
from logvine.storage.engine import LSMStorageEngine

logger = logging.getLogger(__name__)


class PartitionKey:
    """Routing information for distributed scenarios."""

    def __init__(self, key: str, node_id: Optional[int] = None):
        """Initialize partition key.

        Args:
            key: The data key.
            node_id: Node ID if this controller is part of a distributed system.
        """
        self.key = key
        self.node_id = node_id

    @staticmethod
    def hash_key(key: str, num_partitions: int = 1) -> int:
        """Hash a key to determine partition.

        Args:
            key: The key to hash.
            num_partitions: Number of partitions in the cluster.

        Returns:
            Partition ID (0 to num_partitions - 1).
        """
        return hash(key) % num_partitions

    def should_handle(self, num_nodes: int = 1) -> bool:
        """Check if this node should handle the key (for distributed routing).

        Args:
            num_nodes: Total number of nodes in the cluster.

        Returns:
            True if this node should handle the key.
        """
        if self.node_id is None:
            return True
        target_partition = self.hash_key(self.key, num_nodes)
        return target_partition == self.node_id


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

        # Concurrency control
        self._write_lock = threading.RLock()  # Protects writes and flushes
        self._semaphore = asyncio.Semaphore(max_workers)  # Limits concurrent requests

        # Routing table for distributed scenarios
        self._routing_table: Dict[int, str] = {}  # node_id -> host:port
        if node_id is not None:
            logger.info(
                f"Controller initialized as node {node_id}/{num_nodes}. "
                f"Hash-based partitioning enabled."
            )

    def put(self, key: str, value: bytes) -> None:
        """Write a key-value pair.

        Thread-safe with write lock protection.

        Args:
            key: The key to write.
            value: The value to write.

        Raises:
            ValueError: If this node should not handle this key (distributed).
        """
        partition_key = PartitionKey(key, self.node_id)
        if not partition_key.should_handle(self.num_nodes):
            raise ValueError(
                f"Key {key} belongs to partition {self.node_id}, "
                f"use routing table"
            )

        with self._write_lock:
            # Durably record the write in the WAL before applying to MemTable
            pass
        raise NotImplementedError

    def delete(self, key: str) -> None:
        """Mark a key as deleted (tombstone).

        Thread-safe with write lock protection.

        Args:
            key: The key to delete.

        Raises:
            ValueError: If this node should not handle this key (distributed).
        """
        partition_key = PartitionKey(key, self.node_id)
        if not partition_key.should_handle(self.num_nodes):
            raise ValueError(f"Key {key} belongs to a different partition")

        with self._write_lock:
            raise NotImplementedError

    def read_key_range(self, start_key: str, end_key: str) -> Dict[str, bytes]:
        """Read all key-value pairs where start_key <= key < end_key.

        Read-optimized without acquiring write lock.

        Args:
            start_key: Inclusive start of the range.
            end_key: Exclusive end of the range.

        Returns:
            Dictionary of key-value pairs in the range.

        Raises:
            ValueError: If keys in range span multiple partitions (distributed).
        """
        # In distributed mode, range queries must not cross partition boundaries
        if self.node_id is not None:
            start_partition = PartitionKey.hash_key(start_key, self.num_nodes)
            end_partition = PartitionKey.hash_key(end_key, self.num_nodes)
            if start_partition != end_partition:
                raise ValueError(
                    f"ReadKeyRange from {start_key} to {end_key} spans multiple partitions"
                )
            if start_partition != self.node_id:
                raise ValueError(
                    f"Range {start_key}-{end_key} belongs to a different partition"
                )

        raise NotImplementedError

    def batch_put(self, keys: list[str], values: list[bytes]) -> None:
        """Write multiple key-value pairs atomically.

        Thread-safe with write lock protection. All puts are applied together
        as a single atomic transaction.

        Args:
            keys: List of keys to write.
            values: List of values to write (must match keys length).

        Raises:
            ValueError: If keys and values have different lengths or if any key
                        belongs to a different partition (distributed).
            RuntimeError: If the batch operation fails.
        """
        if len(keys) != len(values):
            raise ValueError("keys and values must have the same length")

        # Validate all keys belong to this node
        if self.node_id is not None:
            for key in keys:
                partition_key = PartitionKey(key, self.node_id)
                if not partition_key.should_handle(self.num_nodes):
                    raise ValueError(
                        f"Key {key} belongs to a different partition"
                    )

        with self._write_lock:
            raise NotImplementedError

    def flush(self) -> None:
        """Flush MemTable to SSTable.

        Thread-safe with write lock protection.
        """
        with self._write_lock:
            raise NotImplementedError

    def register_node(self, node_id: int, address: str) -> None:
        """Register a remote node for distributed routing.

        Args:
            node_id: The remote node's ID.
            address: Network address (host:port) of the node.
        """
        self._routing_table[node_id] = address
        logger.info(f"Registered node {node_id} at {address}")

    def get_node_address(self, key: str) -> Optional[str]:
        """Get the address of the node responsible for a key.

        Args:
            key: The key to route.

        Returns:
            Network address (host:port) or None if local.
        """
        partition_key = PartitionKey(key, self.node_id)
        if partition_key.should_handle(self.num_nodes):
            return None  # Local node
        target_partition = PartitionKey.hash_key(key, self.num_nodes)
        return self._routing_table.get(target_partition)

    async def handle_request(self, data: str) -> str:
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

                # Handle routing for distributed systems
                if self.node_id is not None and hasattr(operation, "key"):
                    target_address = self.get_node_address(operation.key)
                    if target_address:
                        logger.info(
                            f"Routing {operation.__class__.__name__} for key "
                            f"{operation.key} to {target_address}"
                        )
                        return await self._forward_to_node(
                            target_address, operation
                        )

                # Process locally
                return self._execute_request(operation)
            except ValueError as e:
                error_resp = {"error": str(e)}
                return json.dumps(error_resp)
            except Exception as e:
                logger.error(f"Unexpected error handling request: {e}")
                error_resp = {"error": "Internal server error"}
                return json.dumps(error_resp)

    def _execute_request(self, operation: RequestOperation) -> str:
        """Execute a request locally using polymorphism.

        Args:
            operation: The operation to execute.

        Returns:
            JSON response.
        """
        try:
            result = operation.execute(self.storage_engine)
            return operation.to_response(result=result)
        except KeyError:
            return operation.to_response(error="Key not found")
        except Exception as e:
            logger.error(f"Error executing request: {e}")
            return operation.to_response(error=str(e))

    async def _forward_to_node(self, address: str, operation: RequestOperation) -> str:
        """Forward a request to another node in the cluster.

        Args:
            address: Target node address (host:port).
            operation: The operation to forward.

        Returns:
            Response from the remote node.
        """
        host, port = address.split(":")
        try:
            # Open connection to remote node
            reader, writer = await asyncio.open_connection(host, int(port))
            
            # Reconstruct JSON request from operation
            request_data = self._operation_to_json(operation)
            
            writer.write(request_data.encode() + b"\n")
            await writer.drain()

            response_data = await reader.readline()
            writer.close()
            await writer.wait_closed()

            return response_data.decode()
        except Exception as e:
            logger.error(f"Error forwarding to {address}: {e}")
            return json.dumps({"error": f"Failed to reach node at {address}"})

    def _operation_to_json(self, operation: RequestOperation) -> str:
        """Convert an operation back to JSON for forwarding.

        Args:
            operation: The operation to serialize.

        Returns:
            JSON string representation of the operation.
        """
        if hasattr(operation, "key") and hasattr(operation, "value"):
            # PutOperation
            return json.dumps({
                "operation": "put",
                "key": operation.key,
                "value": operation.value.decode("utf-8") if isinstance(operation.value, bytes) else operation.value
            })
        elif hasattr(operation, "key"):
            # GetOperation or DeleteOperation
            op_name = "get" if operation.__class__.__name__ == "GetOperation" else "delete"
            return json.dumps({"operation": op_name, "key": operation.key})
        elif hasattr(operation, "start_key"):
            # ReadKeyRangeOperation
            return json.dumps({
                "operation": "read_key_range",
                "start_key": operation.start_key,
                "end_key": operation.end_key
            })
        elif hasattr(operation, "keys"):
            # BatchPutOperation
            return json.dumps({
                "operation": "batch_put",
                "keys": operation.keys,
                "values": [v.decode("utf-8") if isinstance(v, bytes) else v for v in operation.values]
            })
        else:
            # FlushOperation
            return json.dumps({"operation": "flush"})
