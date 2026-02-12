"""Request operation classes using polymorphic design.

Each operation type is represented by its own class that handles
parsing, validation, and execution.
"""

import json
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Optional

from logvine.storage.engine import StorageEngine

logger = logging.getLogger(__name__)


def _ensure_bytes(value: Any) -> bytes:
    """Convert string or bytes to bytes.

    Args:
        value: The value to convert (string or bytes).

    Returns:
        The value as bytes.

    Raises:
        ValueError: If value is neither string nor bytes.
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ValueError(f"Expected str or bytes, got {type(value).__name__}")


class RequestOperation(ABC):
    """Abstract base class for all request operations."""

    @abstractmethod
    def validate(self) -> None:
        """Validate the operation parameters.

        Raises:
            ValueError: If validation fails.
        """
        pass

    @abstractmethod
    def execute(self, storage_engine: StorageEngine) -> Any:
        """Execute the operation on the storage engine.

        Args:
            storage_engine: The storage engine instance to execute on.

        Returns:
            The result of the operation.
        """
        pass

    @abstractmethod
    def to_response(self, result: Any = None, error: Optional[str] = None) -> str:
        """Format the result as a JSON response.

        Args:
            result: The result of the operation.
            error: Error message if the operation failed.

        Returns:
            JSON-formatted response string.
        """
        pass


class PutOperation(RequestOperation):
    """Operation for writing a key-value pair."""

    def __init__(self, key: bytes, value: bytes):
        """Initialize PUT operation.

        Args:
            key: The key to write.
            value: The value to write.
        """
        self.key = key
        self.value = value

    def validate(self) -> None:
        """Validate that key and value are provided."""
        if not self.key:
            raise ValueError("PUT requires 'key'")
        if self.value is None:
            raise ValueError("PUT requires 'value'")

    def execute(self, storage_engine: StorageEngine) -> Any:
        """Execute PUT operation."""
        storage_engine.put(self.key, self.value)
        return "OK"

    def to_response(self, result: Any = None, error: Optional[str] = None) -> str:
        """Format PUT response."""
        response = {
            "operation": "put",
            "key": self.key.decode("utf-8", errors="replace"),
        }
        if error:
            response["error"] = error
        else:
            response["success"] = True
            response["value"] = result
        return json.dumps(response)

    @staticmethod
    def parse(data: Dict[str, Any]) -> "PutOperation":
        """Parse PUT operation from JSON data.

        Args:
            data: Parsed JSON dictionary.

        Returns:
            PutOperation instance.

        Raises:
            ValueError: If required fields are missing.
        """
        key = data.get("key")
        value = data.get("value")

        if not key:
            raise ValueError("PUT requires 'key'")
        if value is None:
            raise ValueError("PUT requires 'value'")

        key = _ensure_bytes(key)
        value = _ensure_bytes(value)

        return PutOperation(key, value)


class GetOperation(RequestOperation):
    """Operation for reading a single key-value pair."""

    def __init__(self, key: bytes):
        """Initialize GET operation.

        Args:
            key: The key to read.
        """
        self.key = key

    def validate(self) -> None:
        """Validate that key is provided."""
        if not self.key:
            raise ValueError("GET requires 'key'")

    def execute(self, storage_engine: StorageEngine) -> Any:
        """Execute GET operation."""
        return storage_engine.get(self.key)

    def to_response(self, result: Any = None, error: Optional[str] = None) -> str:
        """Format GET response."""
        response = {"operation": "get", "key": self.key.decode("utf-8", errors="replace")}
        if error:
            response["error"] = error
        else:
            response["success"] = True
            if isinstance(result, bytes):
                response["value"] = result.decode("utf-8", errors="replace")
            else:
                response["value"] = result
        return json.dumps(response)

    @staticmethod
    def parse(data: Dict[str, Any]) -> "GetOperation":
        """Parse GET operation from JSON data.

        Args:
            data: Parsed JSON dictionary.

        Returns:
            GetOperation instance.

        Raises:
            ValueError: If required fields are missing.
        """
        key = data.get("key")
        if not key:
            raise ValueError("GET requires 'key'")
        key = _ensure_bytes(key)
        return GetOperation(key)


class DeleteOperation(RequestOperation):
    """Operation for deleting a key (tombstone)."""

    def __init__(self, key: bytes):
        """Initialize DELETE operation.

        Args:
            key: The key to delete.
        """
        self.key = key

    def validate(self) -> None:
        """Validate that key is provided."""
        if not self.key:
            raise ValueError("DELETE requires 'key'")

    def execute(self, storage_engine: StorageEngine) -> Any:
        """Execute DELETE operation."""
        storage_engine.delete(self.key)
        return "OK"

    def to_response(self, result: Any = None, error: Optional[str] = None) -> str:
        """Format DELETE response."""
        response = {
            "operation": "delete",
            "key": self.key.decode("utf-8", errors="replace"),
        }
        if error:
            response["error"] = error
        else:
            response["success"] = True
            response["value"] = result
        return json.dumps(response)

    @staticmethod
    def parse(data: Dict[str, Any]) -> "DeleteOperation":
        """Parse DELETE operation from JSON data.

        Args:
            data: Parsed JSON dictionary.

        Returns:
            DeleteOperation instance.

        Raises:
            ValueError: If required fields are missing.
        """
        key = data.get("key")
        if not key:
            raise ValueError("DELETE requires 'key'")
        key = _ensure_bytes(key)
        return DeleteOperation(key)


class ReadKeyRangeOperation(RequestOperation):
    """Operation for reading a range of keys."""

    def __init__(self, start_key: bytes, end_key: bytes):
        """Initialize READ_KEY_RANGE operation.

        Args:
            start_key: Inclusive start of the range.
            end_key: Inclusive end of the range.
        """
        self.start_key = start_key
        self.end_key = end_key

    def validate(self) -> None:
        """Validate that start_key and end_key are provided."""
        if not self.start_key or not self.end_key:
            raise ValueError("READ_KEY_RANGE requires 'start_key' and 'end_key'")

    def execute(self, storage_engine: StorageEngine) -> Any:
        """Execute READ_KEY_RANGE operation."""
        return storage_engine.read_key_range(self.start_key, self.end_key)

    def to_response(self, result: Any = None, error: Optional[str] = None) -> str:
        """Format READ_KEY_RANGE response."""
        response = {
            "operation": "read_key_range",
            "start_key": self.start_key.decode("utf-8", errors="replace"),
            "end_key": self.end_key.decode("utf-8", errors="replace"),
        }
        if error:
            response["error"] = error
        else:
            response["success"] = True
            if isinstance(result, dict):
                response["results"] = {
                    (k.decode("utf-8", errors="replace") if isinstance(k, bytes) else k): (
                        v.decode("utf-8", errors="replace")
                        if isinstance(v, bytes)
                        else v
                    )
                    for k, v in result.items()
                }
            else:
                response["results"] = result
        return json.dumps(response)

    @staticmethod
    def parse(data: Dict[str, Any]) -> "ReadKeyRangeOperation":
        """Parse READ_KEY_RANGE operation from JSON data.

        Args:
            data: Parsed JSON dictionary.

        Returns:
            ReadKeyRangeOperation instance.

        Raises:
            ValueError: If required fields are missing.
        """
        start_key = data.get("start_key")
        end_key = data.get("end_key")
        if not start_key or not end_key:
            raise ValueError(
                "READ_KEY_RANGE requires 'start_key' and 'end_key'"
            )
        start_key = _ensure_bytes(start_key)
        end_key = _ensure_bytes(end_key)
        return ReadKeyRangeOperation(start_key, end_key)


class BatchPutOperation(RequestOperation):
    """Operation for atomically writing multiple key-value pairs."""

    def __init__(self, keys: list[bytes], values: list[bytes]):
        """Initialize BATCH_PUT operation.

        Args:
            keys: List of keys to write.
            values: List of values to write.
        """
        self.keys = keys
        self.values = values

    def validate(self) -> None:
        """Validate that keys and values match in length."""
        if not self.keys or not self.values:
            raise ValueError("BATCH_PUT requires 'keys' and 'values'")
        if len(self.keys) != len(self.values):
            raise ValueError(
                "BATCH_PUT: 'keys' and 'values' must have same length"
            )

    def execute(self, storage_engine: StorageEngine) -> Any:
        """Execute BATCH_PUT operation."""
        storage_engine.batch_put(self.keys, self.values)
        return f"OK ({len(self.keys)} items)"

    def to_response(self, result: Any = None, error: Optional[str] = None) -> str:
        """Format BATCH_PUT response."""
        response = {
            "operation": "batch_put",
            "count": len(self.keys),
        }
        if error:
            response["error"] = error
        else:
            response["success"] = True
            response["value"] = result
        return json.dumps(response)

    @staticmethod
    def parse(data: Dict[str, Any]) -> "BatchPutOperation":
        """Parse BATCH_PUT operation from JSON data.

        Args:
            data: Parsed JSON dictionary.

        Returns:
            BatchPutOperation instance.

        Raises:
            ValueError: If required fields are missing or invalid.
        """
        keys = data.get("keys", [])
        values = data.get("values", [])

        if not keys or not values:
            raise ValueError("BATCH_PUT requires 'keys' and 'values'")
        if len(keys) != len(values):
            raise ValueError(
                "BATCH_PUT: 'keys' and 'values' must have same length"
            )

        # Convert all keys and values to bytes
        keys = [_ensure_bytes(k) for k in keys]
        values = [_ensure_bytes(v) for v in values]

        return BatchPutOperation(keys, values)


# class FlushOperation(RequestOperation):
#     """Operation for flushing MemTable to SSTable."""

#     def validate(self) -> None:
#         """No validation needed for FLUSH operation."""
#         pass

#     def execute(self, controller: "Controller") -> Any:
#         """Execute FLUSH operation."""
#         controller.flush()
#         return "OK"

#     def to_response(self, result: Any = None, error: Optional[str] = None) -> str:
#         """Format FLUSH response."""
#         response = {"operation": "flush"}
#         if error:
#             response["error"] = error
#         else:
#             response["success"] = True
#             response["value"] = result
#         return json.dumps(response)

#     @staticmethod
#     def parse(data: Dict[str, Any]) -> "FlushOperation":
#         """Parse FLUSH operation from JSON data.

#         Args:
#             data: Parsed JSON dictionary.

#         Returns:
#             FlushOperation instance.
#         """
#         return FlushOperation()


# Operation type registry using Enum
class OperationType(Enum):
    """Enumeration of all supported operation types.
    
    Maps operation names to their implementation classes.
    """
    PUT = PutOperation
    GET = GetOperation
    DELETE = DeleteOperation
    READ_KEY_RANGE = ReadKeyRangeOperation
    BATCH_PUT = BatchPutOperation
    # FLUSH = FlushOperation


class OperationFactory:
    """Factory for creating operation instances from JSON requests."""

    @staticmethod
    def from_json(data: str) -> RequestOperation:
        """Parse a request and create the appropriate operation instance.

        Args:
            data: JSON string representing the request.

        Returns:
            RequestOperation instance.

        Raises:
            ValueError: If the request format is invalid or operation is unknown.
        """
        try:
            request_data = json.loads(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in request: {e}")

        op_name = request_data.get("operation", "").upper()

        try:
            operation_type = OperationType[op_name]
        except KeyError:
            raise ValueError(f"Unknown operation: {op_name}")

        operation_class = operation_type.value
        operation = operation_class.parse(request_data)
        operation.validate()

        return operation
