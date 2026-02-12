"""WAL: Write-Ahead Log for crash recovery.

Durably records all writes before they are applied to the MemTable,
ensuring no data loss in case of failure.
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import struct
import zlib


class OperationType(Enum):
        PUT = 1
        DELETE = 2

class WAL:
    """Write-Ahead Log for durability."""

    def __init__(self, path: Path):
        """Initialize the WAL.

        Args:
            path: Path to the WAL file.
        """
        self.path = path
        self.file = None

    def open(self) -> None:
        """Open the WAL file for writing."""
        try:
            self.file = self.path.open("ab+")
        except Exception as e:
            print(f"Error opening WAL: {e}")
            raise


    def close(self) -> None:
        """Close the WAL file."""
        if self.file:
            self.file.close()

    def append(self, operation: int, key: bytes, value: bytes) -> None:
        """Append a write operation to the log.

        Args:
            key: The key being written.
            value: The value being written.
        """
        try:
             self.open()
             self.file.seek(0, 2) # Move to end of file for appending
             record = self.construct_record(operation, key, value)
             self.file.write(record)
             self.file.flush()
             self.close()
        except Exception as e:
            # Handle exceptions (e.g., disk full, permission issues)
            print(f"Error writing to WAL: {e}")
            raise


    def replay(self):
        """Replay all operations from the WAL.

        Yields:
            Tuples of (operation_type, key, value) where operation_type
            is 'put' or 'delete'.
        """

        self.open()
        self.file.seek(0)  # Start from the beginning of the file
        while True:
            length_bytes = self.file.read(4)
            if not length_bytes:
                break  # End of file

            record_length = struct.unpack(">I", length_bytes)[0]
            payload = self.file.read(record_length)

            if len(payload) < record_length:
                break  # Partial record at end of file

            data = payload[:-4]
            stored_checksum = struct.unpack(">I", payload[-4:])[0]
            computed_checksum = zlib.crc32(data)

            if computed_checksum != stored_checksum:
                break  # Corrupted record

            operation_type = data[0]
            key_len = struct.unpack(">I", data[1:5])[0]
            key_start = 5
            key_end = key_start + key_len
            key = data[key_start:key_end]

            value_len = struct.unpack(">I", data[key_end : key_end + 4])[0]
            value_start = key_end + 4
            value_end = value_start + value_len
            value = data[value_start:value_end]

            yield (operation_type, key, value)
        
        self.close()


    def truncate(self) -> None:
        """Truncate the WAL after a successful flush."""
        self.file.seek(0)
        self.file.truncate()

    def construct_record(self, operationType: int, key: bytes, value: bytes) -> bytearray:
        """Construct a WALRecord from the given operation.

        Args:
            operation: The type of operation ('put' or 'delete').
            key: The key being written.
            value: The value being written.

        Returns:
            A WALRecord instance representing the log entry.
        """

        record = bytearray()

        # Write operation type (1 byte)
        record.append(operationType)

        record.extend(struct.pack(">I", len(key)))
        record.extend(key)

        record.extend(struct.pack(">I", len(value)))
        record.extend(value)

        # Compute checksum over payload
        checksum = zlib.crc32(record)

        record.extend(struct.pack(">I", checksum))

        # Now prefix with record length
        record_length = len(record)
        return struct.pack(">I", record_length) + record
        
