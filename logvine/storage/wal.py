"""WAL: Write-Ahead Log for crash recovery.

Durably records all writes before they are applied to the MemTable,
ensuring no data loss in case of failure.
"""

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
import struct
import threading
import zlib
import logging


logger = logging.getLogger(__name__)

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
        self._lock = threading.Lock()

    def open(self) -> None:
        """Open the WAL file for writing."""
        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.file = self.path.open("ab+")
        except Exception as e:
            print(f"Error opening WAL: {e}")
            raise


    def close(self) -> None:
        """Close the WAL file."""
        if self.file:
            self.file.close()

    def append(self, operation: int, key: bytes, value: bytes) -> int:
        """Append a write operation to the log.

        Args:
            key: The key being written.
            value: The value being written.
        """

        logger.info(f"Appending to WAL: operation={operation}, key={key}, value_length={len(value)}")

        try:
             self.open()
             self.file.seek(0, 2) # Move to end of file for appending
             record = self.construct_record(operation, key, value)
             record_offset = self.get_record_offset(record)
             self.file.write(record)
             self.file.flush()
             self.close()
             logger.info(f"Appended to WAL: operation={operation}, key={key}, value_length={len(value)}, offset={record_offset}")
             return record_offset
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

            data = payload[:-12]  # Exclude last 12 bytes (4 for checksum + 8 for offset)
            stored_checksum = struct.unpack(">I", payload[-12:-8])[0]
            computed_checksum = zlib.crc32(data)

            if computed_checksum != stored_checksum:
                logger.warning(f"Corrupted WAL record detected at offset {self.file.tell() - record_length}")
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

    def get_current_offset(self) -> int:
        """Get the current byte offset in the WAL file."""
        if self.file:
            return self.file.tell()
        return 0


    def truncate_upto(self, offset: int) -> None:
        """Delete WAL entries from the beginning up to a byte offset.

        Args:
            offset: Byte position (exclusive) up to which data should be removed.
        """
        with self._lock:
            if offset <= 0 or not self.path.exists():
                return

            file_size = self.path.stat().st_size
            tmp_path = self.path.with_name(f"{self.path.name}.tmp")
            if offset >= file_size:
                with self.path.open("wb") as f:
                    f.flush()
                    os.fsync(f.fileno())
                return

            with self.path.open("rb") as src:
                src.seek(offset)
                remaining = src.read()

            with tmp_path.open("wb") as dst:
                dst.write(remaining)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(tmp_path, self.path)
            dir_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

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

        offset = self.file.tell() if self.file else 0
        record.extend(struct.pack(">Q", offset))  # 8 bytes for offset

        # Now prefix with record length
        record_length = len(record)
        return struct.pack(">I", record_length) + record
    

    def get_record_offset(self, record: bytes) -> int:
        """Extract the byte offset from a WAL record.

        Args:
            record: The complete WAL record as bytes.
        Returns:
            The byte offset where this record is located in the WAL file.
        """
        if len(record) < 4 + 1 + 4 + 4 + 4 + 8:
            raise ValueError("Record is too short to contain offset")
        return struct.unpack(">Q", record[-8:])[0]
    
