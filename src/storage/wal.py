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
        self.file = self.open() 
        self._lock = threading.Lock()

    def open(self) -> None:
        """Open the WAL file for writing."""
        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created WAL directory: {self.path.parent}")
        if not self.path.exists():
            self.path.touch()
            logger.debug(f"Created WAL file: {self.path}")
        try:
            self.file = self.path.open("ab+")
            logger.debug(f"Opened WAL file handle: {self.path}")
        except Exception as e:
            logger.exception(f"Error opening WAL: {self.path}")
            raise


    def close(self) -> None:
        """Close the WAL file."""
        if self.file:
            self.file.close()
            logger.debug(f"Closed WAL file handle: {self.path}")

    def append(self, operation: int, key: bytes, value: bytes) -> int:
        """Append a write operation to the log.

        Args:
            key: The key being written.
            value: The value being written.
        """

        logger.debug(f"Appending to WAL: operation={operation}, key={key}, value_length={len(value)}")

        if not self.file:
            self.open()
        
        with self._lock:
            try:
                record = self.construct_record(operation, key, value)
                record_offset = self.file.tell()
                self.file.write(record)
                self.file.flush()
                os.fsync(self.file.fileno())
                logger.debug(
                    f"Appended to WAL: operation={operation}, key={key}, "
                    f"value_length={len(value)}, offset={record_offset}"
                )
                return record_offset
            except Exception as e:
                logger.exception("Error writing WAL record")
                raise



    def replay(self):
        """Replay all operations from the WAL.

        Yields:
            Tuples of (operation_type, key, value, record_offset) where operation_type
            is 'put' or 'delete'.
        """
        if not self.path.exists():
            logger.info(f"WAL replay skipped (file missing): {self.path}")
            return

        logger.info(f"Starting WAL replay from {self.path}")
        records_replayed = 0

        with self.path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            f.seek(0)
            while f.tell() < file_size:
                record_offset = f.tell()
                length_bytes = f.read(4)
                if not length_bytes:
                    break  # End of file
                record_length = struct.unpack(">I", length_bytes)[0]
                payload = f.read(record_length)

                if len(payload) < record_length:
                    logger.warning(f"Incomplete WAL record detected at offset {f.tell() - len(payload) - 4}")
                    break  # Partial record at end of file

                data = payload[:-4]  # Exclude last 4 bytes of checksum
                stored_checksum = struct.unpack(">I", payload[-4:])[0]
                computed_checksum = zlib.crc32(data)

                if computed_checksum != stored_checksum:
                    logger.warning(f"Corrupted WAL record detected at offset {f.tell() - record_length}")
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

                records_replayed += 1
                yield (operation_type, key, value, record_offset)

        logger.info(f"WAL replay complete: {records_replayed} record(s)")


    def truncate_upto(self, offset: int) -> None:
        """Delete WAL entries from the beginning up to a byte offset.

        Args:
            offset: Byte position (exclusive) up to which data should be removed.
        """
        with self._lock:
            if offset <= 0 or not self.path.exists():
                logger.debug(
                    f"Skipping WAL truncate_upto: offset={offset}, exists={self.path.exists()}"
                )
                return

            file_size = self.path.stat().st_size
            tmp_path = self.path.with_name(f"{self.path.name}.tmp")
            logger.info(f"Truncating WAL up to offset {offset} (size={file_size})")
            if offset >= file_size:
                with self.path.open("wb") as f:
                    f.flush()
                    os.fsync(f.fileno())
                logger.info("WAL fully truncated (offset reached file end)")
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
            logger.info(
                f"WAL truncated up to offset {offset}; remaining bytes={self.path.stat().st_size}"
            )

    @staticmethod
    def construct_record(operationType: int, key: bytes, value: bytes) -> bytearray:
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
    
    
    def append_batch(self, operation: int, keys: list[bytes], values: list[bytes]) -> int:
        """Append a batch of operations to the log.

        Args:
            operation: The type of operation (e.g., OperationType.PUT.value).
            keys: List of keys being written.
            values: List of values being written.

        Returns:
            The byte offset in the WAL file where this batch was written.
        """
        if len(keys) != len(values):
            raise ValueError("keys and values must have the same length")
        
        if not self.file:
            self.open()
        
        if not keys:
            logger.debug("append_batch called with empty keys; no-op")
            return self.file.tell()
        
        with self._lock:   
            try:
                batch_record = bytearray()

                for key, value in zip(keys, values):
                    record = self.construct_record(operation, key, value)
                    batch_record.extend(record)

                
                batch_len = len(batch_record)
                batch_offset = self.file.tell()
                last_record_len = len(record)
                last_record_offset = batch_offset + batch_len - last_record_len

                self.file.write(batch_record)
                self.file.flush()
                os.fsync(self.file.fileno())
                logger.info(
                    f"Appended WAL batch: operation={operation}, "
                    f"num_items={len(keys)}, last_offset={last_record_offset}"
                )
                return last_record_offset
            except Exception as e:
                logger.exception(f"Error writing WAL batch: {e}")
                raise
