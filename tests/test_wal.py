import struct
import zlib

from src.storage.wal import OperationType, WAL


def _decode_records(raw: bytes) -> list[tuple[int, bytes, bytes, int, int]]:
    """Decode WAL records into (op, key, value, checksum, computed_checksum)."""
    records = []
    offset = 0

    while offset < len(raw):
        record_len = struct.unpack(">I", raw[offset : offset + 4])[0]
        offset += 4

        payload = raw[offset : offset + record_len]
        offset += record_len

        op = payload[0]
        key_len = struct.unpack(">I", payload[1:5])[0]
        key_start = 5
        key_end = key_start + key_len
        key = payload[key_start:key_end]

        value_len = struct.unpack(">I", payload[key_end : key_end + 4])[0]
        value_start = key_end + 4
        value_end = value_start + value_len
        value = payload[value_start:value_end]

        checksum = struct.unpack(">I", payload[value_end : value_end + 4])[0]
        computed_checksum = zlib.crc32(payload[:-4])

        records.append((op, key, value, checksum, computed_checksum))

    return records


def test_construct_record_encodes_expected_layout_and_checksum(tmp_path):
    wal = WAL(tmp_path / "wal.log")
    record = wal.construct_record(OperationType.PUT.value, b"alpha", b"bravo")

    record_len = struct.unpack(">I", record[:4])[0]
    assert record_len == len(record) - 4

    decoded = _decode_records(record)
    assert len(decoded) == 1

    op, key, value, checksum, computed = decoded[0]
    assert op == OperationType.PUT.value
    assert key == b"alpha"
    assert value == b"bravo"
    assert checksum == computed


def test_append_writes_single_record(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)

    wal.append(OperationType.PUT.value, b"k1", b"v1")

    assert wal.file is not None
    assert not wal.file.closed

    raw = wal_path.read_bytes()
    decoded = _decode_records(raw)
    assert len(decoded) == 1
    assert decoded[0][0] == OperationType.PUT.value
    assert decoded[0][1] == b"k1"
    assert decoded[0][2] == b"v1"
    assert decoded[0][3] == decoded[0][4]


def test_append_appends_multiple_records_in_order(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)

    wal.append(OperationType.PUT.value, b"k1", b"v1")
    wal.append(OperationType.DELETE.value, b"k1", b"")

    raw = wal_path.read_bytes()
    decoded = _decode_records(raw)

    assert len(decoded) == 2
    assert (decoded[0][0], decoded[0][1], decoded[0][2]) == (
        OperationType.PUT.value,
        b"k1",
        b"v1",
    )
    assert (decoded[1][0], decoded[1][1], decoded[1][2]) == (
        OperationType.DELETE.value,
        b"k1",
        b"",
    )
    assert decoded[0][3] == decoded[0][4]
    assert decoded[1][3] == decoded[1][4]


def test_truncate_upto_clears_existing_wal_file(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)

    wal.append(OperationType.PUT.value, b"key", b"value")
    wal.truncate_upto(wal_path.stat().st_size)

    assert wal_path.read_bytes() == b""


def test_replay_returns_operations_in_append_order(tmp_path):
    wal = WAL(tmp_path / "wal.log")
    wal.append(OperationType.PUT.value, b"k1", b"v1")
    wal.append(OperationType.PUT.value, b"k2", b"v2")
    wal.append(OperationType.DELETE.value, b"k1", b"")

    replayed = list(wal.replay())
    assert [(op, key, value) for op, key, value, _ in replayed] == [
        (OperationType.PUT.value, b"k1", b"v1"),
        (OperationType.PUT.value, b"k2", b"v2"),
        (OperationType.DELETE.value, b"k1", b""),
    ]
    offsets = [offset for _, _, _, offset in replayed]
    assert offsets[0] == 0
    assert offsets == sorted(offsets)


def test_replay_stops_on_corrupt_record_and_returns_valid_prefix(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)
    wal.append(OperationType.PUT.value, b"ok", b"good")
    wal.append(OperationType.PUT.value, b"bad", b"data")

    raw = bytearray(wal_path.read_bytes())
    raw[-1] ^= 0x01  # Corrupt checksum in final record.
    wal_path.write_bytes(raw)

    replayed = list(wal.replay())
    assert [(op, key, value) for op, key, value, _ in replayed] == [
        (OperationType.PUT.value, b"ok", b"good")
    ]
    assert replayed[0][3] == 0
