# Logvine Architecture - StorageEngine Refactoring

## Overview

The architecture has been significantly improved by introducing a **StorageEngine abstraction layer** that decouples storage operations from network protocol handling. This follows clean architecture principles and improves testability and maintainability.

## Architecture Layers

### Layer 1: Network Protocol (Controller)
- **Responsibility**: Handle HTTP/TCP requests, routing, concurrency control
- **Key Components**:
  - `handle_request()`: Async entry point for network requests
  - `PartitionKey`: Routing for distributed scenarios
  - Concurrency control: Write locks + async semaphore
- **Removed**: Direct storage logic (now delegated to StorageEngine)

### Layer 2: Operation Definitions (Operations)
- **Responsibility**: Define what operations are supported
- **Key Components**:
  - `RequestOperation`: Abstract base class for all operations
  - `PutOperation`, `GetOperation`, `DeleteOperation`, etc.
  - `OperationFactory`: Parse JSON → RequestOperation
- **Change**: Now calls `storage_engine.execute()` instead of `controller.execute()`

### Layer 3: Storage Coordination (StorageEngine)
- **Responsibility**: Coordinate all storage components
- **Key Components**:
  - `StorageEngine`: Abstract base with 6 core methods
  - `SimpleStorageEngine`: In-memory implementation (MemTable only)
  - `LSMStorageEngine`: Skeleton for full LSM implementation
- **Methods**:
  - `put(key, value)`: Write operation
  - `get(key)`: Read operation
  - `delete(key)`: Tombstone delete
  - `read_key_range(start, end)`: Range query
  - `batch_put(keys, values)`: Atomic multi-write
  - `flush()`: Durability/compaction

### Layer 4: Storage Components (MemTable, WAL, SSTables, Manifest)
- **Responsibility**: Physical storage implementation
- **Status**:
  - MemTable: ✅ Fully implemented (bytes keys/values)
  - WAL: 🟡 Skeleton (not yet implemented)
  - SSTable: 🟡 Skeleton (not yet implemented)
  - Manifest: 🟡 Skeleton (not yet implemented)

## Data Flow

### Request Processing Flow
```
Network Request (JSON)
    ↓
Controller.handle_request()
    ↓
OperationFactory.from_json() → RequestOperation
    ↓
Controller._execute_request()
    ↓
operation.execute(storage_engine)
    ↓
StorageEngine method (put/get/delete/etc)
    ↓
MemTable / WAL / SSTables / Manifest
    ↓
Network Response (JSON)
```

## Key Design Decisions

### 1. Bytes Consistency
- All keys and values are stored as `bytes` internally
- JSON strings are converted using `_ensure_bytes()` helper
- Ensures consistent encoding throughout the system

### 2. Polymorphic Operations
- Each operation is its own class (`PutOperation`, `GetOperation`, etc.)
- Eliminates if/else chains for operation dispatching
- Supports extensibility: add new operations by subclassing `RequestOperation`

### 3. Enum-based Registry
```python
class OperationType(Enum):
    PUT = PutOperation
    GET = GetOperation
    DELETE = DeleteOperation
    READ_KEY_RANGE = ReadKeyRangeOperation
    BATCH_PUT = BatchPutOperation
```
- Type-safe operation lookup
- Self-documenting code
- Easy to extend with new operations

### 4. Storage Engine Abstraction
- Separates "what to store" (Operations) from "how to store" (StorageEngine)
- Allows different storage strategies without changing operations
- Currently: `SimpleStorageEngine` for development
- Future: `LSMStorageEngine` for production

### 5. Distributed Support
- Hash-based key partitioning via `PartitionKey`
- Automatic routing to responsible node
- Range queries validated for single partition
- Node routing table for cluster coordination

## Testing

Integration tests validate the complete architecture:

✅ **StorageEngine Tests**
- PUT/GET/DELETE operations
- Batch writes
- Range queries
- Tombstone handling

✅ **Operation Tests**
- JSON parsing and validation
- Execution with StorageEngine
- Response formatting

✅ **Controller Tests**
- Request handling via network layer
- Integration with StorageEngine
- Concurrency control

**Test Results**: All 13 tests passing ✅

## Future Enhancements

### Immediate (Phase 2)
1. Implement LSMStorageEngine skeleton → full implementation
2. Implement WAL (Write-Ahead Log) for crash recovery
3. Implement SSTable (Sorted String Table) for disk storage
4. Implement Manifest for level tracking

### Medium-term (Phase 3)
1. Compaction logic for multi-level LSM
2. Bloom filters for faster lookups
3. Key compression for SSTables
4. Cache layer (block cache, row cache)

### Long-term (Phase 4)
1. Distributed transactions
2. Replication
3. Backup/restore
4. Query optimization

## File Structure

```
logvine/
├── __init__.py                  # Package version
├── controller.py                # Network layer + routing
├── operations.py                # Operation definitions
├── server.py                    # TCP server
├── client.py                    # Client library
└── storage/
    ├── __init__.py
    ├── engine.py                # StorageEngine abstraction
    ├── memtable.py              # In-memory buffer
    ├── wal.py                   # Write-ahead log (skeleton)
    ├── sstable.py               # Sorted table (skeleton)
    └── manifest.py              # Level tracking (skeleton)

test_integration.py              # Integration tests
```

## API Examples

### Using the Storage Engine Directly
```python
engine = SimpleStorageEngine(storage_path="/tmp/logvine")
engine.put(b"user:123", b"alice")
value = engine.get(b"user:123")
engine.delete(b"user:123")
```

### Using Operations with StorageEngine
```python
engine = SimpleStorageEngine(storage_path="/tmp/logvine")
operation = PutOperation(b"key", b"value")
operation.execute(engine)
```

### Using Controller with Network
```python
controller = Controller(storage_path="/tmp/logvine")
response = asyncio.run(controller.handle_request(
    '{"operation": "put", "key": "mykey", "value": "myvalue"}'
))
```

## Metrics

- **Total Operations**: 5 core APIs
- **Storage Implementations**: 2 (SimpleStorageEngine, LSMStorageEngine skeleton)
- **Concurrency Control**: Write locks + async semaphore
- **Supported Scenarios**: Single-node, distributed with partitioning
- **Test Coverage**: 13 integration tests (all passing)

---

**Status**: ✅ Architecture refactored successfully. Ready for LSM implementation.
