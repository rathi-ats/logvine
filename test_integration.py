#!/usr/bin/env python3
"""Integration test for the logvine architecture.

Tests that:
1. StorageEngine abstraction works
2. Operations execute correctly with StorageEngine
3. Controller coordinates storage operations
"""

import json
import tempfile
from pathlib import Path

from logvine.controller import Controller
from logvine.operations import OperationFactory
from logvine.storage.engine import LSMStorageEngine


def test_storage_engine_basic():
    """Test basic StorageEngine operations."""
    print("Testing StorageEngine basic operations...")
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = LSMStorageEngine(tmpdir)

        # Test PUT
        engine.put(b"key1", b"value1")
        print("✓ PUT key1=value1")

        # Test GET
        value = engine.get(b"key1")
        assert value == b"value1", f"Expected b'value1', got {value}"
        print("✓ GET key1 -> value1")

        # Test DELETE
        engine.delete(b"key1")
        print("✓ DELETE key1")

        try:
            engine.get(b"key1")
            assert False, "Should have raised KeyError"
        except KeyError:
            print("✓ GET deleted key raises KeyError")

        # Test BATCH_PUT
        engine.batch_put([b"a", b"b", b"c"], [b"val_a", b"val_b", b"val_c"])
        print("✓ BATCH_PUT 3 items")

        # Test READ_KEY_RANGE
        result = engine.read_key_range(b"a", b"d")
        assert len(result) == 3, f"Expected 3 items in range, got {len(result)}"
        print(f"✓ READ_KEY_RANGE a-d -> {len(result)} items")


def test_operations_with_storage_engine():
    """Test that operations correctly execute with StorageEngine."""
    print("\nTesting Operations with StorageEngine...")
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = LSMStorageEngine(tmpdir)

        # Test PUT operation
        put_json = json.dumps({"operation": "put", "key": "test_key", "value": "test_value"})
        put_op = OperationFactory.from_json(put_json)
        result = put_op.execute(engine)
        assert result == "OK", f"PUT should return 'OK', got {result}"
        print("✓ PUT operation executed successfully")

        # Test GET operation
        get_json = json.dumps({"operation": "get", "key": "test_key"})
        get_op = OperationFactory.from_json(get_json)
        result = get_op.execute(engine)
        assert result == b"test_value", f"GET should return b'test_value', got {result}"
        print("✓ GET operation executed successfully")

        # Test DELETE operation
        delete_json = json.dumps({"operation": "delete", "key": "test_key"})
        delete_op = OperationFactory.from_json(delete_json)
        result = delete_op.execute(engine)
        assert result == "OK", f"DELETE should return 'OK', got {result}"
        print("✓ DELETE operation executed successfully")

        # Test BATCH_PUT operation
        batch_json = json.dumps({
            "operation": "batch_put",
            "keys": ["x", "y", "z"],
            "values": ["val_x", "val_y", "val_z"]
        })
        batch_op = OperationFactory.from_json(batch_json)
        result = batch_op.execute(engine)
        assert "OK" in result, f"BATCH_PUT should return status, got {result}"
        print(f"✓ BATCH_PUT operation executed successfully: {result}")

        # Test READ_KEY_RANGE operation
        range_json = json.dumps({
            "operation": "read_key_range",
            "start_key": "x",
            "end_key": "z~"
        })
        range_op = OperationFactory.from_json(range_json)
        result = range_op.execute(engine)
        assert len(result) >= 2, f"Range query should return at least 2 items, got {len(result)}"
        print(f"✓ READ_KEY_RANGE operation executed successfully: {len(result)} items")


def test_controller_integration():
    """Test that Controller correctly coordinates with StorageEngine."""
    print("\nTesting Controller integration with StorageEngine...")
    import asyncio
    
    async def async_test():
        with tempfile.TemporaryDirectory() as tmpdir:
            controller = Controller(storage_path=tmpdir)

            # Verify controller has storage engine
            assert hasattr(controller, "storage_engine"), "Controller should have storage_engine"
            assert isinstance(
                controller.storage_engine, LSMStorageEngine
            ), "Controller.storage_engine should be SimpleStorageEngine"
            print("✓ Controller has SimpleStorageEngine instance")

            # Test handle_request
            put_json = json.dumps({"operation": "put", "key": "ctrl_key", "value": "ctrl_value"})
            response = await controller.handle_request(put_json)
            response_data = json.loads(response)
            assert "success" in response_data or "error" not in response_data, f"PUT response: {response_data}"
            print(f"✓ Controller.handle_request PUT successful: {response_data.get('operation')}")

            # Test GET through controller
            get_json = json.dumps({"operation": "get", "key": "ctrl_key"})
            response = await controller.handle_request(get_json)
            response_data = json.loads(response)
            assert response_data.get("success"), f"GET failed: {response_data}"
            print(f"✓ Controller.handle_request GET successful: {response_data.get('value')}")
    
    asyncio.run(async_test())


if __name__ == "__main__":
    test_storage_engine_basic()
    test_operations_with_storage_engine()
    test_controller_integration()
    print("\n✅ All integration tests passed!")
