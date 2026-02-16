import json

from src.storage.manifest import Manifest


def _meta(path: str, level: int = 0) -> dict:
    return {
        "path": path,
        "level": level,
        "created_at_ms": 1,
        "size_bytes": 10,
        "entry_count": 2,
        "min_key_hex": "61",
        "max_key_hex": "62",
    }


def test_load_missing_file_initializes_empty_state(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    manifest.load()
    assert manifest.get_version() == 0
    assert manifest.get_levels_snapshot() == {}


def test_add_and_remove_sstable_persist_to_disk(tmp_path):
    path = tmp_path / "manifest.json"
    manifest = Manifest(path)

    m1 = _meta(str(tmp_path / "sst_1.sst"), level=0)
    m2 = _meta(str(tmp_path / "sst_2.sst"), level=1)
    manifest.add_sstable(0, m1)
    manifest.add_sstable(1, m2)

    assert manifest.get_version() == 2
    assert manifest.get_level_sstables(0) == [m1]
    assert manifest.get_level_sstables(1) == [m2]

    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk["version"] == 2
    assert len(on_disk["levels"]["0"]) == 1
    assert len(on_disk["levels"]["1"]) == 1

    manifest.remove_sstable(0, m1["path"])
    assert manifest.get_version() == 3
    assert manifest.get_level_sstables(0) == []
    assert manifest.get_level_sstables(1) == [m2]

    on_disk_after = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk_after["version"] == 3
    assert on_disk_after["levels"]["0"] == []
    assert len(on_disk_after["levels"]["1"]) == 1


def test_load_restores_levels_and_version_from_disk(tmp_path):
    path = tmp_path / "manifest.json"
    m1 = _meta(str(tmp_path / "sst_a.sst"), level=0)
    m2 = _meta(str(tmp_path / "sst_b.sst"), level=2)
    data = {"version": 7, "levels": {"0": [m1], "2": [m2]}}
    path.write_text(json.dumps(data), encoding="utf-8")

    manifest = Manifest(path)
    manifest.load()

    assert manifest.get_version() == 7
    assert manifest.get_level_sstables(0) == [m1]
    assert manifest.get_level_sstables(2) == [m2]
