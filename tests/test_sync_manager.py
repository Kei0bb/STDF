import json
from pathlib import Path

from stdf_platform.sync_manager import SyncManager


def test_mark_downloaded_can_defer_save(tmp_path):
    h = SyncManager(tmp_path / "h.json")
    h.mark_downloaded("r1", tmp_path / "a.stdf", "P", "CP", save=False)
    assert h.is_downloaded("r1")
    assert not (tmp_path / "h.json").exists()   # まだ書いていない
    h.save()
    assert SyncManager(tmp_path / "h.json").is_downloaded("r1")


def test_mark_ingested_can_defer_save(tmp_path):
    h = SyncManager(tmp_path / "h.json")
    h.mark_downloaded("r1", tmp_path / "a.stdf", "P", "CP")
    h.mark_ingested("r1", save=False)
    assert json.loads((tmp_path / "h.json").read_text())["files"]["r1"]["ingested"] is False
    h.save()
    assert json.loads((tmp_path / "h.json").read_text())["files"]["r1"]["ingested"] is True


def test_mark_downloaded_records_file_size(tmp_path):
    f = tmp_path / "a.stdf"
    f.write_bytes(b"12345")
    h = SyncManager(tmp_path / "h.json")
    h.mark_downloaded("r1", f, "P", "CP")
    entry = json.loads((tmp_path / "h.json").read_text())["files"]["r1"]
    assert entry["file_size"] == 5
