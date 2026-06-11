"""Atomic-write tests for SyncManager._save and IngestHistory._save."""

import json
from pathlib import Path

from stdf_platform.sync_manager import SyncManager
from stdf_platform.ingest_history import IngestHistory


def test_sync_manager_save_is_atomic_and_clean(tmp_path):
    hist = tmp_path / "sync_history.json"
    mgr = SyncManager(hist)
    mgr.mark_downloaded("remote/a.stdf", tmp_path / "a.stdf", "PROD", "CP")

    # File is valid JSON and round-trips through a fresh reader.
    data = json.loads(hist.read_text(encoding="utf-8"))
    assert "remote/a.stdf" in data["files"]
    assert SyncManager(hist).is_downloaded("remote/a.stdf")

    # No temp residue left behind.
    leftovers = [p.name for p in tmp_path.iterdir() if p.name != "sync_history.json"]
    assert leftovers == []


def test_ingest_history_save_is_atomic_and_clean(tmp_path):
    hist = tmp_path / "ingest_history.json"
    h = IngestHistory(hist)
    f = tmp_path / "x.stdf"
    f.write_text("x")
    h.mark_done_batch([f])

    assert IngestHistory(hist).is_done(f)
    leftovers = [p.name for p in tmp_path.iterdir()
                 if p.name not in {"ingest_history.json", "x.stdf"}]
    assert leftovers == []
