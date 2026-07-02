"""`_run_ingest_batch` writes data/ingest_failures.json atomically."""

import json

from stdf_platform import cli
from stdf_platform.config import Config, StorageConfig
from stdf_platform.sync_manager import SyncManager
from stdf_platform.worker import IngestResult


def test_writes_failures_json(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    config = Config(storage=StorageConfig(data_dir=data_dir, database=data_dir / "db"))
    sync = SyncManager(data_dir / "sync_history.json")

    bad = data_dir / "broken.stdf"

    def fake_pool(files, data_dir, compression, max_workers, timeout):
        fail = IngestResult(local_path=bad, remote_path="r/broken.stdf",
                            success=False, error="boom")
        return [], [fail]

    monkeypatch.setattr(cli, "run_ingest_pool", fake_pool, raising=False)
    # _run_ingest_batch imports run_ingest_pool locally; patch the source too.
    monkeypatch.setattr("stdf_platform.worker.run_ingest_pool", fake_pool)

    cli._run_ingest_batch(
        config, sync, [("r/broken.stdf", bad, "PROD", "CP")],
        cleanup=False, verbose=False,
    )

    failures_file = data_dir / "ingest_failures.json"
    assert failures_file.exists()
    payload = json.loads(failures_file.read_text(encoding="utf-8"))
    assert payload["failures"][0]["error"] == "boom"
    assert payload["failures"][0]["path"].endswith("broken.stdf")

    leftovers = [p.name for p in data_dir.iterdir() if p.name.startswith(".ingest_failures")]
    assert leftovers == []
