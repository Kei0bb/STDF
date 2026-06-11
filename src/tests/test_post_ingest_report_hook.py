"""Post-ingest report hook must regenerate reports but never fail the ingest."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from stdf_platform import cli
from stdf_platform.config import Config, StorageConfig
from test_reporting_queries import _write_cp


class _Result:
    def __init__(self, local_path, remote_path):
        self.local_path = Path(local_path)
        self.remote_path = remote_path
        self.error = None


class _Sync:
    def mark_ingested(self, remote_path):  # no-op
        pass


def _config(data_dir):
    return Config(storage=StorageConfig(data_dir=data_dir, database=data_dir / "db.duckdb"))


def test_hook_generates_reports(tmp_path, monkeypatch):
    _write_cp(tmp_path)  # lot data already on disk -> pending_lots finds it
    cfg = _config(tmp_path)
    ok = [_Result(tmp_path / "f.stdf", "/r/f.stdf")]
    monkeypatch.setattr(cli, "_run_ingest_batch", cli._run_ingest_batch)  # use real
    monkeypatch.setattr(
        "stdf_platform.worker.run_ingest_pool",
        lambda **kw: (ok, []),
        raising=False,
    )
    # also patch the import target used inside _run_ingest_batch
    import stdf_platform.worker as worker
    monkeypatch.setattr(worker, "run_ingest_pool", lambda **kw: (ok, []))

    successes, failures = cli._run_ingest_batch(
        cfg, _Sync(), [(None, tmp_path / "f.stdf", "PROD", "")],
        cleanup=False, verbose=False,
    )
    assert successes == ok and failures == []
    from stdf_platform.reporting.generator import report_path
    assert report_path(tmp_path, "PROD", "CP", "LOT1").exists()


def test_hook_failure_does_not_fail_ingest(tmp_path, monkeypatch):
    _write_cp(tmp_path)
    cfg = _config(tmp_path)
    ok = [_Result(tmp_path / "f.stdf", "/r/f.stdf")]
    import stdf_platform.worker as worker
    monkeypatch.setattr(worker, "run_ingest_pool", lambda **kw: (ok, []))
    # make report generation explode
    import stdf_platform.reporting.generator as gen
    def _boom(config):
        raise RuntimeError("disk full")
    monkeypatch.setattr(gen, "pending_lots", _boom)

    successes, failures = cli._run_ingest_batch(
        cfg, _Sync(), [(None, tmp_path / "f.stdf", "PROD", "")],
        cleanup=False, verbose=False,
    )
    # ingest still succeeds despite the report hook blowing up
    assert successes == ok and failures == []
