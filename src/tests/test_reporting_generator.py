"""generator: writes report.html, overwrites, --pending detects stale/missing."""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from click.testing import CliRunner

from stdf_platform import cli
from stdf_platform.config import Config, StorageConfig
from stdf_platform.reporting.generator import (
    generate_lot_report, report_path, pending_lots,
)
from test_reporting_queries import _write_cp, _write_ft


def _config(data_dir) -> Config:
    return Config(storage=StorageConfig(data_dir=data_dir, database=data_dir / "db.duckdb"))


def test_generate_writes_html(tmp_path):
    _write_cp(tmp_path)
    cfg = _config(tmp_path)
    out = generate_lot_report(cfg, "PROD", "CP", "LOT1")
    assert out == report_path(tmp_path, "PROD", "CP", "LOT1")
    assert out.exists()
    assert "Yield summary" in out.read_text(encoding="utf-8")


def test_generate_overwrites(tmp_path):
    _write_cp(tmp_path)
    cfg = _config(tmp_path)
    out = generate_lot_report(cfg, "PROD", "CP", "LOT1")
    out.write_text("STALE", encoding="utf-8")
    out2 = generate_lot_report(cfg, "PROD", "CP", "LOT1")
    assert out2 == out
    assert "STALE" not in out.read_text(encoding="utf-8")


def test_pending_detects_missing_then_clears(tmp_path):
    _write_cp(tmp_path)
    _write_ft(tmp_path)
    cfg = _config(tmp_path)
    pend = pending_lots(cfg)
    assert ("PROD", "CP", "LOT1") in pend
    assert ("CHIP", "FT", "FT1") in pend
    for p, c, l in pend:
        generate_lot_report(cfg, p, c, l)
    # all fresh now (reports newer than parquet)
    assert pending_lots(cfg) == []


def test_pending_detects_stale(tmp_path):
    _write_cp(tmp_path)
    cfg = _config(tmp_path)
    generate_lot_report(cfg, "PROD", "CP", "LOT1")
    assert pending_lots(cfg) == []
    # bump a parquet mtime into the future -> report is now stale
    future = time.time() + 100
    pq = next((tmp_path / "parts").glob("**/*.parquet"))
    os.utime(pq, (future, future))
    assert ("PROD", "CP", "LOT1") in pending_lots(cfg)


def test_report_cli_lot(tmp_path, monkeypatch):
    _write_cp(tmp_path)
    monkeypatch.setattr(
        cli.Config, "load",
        classmethod(lambda c, p=None: _config(tmp_path)),
    )
    res = CliRunner().invoke(cli.main, ["report", "--lot", "LOT1", "-p", "PROD"])
    assert res.exit_code == 0, res.output
    assert report_path(tmp_path, "PROD", "CP", "LOT1").exists()
