"""AnalysisSession lifecycle + helpers over synthetic Parquet."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stdf_platform.analysis import AnalysisSession
from test_reporting_queries import _write_cp, _write_ft


def test_session_registers_views(tmp_path):
    _write_cp(tmp_path)
    with AnalysisSession(tmp_path) as s:
        assert "parts_final" in s.registered
        assert "lots" in s.registered


def test_session_q_binds_params(tmp_path):
    _write_cp(tmp_path)
    with AnalysisSession(tmp_path) as s:
        df = s.q("SELECT COUNT(*) AS n FROM parts_final WHERE lot_id = ?", ["LOT1"])
        assert int(df.iloc[0]["n"]) == 4   # 4 distinct dies, retest-aware


def test_session_lots_filter(tmp_path):
    _write_cp(tmp_path)
    _write_ft(tmp_path)
    with AnalysisSession(tmp_path) as s:
        allrows = s.lots()
        assert set(allrows["lot_id"]) == {"LOT1", "FT1"}
        cp = s.lots(product="PROD", test_category="CP")
        assert list(cp["lot_id"]) == ["LOT1"]


def test_session_lots_dedup_latest(tmp_path):
    _write_cp(tmp_path)
    with AnalysisSession(tmp_path) as s:
        rows = s.lots(product="PROD")
        assert len(rows) == 1   # single MIR row, not duplicated


def test_session_default_data_dir(monkeypatch, tmp_path):
    _write_cp(tmp_path)
    # Config.load() with no config.yaml → defaults; point STDF_CONFIG at a yaml
    cfg = tmp_path / "c.yaml"
    cfg.write_text(f"storage:\n  data_dir: {tmp_path.as_posix()}\n")
    monkeypatch.setenv("STDF_CONFIG", str(cfg))
    with AnalysisSession() as s:    # data_dir=None → resolved from config
        assert "parts_final" in s.registered
