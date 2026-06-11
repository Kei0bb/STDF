"""reporting.sections: figures are valid Plotly JSON, CP/FT lists correct."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import duckdb

from stdf_platform.views import setup_views
from stdf_platform.config import Config
from stdf_platform.reporting import sections as S
from test_reporting_queries import _write_cp, _write_ft  # fixture builders


def _conn(data_dir):
    conn = duckdb.connect(":memory:")
    setup_views(conn, data_dir)
    return conn


def test_cp_section_list_has_wafer_maps_no_chipid(tmp_path):
    _write_cp(tmp_path)
    conn = _conn(tmp_path)
    secs = S.build_sections(conn, "PROD", "CP", "LOT1", Config())
    titles = [s.title for s in secs]
    assert titles[0] == "Header"
    assert "Wafer maps" in titles
    assert "ChipID summary" not in titles
    # retest history present because the CP fixture has a retest=1
    assert "Retest history" in titles


def test_ft_section_list_has_chipid_no_wafer_maps(tmp_path):
    _write_ft(tmp_path)
    conn = _conn(tmp_path)
    secs = S.build_sections(conn, "CHIP", "FT", "FT1", Config())
    titles = [s.title for s in secs]
    assert "Wafer maps" not in titles
    assert "ChipID summary" in titles
    assert "Retest history" not in titles  # single FT run


def test_figures_are_valid_plotly_json(tmp_path):
    _write_cp(tmp_path)
    conn = _conn(tmp_path)
    secs = S.build_sections(conn, "PROD", "CP", "LOT1", Config())
    figs = [f for s in secs for f in s.figures]
    assert figs, "expected at least one figure"
    for f in figs:
        obj = json.loads(f)
        assert "data" in obj and "layout" in obj


def test_yield_section_total_row(tmp_path):
    _write_cp(tmp_path)
    conn = _conn(tmp_path)
    sec = S.yield_section(conn, "PROD", "CP", "LOT1", Config())
    last = sec.tables[0]["rows"][-1]
    assert last[0] == "TOTAL"
    assert last[1] == 4  # 4 distinct dies


def test_always_include_tests_added(tmp_path):
    _write_cp(tmp_path)
    conn = _conn(tmp_path)
    cfg = Config()
    cfg.reporting.histogram_top_n = 0          # no fail-driven histograms
    cfg.reporting.always_include_tests = {"PROD": [1001]}
    sec = S.parametric_section(conn, "PROD", "CP", "LOT1", cfg)
    # 1001 forced in despite top_n=0 -> exactly one histogram figure
    assert len(sec.figures) == 1
