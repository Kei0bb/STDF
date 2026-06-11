"""render_report smoke test: HTML contains sections, tables, fig JSON, plotly.js."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import duckdb

from stdf_platform.views import setup_views
from stdf_platform.config import Config
from stdf_platform.reporting import sections as S
from stdf_platform.reporting.render import render_report
from test_reporting_queries import _write_cp, _write_ft


def _html(data_dir, product, category, lot):
    conn = duckdb.connect(":memory:")
    setup_views(conn, data_dir)
    secs = S.build_sections(conn, product, category, lot, Config())
    return render_report(product, category, lot, secs, "2026-06-11T00:00:00Z")


def test_cp_report_html_has_sections_and_plotly(tmp_path):
    _write_cp(tmp_path)
    html = _html(tmp_path, "PROD", "CP", "LOT1")
    assert "Yield summary" in html
    assert "Wafer maps" in html
    assert "Plotly.newPlot" in html      # embedded bundle exposes Plotly
    assert "Histogram" in html or "Bar" in html  # a fig spec is embedded
    assert "<title>PROD" in html


def test_ft_report_html_has_chipid(tmp_path):
    _write_ft(tmp_path)
    html = _html(tmp_path, "CHIP", "FT", "FT1")
    assert "ChipID summary" in html
    assert "Wafer maps" not in html


def test_report_embeds_single_plotly_bundle(tmp_path):
    _write_cp(tmp_path)
    html = _html(tmp_path, "PROD", "CP", "LOT1")
    # the bundle defines this once; ensure it is present and not duplicated
    assert html.count("Plotly.register") <= 2  # bundle internals, not per-fig
    assert len(html) > 1_000_000              # plotly.js embedded (~4MB)
