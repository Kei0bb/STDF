import sys
from pathlib import Path

import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_reporting_queries import _write_cp  # noqa: E402
from test_analysis_compare import _write_cp_lot2  # noqa: E402
from stdf_platform.analysis import AnalysisSession, trend  # noqa: E402


def _sess(tmp_path):
    _write_cp(tmp_path)
    _write_cp_lot2(tmp_path)
    return AnalysisSession(tmp_path)


def test_lot_trend_ordered_by_start_t(tmp_path):
    with _sess(tmp_path) as s:
        df = trend.lot_trend(s, "PROD", "CP")
        assert list(df.lot_id) == ["LOT1", "LOT2"]   # start_t ascending
        assert df.start_t.is_monotonic_increasing


def test_lot_trend_yield_values(tmp_path):
    with _sess(tmp_path) as s:
        df = trend.lot_trend(s, "PROD", "CP").set_index("lot_id")
        assert float(df.loc["LOT1", "yield_pct"]) == 100.0   # retest passes A
        assert float(df.loc["LOT2", "yield_pct"]) == 50.0    # 1 of 2 dies


def test_lot_trend_last_n(tmp_path):
    with _sess(tmp_path) as s:
        df = trend.lot_trend(s, "PROD", "CP", last_n=1)
        assert list(df.lot_id) == ["LOT2"]   # most recent only


def test_test_trend_stats(tmp_path):
    with _sess(tmp_path) as s:
        df = trend.test_trend(s, "PROD", "CP", 1001).set_index("lot_id")
        assert int(df.loc["LOT1", "n"]) == 3
        assert int(df.loc["LOT2", "n"]) == 2


def test_trend_fig_control_limits(tmp_path):
    with _sess(tmp_path) as s:
        df = trend.lot_trend(s, "PROD", "CP")
        fig = trend.trend_fig(df, "yield_pct", control_limits=True)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1               # the trend line
        # 3 control hlines added as layout shapes
        assert len(fig.layout.shapes) == 3
