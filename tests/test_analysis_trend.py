import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from synth_data import _write_cp  # noqa: E402
from test_analysis_compare import _write_cp_lot2  # noqa: E402
from stdf_platform.analysis import AnalysisSession, trend  # noqa: E402


def _sess(tmp_path):
    _write_cp(tmp_path)
    _write_cp_lot2(tmp_path)
    return AnalysisSession(tmp_path)


def test_lot_trend(tmp_path):
    with _sess(tmp_path) as s:
        df = trend.lot_trend(s, "PROD", "CP")
        assert list(df.lot_id) == ["LOT1", "LOT2"]   # start_t ascending
        assert df.start_t.is_monotonic_increasing
        by_lot = df.set_index("lot_id")
        assert float(by_lot.loc["LOT1", "yield_pct"]) == 100.0   # retest passes A
        assert float(by_lot.loc["LOT2", "yield_pct"]) == 50.0    # 1 of 2 dies

        recent = trend.lot_trend(s, "PROD", "CP", last_n=1)
        assert list(recent.lot_id) == ["LOT2"]   # most recent only


def test_test_trend_stats(tmp_path):
    with _sess(tmp_path) as s:
        df = trend.test_trend(s, "PROD", "CP", 1001).set_index("lot_id")
        assert int(df.loc["LOT1", "n"]) == 3
        assert int(df.loc["LOT2", "n"]) == 2
