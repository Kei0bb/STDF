import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent))
from synth_data import _write_cp, _cpk  # noqa: E402
from stdf_platform.analysis import AnalysisSession  # noqa: E402
from stdf_platform.analysis import compare  # noqa: E402


def _write_cp_lot2(data_dir: Path):
    base = (data_dir / "parts" / "product=PROD" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=LOT2" / "wafer_id=W1"
            / "retest=0" / "data.parquet")
    base.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT2", "LOT2"], "wafer_id": ["W1", "W1"],
        "part_id": ["A", "B"], "part_txt": ["", ""],
        "x_coord": [0, 1], "y_coord": [0, 0],
        "hard_bin": [3, 1], "soft_bin": [3, 1],
        "passed": [False, True], "retest_num": [0, 0],
    }), base)
    td = (data_dir / "test_data" / "product=PROD" / "test_category=CP"
          / "sub_process=CP1" / "lot_id=LOT2" / "data.parquet")
    td.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT2", "LOT2"], "wafer_id": ["W1", "W1"],
        "part_id": ["A", "B"], "part_txt": ["", ""],
        "x_coord": [0, 1], "y_coord": [0, 0],
        "test_num": [1001, 1001], "pin_num": [0, 0],
        "test_name": ["Vth_N", "Vth_N"], "rec_type": ["PTR", "PTR"],
        "lo_limit": [0.3, 0.3], "hi_limit": [0.8, 0.8], "units": ["V", "V"],
        "result": [0.95, 0.55], "passed": ["F", "P"], "retest_num": [0, 0],
        "exec_seq": [0, 0], "retest_flag": [0, 0],
    }), td)
    lots = (data_dir / "lots" / "product=PROD" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=LOT2" / "data.parquet")
    lots.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT2"], "product": ["PROD"], "test_category": ["CP"],
        "sub_process": ["CP1"], "part_type": ["SCT101A"],
        "job_name": ["CP_TEST"], "job_rev": ["Rev01"],
        "tester_type": ["J750"], "operator": ["OPE01"],
        "start_time": [pa.scalar(1_700_100_000_000, pa.timestamp("ms", tz="UTC"))],
        "finish_time": [pa.scalar(1_700_103_600_000, pa.timestamp("ms", tz="UTC"))],
    }), lots)


def _sess(tmp_path):
    _write_cp(tmp_path)
    _write_cp_lot2(tmp_path)
    return AnalysisSession(tmp_path)


def test_yield_by_lot_per_wafer(tmp_path):
    with _sess(tmp_path) as s:
        df = compare.yield_by_lot(s, "PROD", ["LOT1", "LOT2"], "CP")
        # LOT1: W1 2/2, W2 2/2 (retest makes A pass); LOT2: W1 1/2
        row = df[(df.lot_id == "LOT2") & (df.wafer_id == "W1")].iloc[0]
        assert int(row.total) == 2 and int(row.good) == 1
        assert float(row.yield_pct) == 50.0
        l1 = df[(df.lot_id == "LOT1") & (df.wafer_id == "W1")].iloc[0]
        assert int(l1.good) == 2


def test_bin_pareto_by_lot_pct_sums_100(tmp_path):
    with _sess(tmp_path) as s:
        df = compare.bin_pareto_by_lot(s, "PROD", ["LOT1", "LOT2"], "CP")
        for lot in ("LOT1", "LOT2"):
            assert round(df[df.lot_id == lot]["pct"].sum(), 1) == 100.0


def test_test_stats_by_lot_explicit(tmp_path):
    with _sess(tmp_path) as s:
        df = compare.test_stats_by_lot(s, "PROD", ["LOT1", "LOT2"], "CP", [1001])
        assert set(df.columns) >= {"lot_id", "test_num", "n", "mean",
                                   "std", "cpk", "lo_limit", "hi_limit"}
        r1 = df[df.lot_id == "LOT1"].iloc[0]
        assert int(r1.n) == 3          # LOT1 td has 3 rows for 1001
        assert float(r1.lo_limit) == 0.3 and float(r1.hi_limit) == 0.8


def test_test_stats_default_picks_fail_tests(tmp_path):
    with _sess(tmp_path) as s:
        df = compare.test_stats_by_lot(s, "PROD", ["LOT1", "LOT2"], "CP")
        # test 1001 fails in both lots → auto-selected
        assert 1001 in set(int(t) for t in df.test_num)


def test_test_stats_cpk_matches_python_helper(tmp_path):
    with _sess(tmp_path) as s:
        df = compare.test_stats_by_lot(s, "PROD", ["LOT2"], "CP", [1001])
        got = float(df.iloc[0]["cpk"])
        expected = _cpk([0.95, 0.55], 0.3, 0.8)   # same population formula
        assert abs(got - expected) < 1e-3


def test_test_distribution_fig_smoke(tmp_path):
    with _sess(tmp_path) as s:
        fig = compare.test_distribution_fig(s, "PROD", ["LOT1", "LOT2"], "CP", 1001)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 2      # one histogram trace per lot
