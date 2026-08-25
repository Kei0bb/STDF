import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent))
from synth_data import _write_cp, _cpk  # noqa: E402
from stdf_platform.analysis import AnalysisSession  # noqa: E402
from stdf_platform.analysis import compare  # noqa: E402
from stdf_platform.build import run_build  # noqa: E402
from stdf_platform.config import Config, StorageConfig  # noqa: E402


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
    # `lots` is a VIEW derived from `runs` (see views.py) — write the MIR info
    # at the `runs` partition instead of the old lot-level data.parquet.
    runs = (data_dir / "runs" / "product=PROD" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=LOT2" / "wafer_id=W1"
            / "retest=0" / "data.parquet")
    runs.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT2"], "wafer_id": ["W1"], "product": ["PROD"],
        "test_category": ["CP"], "sub_process": ["CP1"], "retest_num": [0],
        "part_type": ["SCT101A"],
        "job_name": ["CP_TEST"], "job_rev": ["Rev01"],
        "tester_type": ["J750"], "operator": ["OPE01"],
        "start_time": [pa.scalar(1_700_100_000_000, pa.timestamp("ms", tz="UTC"))],
        "finish_time": [pa.scalar(1_700_103_600_000, pa.timestamp("ms", tz="UTC"))],
        "test_rev": [""], "source_file": [""],
    }), runs)


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


def test_yield_by_lot_uses_mart_when_present(synth_store):
    # Before `stdf build`, no mart is mounted: yield_by_lot falls back to the
    # current wafer-grain query over wafer_yield_final (per-wafer rows).
    # LOT1/W1 after dedup: die(1,1) retest-1 passes, die(2,2) passes,
    # die(3,3) fails (never retested) -> total=3, good=2 (see conftest's
    # synth_store docstring; matches test_dbt_marts.test_lot_yield_summary_values).
    with AnalysisSession(synth_store) as s_pre:
        assert "lot_yield_summary" not in s_pre.registered
        fallback = compare.yield_by_lot(s_pre, "PROD", ["LOT1"], "CP")
        assert {"lot_id", "wafer_id", "total", "good", "yield_pct"} <= set(fallback.columns)
        row = fallback[(fallback.lot_id == "LOT1") & (fallback.wafer_id == "W1")].iloc[0]
        wafer_total, wafer_good = int(row.total), int(row.good)
        assert (wafer_total, wafer_good) == (3, 2)

    # After `stdf build`, lot_yield_summary is mounted and yield_by_lot
    # delegates to it. LOT1 has exactly one wafer (W1), so the mart's
    # lot-grain total_parts/good_parts/yield_pct carry the same underlying
    # yield numbers as the wafer-grain fallback above — this is the "same
    # value" Step 4 asks for; the column *set* legitimately differs (the
    # mart is one row per lot with no wafer_id, since it aggregates across
    # all of a lot's wafers) and is not reconciled to match the fallback's
    # shape (see task-8-report.md for why).
    run_build(Config(storage=StorageConfig(
        data_dir=synth_store, database=synth_store / "db.duckdb")))
    with AnalysisSession(synth_store) as s_post:
        assert "lot_yield_summary" in s_post.registered
        mart = compare.yield_by_lot(s_post, "PROD", ["LOT1"], "CP")
        assert list(mart.lot_id) == ["LOT1"]
        row = mart.iloc[0]
        assert int(row.total_parts) == wafer_total
        assert int(row.good_parts) == wafer_good
        assert float(row.yield_pct) == 66.67
