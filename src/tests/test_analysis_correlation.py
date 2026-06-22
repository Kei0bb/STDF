import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from synth_data import _write_ft  # noqa: E402
from stdf_platform.analysis import AnalysisSession, correlation  # noqa: E402


def _write_cp_origin(data_dir: Path):
    """CP parts for origin lot HKPFJK wafer 11 with a die at (12,22)
    matching the FT chipid die-0 (TSMC1)."""
    p = (data_dir / "parts" / "product=CHIP" / "test_category=CP"
         / "sub_process=CP1" / "lot_id=HKPFJK" / "wafer_id=11"
         / "retest=0" / "data.parquet")
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["HKPFJK"], "wafer_id": ["11"],
        "part_id": ["cpA"], "part_txt": [""],
        "x_coord": [12], "y_coord": [22],
        "hard_bin": [1], "soft_bin": [1],
        "passed": [True], "retest_num": [0],
    }), p)
    lots = (data_dir / "lots" / "product=CHIP" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=HKPFJK" / "data.parquet")
    lots.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["HKPFJK"], "product": ["CHIP"], "test_category": ["CP"],
        "sub_process": ["CP1"], "part_type": ["CHIP"],
        "job_name": ["CP"], "job_rev": ["Rev01"],
        "tester_type": ["J750"], "operator": ["OPE01"],
        "start_time": [pa.scalar(1_699_000_000_000, pa.timestamp("ms", tz="UTC"))],
        "finish_time": [pa.scalar(1_699_003_600_000, pa.timestamp("ms", tz="UTC"))],
    }), lots)


def _write_corr_tests(data_dir: Path):
    """Two correlated tests in CP lot HKPFJK for test_correlation()."""
    td = (data_dir / "test_data" / "product=CHIP" / "test_category=CP"
          / "sub_process=CP1" / "lot_id=HKPFJK" / "data.parquet")
    td.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["HKPFJK"] * 6,
        "wafer_id": ["11"] * 6,
        "part_id": ["d0", "d0", "d1", "d1", "d2", "d2"],
        "part_txt": [""] * 6,
        "x_coord": [12, 12, 13, 13, 14, 14],
        "y_coord": [22, 22, 22, 22, 22, 22],
        "test_num": [1, 2, 1, 2, 1, 2],
        "pin_num": [0] * 6,
        "test_name": ["A", "B"] * 3, "rec_type": ["PTR"] * 6,
        "lo_limit": [0.0] * 6, "hi_limit": [10.0] * 6, "units": ["V"] * 6,
        "result": [1.0, 2.0, 2.0, 4.0, 3.0, 6.0],   # test2 = 2*test1 → corr 1.0
        "passed": ["P"] * 6, "retest_num": [0] * 6,
    }), td)


def test_die_cp_ft_join_matches_origin(tmp_path):
    _write_ft(tmp_path)        # FT lot FT1 + chipid (origin HKPFJK/ABCDEF)
    _write_cp_origin(tmp_path)
    with AnalysisSession(tmp_path) as s:
        df = correlation.die_cp_ft_join(s, "CHIP", "FT1")
        # die-0 (TSMC1, origin HKPFJK, x12 y22) matches the CP die we wrote
        hit = df[df.cp_lot_id == "HKPFJK"]
        assert len(hit) == 1
        assert int(hit.iloc[0].cp_x) == 12 and int(hit.iloc[0].cp_y) == 22
        assert bool(hit.iloc[0].cp_passed) is True
        assert hit.iloc[0].ft_part_txt == "2D-FT1-0000"


def test_die_cp_ft_join_unmatched_origin_dropped(tmp_path):
    _write_ft(tmp_path)        # ABCDEF origin has no CP parts written
    _write_cp_origin(tmp_path)
    with AnalysisSession(tmp_path) as s:
        df = correlation.die_cp_ft_join(s, "CHIP", "FT1")
        assert "ABCDEF" not in set(df.cp_lot_id)   # no CP die → inner-join drop


def test_cp_ft_yield_pairs_by_lot(tmp_path):
    _write_ft(tmp_path)        # FT lot FT1 (product CHIP): 1 of 2 passes
    _write_cp_origin(tmp_path) # CP lot HKPFJK (product CHIP): 1 of 1 passes
    with AnalysisSession(tmp_path) as s:
        df = correlation.cp_ft_yield(s, "CHIP").set_index("lot_id")
        assert float(df.loc["FT1", "ft_yield_pct"]) == 50.0
        assert float(df.loc["HKPFJK", "cp_yield_pct"]) == 100.0


def test_test_correlation_perfect(tmp_path):
    _write_cp_origin(tmp_path)
    _write_corr_tests(tmp_path)
    with AnalysisSession(tmp_path) as s:
        m = correlation.test_correlation(s, "CHIP", "HKPFJK", "CP", [1, 2])
        assert abs(float(m.loc[1, 2]) - 1.0) < 1e-9   # test2 = 2*test1
        assert abs(float(m.loc[1, 1]) - 1.0) < 1e-9


def test_test_correlation_shape(tmp_path):
    _write_cp_origin(tmp_path)
    _write_corr_tests(tmp_path)
    with AnalysisSession(tmp_path) as s:
        m = correlation.test_correlation(s, "CHIP", "HKPFJK", "CP", [1, 2])
        assert list(m.columns) == [1, 2] and list(m.index) == [1, 2]
