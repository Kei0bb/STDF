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
    # `lots` is a VIEW derived from `runs` (see views.py) — write the MIR info
    # at the `runs` partition instead of the old lot-level data.parquet.
    runs = (data_dir / "runs" / "product=CHIP" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=HKPFJK" / "wafer_id=11"
            / "retest=0" / "data.parquet")
    runs.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["HKPFJK"], "wafer_id": ["11"], "product": ["CHIP"],
        "test_category": ["CP"], "sub_process": ["CP1"], "retest_num": [0],
        "part_type": ["CHIP"],
        "job_name": ["CP"], "job_rev": ["Rev01"],
        "tester_type": ["J750"], "operator": ["OPE01"],
        "start_time": [pa.scalar(1_699_000_000_000, pa.timestamp("ms", tz="UTC"))],
        "finish_time": [pa.scalar(1_699_003_600_000, pa.timestamp("ms", tz="UTC"))],
        "test_rev": [""], "source_file": [""],
    }), runs)


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
        # ABCDEF origin has no CP parts written -> inner-join drop
        assert "ABCDEF" not in set(df.cp_lot_id)


def test_cp_ft_yield_pairs_by_lot(tmp_path):
    _write_ft(tmp_path)        # FT lot FT1 (product CHIP): 1 of 2 passes
    _write_cp_origin(tmp_path) # CP lot HKPFJK (product CHIP): 1 of 1 passes
    with AnalysisSession(tmp_path) as s:
        df = correlation.cp_ft_yield(s, "CHIP").set_index("lot_id")
        assert float(df.loc["FT1", "ft_yield_pct"]) == 50.0
        assert float(df.loc["HKPFJK", "cp_yield_pct"]) == 100.0
