"""Synthetic Parquet fixtures + helpers shared across analysis tests.

`_write_cp` / `_write_ft` build a minimal Hive-partitioned store that the
Phase-1 views (setup_views) read directly, so tests need no STDF parsing.
`_cpk` is the reference process-capability formula the SQL Cpk is checked against.
"""

from pathlib import Path
from statistics import mean, pstdev

import pyarrow as pa
import pyarrow.parquet as pq


def _cpk(values, lo, hi) -> float | None:
    """Process capability. Needs at least one finite limit and >1 sample."""
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return None
    sigma = pstdev(vals)
    if sigma == 0:
        return None
    mu = mean(vals)
    cands = []
    if hi is not None:
        cands.append((hi - mu) / (3 * sigma))
    if lo is not None:
        cands.append((mu - lo) / (3 * sigma))
    return round(min(cands), 3) if cands else None


def _write_cp(data_dir: Path):
    """One CP lot, 2 wafers; W1 has a fail (soft_bin 3), plus a retest=1 of W1
    where the failing die now passes (so parts_final must pick retest=1)."""
    def parts(wafer, retest, rows):
        p = (data_dir / "parts" / "product=PROD" / "test_category=CP"
             / "sub_process=CP1" / f"lot_id=LOT1" / f"wafer_id={wafer}"
             / f"retest={retest}" / "data.parquet")
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(rows), p)

    # W1 retest0: die (0,0) fails (soft_bin 3), die (1,0) passes
    parts("W1", 0, {
        "lot_id": ["LOT1", "LOT1"], "wafer_id": ["W1", "W1"],
        "part_id": ["A", "B"], "part_txt": ["", ""],
        "x_coord": [0, 1], "y_coord": [0, 0],
        "hard_bin": [2, 1], "soft_bin": [3, 1],
        "passed": [False, True], "retest_num": [0, 0],
    })
    # W1 retest1: die (0,0) now passes
    parts("W1", 1, {
        "lot_id": ["LOT1"], "wafer_id": ["W1"],
        "part_id": ["A"], "part_txt": [""],
        "x_coord": [0], "y_coord": [0],
        "hard_bin": [1], "soft_bin": [1],
        "passed": [True], "retest_num": [1],
    })
    # W2 retest0: both pass
    parts("W2", 0, {
        "lot_id": ["LOT1", "LOT1"], "wafer_id": ["W2", "W2"],
        "part_id": ["C", "D"], "part_txt": ["", ""],
        "x_coord": [0, 1], "y_coord": [0, 0],
        "hard_bin": [1, 1], "soft_bin": [1, 1],
        "passed": [True, True], "retest_num": [0, 0],
    })

    lots = (data_dir / "lots" / "product=PROD" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=LOT1" / "data.parquet")
    lots.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT1"], "product": ["PROD"], "test_category": ["CP"],
        "sub_process": ["CP1"], "part_type": ["SCT101A"],
        "job_name": ["CP_TEST"], "job_rev": ["Rev01"],
        "tester_type": ["J750"], "operator": ["OPE01"],
        "start_time": [pa.scalar(1_700_000_000_000, pa.timestamp("ms", tz="UTC"))],
        "finish_time": [pa.scalar(1_700_003_600_000, pa.timestamp("ms", tz="UTC"))],
    }), lots)

    td = (data_dir / "test_data" / "product=PROD" / "test_category=CP"
          / "sub_process=CP1" / "lot_id=LOT1" / "data.parquet")
    td.parent.mkdir(parents=True, exist_ok=True)
    # test 1001 fails on die A (one fail of two -> 50% fail rate after dedup is
    # actually 1/3 since retest1 adds a passing A row; we assert >0 only)
    pq.write_table(pa.table({
        "lot_id": ["LOT1", "LOT1", "LOT1"], "wafer_id": ["W1", "W1", "W2"],
        "part_id": ["A", "B", "C"], "part_txt": ["", "", ""],
        "x_coord": [0, 1, 0], "y_coord": [0, 0, 0],
        "test_num": [1001, 1001, 1001], "pin_num": [0, 0, 0],
        "test_name": ["Vth_N", "Vth_N", "Vth_N"], "rec_type": ["PTR", "PTR", "PTR"],
        "lo_limit": [0.3, 0.3, 0.3], "hi_limit": [0.8, 0.8, 0.8], "units": ["V", "V", "V"],
        "result": [0.95, 0.55, 0.60], "passed": ["F", "P", "P"],
        "retest_num": [0, 0, 0],
    }), td)


def _write_ft(data_dir: Path):
    """FT lot: wafer_id='', x=y=-32768, plus a chipid table."""
    p = (data_dir / "parts" / "product=CHIP" / "test_category=FT"
         / "sub_process=FT1" / "lot_id=FT1" / "wafer_id=" / "retest=0" / "data.parquet")
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["FT1", "FT1"], "wafer_id": ["", ""],
        "part_id": ["U0", "U1"], "part_txt": ["2D-FT1-0000", "2D-FT1-0001"],
        "x_coord": [-32768, -32768], "y_coord": [-32768, -32768],
        "hard_bin": [1, 2], "soft_bin": [1, 3],
        "passed": [True, False], "retest_num": [0, 0],
    }), p)

    lots = (data_dir / "lots" / "product=CHIP" / "test_category=FT"
            / "sub_process=FT1" / "lot_id=FT1" / "data.parquet")
    lots.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["FT1"], "product": ["CHIP"], "test_category": ["FT"],
        "sub_process": ["FT1"], "part_type": ["CHIP"],
        "job_name": ["FT_TEST"], "job_rev": ["Rev01"],
        "tester_type": ["J750"], "operator": ["OPE01"],
        "start_time": [pa.scalar(1_700_000_000_000, pa.timestamp("ms", tz="UTC"))],
        "finish_time": [pa.scalar(1_700_003_600_000, pa.timestamp("ms", tz="UTC"))],
    }), lots)

    cp = (data_dir / "chipid" / "product=CHIP" / "test_category=FT"
          / "sub_process=FT1" / "lot_id=FT1" / "wafer_id=" / "retest=0" / "data.parquet")
    cp.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["FT1", "FT1"], "part_id": ["U0", "U0"],
        "part_txt": ["2D-FT1-0000", "2D-FT1-0000"],
        "chip_occurrence_index": [0, 1],
        "efuse_raw": ["0b" + "0" * 64, "0b" + "1" * 64],
        "valid": [True, True],
        "origin_fab_code": [1, 6], "origin_fab": ["TSMC1", "TSMC2"],
        "origin_lot": ["HKPFJK", "ABCDEF"], "origin_wafer": [11, 7],
        "origin_x": [12, 102], "origin_y": [22, 202],
        "reserved_bits": ["00", "00"], "retest_num": [0, 0],
    }), cp)
