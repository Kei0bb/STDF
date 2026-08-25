"""Shared pytest fixtures.

`synth_store` builds a small synthetic Parquet store via the real
`ParquetStorage` (storage.py) write path — not hand-rolled Parquet files —
so its schemas (runs/parts/test_data) are guaranteed to match production,
including derived columns like `retest_flag`/`exec_seq` and `runs.retest_num`
that storage.py itself computes. Introduced for tests/test_dbt_staging_parity.py
(Task 3); reusable by later mart tests (Task 4).
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
from stdf_platform.storage import CHIPID_SCHEMA, ParquetStorage


def _cp_run(lot_id, wafer_id, job_name, job_rev, start_time, finish_time, parts, test_results):
    """One CP STDF file (WIR-derived wafer identity)."""
    data = STDFData()
    data.lot_id = lot_id
    data.part_type = "TEST"
    data.job_name = job_name
    data.job_rev = job_rev
    data.start_time = start_time
    data.finish_time = finish_time
    data.tester_type = "TESTER"
    data.operator = "OP"
    data.wafers = [{
        "wafer_id": wafer_id, "head_num": 1,
        "start_time": start_time, "finish_time": finish_time,
        "part_count": len(parts),
        "good_count": sum(1 for p in parts if p["passed"]),
        "rtst_count": 0, "abrt_count": 0,
    }]
    data.parts = parts
    data.tests = {1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"}}
    data.test_results = test_results
    return data


@pytest.fixture
def synth_store(tmp_path) -> Path:
    """Synthetic CP store: 1 lot (LOT1), 1 wafer (W1), 2 retest runs where the
    second retest re-measures one of the two dies from the first run.

    Exercises:
      - parts_final / stg_parts_final dedup (die (1,1) retested; die (2,2) only
        probed once) — the "2 retests of the same die" dedup case.
      - runs (one row per file x wafer identity) / lots aggregation, including
        job_variant_count / job_mixed via a job_rev change on the second run.
      - test_data_final / stg_test_data_final via storage.py's ingest-time
        retest_flag (die (1,1)'s retest=0 test_data row gets demoted to
        retest_flag=1; retest=1's row is retest_flag=0).
    """
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    storage = ParquetStorage(cfg)

    # retest 0: die (1,1) fails, die (2,2) passes.
    run0 = _cp_run(
        "LOT1", "W1", "CP_JOB", "RevA", 1000, 2000,
        parts=[
            {"part_id": "LOT1_W1_0", "lot_id": "LOT1", "wafer_id": "W1",
             "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
             "hard_bin": 0, "soft_bin": 0, "passed": False, "test_count": 1, "test_time": 100},
            {"part_id": "LOT1_W1_1", "lot_id": "LOT1", "wafer_id": "W1",
             "head_num": 1, "site_num": 1, "x_coord": 2, "y_coord": 2,
             "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
        ],
        test_results=[
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_0",
             "test_num": 1, "head_num": 1, "site_num": 1, "result": 0.5, "passed": False},
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_1",
             "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
        ],
    )
    storage.save_stdf_data(run0, product="PROD", test_category="CP", sub_process="CP1", source_file="run0.stdf")

    # retest 1: die (1,1) re-probed and now passes; job_rev bumped to RevB so
    # lots.job_mixed / job_variant_count have something to detect.
    run1 = _cp_run(
        "LOT1", "W1", "CP_JOB", "RevB", 3000, 4000,
        parts=[
            {"part_id": "LOT1_W1_0", "lot_id": "LOT1", "wafer_id": "W1",
             "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
             "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
        ],
        test_results=[
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_0",
             "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
        ],
    )
    storage.save_stdf_data(run1, product="PROD", test_category="CP", sub_process="CP1", source_file="run1.stdf")

    # storage.py only writes the chipid table for FT files with decoded
    # EN-SO-CHIPID_R data (see storage.py's `if data.chip_ids and
    # test_category == "FT"`), which this CP-only fixture never triggers.
    # dbt's stg_chipid_final source glob errors out on zero matching files
    # (unlike setup_views(), which just skips registering the view when the
    # table dir is absent), so write one minimal chipid row directly to keep
    # `dbt run --select staging` buildable.
    chipid_path = (
        tmp_path / "chipid" / "product=PROD" / "test_category=FT"
        / "lot_id=FTLOT1" / "wafer_id=" / "retest=0" / "data.parquet"
    )
    chipid_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({
            "lot_id": ["FTLOT1"], "part_id": ["FTLOT1_0"], "part_txt": ["PKG0001"],
            "chip_occurrence_index": [0], "efuse_raw": ["0" * 64], "valid": [True],
            "origin_fab_code": [1], "origin_fab": ["TSMC1"], "origin_lot": ["LOT1"],
            "origin_wafer": [1], "origin_x": [1], "origin_y": [1],
            "reserved_bits": [""], "retest_num": [0],
        }, schema=CHIPID_SCHEMA),
        chipid_path,
    )

    return tmp_path
