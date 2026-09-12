"""Shared pytest fixtures.

`synth_store` builds a small synthetic Parquet store via the real
`ParquetStorage` (storage.py) write path — not hand-rolled Parquet files —
so its schemas (runs/parts/test_data) are guaranteed to match production,
including derived columns like `retest_flag`/`exec_seq` and `runs.retest_num`
that storage.py itself computes.
(Task 3); reusable by later mart tests (Task 4).
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
from stdf_platform.storage import CHIPID_SCHEMA, TEST_DATA_SCHEMA, ParquetStorage


def _cp_run(lot_id, wafer_id, job_name, job_rev, start_time, finish_time, parts, test_results, tests=None):
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
    data.tests = tests or {
        1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"},
    }
    data.test_results = test_results
    return data


def _ft_run(lot_id, job_name, job_rev, start_time, finish_time, parts, test_results=None):
    """One FT STDF file (no WIR -> wafer_id='' package identity)."""
    data = STDFData()
    data.lot_id = lot_id
    data.part_type = "TEST"
    data.job_name = job_name
    data.job_rev = job_rev
    data.start_time = start_time
    data.finish_time = finish_time
    data.tester_type = "TESTER"
    data.operator = "OP"
    data.wafers = []  # FT: no WIR, so no wafer identity to derive from wafers
    data.parts = parts
    data.tests = {1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"}}
    data.test_results = test_results or []
    return data


@pytest.fixture
def synth_store(tmp_path) -> Path:
    """Synthetic store: 1 CP lot (LOT1, 1 wafer W1, 2 retest runs where the
    second retest re-measures one of the two dies from the first run) plus
    1 FT lot (FTLOT1, no wafer identity).

    Exercises:
      - parts_final / stg_parts_final dedup (die (1,1) retested; die (2,2) only
        probed once) — the "2 retests of the same die" dedup case.
      - runs (one row per file x wafer identity) / lots aggregation, including
        job_variant_count / job_mixed via a job_rev change on the second run
        (LOT1 run0=RevA, run1=RevB -> job_variant_count=2, job_mixed=true).
      - test_data_final / stg_test_data_final via storage.py's ingest-time
        retest_flag (die (1,1)'s retest=0 test_data row gets demoted to
        retest_flag=1; retest=1's row is retest_flag=0).
      - die (3,3): fails in run0 and is never retested, so it stays failed
        after dedup -> feeds bin_pareto/bin_fail_tests hand-computed cases.
      - test_num=50 (FAIL_TEST) and test_num=100 (CPK_TEST): measured only in
        run0, never re-measured in run1, so both dies' rows keep
        retest_flag=0 from run0 regardless of the run1 retest on test_num=1
        -> isolated, hand-computable fail-rate / Cpk cases.
      - FT lot (FTLOT1, wafer_id='', x=y=-32768, part_txt=package barcode):
        lot-yield's wafer_count=0 (FT has no wafer) while total/good
        are still probed-based (2 packages, 1 pass) -> yield 50.0.
    """
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    storage = ParquetStorage(cfg)

    # retest 0: die (1,1) fails, die (2,2) passes, die (3,3) fails (never
    # retested -> stays failed after dedup).
    run0 = _cp_run(
        "LOT1", "W1", "CP_JOB", "RevA", 1000, 2000,
        parts=[
            {"part_id": "LOT1_W1_0", "lot_id": "LOT1", "wafer_id": "W1",
             "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
             "hard_bin": 0, "soft_bin": 0, "passed": False, "test_count": 1, "test_time": 100},
            {"part_id": "LOT1_W1_1", "lot_id": "LOT1", "wafer_id": "W1",
             "head_num": 1, "site_num": 1, "x_coord": 2, "y_coord": 2,
             "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
            {"part_id": "LOT1_W1_2", "lot_id": "LOT1", "wafer_id": "W1",
             "head_num": 1, "site_num": 1, "x_coord": 3, "y_coord": 3,
             "hard_bin": 5, "soft_bin": 5, "passed": False, "test_count": 1, "test_time": 100},
        ],
        test_results=[
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_0",
             "test_num": 1, "head_num": 1, "site_num": 1, "result": 0.5, "passed": False},
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_1",
             "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
            # test_num=50 (FAIL_TEST): die (1,1) fails, die (2,2) passes.
            # Neither is retested for this test_num -> total=2, fails=1,
            # fail_pct=50.0 after dedup (fail_ranking hand-computed case).
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_0",
             "test_num": 50, "head_num": 1, "site_num": 1, "result": 0.0, "passed": False},
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_1",
             "test_num": 50, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
            # test_num=100 (CPK_TEST, lo=0 hi=10): results [4.0, 6.0] ->
            # mean=5.0, sample stddev=sqrt(2), cp=10/(6*sqrt(2)),
            # cpk=min(5,5)/(3*sqrt(2)) (hand-computed Cpk case).
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_0",
             "test_num": 100, "head_num": 1, "site_num": 1, "result": 4.0, "passed": True},
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_1",
             "test_num": 100, "head_num": 1, "site_num": 1, "result": 6.0, "passed": True},
            # test_num=75 (BIN_FAIL_TEST): only die (3,3), which stays failed
            # after dedup -> the sole bin_fail_tests hand-computed row
            # (hard_bin=5, soft_bin=5, fail_count=1).
            {"lot_id": "LOT1", "wafer_id": "W1", "part_id": "LOT1_W1_2",
             "test_num": 75, "head_num": 1, "site_num": 1, "result": 99.0, "passed": False},
        ],
        tests={
            1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"},
            50: {"test_name": "FAIL_TEST", "rec_type": "PTR", "lo_limit": None, "hi_limit": None, "units": ""},
            100: {"test_name": "CPK_TEST", "rec_type": "PTR", "lo_limit": 0, "hi_limit": 10, "units": "V"},
            75: {"test_name": "BIN_FAIL_TEST", "rec_type": "PTR", "lo_limit": None, "hi_limit": None, "units": ""},
        },
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

    # FT lot: no WIR -> wafer_id='', x=y=-32768, part_txt is the package
    # barcode key. 2 packages, 1 pass / 1 fail, single run (no retest) ->
    # lot-yield's wafer_count=0 (FT has no wafer identity to count)
    # while total_parts=2/good_parts=1/yield_pct=50.0 stay probed-based.
    ft_run = _ft_run(
        "FTLOT1", "FT_JOB", "RevA", 5000, 6000,
        parts=[
            {"part_id": "FTLOT1_0", "part_txt": "PKG0001", "lot_id": "FTLOT1", "wafer_id": "",
             "head_num": 1, "site_num": 1, "x_coord": -32768, "y_coord": -32768,
             "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 50},
            {"part_id": "FTLOT1_1", "part_txt": "PKG0002", "lot_id": "FTLOT1", "wafer_id": "",
             "head_num": 1, "site_num": 1, "x_coord": -32768, "y_coord": -32768,
             "hard_bin": 0, "soft_bin": 0, "passed": False, "test_count": 1, "test_time": 50},
        ],
    )
    storage.save_stdf_data(ft_run, product="PROD", test_category="FT", sub_process="FT2", source_file="ft_run0.stdf")

    # storage.py only writes the chipid table for FT files with decoded
    # EN-SO-CHIPID_R data (see storage.py's `if data.chip_ids and
    # test_category == "FT"`), which the FT run above never triggers (it
    # carries no chip_ids). A missing chipid/ directory makes any glob over
    # zero matching files (unlike setup_views(), which just skips
    # registering the view when the table dir is absent), so write one
    # minimal chipid row directly so the fixture exercises chipid_final.
    # Reuses FTLOT1/FTLOT1_0/PKG0001 to line up with the FT parts row above.
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


def _write_null_flag_row(data_dir: Path) -> None:
    """Write one test_data row from a pre-retest_flag file: same shape as
    test_verify_flags.py's `old_schema` case (exec_seq/retest_flag columns
    entirely absent, not just NULL-valued) for a lot/wafer/die not otherwise
    in synth_store, so it doesn't disturb the other synth_store-derived
    fixtures/assertions.
    """
    old_schema = pa.schema([
        f for f in TEST_DATA_SCHEMA
        if f.name not in ("exec_seq", "retest_flag", "part_serial")
    ])
    row = {
        "lot_id": ["LOTCORRUPT"], "wafer_id": ["WBAD"], "part_id": ["PBAD"], "part_txt": [""],
        "x_coord": [9], "y_coord": [9], "test_num": [1], "test_name": ["VCC"],
        "rec_type": ["PTR"], "lo_limit": [0.9], "hi_limit": [1.1], "units": ["V"],
        "result": [1.0], "passed": ["P"], "retest_num": [0],
        "pin_num": pa.array([None], type=pa.int64()), "pin_name": [None],
    }
    path = (
        data_dir / "test_data" / "product=PROD" / "test_category=CP" / "sub_process=CP1"
        / "lot_id=LOTCORRUPT" / "wafer_id=WBAD" / "retest=0" / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(row, schema=old_schema), path)


@pytest.fixture
def corrupt_store(synth_store) -> Path:
    """`synth_store` plus one test_data row with retest_flag IS NULL (a
    pre-flag file — see storage.py / mounts.py's `test_data_final` docstring).

    Must fail the `null_flags` invariant, i.e. `stdf db verify` exits 1.
    """
    _write_null_flag_row(synth_store)
    return synth_store
