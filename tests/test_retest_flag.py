"""Ingest-time retest_flag / exec_seq marking on test_data (see storage.py
_demote_superseded) and the flag-based test_data_final view.
"""

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
from stdf_platform.storage import ParquetStorage
from stdf_platform.mounts import setup_views


def _make_storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _make_stdf_data(lot_id="LOT1", wafer_id="W1", parts=None, test_results=None, tests=None) -> STDFData:
    """Minimal STDFData with sensible defaults; override parts/test_results/tests
    to shape specific dedup-key scenarios."""
    data = STDFData()
    data.lot_id = lot_id
    data.part_type = "TEST"
    data.job_name = "TEST_JOB"
    data.job_rev = "A"
    data.start_time = 0
    data.finish_time = 0
    data.tester_type = "TESTER"
    data.operator = "OP"
    data._current_wafer = wafer_id
    data.wafers = [{
        "wafer_id": wafer_id, "head_num": 1,
        "start_time": 0, "finish_time": 0,
        "part_count": 2, "good_count": 1,
        "rtst_count": 0, "abrt_count": 0,
    }]
    data.parts = parts if parts is not None else [
        {"part_id": f"{lot_id}_{wafer_id}_0", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
        {"part_id": f"{lot_id}_{wafer_id}_1", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 2, "y_coord": 2,
         "hard_bin": 0, "soft_bin": 0, "passed": False, "test_count": 1, "test_time": 100},
    ]
    data.tests = tests if tests is not None else {
        1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"},
    }
    data.test_results = test_results if test_results is not None else [
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": f"{lot_id}_{wafer_id}_0",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": f"{lot_id}_{wafer_id}_1",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 0.5, "passed": False},
    ]
    return data


def _read_test_data(path: Path) -> pa.Table:
    return pq.ParquetFile(path).read()


def _wafer_dir(tmp_path: Path, wafer_id: str, product="PROD", test_category="CP", lot_id="LOT1") -> Path:
    return (tmp_path / "test_data" / f"product={product}" / f"test_category={test_category}"
            / f"lot_id={lot_id}" / f"wafer_id={wafer_id}")


# ---------------------------------------------------------------------------
# Demote pass — a re-measured die is bumped once per later retest; an
# unrelated die keeps flag 0. The PTR rows carry pin_num=None, so this also
# pins the NULL-safe pin_num match (a naive `=` join would never demote).
# ---------------------------------------------------------------------------

def test_second_retest_demotes_again(tmp_path):
    storage = _make_storage(tmp_path)
    lot_id, wafer_id = "LOT1", "W1"

    data0 = _make_stdf_data(lot_id, wafer_id)
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")

    data1 = _make_stdf_data(lot_id, wafer_id)
    data1.parts = [data1.parts[0]]
    data1.test_results = [data1.test_results[0]]
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    data2 = _make_stdf_data(lot_id, wafer_id)
    data2.parts = [data2.parts[0]]
    data2.test_results = [data2.test_results[0]]
    storage.save_stdf_data(data2, product="PROD", test_category="CP", source_file="run2.stdf")

    wdir = _wafer_dir(tmp_path, wafer_id)
    run0 = _read_test_data(wdir / "retest=0" / "data.parquet").to_pydict()
    run1 = _read_test_data(wdir / "retest=1" / "data.parquet").to_pydict()
    run2 = _read_test_data(wdir / "retest=2" / "data.parquet").to_pydict()

    flags0 = {(x, y): f for x, y, f in zip(run0["x_coord"], run0["y_coord"], run0["retest_flag"])}
    assert flags0[(1, 1)] == 2  # die A demoted twice
    assert flags0[(2, 2)] == 0  # die B never touched

    assert run1["retest_flag"] == [1]  # die A's retest=1 row demoted once by run2
    assert run2["retest_flag"] == [0]


# ---------------------------------------------------------------------------
# FT keying: x=y=-32768, package identity is the eFuse chip_key ONLY. Barcodes
# (part_txt / part_serial) and the per-file part_id are never identity; a
# package without chip_key is unique to its run and is never demoted later.
# ---------------------------------------------------------------------------

def _ft_data(chip_keys, lot_id="LOTFT", part_txt="SAME"):
    """One FT run; one package per entry of chip_keys ('' = no eFuse). Every
    package shares part_txt / part_serial / part_id numbering on purpose."""
    parts = [
        {"part_id": f"P{i}", "lot_id": lot_id, "wafer_id": "", "part_txt": part_txt,
         "part_serial": "SER", "chip_key": key,
         "head_num": 1, "site_num": 1, "x_coord": -32768, "y_coord": -32768,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100}
        for i, key in enumerate(chip_keys)
    ]
    test_results = [
        {"lot_id": lot_id, "wafer_id": "", "part_id": f"P{i}",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True}
        for i in range(len(chip_keys))
    ]
    return _make_stdf_data(lot_id, "", parts=parts, test_results=test_results)


def _ft_run0_flags(tmp_path, runs):
    """Ingest `runs` (lists of chip_keys) in order; return run0's retest_flag per
    package, keyed by chip_key (or P<i> when it has none)."""
    storage = _make_storage(tmp_path)
    for n, keys in enumerate(runs):
        storage.save_stdf_data(_ft_data(keys), product="PROD", test_category="FT",
                               source_file=f"run{n}.stdf")
    wdir = _wafer_dir(tmp_path, "", test_category="FT", lot_id="LOTFT")
    run0 = _read_test_data(wdir / "retest=0" / "data.parquet").to_pydict()
    return {k or p: f for k, p, f in zip(run0["chip_key"], run0["part_id"], run0["retest_flag"])}


def test_ft_overlay_keyed_by_chip_key(tmp_path):
    # run1 re-measures only EF|A, at a different position (P0 here, P1 in run0)
    flags = _ft_run0_flags(tmp_path, [["EF|B", "EF|A", "EF|C"], ["EF|A"]])
    assert flags == {"EF|B": 0, "EF|A": 1, "EF|C": 0}


def test_ft_without_chip_key_is_never_demoted(tmp_path):
    # equal part_txt, part_serial and part_id in both runs -> still no overlay
    flags = _ft_run0_flags(tmp_path, [["", ""], [""]])
    assert flags == {"P0": 0, "P1": 0}


# ---------------------------------------------------------------------------
# test_data_final (flag-based, see views.py): returns exactly the flag-0
#    rows, including every loop-measurement row of the winning run.
# ---------------------------------------------------------------------------

def test_final_view_flag_zero_rows_with_loop(tmp_path):
    storage = _make_storage(tmp_path)
    lot_id, wafer_id = "LOT1", "W1"

    parts = [
        {"part_id": "PA", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
        {"part_id": "PB", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 2, "y_coord": 2,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
    ]
    tests = {
        5: {"test_name": "OTP_WORD", "rec_type": "PTR", "lo_limit": 0, "hi_limit": 1, "units": ""},
        1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"},
    }

    def loop_results(part_id, values):
        return [
            {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": part_id,
             "test_num": 5, "head_num": 1, "site_num": 1, "result": v, "passed": True}
            for v in values
        ]

    # run0: die A loops x3 (test_num 5) + die B single test (test_num 1)
    test_results0 = loop_results("PA", [0.1, 0.2, 0.3]) + [
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": "PB",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
    ]
    data0 = _make_stdf_data(lot_id, wafer_id, parts=parts, test_results=test_results0, tests=tests)
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")

    # run1: die A re-measured (loops x3 again); die B untouched.
    test_results1 = loop_results("PA", [0.4, 0.5, 0.6])
    data1 = _make_stdf_data(lot_id, wafer_id, parts=[parts[0]], test_results=test_results1, tests=tests)
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    conn = duckdb.connect()
    setup_views(conn, tmp_path)
    rows = conn.execute(
        "SELECT x_coord, y_coord, test_num, exec_seq, result FROM test_data_final ORDER BY x_coord, exec_seq"
    ).fetchall()

    # Die A (1,1): all 3 loop rows come from run1 (the winning run) — no rows
    # get silently discarded the way the old ROW_NUMBER window would.
    a_rows = [r for r in rows if r[0] == 1]
    assert sorted(r[3] for r in a_rows) == [0, 1, 2]
    assert sorted(r[4] for r in a_rows) == [0.4, 0.5, 0.6]

    # Die B (2,2): untouched, its one run0 row is still flag 0.
    b_rows = [r for r in rows if r[0] == 2]
    assert len(b_rows) == 1
    assert b_rows[0][2] == 1
    assert b_rows[0][4] == 1.0
