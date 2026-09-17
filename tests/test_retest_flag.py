"""Ingest-time retest_flag / exec_seq marking on test_data (see storage.py
_demote_superseded) and the flag-based test_data_final view.
"""

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
from stdf_platform.storage import ParquetStorage, TEST_DATA_SCHEMA
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
# FT keying: x=y=-32768, identity is part_txt (falling back to part_serial
# when the barcode is empty). Demote must not cross package boundaries.
# ---------------------------------------------------------------------------

def test_ft_keying_distinct_part_txt(tmp_path):
    storage = _make_storage(tmp_path)
    lot_id, wafer_id = "LOTFT", ""

    def ft_data(part_txts):
        parts = [
            {"part_id": f"P{i}", "lot_id": lot_id, "wafer_id": wafer_id, "part_txt": txt,
             "head_num": 1, "site_num": 1, "x_coord": -32768, "y_coord": -32768,
             "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100}
            for i, txt in enumerate(part_txts)
        ]
        test_results = [
            {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": f"P{i}",
             "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True}
            for i in range(len(part_txts))
        ]
        return _make_stdf_data(lot_id, wafer_id, parts=parts, test_results=test_results)

    data0 = ft_data(["AAA", "BBB"])
    storage.save_stdf_data(data0, product="PROD", test_category="FT", source_file="run0.stdf")

    data1 = ft_data(["AAA"])  # re-measure package AAA only
    storage.save_stdf_data(data1, product="PROD", test_category="FT", source_file="run1.stdf")

    wdir = _wafer_dir(tmp_path, wafer_id, test_category="FT", lot_id=lot_id)
    run0 = _read_test_data(wdir / "retest=0" / "data.parquet").to_pydict()
    flags_by_txt = {t: f for t, f in zip(run0["part_txt"], run0["retest_flag"])}
    assert flags_by_txt["AAA"] == 1
    assert flags_by_txt["BBB"] == 0


def test_empty_barcode_ft_demotes_only_same_serial(tmp_path):
    """part_txt 空でも PRR.PART_ID (part_serial) でパッケージを区別する。"""
    storage = _make_storage(tmp_path)
    wafer_dir = (tmp_path / "test_data" / "product=P" / "test_category=FT"
                 / "sub_process=FT1" / "lot_id=FL" / "wafer_id=")
    old = wafer_dir / "retest=0" / "data.parquet"
    old.parent.mkdir(parents=True, exist_ok=True)

    def rows(serial, flag):
        return {"lot_id": "FL", "wafer_id": "", "part_id": f"FL__{serial}",
                "part_txt": "", "part_serial": serial, "x_coord": -32768,
                "y_coord": -32768, "test_num": 1, "test_name": "T",
                "rec_type": "PTR", "lo_limit": 0.0, "hi_limit": 1.0, "units": "V",
                "result": 1.0, "passed": "P", "retest_num": 0, "pin_num": None,
                "pin_name": None, "exec_seq": 0, "retest_flag": flag}

    pq.write_table(pa.table({k: [v] for k, v in rows("SER0", 0).items()},
                            schema=TEST_DATA_SCHEMA), old)
    t = pq.ParquetFile(old).read()
    pq.write_table(pa.concat_tables([t, pa.Table.from_pylist(
        [rows("SER1", 0)], schema=TEST_DATA_SCHEMA)]), old)

    storage._demote_superseded(
        wafer_dir, new_keys={("", -32768, -32768, "SER1", 1, None)}, up_to_retest=1,
    )

    updated = pq.ParquetFile(old).read()
    flags = dict(zip(updated["part_serial"].to_pylist(),
                     updated["retest_flag"].to_pylist()))
    assert flags == {"SER0": 0, "SER1": 1}


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
