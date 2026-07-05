"""Ingest-time retest_flag / exec_seq marking on test_data (see storage.py
_demote_superseded and views.py union_by_name).

Fixture pattern copied/adapted from test_retest_storage.py (_make_storage,
_make_stdf_data) per task instructions — not imported, to keep this file
self-contained.
"""

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
from stdf_platform.storage import ParquetStorage, TEST_DATA_SCHEMA
from stdf_platform.views import setup_views


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
# 1. exec_seq / retest_flag on a fresh ingest, including a loop (same
#    part+test_num repeated 3x, e.g. an OTP dump writing 512 PTRs under one
#    test_num).
# ---------------------------------------------------------------------------

def test_new_ingest_exec_seq_and_retest_flag(tmp_path):
    storage = _make_storage(tmp_path)
    lot_id, wafer_id = "LOT1", "W1"
    parts = [
        {"part_id": "P0", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
    ]
    tests = {
        1: {"test_name": "SINGLE", "rec_type": "PTR", "lo_limit": 0, "hi_limit": 1, "units": "V"},
        5: {"test_name": "OTP_WORD", "rec_type": "PTR", "lo_limit": 0, "hi_limit": 1, "units": ""},
    }
    test_results = [
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": "P0",
         "test_num": 5, "head_num": 1, "site_num": 1, "result": 0.1, "passed": True},
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": "P0",
         "test_num": 5, "head_num": 1, "site_num": 1, "result": 0.2, "passed": True},
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": "P0",
         "test_num": 5, "head_num": 1, "site_num": 1, "result": 0.3, "passed": True},
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": "P0",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
    ]
    data = _make_stdf_data(lot_id, wafer_id, parts=parts, test_results=test_results, tests=tests)
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="t.stdf")

    table = _read_test_data(_wafer_dir(tmp_path, wafer_id) / "retest=0" / "data.parquet")
    df = table.to_pydict()
    assert df["retest_flag"] == [0, 0, 0, 0]

    loop_seq = [df["exec_seq"][i] for i in range(4) if df["test_num"][i] == 5]
    single_seq = [df["exec_seq"][i] for i in range(4) if df["test_num"][i] == 1]
    assert sorted(loop_seq) == [0, 1, 2]
    assert single_seq == [0]


# ---------------------------------------------------------------------------
# 2/3. Demote pass — the critical "unrelated die keeps flag 0" invariant,
#      across two successive retests of the same die.
# ---------------------------------------------------------------------------

def test_retest_demote_only_remeasured_die(tmp_path):
    storage = _make_storage(tmp_path)
    lot_id, wafer_id = "LOT1", "W1"

    data0 = _make_stdf_data(lot_id, wafer_id)  # dies A(1,1) and B(2,2), test_num 1
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")

    # Retest re-measuring only die A.
    data1 = _make_stdf_data(lot_id, wafer_id)
    data1.parts = [data1.parts[0]]
    data1.test_results = [data1.test_results[0]]
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    wdir = _wafer_dir(tmp_path, wafer_id)
    retests = sorted(d.name for d in wdir.iterdir())
    assert retests == ["retest=0", "retest=1"]

    run0 = _read_test_data(wdir / "retest=0" / "data.parquet").to_pydict()
    flags_by_coord = {(x, y): f for x, y, f in zip(run0["x_coord"], run0["y_coord"], run0["retest_flag"])}
    assert flags_by_coord[(1, 1)] == 1  # die A demoted
    assert flags_by_coord[(2, 2)] == 0  # die B untouched

    run1 = _read_test_data(wdir / "retest=1" / "data.parquet").to_pydict()
    assert run1["retest_flag"] == [0]


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
# 4. FT keying: x=y=-32768, identity is part_txt. Demote must not cross
#    part_txt boundaries.
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


# ---------------------------------------------------------------------------
# 5. PTR NULL pin_num must match NULL on both sides (a naive `=` join would
#    drop these rows instead of demoting them).
# ---------------------------------------------------------------------------

def test_ptr_null_pin_num_matches(tmp_path):
    storage = _make_storage(tmp_path)
    lot_id, wafer_id = "LOT1", "W1"

    parts = [
        {"part_id": "P0", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
    ]
    test_results = [
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": "P0",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},  # pin_num absent -> None
    ]
    data0 = _make_stdf_data(lot_id, wafer_id, parts=parts, test_results=test_results)
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")

    data1 = _make_stdf_data(lot_id, wafer_id, parts=parts, test_results=test_results)
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    wdir = _wafer_dir(tmp_path, wafer_id)
    run0 = _read_test_data(wdir / "retest=0" / "data.parquet").to_pydict()
    assert run0["pin_num"] == [None]
    assert run0["retest_flag"] == [1]


# ---------------------------------------------------------------------------
# 6. Old-schema store (pre-migration, no retest_flag/exec_seq columns): demote
#    must skip it without error and leave the file untouched.
# ---------------------------------------------------------------------------

def _old_schema() -> pa.Schema:
    return pa.schema([f for f in TEST_DATA_SCHEMA if f.name not in ("exec_seq", "retest_flag")])


def test_old_schema_store_skipped_by_demote(tmp_path):
    lot_id, wafer_id = "LOT1", "W1"
    old_schema = _old_schema()
    old_table = pa.table({
        "lot_id": [lot_id], "wafer_id": [wafer_id], "part_id": ["P0"], "part_txt": [""],
        "x_coord": [1], "y_coord": [1], "test_num": [1], "test_name": ["VCC"],
        "rec_type": ["PTR"], "lo_limit": [0.9], "hi_limit": [1.1], "units": ["V"],
        "result": [1.0], "passed": ["P"], "retest_num": [0],
        "pin_num": pa.array([None], type=pa.int64()), "pin_name": [None],
    }, schema=old_schema)

    wdir = _wafer_dir(tmp_path, wafer_id)
    retest0 = wdir / "retest=0"
    retest0.mkdir(parents=True)
    pq.write_table(old_table, retest0 / "data.parquet")
    before = _read_test_data(retest0 / "data.parquet")

    storage = _make_storage(tmp_path)
    parts = [
        {"part_id": "P0", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
    ]
    test_results = [
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": "P0",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
    ]
    data1 = _make_stdf_data(lot_id, wafer_id, parts=parts, test_results=test_results)
    # storage sees an existing retest=0 dir under *parts* too? No — parts dir is
    # separate and empty here, so _get_next_retest_num for parts returns 0. Seed
    # a parts retest=0 dir so the wafer is correctly recognized as already at
    # retest 0, and this ingest becomes retest=1.
    parts_dir = (tmp_path / "parts" / "product=PROD" / "test_category=CP"
                 / f"lot_id={lot_id}" / f"wafer_id={wafer_id}" / "retest=0")
    parts_dir.mkdir(parents=True)
    pq.write_table(pa.table({
        "part_id": ["P0"], "part_txt": [""], "lot_id": [lot_id], "wafer_id": [wafer_id],
        "head_num": [1], "site_num": [1], "x_coord": [1], "y_coord": [1],
        "hard_bin": [1], "soft_bin": [1], "passed": [True], "test_count": [1],
        "test_time": [100], "retest_num": [0],
    }), parts_dir / "data.parquet")

    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    assert (wdir / "retest=1").exists()
    after = _read_test_data(retest0 / "data.parquet")
    assert after.equals(before)
    assert "retest_flag" not in after.schema.names


# ---------------------------------------------------------------------------
# 7. _write_parquet atomicity.
# ---------------------------------------------------------------------------

def test_write_parquet_atomic_failure_leaves_no_partial_file(tmp_path, monkeypatch):
    storage = _make_storage(tmp_path)
    path = tmp_path / "atomic" / "data.parquet"
    path.parent.mkdir(parents=True)
    table = pa.table({"a": [1, 2, 3]})

    def boom(*args, **kwargs):
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr("stdf_platform.storage.pq.write_table", boom)

    with pytest.raises(RuntimeError):
        storage._write_parquet(table, path)

    assert not path.exists()
    assert list(path.parent.glob("*.tmp")) == []


def test_write_parquet_atomic_success_replaces_content(tmp_path):
    storage = _make_storage(tmp_path)
    path = tmp_path / "atomic2" / "data.parquet"
    path.parent.mkdir(parents=True)

    storage._write_parquet(pa.table({"a": [1]}), path)
    storage._write_parquet(pa.table({"a": [2, 3]}), path)

    result = pq.ParquetFile(path).read()
    assert result["a"].to_pylist() == [2, 3]
    assert list(path.parent.glob("*.tmp")) == []


# ---------------------------------------------------------------------------
# 8. union_by_name: old- and new-schema test_data files coexist under the
#    `test_data` view; old rows surface NULL retest_flag instead of erroring.
# ---------------------------------------------------------------------------

def test_union_by_name_mixed_schema_store(tmp_path):
    old_schema = _old_schema()
    old_table = pa.table({
        "lot_id": ["LOT1"], "wafer_id": ["WOLD"], "part_id": ["P0"], "part_txt": [""],
        "x_coord": [1], "y_coord": [1], "test_num": [1], "test_name": ["VCC"],
        "rec_type": ["PTR"], "lo_limit": [0.9], "hi_limit": [1.1], "units": ["V"],
        "result": [1.0], "passed": ["P"], "retest_num": [0],
        "pin_num": pa.array([None], type=pa.int64()), "pin_name": [None],
    }, schema=old_schema)
    old_dir = (tmp_path / "test_data" / "product=PROD" / "test_category=CP"
               / "lot_id=LOT1" / "wafer_id=WOLD" / "retest=0")
    old_dir.mkdir(parents=True)
    pq.write_table(old_table, old_dir / "data.parquet")

    storage = _make_storage(tmp_path)
    data_new = _make_stdf_data("LOT1", "WNEW")
    storage.save_stdf_data(data_new, product="PROD", test_category="CP", source_file="new.stdf")

    conn = duckdb.connect()
    registered = setup_views(conn, tmp_path)
    assert "test_data" in registered

    rows = conn.execute(
        "SELECT wafer_id, retest_flag FROM test_data WHERE wafer_id IN ('WOLD', 'WNEW') ORDER BY wafer_id"
    ).fetchall()
    by_wafer = {}
    for wafer_id, flag in rows:
        by_wafer.setdefault(wafer_id, []).append(flag)

    assert all(f is None for f in by_wafer["WOLD"])
    assert all(f == 0 for f in by_wafer["WNEW"])


# ---------------------------------------------------------------------------
# 9. test_data_final (flag-based, see views.py): returns exactly the flag-0
#    rows, including every loop-measurement row of the winning run, and
#    EXCLUDES rows with a NULL retest_flag (pre-flag files).
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


def test_final_view_excludes_null_flag_rows(tmp_path):
    old_schema = _old_schema()
    old_table = pa.table({
        "lot_id": ["LOT1"], "wafer_id": ["WOLD"], "part_id": ["P0"], "part_txt": [""],
        "x_coord": [1], "y_coord": [1], "test_num": [1], "test_name": ["VCC"],
        "rec_type": ["PTR"], "lo_limit": [0.9], "hi_limit": [1.1], "units": ["V"],
        "result": [1.0], "passed": ["P"], "retest_num": [0],
        "pin_num": pa.array([None], type=pa.int64()), "pin_name": [None],
    }, schema=old_schema)
    old_dir = (tmp_path / "test_data" / "product=PROD" / "test_category=CP"
               / "lot_id=LOT1" / "wafer_id=WOLD" / "retest=0")
    old_dir.mkdir(parents=True)
    pq.write_table(old_table, old_dir / "data.parquet")

    storage = _make_storage(tmp_path)
    data_new = _make_stdf_data("LOT1", "WNEW")
    storage.save_stdf_data(data_new, product="PROD", test_category="CP", source_file="new.stdf")

    conn = duckdb.connect()
    setup_views(conn, tmp_path)
    wafer_ids = {
        row[0] for row in conn.execute(
            "SELECT DISTINCT wafer_id FROM test_data_final WHERE lot_id = 'LOT1'"
        ).fetchall()
    }
    # The old-schema (NULL-flag) wafer must not surface via test_data_final —
    # pins the "re-ingest required" semantics from the views.py docstring.
    assert wafer_ids == {"WNEW"}
