"""Tests for the `runs` table (storage.py) and the `lots` view derived from
it (views.py). See docs/schema.md for the runs / lots column reference.
"""

from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
from stdf_platform.storage import ParquetStorage
from stdf_platform.views import setup_views


def _make_storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _make_cp_data(
    lot_id="LOT1",
    wafer_ids=("W1",),
    job_name="CP_JOB",
    job_rev="RevA",
    start_time=1000,
    finish_time=2000,
) -> STDFData:
    """One CP file with one or more wafers (WIR-derived identities)."""
    data = STDFData()
    data.lot_id = lot_id
    data.part_type = "TEST"
    data.job_name = job_name
    data.job_rev = job_rev
    data.start_time = start_time
    data.finish_time = finish_time
    data.tester_type = "TESTER"
    data.operator = "OP"
    data.wafers = [
        {
            "wafer_id": wid, "head_num": 1,
            "start_time": start_time, "finish_time": finish_time,
            "part_count": 2, "good_count": 1,
            "rtst_count": 0, "abrt_count": 0,
        }
        for wid in wafer_ids
    ]
    data.parts = []
    data.tests = {1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"}}
    data.test_results = []
    for wid in wafer_ids:
        data.parts.append(
            {"part_id": f"{lot_id}_{wid}_0", "lot_id": lot_id, "wafer_id": wid,
             "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
             "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
        )
        data.test_results.append(
            {"lot_id": lot_id, "wafer_id": wid, "part_id": f"{lot_id}_{wid}_0",
             "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
        )
    return data


def _make_ft_data(
    lot_id="FTLOT1",
    job_name="FT_JOB",
    job_rev="RevA",
    start_time=1000,
    finish_time=2000,
) -> STDFData:
    """FT file: no WIR, wafer_id='' for parts/test_results."""
    data = STDFData()
    data.lot_id = lot_id
    data.part_type = "TEST"
    data.job_name = job_name
    data.job_rev = job_rev
    data.start_time = start_time
    data.finish_time = finish_time
    data.tester_type = "TESTER"
    data.operator = "OP"
    data.wafers = []
    data.parts = [
        {"part_id": f"{lot_id}_0", "lot_id": lot_id, "wafer_id": "",
         "head_num": 1, "site_num": 1, "x_coord": -32768, "y_coord": -32768,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
    ]
    data.tests = {1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"}}
    data.test_results = [
        {"lot_id": lot_id, "wafer_id": "", "part_id": f"{lot_id}_0",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
    ]
    return data


def test_cp_two_wafers_one_file_produces_two_runs_rows(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_cp_data(wafer_ids=("W1", "W2"))
    counts = storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="f.stdf")
    assert counts["runs"] == 2

    runs_base = tmp_path / "runs" / "product=PROD" / "test_category=CP" / "lot_id=LOT1"
    for wid in ("W1", "W2"):
        p = runs_base / f"wafer_id={wid}" / "retest=0" / "data.parquet"
        assert p.exists()
        table = pq.ParquetFile(p).read()
        assert table.num_rows == 1
        assert table["wafer_id"][0].as_py() == wid
        # Same file → both rows share source_file.
        assert table["source_file"][0].as_py() == "f.stdf"


def test_ft_produces_one_row_at_empty_wafer_id_retest_0(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_ft_data()
    counts = storage.save_stdf_data(data, product="CHIP", test_category="FT", source_file="ft.stdf")
    assert counts["runs"] == 1

    p = (tmp_path / "runs" / "product=CHIP" / "test_category=FT" / "lot_id=FTLOT1"
         / "wafer_id=" / "retest=0" / "data.parquet")
    assert p.exists()
    table = pq.ParquetFile(p).read()
    assert table.num_rows == 1
    assert table["wafer_id"][0].as_py() == ""
    assert table["retest_num"][0].as_py() == 0


def test_retest_adds_new_runs_row_keeps_old(tmp_path):
    storage = _make_storage(tmp_path)
    data0 = _make_cp_data(wafer_ids=("W1",))
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")
    data1 = _make_cp_data(wafer_ids=("W1",))
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    wafer_base = (tmp_path / "runs" / "product=PROD" / "test_category=CP"
                  / "lot_id=LOT1" / "wafer_id=W1")
    retest_dirs = sorted(d.name for d in wafer_base.iterdir())
    assert retest_dirs == ["retest=0", "retest=1"]
    assert (wafer_base / "retest=0" / "data.parquet").exists()
    assert (wafer_base / "retest=1" / "data.parquet").exists()


def test_runs_retest_num_matches_parts_and_test_data(tmp_path):
    storage = _make_storage(tmp_path)
    data0 = _make_cp_data(wafer_ids=("W1",))
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")
    data1 = _make_cp_data(wafer_ids=("W1",))
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    run_p = (tmp_path / "runs" / "product=PROD" / "test_category=CP" / "lot_id=LOT1"
             / "wafer_id=W1" / "retest=1" / "data.parquet")
    parts_p = (tmp_path / "parts" / "product=PROD" / "test_category=CP" / "lot_id=LOT1"
               / "wafer_id=W1" / "retest=1" / "data.parquet")
    td_p = (tmp_path / "test_data" / "product=PROD" / "test_category=CP" / "lot_id=LOT1"
            / "wafer_id=W1" / "retest=1" / "data.parquet")

    run_retest = pq.ParquetFile(run_p).read()["retest_num"][0].as_py()
    parts_retest = pq.ParquetFile(parts_p).read()["retest_num"][0].as_py()
    td_retest = pq.ParquetFile(td_p).read()["retest_num"][0].as_py()
    assert run_retest == parts_retest == td_retest == 1


def test_lots_view_aggregates_min_max_and_latest_job(tmp_path):
    storage = _make_storage(tmp_path)
    data0 = _make_cp_data(
        wafer_ids=("W1",), job_name="CP_JOB", job_rev="RevA",
        start_time=1000, finish_time=2000,
    )
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")
    data1 = _make_cp_data(
        wafer_ids=("W2",), job_name="CP_JOB", job_rev="RevB",
        start_time=3000, finish_time=4000,
    )
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path)
    rows = conn.execute(
        "SELECT lot_id, start_time, finish_time, job_name, job_rev "
        "FROM lots WHERE lot_id = 'LOT1'"
    ).fetchall()
    assert len(rows) == 1   # 1 lot = 1 row
    lot_id, start_time, finish_time, job_name, job_rev = rows[0]
    assert start_time.timestamp() == 1000.0   # MIN over both runs (data0.start_time)
    assert finish_time.timestamp() == 4000.0  # MAX over both runs (data1.finish_time)
    # Latest run (start_time=3000) is data1 → job_rev RevB
    assert job_rev == "RevB"
    assert job_name == "CP_JOB"


def test_job_mixed_false_when_same_job(tmp_path):
    storage = _make_storage(tmp_path)
    data0 = _make_cp_data(wafer_ids=("W1",), job_name="CP_JOB", job_rev="RevA", start_time=1000)
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")
    data1 = _make_cp_data(wafer_ids=("W2",), job_name="CP_JOB", job_rev="RevA", start_time=2000)
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path)
    job_mixed, job_variant_count = conn.execute(
        "SELECT job_mixed, job_variant_count FROM lots WHERE lot_id = 'LOT1'"
    ).fetchone()
    assert job_mixed is False
    assert job_variant_count == 1


def test_job_mixed_true_when_job_rev_differs(tmp_path):
    storage = _make_storage(tmp_path)
    data0 = _make_cp_data(wafer_ids=("W1",), job_name="CP_JOB", job_rev="RevA", start_time=1000)
    storage.save_stdf_data(data0, product="PROD", test_category="CP", source_file="run0.stdf")
    data1 = _make_cp_data(wafer_ids=("W2",), job_name="CP_JOB", job_rev="RevB", start_time=2000)
    storage.save_stdf_data(data1, product="PROD", test_category="CP", source_file="run1.stdf")

    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path)
    job_mixed, job_variant_count = conn.execute(
        "SELECT job_mixed, job_variant_count FROM lots WHERE lot_id = 'LOT1'"
    ).fetchone()
    assert job_mixed is True
    assert job_variant_count == 2


def test_legacy_lots_directory_raises(tmp_path):
    (tmp_path / "lots").mkdir(parents=True)
    conn = duckdb.connect(":memory:")
    with pytest.raises(RuntimeError, match="Legacy store detected"):
        setup_views(conn, tmp_path)


def test_mir_only_file_still_writes_a_runs_row(tmp_path):
    """A file with a MIR but no WIR/PRR (aborted run) must not lose its MIR.

    The old `lots` write was unconditional, so such a file still showed up in
    `stdf db lots`. `runs` derives its rows from the wafer/part identity set,
    which is empty here — it falls back to the FT-shaped empty identity.
    """
    storage = _make_storage(tmp_path)
    data = _make_cp_data(wafer_ids=())
    data.wafers = []
    data.parts = []
    data.test_results = []

    counts = storage.save_stdf_data(
        data, product="PROD", test_category="CP", source_file="aborted.stdf"
    )
    assert counts["runs"] == 1

    p = (tmp_path / "runs" / "product=PROD" / "test_category=CP" / "lot_id=LOT1"
         / "wafer_id=" / "retest=0" / "data.parquet")
    assert p.exists()

    conn = duckdb.connect()
    setup_views(conn, tmp_path)
    row = conn.execute(
        "SELECT lot_id, job_name, job_rev, job_mixed FROM lots"
    ).fetchall()
    assert row == [("LOT1", "CP_JOB", "RevA", False)]
    conn.close()
