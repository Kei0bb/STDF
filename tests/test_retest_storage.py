import pyarrow as pa
from stdf_platform.storage import PARTS_SCHEMA, TEST_DATA_SCHEMA


def test_parts_schema_has_retest_num():
    assert PARTS_SCHEMA.get_field_index("retest_num") >= 0
    field = PARTS_SCHEMA.field("retest_num")
    assert pa.types.is_integer(field.type)


def test_test_data_schema_has_retest_num():
    assert TEST_DATA_SCHEMA.get_field_index("retest_num") >= 0
    field = TEST_DATA_SCHEMA.field("retest_num")
    assert pa.types.is_integer(field.type)


import tempfile
from pathlib import Path
from stdf_platform.storage import ParquetStorage
from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
import pyarrow.parquet as pq


def _make_storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _make_stdf_data(lot_id="LOT1", wafer_id="W1") -> STDFData:
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
    data.parts = [
        {"part_id": f"{lot_id}_{wafer_id}_0", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
        {"part_id": f"{lot_id}_{wafer_id}_1", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 2, "y_coord": 2,
         "hard_bin": 0, "soft_bin": 0, "passed": False, "test_count": 1, "test_time": 100},
    ]
    data.tests = {1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"}}
    data.test_results = [
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": f"{lot_id}_{wafer_id}_0",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": f"{lot_id}_{wafer_id}_1",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 0.5, "passed": False},
    ]
    return data


def test_parts_first_ingest_writes_retest_0(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_stdf_data()
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="test.stdf")

    # sub_process="" means no sub_process= directory in path
    parts_path = tmp_path / "parts"
    wafer_dir = (parts_path / "product=PROD" / "test_category=CP" /
                 "lot_id=LOT1" / "wafer_id=W1")
    retest_dirs = list(wafer_dir.iterdir())
    assert len(retest_dirs) == 1
    assert retest_dirs[0].name == "retest=0"


def test_parts_retest_writes_retest_1(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_stdf_data()
    # First ingest
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="test.stdf")
    # Second ingest (retest — only failing die)
    data2 = _make_stdf_data()
    data2.parts = [data2.parts[1]]  # Only the failing die
    data2.test_results = [data2.test_results[1]]
    storage.save_stdf_data(data2, product="PROD", test_category="CP", source_file="test_retest.stdf")

    parts_base = (tmp_path / "parts" / "product=PROD" / "test_category=CP" /
                  "lot_id=LOT1" / "wafer_id=W1")
    retest_dirs = sorted([d.name for d in parts_base.iterdir()])
    assert retest_dirs == ["retest=0", "retest=1"]


def test_parts_parquet_has_retest_num_column(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_stdf_data()
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="test.stdf")

    parquet_path = (tmp_path / "parts" / "product=PROD" / "test_category=CP" /
                    "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet")
    table = pq.ParquetFile(parquet_path).read()
    assert "retest_num" in table.schema.names
    assert table["retest_num"][0].as_py() == 0


def test_test_data_parquet_has_retest_num_column(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_stdf_data()
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="test.stdf")

    parquet_path = (tmp_path / "test_data" / "product=PROD" / "test_category=CP" /
                    "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet")
    table = pq.ParquetFile(parquet_path).read()
    assert "retest_num" in table.schema.names
    assert table["retest_num"][0].as_py() == 0
