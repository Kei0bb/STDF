from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from stdf_platform.config import StorageConfig
from stdf_platform.storage import TEST_DATA_SCHEMA, ParquetStorage


def _storage(root: Path) -> ParquetStorage:
    return ParquetStorage(StorageConfig(data_dir=root))


def test_demote_appends_missing_part_serial_column(tmp_path):
    """pre-part_serial の test_data ファイルが書き換え時も読めること。"""
    wafer_dir = (tmp_path / "test_data" / "product=P" / "test_category=CP"
                 / "sub_process=CP1" / "lot_id=LOT" / "wafer_id=W1")
    old = wafer_dir / "retest=0" / "data.parquet"
    old.parent.mkdir(parents=True, exist_ok=True)
    legacy_schema = pa.schema([f for f in TEST_DATA_SCHEMA if f.name != "part_serial"])
    row = {f.name: None for f in legacy_schema}
    row.update({"lot_id": "LOT", "wafer_id": "W1", "part_id": "LOT_W1_0",
                "part_txt": "", "x_coord": 0, "y_coord": 0, "test_num": 1,
                "test_name": "T", "rec_type": "PTR", "result": 1.0,
                "passed": "P", "retest_num": 0, "exec_seq": 0, "retest_flag": 0})
    pq.write_table(pa.table({k: [v] for k, v in row.items()}, schema=legacy_schema), old)

    _storage(tmp_path)._demote_superseded(
        wafer_dir, new_keys={("W1", 0, 0, "", 1, None)}, up_to_retest=1,
    )

    updated = pq.ParquetFile(old).read()
    assert updated.schema.names == TEST_DATA_SCHEMA.names
    assert updated["retest_flag"].to_pylist() == [1]
    assert updated["part_serial"].to_pylist() == [None]


def test_empty_barcode_ft_demotes_only_same_serial(tmp_path):
    """part_txt 空でも PRR.PART_ID (part_serial) でパッケージを区別する。"""
    storage = _storage(tmp_path)
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


def test_demote_does_not_rewrite_when_no_key_matches(tmp_path):
    """一致キーが無い旧ファイルを毎回書き直さない（O(n^2) I/O の削減）。"""
    storage = _storage(tmp_path)
    wafer_dir = (tmp_path / "test_data" / "product=P" / "test_category=CP"
                 / "sub_process=CP1" / "lot_id=LOT" / "wafer_id=W1")
    old = wafer_dir / "retest=0" / "data.parquet"
    old.parent.mkdir(parents=True, exist_ok=True)
    row = {f.name: None for f in TEST_DATA_SCHEMA}
    row.update({"lot_id": "LOT", "wafer_id": "W1", "part_id": "LOT_W1_0",
                "part_txt": "", "part_serial": "", "x_coord": 0, "y_coord": 0,
                "test_num": 1, "test_name": "T", "rec_type": "PTR",
                "result": 1.0, "passed": "P", "retest_num": 0,
                "exec_seq": 0, "retest_flag": 0})
    pq.write_table(pa.table({k: [v] for k, v in row.items()},
                            schema=TEST_DATA_SCHEMA), old)
    before = old.stat().st_mtime_ns

    storage._demote_superseded(
        wafer_dir, new_keys={("W1", 999, 999, "", 1, None)}, up_to_retest=1)

    assert old.stat().st_mtime_ns == before


def test_legacy_ft_without_part_serial_warns(tmp_path, caplog):
    """part_serial 以前の barcode 空 FT は flag を整合させられない — 警告を出す。

    旧ファイルには part_serial が無く part_id はファイル内連番なので、新 run の
    identity と一致させる術がない。黙って重複を作らず、再 ingest が必要だと
    言わせる。
    """
    import logging

    from stdf_platform.storage import TEST_DATA_SCHEMA

    legacy_schema = pa.schema([f for f in TEST_DATA_SCHEMA if f.name != "part_serial"])
    old_dir = tmp_path / "retest=0"
    old_dir.mkdir(parents=True)
    row = {f.name: None for f in legacy_schema}
    row.update({
        "lot_id": "LOT", "wafer_id": "", "part_id": "LOT__0", "part_txt": "",
        "x_coord": -32768, "y_coord": -32768, "test_num": 1, "test_name": "T1",
        "pin_num": None, "result": 1.0, "rec_type": "PTR", "passed": "P",
        "retest_flag": 0, "exec_seq": 0,
    })
    pq.write_table(
        pa.Table.from_pylist([row], schema=legacy_schema), old_dir / "data.parquet")

    storage = ParquetStorage(StorageConfig(data_dir=tmp_path))
    with caplog.at_level(logging.WARNING, logger="stdf_platform.storage"):
        storage._demote_superseded(
            tmp_path, {("", -32768, -32768, "SN-001", 1, None)}, up_to_retest=1)

    assert any("re-ingested" in r.getMessage() for r in caplog.records), caplog.text
