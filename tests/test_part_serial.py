from pathlib import Path

import pyarrow.parquet as pq

from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFParser
from stdf_platform.storage import ParquetStorage
from make_test_stdf import make_ft_stdf  # tests/ 直下の生成器（PART_ID=UNIT%04d を書く）


def _part_dir(root: Path, table: str) -> Path:
    return (root / table / "product=PROD" / "test_category=FT"
            / "sub_process=FT1" / "lot_id=FL" / "wafer_id=" / "retest=0")


def test_parser_keeps_prr_part_id(tmp_path):
    make_ft_stdf(tmp_path / "ft.stdf", lot_id="FL", parts=2)
    data = STDFParser().parse(tmp_path / "ft.stdf")
    assert [p["part_serial"] for p in data.parts] == ["UNIT0000", "UNIT0001"]


def test_storage_writes_part_serial_to_parts_and_test_data(tmp_path):
    make_ft_stdf(tmp_path / "ft.stdf", lot_id="FL", parts=1)
    data = STDFParser().parse(tmp_path / "ft.stdf")
    ParquetStorage(StorageConfig(data_dir=tmp_path)).save_stdf_data(
        data, product="PROD", test_category="FT", sub_process="FT1",
        source_file="ft.stdf",
    )
    parts = pq.ParquetFile(_part_dir(tmp_path, "parts") / "data.parquet").read()
    assert parts["part_serial"].to_pylist() == ["UNIT0000"]
    td = pq.ParquetFile(_part_dir(tmp_path, "test_data") / "data.parquet").read()
    assert td["part_serial"].to_pylist() == ["UNIT0000"]
