"""Regression test: `stdf export lot` must query parts_final/test_data_final."""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from click.testing import CliRunner

from stdf_platform import cli
from stdf_platform.config import Config, StorageConfig


def _write(data_dir: Path):
    parts_path = (
        data_dir / "parts" / "product=PROD" / "test_category=CP"
        / "sub_process=" / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet"
    )
    parts_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT1"], "wafer_id": ["W1"], "part_id": ["P0"],
        "part_txt": [""], "x_coord": [1], "y_coord": [2],
        "hard_bin": [1], "soft_bin": [1],
        "passed": [True], "retest_num": [0],
    }), parts_path)

    td_path = (
        data_dir / "test_data" / "product=PROD" / "test_category=CP"
        / "sub_process=" / "lot_id=LOT1" / "data.parquet"
    )
    td_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT1"], "wafer_id": ["W1"], "part_id": ["P0"],
        "x_coord": [1], "y_coord": [2], "part_txt": [""],
        "test_num": [100], "pin_num": [0], "test_name": ["VDD"],
        "result": [1.23], "passed": ["P"], "lo_limit": [0.0], "hi_limit": [2.0],
        "units": ["V"], "retest_num": [0],
    }), td_path)


def _patched_config(data_dir: Path) -> Config:
    return Config(storage=StorageConfig(
        data_dir=data_dir, database=data_dir / "stdf.duckdb"
    ))


def test_export_lot_long_format(tmp_path, monkeypatch):
    _write(tmp_path)
    monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))
    out = tmp_path / "out.csv"
    result = CliRunner().invoke(cli.main, ["export", "lot", "LOT1", str(out), "--no-pivot"])
    assert result.exit_code == 0, result.output
    text = out.read_text()
    assert "test_name" in text
    assert "VDD" in text
    assert "1.23" in text


def test_export_lot_pivot(tmp_path, monkeypatch):
    _write(tmp_path)
    monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))
    out = tmp_path / "pivot.csv"
    result = CliRunner().invoke(cli.main, ["export", "lot", "LOT1", str(out)])
    assert result.exit_code == 0, result.output
    header = out.read_text().splitlines()[0]
    assert "VDD" in header  # pivoted test_name becomes a column
    assert "part_id" in header
