"""CLI `stdf db verify-flags` — retest_flag/exec_seq invariant checks.

Config-patching pattern copied from test_export_lot.py.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from click.testing import CliRunner

from stdf_platform import cli
from stdf_platform.config import Config, StorageConfig
from stdf_platform.storage import TEST_DATA_SCHEMA


def _patched_config(data_dir: Path) -> Config:
    return Config(storage=StorageConfig(data_dir=data_dir, database=data_dir / "stdf.duckdb"))


def _write_test_data_file(path: Path, schema: pa.Schema, **cols):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(cols, schema=schema), path)


def _clean_row(**overrides):
    row = {
        "lot_id": ["LOT1"], "wafer_id": ["W1"], "part_id": ["P0"], "part_txt": [""],
        "x_coord": [1], "y_coord": [1], "test_num": [1], "test_name": ["VCC"],
        "rec_type": ["PTR"], "lo_limit": [0.9], "hi_limit": [1.1], "units": ["V"],
        "result": [1.0], "passed": ["P"], "retest_num": [0],
        "pin_num": pa.array([None], type=pa.int64()), "pin_name": [None],
        "exec_seq": [0], "retest_flag": [0],
    }
    row.update(overrides)
    return row


def test_verify_flags_clean_store_exits_zero(tmp_path, monkeypatch):
    td_path = (
        tmp_path / "test_data" / "product=PROD" / "test_category=CP"
        / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet"
    )
    _write_test_data_file(td_path, TEST_DATA_SCHEMA, **_clean_row())

    monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))
    result = CliRunner().invoke(cli.main, ["db", "verify-flags"])

    assert result.exit_code == 0, result.output
    assert "OK" in result.output
    assert "FAILED" not in result.output


def test_verify_flags_broken_store_exits_one(tmp_path, monkeypatch):
    # (a) NULL-flag row (old-schema / pre-flag file): stray wafer WOLD.
    old_schema = pa.schema([f for f in TEST_DATA_SCHEMA if f.name not in ("exec_seq", "retest_flag")])
    old_row = _clean_row(wafer_id=["WOLD"])
    del old_row["exec_seq"]
    del old_row["retest_flag"]
    old_path = (
        tmp_path / "test_data" / "product=PROD" / "test_category=CP"
        / "lot_id=LOT1" / "wafer_id=WOLD" / "retest=0" / "data.parquet"
    )
    _write_test_data_file(old_path, old_schema, **old_row)

    # (b) Orphaned key: only row for this (die, test) has retest_flag=1, i.e.
    # MIN(retest_flag) != 0 — no row reads as "current" for test_data_final.
    broken_path = (
        tmp_path / "test_data" / "product=PROD" / "test_category=CP"
        / "lot_id=LOT1" / "wafer_id=W2" / "retest=0" / "data.parquet"
    )
    _write_test_data_file(
        broken_path, TEST_DATA_SCHEMA,
        **_clean_row(wafer_id=["W2"], x_coord=[2], y_coord=[2], retest_flag=[1]),
    )

    monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))
    result = CliRunner().invoke(cli.main, ["db", "verify-flags"])

    assert result.exit_code == 1, result.output
    assert "FAILED" in result.output


def test_verify_flags_lot_filter(tmp_path, monkeypatch):
    _write_test_data_file(
        tmp_path / "test_data" / "product=PROD" / "test_category=CP"
        / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet",
        TEST_DATA_SCHEMA, **_clean_row(),
    )
    _write_test_data_file(
        tmp_path / "test_data" / "product=PROD" / "test_category=CP"
        / "lot_id=LOT2" / "wafer_id=W1" / "retest=0" / "data.parquet",
        TEST_DATA_SCHEMA, **_clean_row(lot_id=["LOT2"], retest_flag=[1]),
    )

    monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))

    # LOT2 alone is broken (orphaned key), but --lot LOT1 scopes the check
    # to the clean lot only.
    result = CliRunner().invoke(cli.main, ["db", "verify-flags", "--lot", "LOT1"])
    assert result.exit_code == 0, result.output

    result_all = CliRunner().invoke(cli.main, ["db", "verify-flags"])
    assert result_all.exit_code == 1, result_all.output
