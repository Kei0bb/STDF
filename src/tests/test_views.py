"""Tests for the single-source DuckDB view module (stdf_platform.views)."""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from stdf_platform.views import _DEDUP_UNIT, setup_views


def _write_parts(data_dir: Path):
    path = (
        data_dir / "parts" / "product=PROD" / "test_category=CP"
        / "sub_process=" / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([
        ("lot_id", pa.string()), ("wafer_id", pa.string()), ("part_txt", pa.string()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("soft_bin", pa.int64()), ("passed", pa.bool_()), ("retest_num", pa.int64()),
    ])
    table = pa.table({
        "lot_id": ["LOT1", "LOT1"], "wafer_id": ["W1", "W1"], "part_txt": ["", ""],
        "x_coord": [1, 2], "y_coord": [1, 2], "soft_bin": [1, 0],
        "passed": [True, False], "retest_num": [0, 0],
    }, schema=schema)
    pq.write_table(table, path)


def test_dedup_unit_constant():
    assert _DEDUP_UNIT == "wafer_id, x_coord, y_coord, part_txt"


def test_setup_views_registers_base_and_final(tmp_path):
    _write_parts(tmp_path)
    conn = duckdb.connect(":memory:")
    registered = setup_views(conn, tmp_path)
    assert "parts" in registered
    assert "parts_final" in registered
    n = conn.execute("SELECT COUNT(*) FROM parts_final WHERE lot_id='LOT1'").fetchone()[0]
    assert n == 2


def test_setup_views_empty_dir_returns_empty(tmp_path):
    conn = duckdb.connect(":memory:")
    assert setup_views(conn, tmp_path) == []
