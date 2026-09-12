from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from stdf_platform.mounts import _DEDUP_UNIT, _FLAG_KEY, die_key_expr, ft_identity, setup_views


def _write_legacy_parts(data_dir: Path, rows):
    """pre-part_serial スキーマ（列が無い）の parts ファイル。"""
    p = (data_dir / "parts" / "product=PROD" / "test_category=CP"
         / "sub_process=CP1" / "lot_id=LOT" / "wafer_id=W1"
         / "retest=0" / "data.parquet")
    p.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([
        ("part_id", pa.string()), ("part_txt", pa.string()),
        ("lot_id", pa.string()), ("wafer_id", pa.string()),
        ("head_num", pa.int64()), ("site_num", pa.int64()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("hard_bin", pa.int64()), ("soft_bin", pa.int64()),
        ("passed", pa.bool_()), ("test_count", pa.int64()),
        ("test_time", pa.int64()), ("retest_num", pa.int64()),
    ])
    pq.write_table(pa.table({k: [r[k] for r in rows] for k in schema.names}, schema=schema), p)
    return p


def test_legacy_parts_without_part_serial_mounts_with_die_key(tmp_path):
    _write_legacy_parts(tmp_path, [
        {"part_id": "L_W_0", "part_txt": "", "lot_id": "LOT", "wafer_id": "W1",
         "head_num": 1, "site_num": 1, "x_coord": 0, "y_coord": 0,
         "hard_bin": 1, "soft_bin": 1, "passed": True,
         "test_count": 1, "test_time": 0, "retest_num": 0},
    ])
    con = duckdb.connect()
    setup_views(con, tmp_path)
    cols = [c[0] for c in con.execute("DESCRIBE parts_final").fetchall()]
    assert "die_key" in cols
    assert con.execute("SELECT COUNT(*) FROM parts_final").fetchone()[0] == 1


def test_parts_only_store_yields_without_conversion_error(tmp_path):
    """mounts.py の lot_product NULL arm が INT32 になり 'LOT' の比較で
    ConversionException になっていた退行の防止。"""
    _write_legacy_parts(tmp_path, [
        {"part_id": "L_W_0", "part_txt": "", "lot_id": "LOT", "wafer_id": "W1",
         "head_num": 1, "site_num": 1, "x_coord": 0, "y_coord": 0,
         "hard_bin": 1, "soft_bin": 1, "passed": True,
         "test_count": 1, "test_time": 0, "retest_num": 0},
    ])
    con = duckdb.connect()
    setup_views(con, tmp_path)
    row = con.execute(
        "SELECT lot_id, probed, good, yield_pct FROM wafer_yield_final"
    ).fetchone()
    assert row == ("LOT", 1, 1, 100.0)


def test_identity_helpers_are_consistent():
    assert ft_identity() in _DEDUP_UNIT
    assert "part_serial" in die_key_expr()
    assert "part_serial" in _FLAG_KEY
