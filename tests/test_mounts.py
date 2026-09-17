"""Tests for the single-source DuckDB view module (stdf_platform.mounts)."""

import re

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from stdf_platform.mounts import setup_views


def _write_parts(data_dir: Path):
    path = (
        data_dir / "parts" / "product=PROD" / "test_category=CP"
        / "sub_process=" / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([
        ("part_id", pa.string()),
        ("lot_id", pa.string()), ("wafer_id", pa.string()), ("part_txt", pa.string()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("soft_bin", pa.int64()), ("passed", pa.bool_()), ("retest_num", pa.int64()),
    ])
    table = pa.table({
        "part_id": ["P0", "P1"],
        "lot_id": ["LOT1", "LOT1"], "wafer_id": ["W1", "W1"], "part_txt": ["", ""],
        "x_coord": [1, 2], "y_coord": [1, 2], "soft_bin": [1, 0],
        "passed": [True, False], "retest_num": [0, 0],
    }, schema=schema)
    pq.write_table(table, path)


def _write_two_retests_same_die(data_dir: Path):
    """Same physical CP die (W1, x=1, y=1) probed twice with DIFFERENT part_txt
    serials — must collapse to one row in parts_final."""
    schema = pa.schema([
        ("part_id", pa.string()),
        ("lot_id", pa.string()), ("wafer_id", pa.string()), ("part_txt", pa.string()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("soft_bin", pa.int64()), ("passed", pa.bool_()), ("retest_num", pa.int64()),
    ])
    for retest, sn, passed in [(0, "SN-A", False), (1, "SN-B", True)]:
        path = (
            data_dir / "parts" / "product=PROD" / "test_category=CP"
            / "sub_process=" / "lot_id=LOT1" / "wafer_id=W1"
            / f"retest={retest}" / "data.parquet"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({
            "part_id": [f"P{retest}"],
            "lot_id": ["LOT1"], "wafer_id": ["W1"], "part_txt": [sn],
            "x_coord": [1], "y_coord": [1], "soft_bin": [1],
            "passed": [passed], "retest_num": [retest],
        }, schema=schema), path)


def _write_parts_parquet(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([
        ("part_id", pa.string()), ("part_txt", pa.string()),
        ("lot_id", pa.string()), ("wafer_id", pa.string()),
        ("head_num", pa.int64()), ("site_num", pa.int64()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("hard_bin", pa.int64()), ("soft_bin", pa.int64()),
        ("passed", pa.bool_()), ("test_count", pa.int64()), ("test_time", pa.int64()),
        ("retest_num", pa.int64()),
    ])
    table = pa.table({k: [r[k] for r in rows] for k in schema.names}, schema=schema)
    pq.write_table(table, path)


def _setup_conn(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """本番と同じ setup_views を使う。

    以前は parts_final の SQL をテスト内にコピーしていたため、本体の定義
    (FT の part_txt キー等)が変わってもテストが古い定義のまま通っていた。
    """
    conn = duckdb.connect(":memory:")
    setup_views(conn, data_dir)
    return conn


def test_dedup_ignores_part_txt_for_cp_dies(tmp_path):
    """CP die identity is (wafer, x, y); a per-part part_txt must not defeat
    retest dedup. The latest retest (retest_num DESC) wins."""
    _write_two_retests_same_die(tmp_path)
    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path)
    rows = conn.execute(
        "SELECT COUNT(*) AS n, BOOL_OR(passed) AS passed"
        " FROM parts_final WHERE lot_id='LOT1'"
    ).fetchone()
    assert rows[0] == 1          # collapsed to one die, not summed
    assert rows[1] is True       # kept the latest retest (passing) result


def test_parts_final_takes_latest_retest(tmp_path):
    # retest=0: die (1,1) passed=False, die (2,2) passed=True
    _write_parts_parquet(
        tmp_path / "parts" / "product=PROD" / "test_category=CP" / "sub_process=" /
        "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet",
        [
            {"part_id": "L_W_0", "part_txt": "", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": 1, "y_coord": 1, "hard_bin": 0, "soft_bin": 0, "passed": False,
             "test_count": 1, "test_time": 100, "retest_num": 0},
            {"part_id": "L_W_1", "part_txt": "", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": 2, "y_coord": 2, "hard_bin": 1, "soft_bin": 1, "passed": True,
             "test_count": 1, "test_time": 100, "retest_num": 0},
        ]
    )
    # retest=1: only die (1,1) retested — now passed=True
    _write_parts_parquet(
        tmp_path / "parts" / "product=PROD" / "test_category=CP" / "sub_process=" /
        "lot_id=LOT1" / "wafer_id=W1" / "retest=1" / "data.parquet",
        [
            {"part_id": "L_W_0", "part_txt": "", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": 1, "y_coord": 1, "hard_bin": 1, "soft_bin": 1, "passed": True,
             "test_count": 1, "test_time": 100, "retest_num": 1},
        ]
    )
    conn = _setup_conn(tmp_path)
    rows = conn.execute(
        "SELECT x_coord, y_coord, passed, retest_num FROM parts_final WHERE lot_id='LOT1' ORDER BY x_coord"
    ).fetchall()
    # die (1,1): should pick retest=1 (passed=True)
    assert rows[0] == (1, 1, True, 1)
    # die (2,2): should pick retest=0 (passed=True, only copy)
    assert rows[1] == (2, 2, True, 0)
    assert len(rows) == 2  # full wafer, not just retest dies


def test_setup_views_registers_base_and_final(tmp_path):
    _write_parts(tmp_path)
    conn = duckdb.connect(":memory:")
    registered = setup_views(conn, tmp_path)
    assert "parts" in registered
    assert "parts_final" in registered
    n = conn.execute("SELECT COUNT(*) FROM parts_final WHERE lot_id='LOT1'").fetchone()[0]
    assert n == 2


def test_setup_views_raises_on_empty_dir_naming_the_resolved_path(tmp_path):
    """Was: an empty data_dir returned []. That silence is exactly how a
    mistyped/cwd-relative data_dir went unnoticed until a later Catalog
    Error, so it now raises — naming the path it actually resolved."""
    import pytest
    conn = duckdb.connect(":memory:")
    with pytest.raises(RuntimeError, match=re.escape(str(tmp_path.resolve()))):
        setup_views(conn, tmp_path)
