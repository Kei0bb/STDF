import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path


def _write_parts_parquet(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([
        ("part_id", pa.string()), ("lot_id", pa.string()), ("wafer_id", pa.string()),
        ("head_num", pa.int64()), ("site_num", pa.int64()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("hard_bin", pa.int64()), ("soft_bin", pa.int64()),
        ("passed", pa.bool_()), ("test_count", pa.int64()), ("test_time", pa.int64()),
        ("retest_num", pa.int64()),
    ])
    table = pa.table({k: [r[k] for r in rows] for k in schema.names}, schema=schema)
    pq.write_table(table, path)


def _setup_conn(data_dir: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    parts_path = data_dir / "parts"
    conn.execute(f"""
        CREATE VIEW parts AS
        SELECT * FROM read_parquet('{parts_path.as_posix()}/**/*.parquet', hive_partitioning=true)
    """)
    conn.execute("""
        CREATE VIEW parts_final AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY lot_id, wafer_id, x_coord, y_coord
                ORDER BY retest_num DESC
            ) AS rn FROM parts
        ) WHERE rn = 1
    """)
    return conn


def test_parts_final_takes_latest_retest(tmp_path):
    # retest=0: die (1,1) passed=False, die (2,2) passed=True
    _write_parts_parquet(
        tmp_path / "parts" / "product=PROD" / "test_category=CP" / "sub_process=" /
        "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet",
        [
            {"part_id": "L_W_0", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": 1, "y_coord": 1, "hard_bin": 0, "soft_bin": 0, "passed": False,
             "test_count": 1, "test_time": 100, "retest_num": 0},
            {"part_id": "L_W_1", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": 2, "y_coord": 2, "hard_bin": 1, "soft_bin": 1, "passed": True,
             "test_count": 1, "test_time": 100, "retest_num": 0},
        ]
    )
    # retest=1: only die (1,1) retested — now passed=True
    _write_parts_parquet(
        tmp_path / "parts" / "product=PROD" / "test_category=CP" / "sub_process=" /
        "lot_id=LOT1" / "wafer_id=W1" / "retest=1" / "data.parquet",
        [
            {"part_id": "L_W_0", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
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


def test_parts_final_fail_rate_uses_full_wafer_denominator(tmp_path):
    _write_parts_parquet(
        tmp_path / "parts" / "product=PROD" / "test_category=CP" / "sub_process=" /
        "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet",
        [
            {"part_id": f"L_W_{i}", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": i, "y_coord": 0, "hard_bin": 1 if i < 8 else 0,
             "soft_bin": 1 if i < 8 else 0, "passed": i < 8,
             "test_count": 1, "test_time": 100, "retest_num": 0}
            for i in range(10)
        ]
    )
    conn = _setup_conn(tmp_path)
    total = conn.execute("SELECT COUNT(*) FROM parts_final WHERE lot_id='LOT1'").fetchone()[0]
    assert total == 10  # all 10 dies present
