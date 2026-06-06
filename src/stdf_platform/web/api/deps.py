"""Shared dependencies for FastAPI routes."""

import os
from pathlib import Path
from threading import Lock

import duckdb
from fastapi import Request


# ── path helper ──────────────────────────────────────────────────────────────

def get_data_dir() -> Path:
    return Path(os.environ.get("STDF_DATA_DIR", "./data"))


# Dedup identity within a (lot, retest) group: CP uses die location, FT uses the
# package 2D barcode (PART_TXT). Mirrors stdf_platform.database._DEDUP_UNIT.
_DEDUP_UNIT = (
    "CASE WHEN test_category = 'FT' THEN part_txt "
    "ELSE CONCAT(wafer_id, '|', x_coord, '|', y_coord) END"
)


# ── DuckDB view setup (called once at lifespan) ───────────────────────────────

def setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> list[str]:
    """Register Parquet glob views and final-bin merge VIEWs."""
    registered = []
    for table in ["lots", "wafers", "parts", "test_data", "chipid"]:
        path = data_dir / table
        if path.exists():
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table} AS
                SELECT * FROM read_parquet(
                    '{path.as_posix()}/**/*.parquet', hive_partitioning=true
                )
            """)
            registered.append(table)

    if "parts" in registered:
        conn.execute(f"""
            CREATE OR REPLACE VIEW parts_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, {_DEDUP_UNIT}
                    ORDER BY retest_num DESC
                ) AS rn FROM parts
            ) WHERE rn = 1
        """)
        registered.append("parts_final")

    if "test_data" in registered:
        conn.execute(f"""
            CREATE OR REPLACE VIEW test_data_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, {_DEDUP_UNIT}, test_num, pin_num
                    ORDER BY retest_num DESC
                ) AS rn FROM test_data
            ) WHERE rn = 1
        """)
        registered.append("test_data_final")

    if "chipid" in registered:
        # die identity = decoded ChipID (efuse_raw), not positional occurrence.
        conn.execute("""
            CREATE OR REPLACE VIEW chipid_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, efuse_raw
                    ORDER BY retest_num DESC
                ) AS rn FROM chipid
            ) WHERE rn = 1
        """)
        registered.append("chipid_final")

    return registered


# ── FastAPI dependency ────────────────────────────────────────────────────────

def get_db(request: Request) -> tuple[duckdb.DuckDBPyConnection, Lock]:
    """Return (db_connection, lock) from app state. Use: db, lock = Depends(get_db)."""
    return request.app.state.db, request.app.state.db_lock
