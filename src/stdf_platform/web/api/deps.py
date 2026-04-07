"""Shared dependencies for FastAPI routes."""

import os
from functools import lru_cache
from pathlib import Path
from typing import Generator

import duckdb


@lru_cache(maxsize=1)
def _get_paths() -> tuple[Path, Path]:
    """Return (data_dir, db_path) from env vars set by CLI."""
    data_dir = Path(os.environ.get("STDF_DATA_DIR", "./data"))
    db_path = Path(os.environ.get("STDF_DB_PATH", "./data/stdf.duckdb"))
    return data_dir, db_path


def _setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> None:
    """Create Parquet-backed views on an open connection."""
    for table in ["lots", "wafers", "parts", "test_data"]:
        path = data_dir / table
        if path.exists():
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table} AS
                SELECT * FROM read_parquet(
                    '{path}/**/*.parquet', hive_partitioning=true
                )
            """)


def get_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """FastAPI dependency: yields a per-request in-memory DuckDB connection.

    Uses :memory: to avoid file-locking conflicts when multiple requests
    run concurrently. Views are rebuilt from Parquet on each request.
    """
    data_dir, _ = _get_paths()
    conn = duckdb.connect(":memory:")
    try:
        _setup_views(conn, data_dir)
        yield conn
    finally:
        conn.close()
