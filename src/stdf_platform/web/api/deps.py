"""Shared dependencies for FastAPI routes."""

import os
from functools import lru_cache
from pathlib import Path
from typing import Generator

import duckdb
import clickhouse_connect
from clickhouse_connect.driver import Client
from fastapi import Request


# ── path helpers (used by DuckDB fallback) ────────────────────────────────

@lru_cache(maxsize=1)
def _get_paths() -> tuple[Path, Path]:
    data_dir = Path(os.environ.get("STDF_DATA_DIR", "./data"))
    db_path = Path(os.environ.get("STDF_DB_PATH", "./data/stdf.duckdb"))
    return data_dir, db_path


# ── ClickHouse (primary) ──────────────────────────────────────────────────

def create_ch_client() -> Client | None:
    """Create a ClickHouse client from env vars. Returns None if CH_HOST not set."""
    host = os.environ.get("STDF_CH_HOST", "")
    if not host:
        return None
    return clickhouse_connect.get_client(
        host=host,
        port=int(os.environ.get("STDF_CH_PORT", "8123")),
        database=os.environ.get("STDF_CH_DB", "stdf"),
        username=os.environ.get("STDF_CH_USER", "default"),
        password=os.environ.get("STDF_CH_PASS", ""),
        connect_timeout=10,
        send_receive_timeout=120,
    )


def get_ch(request: Request) -> Client:
    """FastAPI dependency: return the shared ClickHouse client from app state."""
    return request.app.state.ch


# ── DuckDB (fallback for CLI / query.py) ──────────────────────────────────

def _setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> None:
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
    """Per-request DuckDB :memory: connection (Parquet-backed). Used by CLI tools."""
    data_dir, _ = _get_paths()
    conn = duckdb.connect(":memory:")
    try:
        _setup_views(conn, data_dir)
        yield conn
    finally:
        conn.close()
