"""Shared dependencies for FastAPI routes."""

import os
from pathlib import Path

import duckdb

from stdf_platform.views import setup_views  # re-exported for server.py imports


# ── path helper ──────────────────────────────────────────────────────────────

def get_data_dir() -> Path:
    return Path(os.environ.get("STDF_DATA_DIR", "./data"))


# ── FastAPI dependency: one fresh in-memory connection per request ────────────

def get_db():
    """Yield a per-request DuckDB :memory: connection with views registered.

    Parquet is the source of truth and connection setup is cheap, so each
    request gets its own connection — no shared singleton, no global lock,
    no read serialization across users.
    """
    conn = duckdb.connect(":memory:")
    try:
        setup_views(conn, get_data_dir())
        yield conn
    finally:
        conn.close()
