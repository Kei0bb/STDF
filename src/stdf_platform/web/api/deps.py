"""Shared dependencies for FastAPI routes."""

import os
from pathlib import Path
from threading import Lock

import duckdb
from fastapi import Request

from stdf_platform.views import setup_views  # re-exported for server.py imports


# ── path helper ──────────────────────────────────────────────────────────────

def get_data_dir() -> Path:
    return Path(os.environ.get("STDF_DATA_DIR", "./data"))


# ── FastAPI dependency ────────────────────────────────────────────────────────

def get_db(request: Request) -> tuple[duckdb.DuckDBPyConnection, Lock]:
    """Return (db_connection, lock) from app state. Use: db, lock = Depends(get_db)."""
    return request.app.state.db, request.app.state.db_lock
