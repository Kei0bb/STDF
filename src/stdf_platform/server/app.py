"""Read-only HTTP query API over the Parquet store.

A thin FastAPI layer over AnalysisSession: one request = one :memory: DuckDB
session with the canonical views (setup_views), so every consumer computes
yield/Cpk from the same definitions in views.py. No analysis logic lives here.

Built as an APIRouter so a future dashboard backend can mount it via
app.include_router(router); `stdf serve` wraps it in a standalone app
(create_app). A mounting app may put a Config on app.state.stdf_config to
override the per-request Config.load() resolution.

Security: user SQL runs on a connection locked down AFTER view registration —
filesystem access is restricted to data_dir (allowed_directories +
enable_external_access=false, lock_configuration=true) and only a single
SELECT statement is accepted. Result size is capped at server.max_rows.
"""

import math
from datetime import date, datetime
from decimal import Decimal

import duckdb
from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, Response
from pydantic import BaseModel

from .. import __version__
from ..analysis import AnalysisSession
from ..config import Config

router = APIRouter()


class QueryRequest(BaseModel):
    sql: str
    limit: int | None = None    # row cap; server max_rows still applies
    format: str = "json"        # "json" | "csv"


def _resolve_config(request: Request) -> Config:
    """Config for this request: the mounting app's, or STDF_CONFIG/cwd."""
    cfg = getattr(request.app.state, "stdf_config", None)
    return cfg if cfg is not None else Config.load()


def _open_locked_session(config: Config) -> AnalysisSession:
    """Open an AnalysisSession whose connection cannot touch anything but data_dir.

    Views are registered first (setup_views needs glob access), then the
    filesystem allowlist is applied and the configuration is locked so user
    SQL cannot undo it.
    """
    session = AnalysisSession(config.storage.data_dir)
    conn = session.conn
    conn.execute(
        f"SET allowed_directories = ['{session.data_dir.as_posix()}']"
    )
    conn.execute("SET enable_external_access = false")
    conn.execute("SET lock_configuration = true")
    return session


def _check_select_only(sql: str) -> None:
    try:
        statements = duckdb.extract_statements(sql)
    except duckdb.Error as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise HTTPException(
            status_code=400, detail="only a single SELECT statement is allowed"
        )


def _jsonable(value):
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


@router.get("/health")
def health():
    return {"status": "ok", "version": __version__}


@router.get("/api/views")
def list_views(request: Request):
    config = _resolve_config(request)
    session = _open_locked_session(config)
    try:
        return {"views": session.registered}
    finally:
        session.close()


@router.post("/api/query")
def query(req: QueryRequest, request: Request):
    config = _resolve_config(request)
    _check_select_only(req.sql)

    cap = config.server.max_rows
    if req.limit is not None:
        cap = max(0, min(req.limit, cap))

    session = _open_locked_session(config)
    try:
        try:
            cursor = session.conn.execute(req.sql)
        except duckdb.Error as exc:
            # Full DuckDB message: it is the user's debugging feedback.
            raise HTTPException(status_code=400, detail=str(exc))
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchmany(cap + 1)
        truncated = len(rows) > cap
        rows = rows[:cap]
    finally:
        session.close()

    if req.format == "csv":
        import csv
        import io

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(columns)
        writer.writerows(rows)
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=query.csv"},
        )

    return {
        "columns": columns,
        "rows": [[_jsonable(v) for v in row] for row in rows],
        "row_count": len(rows),
        "truncated": truncated,
    }


@router.get("/", response_class=PlainTextResponse)
def index():
    return (
        f"stdf query server {__version__}\n"
        "\n"
        "POST /api/query   {\"sql\": \"SELECT ...\", \"limit\": 100, \"format\": \"json|csv\"}\n"
        "GET  /api/views   available views\n"
        "GET  /health      liveness\n"
        "\n"
        "VSCode: use client/stdf_client.py  (see docs/multi-user-server.md)\n"
    )


def create_app(config: Config | None = None) -> FastAPI:
    """Standalone app for `stdf serve`. Pass config to pin CLI --config/--env."""
    app = FastAPI(title="stdf query server", version=__version__)
    if config is not None:
        app.state.stdf_config = config
    app.include_router(router)
    return app
