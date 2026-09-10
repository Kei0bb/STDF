"""Read-only HTTP query API over the Parquet store.

A thin FastAPI layer over AnalysisSession: one request = one :memory: DuckDB
session with the canonical views (setup_views), so every consumer computes
yield/Cpk from the same definitions in mounts.py. No analysis logic lives here.

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
import re
import threading
from datetime import date, datetime
from decimal import Decimal
from importlib.resources import files

import duckdb
from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from pydantic import BaseModel

from .. import __version__
from ..analysis import AnalysisSession
from ..analysis.library import sql_literal
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
    session = AnalysisSession(config.storage.data_dir, config=config)
    conn = session.conn
    # Resource caps must be set before lock_configuration; the DuckDB defaults
    # (80% of RAM, all threads) let one request's operators consume the host.
    # DuckDB has no statement_timeout, so a runaway query is cancelled by
    # Connection.interrupt() from the timer in query() below.
    limit = config.server.memory_limit
    if not re.fullmatch(r"\d+(\.\d+)?\s*(KB|MB|GB|TB|%)?", limit, re.IGNORECASE):
        session.close()
        raise RuntimeError(f"invalid server.memory_limit: {limit!r}")
    conn.execute(f"SET memory_limit = '{limit}'")
    conn.execute(f"SET threads = {int(config.server.threads)}")
    conn.execute(
        f"SET allowed_directories = ['{sql_literal(session.data_dir.as_posix())}']"
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
    """JSON 化可能な値へ再帰的に正規化する。

    DuckDB の LIST/STRUCT はネストした値をそのまま返す。トップレベルだけを
    見るとネストした NaN が Starlette の allow_nan=False で 500、非 UTF8 の
    ネスト BLOB が jsonable_encoder の decode で 500 になる（検証済み）。
    """
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
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


@router.get("/api/schema")
def schema(request: Request):
    config = _resolve_config(request)
    session = _open_locked_session(config)
    try:
        tables = []
        for name in session.registered:
            # A view is registered when its backing directory exists at
            # session-open time, but read_parquet globs are resolved lazily
            # on each query — a view whose files vanish or were never
            # written (e.g. an empty chipid/ dir mid-ingest) can still be
            # registered yet fail here. Isolate that failure to this one
            # entry instead of 500-ing the whole sidebar.
            try:
                cols = session.conn.execute(f"DESCRIBE {name}").fetchall()
            except duckdb.Error as exc:
                tables.append({"name": name, "error": str(exc)})
                continue
            tables.append({
                "name": name,
                "columns": [{"name": c[0], "type": c[1]} for c in cols],
            })
        return {"tables": tables}
    finally:
        session.close()


@router.post("/api/query")
def query(req: QueryRequest, request: Request):
    config = _resolve_config(request)
    _check_select_only(req.sql)
    if req.format not in ("json", "csv"):
        raise HTTPException(status_code=400,
                            detail="format must be 'json' or 'csv'")

    cap = config.server.max_rows
    if req.limit is not None:
        cap = max(0, min(req.limit, cap))

    timeout = config.server.query_timeout_seconds
    session = _open_locked_session(config)
    timer = None
    try:
        try:
            if timeout and timeout > 0:
                timer = threading.Timer(timeout, session.conn.interrupt)
                timer.daemon = True
                timer.start()
            cursor = session.conn.execute(req.sql)
            columns = [d[0] for d in cursor.description]
            rows = cursor.fetchmany(cap + 1)
        except duckdb.InterruptException:
            raise HTTPException(
                status_code=504,
                detail=f"query timed out after {timeout}s (server limit)",
            )
        except duckdb.Error as exc:
            # Full DuckDB message: it is the user's debugging feedback.
            raise HTTPException(status_code=400, detail=str(exc))
        truncated = len(rows) > cap
        rows = rows[:cap]
    finally:
        if timer is not None:
            timer.cancel()
        session.close()

    if req.format == "csv":
        import csv
        import io

        def _csv_cell(v):
            s = _jsonable(v)
            if isinstance(s, str) and s[:1] in ("=", "+", "-", "@", "\t", "\r"):
                return "'" + s   # 表計算ソフトの数式として解釈させない
            return s

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([_csv_cell(v) for v in columns])
        writer.writerows([_csv_cell(v) for v in row] for row in rows)
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


@router.get("/", response_class=HTMLResponse)
def index():
    return (files("stdf_platform.server") / "console.html").read_text(encoding="utf-8")


@router.get("/api", response_class=PlainTextResponse)
def api_index():
    return (
        f"stdf query server {__version__}\n"
        "\n"
        "POST /api/query   {\"sql\": \"SELECT ...\", \"limit\": 100, \"format\": \"json|csv\"}\n"
        "GET  /api/views   available views\n"
        "GET  /api/schema  tables + columns\n"
        "GET  /health      liveness\n"
        "\n"
        "Browser: GET /  — self-contained SQL console\n"
        "VSCode: use client/stdf_client.py  (see docs/multi-user-server.md)\n"
    )


def create_app(config: Config | None = None) -> FastAPI:
    """Standalone app for `stdf serve`. Pass config to pin CLI --config/--env."""
    app = FastAPI(title="stdf query server", version=__version__)
    if config is not None:
        app.state.stdf_config = config
    app.include_router(router)
    return app
