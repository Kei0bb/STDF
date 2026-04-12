"""FastAPI web server for stdf2pq."""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock

import duckdb
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .api.deps import get_data_dir, setup_views
from .api.filters import router as filters_router
from .api.data import router as data_router
from .api.export import router as export_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create DuckDB singleton connection at startup; close at shutdown."""
    data_dir = get_data_dir()
    con = duckdb.connect(":memory:")
    registered = setup_views(con, data_dir)
    app.state.db = con
    app.state.db_lock = Lock()
    if registered:
        print(f"[server] DuckDB views registered: {', '.join(registered)}")
    else:
        print(f"[server] Warning: no Parquet data found in {data_dir}")
    yield
    con.close()


app = FastAPI(title="stdf2pq", version="2.0", docs_url="/api/docs", lifespan=lifespan)

app.include_router(filters_router, prefix="/api")
app.include_router(data_router, prefix="/api")
app.include_router(export_router, prefix="/api")

_static = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_static)), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(_static / "index.html"))
