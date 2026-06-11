"""FastAPI web server for stdf."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .api.filters import router as filters_router
from .api.data import router as data_router
from .api.export import router as export_router
from .api.reports import router as reports_router
from .api.deps import get_data_dir


app = FastAPI(title="stdf", version="2.0", docs_url="/api/docs")

app.include_router(filters_router, prefix="/api")
app.include_router(data_router, prefix="/api")
app.include_router(export_router, prefix="/api")
app.include_router(reports_router, prefix="/api")

_static = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_static)), name="static")

_reports = get_data_dir() / "reports"
_reports.mkdir(parents=True, exist_ok=True)
app.mount("/reports", StaticFiles(directory=str(_reports), html=True), name="reports")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(_static / "index.html"))
