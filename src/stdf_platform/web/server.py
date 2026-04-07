"""FastAPI web server for stdf2pq."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .api.filters import router as filters_router
from .api.data import router as data_router
from .api.export import router as export_router

app = FastAPI(title="stdf2pq", version="2.0", docs_url="/api/docs")

app.include_router(filters_router, prefix="/api")
app.include_router(data_router, prefix="/api")
app.include_router(export_router, prefix="/api")

_static = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_static)), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(_static / "index.html"))
