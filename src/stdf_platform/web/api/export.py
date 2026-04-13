"""CSV export endpoint."""

import io
import logging
from threading import Lock
from typing import Annotated, Literal

import duckdb
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)

from .deps import get_db

router = APIRouter(tags=["export"])
DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]


class ExportRequest(BaseModel):
    lots: list[str]
    wafers: list[str] | None = None
    test_nums: list[int] | None = None
    format: Literal["pivot", "long"] = "pivot"
    filename: str = "stdf_export.csv"


def _build_where(
    lots: list[str],
    wafers: list[str] | None,
    test_nums: list[int] | None,
) -> tuple[str, list]:
    """Return (WHERE clause, params list) for DuckDB positional params."""
    lot_ph = ",".join(["?" for _ in lots])
    conds = [f"td.lot_id IN ({lot_ph})"]
    params: list = list(lots)

    if wafers:
        w_ph = ",".join(["?" for _ in wafers])
        conds.append(f"td.wafer_id IN ({w_ph})")
        params.extend(wafers)

    if test_nums:
        t_ph = ",".join(["?" for _ in test_nums])
        conds.append(f"td.test_num IN ({t_ph})")
        params.extend(test_nums)

    return " AND ".join(conds), params


@router.post("/export/csv")
def export_csv(req: ExportRequest, db_tuple: DB) -> StreamingResponse:
    db, lock = db_tuple
    where, params = _build_where(req.lots, req.wafers, req.test_nums)
    try:
        if req.format == "pivot":
            sql = f"""
                SELECT
                    td.lot_id, td.wafer_id, td.part_id,
                    td.x_coord, td.y_coord,
                    p.hard_bin, p.soft_bin,
                    p.passed AS part_passed,
                    td.test_name,
                    td.result
                FROM test_data td
                JOIN parts p ON td.part_id = p.part_id AND td.lot_id = p.lot_id
                WHERE {where}
                ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_name
            """
            with lock:
                df = db.execute(sql, params).fetchdf()
            index_cols = ["lot_id", "wafer_id", "part_id", "x_coord", "y_coord",
                          "hard_bin", "soft_bin", "part_passed"]
            df = df.pivot_table(index=index_cols, columns="test_name",
                                values="result", aggfunc="first")
            df.columns.name = None
            df = df.reset_index()
        else:
            sql = f"""
                SELECT
                    td.lot_id, td.wafer_id, td.part_id,
                    td.x_coord, td.y_coord,
                    p.hard_bin, p.soft_bin,
                    td.test_num, td.test_name,
                    td.result, td.passed,
                    td.lo_limit, td.hi_limit, td.units
                FROM test_data td
                JOIN parts p ON td.part_id = p.part_id AND td.lot_id = p.lot_id
                WHERE {where}
                ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_num
            """
            with lock:
                df = db.execute(sql, params).fetchdf()

        buf = io.StringIO()
        df.to_csv(buf, index=False)
        csv_bytes = buf.getvalue().encode("utf-8")

        return StreamingResponse(
            iter([csv_bytes]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{req.filename}"',
                "Content-Length": str(len(csv_bytes)),
                "X-Row-Count": str(len(df)),
            },
        )
    except Exception as e:
        logger.error("export_csv failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export/preview")
def export_preview(req: ExportRequest, db_tuple: DB) -> dict:
    db, lock = db_tuple
    where, params = _build_where(req.lots, req.wafers, req.test_nums)
    try:
        with lock:
            row = db.execute(f"""
                SELECT
                    COUNT(DISTINCT td.part_id) AS parts,
                    COUNT(DISTINCT td.test_num) AS tests,
                    COUNT(*) AS total_rows
                FROM test_data td
                WHERE {where}
            """, params).fetchone()
        parts, tests, total = row or (0, 0, 0)
        return {
            "parts": int(parts or 0),
            "tests": int(tests or 0),
            "long_rows": int(total or 0),
            "pivot_rows": int(parts or 0),
        }
    except Exception as e:
        logger.warning("export_preview failed: %s", e)
        return {"parts": 0, "tests": 0, "long_rows": 0, "pivot_rows": 0}
