"""CSV export endpoint — most important feature."""

import io
from typing import Annotated, Literal

import duckdb
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .deps import get_db

router = APIRouter(tags=["export"])
DB = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]


class ExportRequest(BaseModel):
    lots: list[str]
    wafers: list[str] | None = None       # None = all wafers
    test_nums: list[int] | None = None    # None = all tests
    format: Literal["pivot", "long"] = "pivot"
    filename: str = "stdf_export.csv"


def _build_where(
    lots: list[str],
    wafers: list[str] | None,
    test_nums: list[int] | None,
) -> tuple[str, list]:
    params: list = []
    conds: list[str] = []

    lot_ph = ", ".join("?" * len(lots))
    conds.append(f"td.lot_id IN ({lot_ph})")
    params.extend(lots)

    if wafers:
        w_ph = ", ".join("?" * len(wafers))
        conds.append(f"td.wafer_id IN ({w_ph})")
        params.extend(wafers)

    if test_nums:
        t_ph = ", ".join("?" * len(test_nums))
        conds.append(f"td.test_num IN ({t_ph})")
        params.extend(test_nums)

    return " AND ".join(conds), params


@router.post("/export/csv")
def export_csv(req: ExportRequest, db: DB) -> StreamingResponse:
    where, params = _build_where(req.lots, req.wafers, req.test_nums)

    if req.format == "pivot":
        # Fetch long format; pandas pivot_table handles the wide transform below
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

    if req.format == "pivot":
        # DuckDB dynamic PIVOT doesn't support parameters — fetch long format
        # then pivot with pandas (already a dependency)
        df = db.execute(sql, params).fetchdf()
        index_cols = ["lot_id", "wafer_id", "part_id", "x_coord", "y_coord",
                      "hard_bin", "soft_bin", "part_passed"]
        df = df.pivot_table(index=index_cols, columns="test_name",
                            values="result", aggfunc="first")
        df.columns.name = None
        df = df.reset_index()
    else:
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


@router.post("/export/preview")
def export_preview(req: ExportRequest, db: DB) -> dict:
    """Return row/column count estimate without generating the full CSV."""
    where, params = _build_where(req.lots, req.wafers, req.test_nums)
    try:
        row = db.execute(f"""
            SELECT
                COUNT(DISTINCT td.part_id) AS parts,
                COUNT(DISTINCT td.test_num) AS tests,
                COUNT(*) AS total_rows
            FROM test_data td
            WHERE {where}
        """, params).fetchone()
        parts, tests, total = row or (0, 0, 0)
        pivot_rows = parts if parts else 0
        return {
            "parts": int(parts or 0),
            "tests": int(tests or 0),
            "long_rows": int(total or 0),
            "pivot_rows": int(pivot_rows),
        }
    except Exception:
        return {"parts": 0, "tests": 0, "long_rows": 0, "pivot_rows": 0}
