"""CSV export endpoint — most important feature."""

import io
from typing import Annotated, Literal

import pandas as pd
from clickhouse_connect.driver import Client
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .deps import get_ch

router = APIRouter(tags=["export"])
CH = Annotated[Client, Depends(get_ch)]


class ExportRequest(BaseModel):
    lots: list[str]
    wafers: list[str] | None = None
    test_nums: list[int] | None = None
    format: Literal["pivot", "long"] = "pivot"
    filename: str = "stdf_export.csv"


def _build_params(
    lots: list[str],
    wafers: list[str] | None,
    test_nums: list[int] | None,
) -> tuple[str, dict]:
    """Return (WHERE clause, parameters dict) for ClickHouse query."""
    conds: list[str] = ["td.lot_id IN {lot_ids:Array(String)}"]
    params: dict = {"lot_ids": lots}

    if wafers:
        conds.append("td.wafer_id IN {wafer_ids:Array(String)}")
        params["wafer_ids"] = wafers

    if test_nums:
        conds.append("td.test_num IN {test_nums:Array(Int32)}")
        params["test_nums"] = test_nums

    return " AND ".join(conds), params


@router.post("/export/csv")
def export_csv(req: ExportRequest, ch: CH) -> StreamingResponse:
    where, params = _build_params(req.lots, req.wafers, req.test_nums)

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
        df = ch.query_df(sql, parameters=params)
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
        df = ch.query_df(sql, parameters=params)

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
def export_preview(req: ExportRequest, ch: CH) -> dict:
    where, params = _build_params(req.lots, req.wafers, req.test_nums)
    try:
        row = ch.query(f"""
            SELECT
                countDistinct(td.part_id) AS parts,
                countDistinct(td.test_num) AS tests,
                count() AS total_rows
            FROM test_data td
            WHERE {where}
        """, parameters=params).first_row
        parts, tests, total = row or (0, 0, 0)
        return {
            "parts": int(parts or 0),
            "tests": int(tests or 0),
            "long_rows": int(total or 0),
            "pivot_rows": int(parts or 0),
        }
    except Exception:
        return {"parts": 0, "tests": 0, "long_rows": 0, "pivot_rows": 0}
