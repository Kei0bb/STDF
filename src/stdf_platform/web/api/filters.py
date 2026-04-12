"""Filter option endpoints — products, lots, wafers."""

from typing import Annotated

from clickhouse_connect.driver import Client
from fastapi import APIRouter, Depends, Query

from .deps import get_ch

router = APIRouter(tags=["filters"])
CH = Annotated[Client, Depends(get_ch)]


@router.get("/products")
def get_products(ch: CH) -> list[str]:
    try:
        rows = ch.query(
            "SELECT DISTINCT product FROM lots WHERE product != '' ORDER BY product"
        ).result_rows
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/test-categories")
def get_test_categories(ch: CH) -> list[str]:
    try:
        rows = ch.query(
            "SELECT DISTINCT test_category FROM lots WHERE test_category != '' ORDER BY test_category"
        ).result_rows
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/lots")
def get_lots(
    ch: CH,
    product: Annotated[list[str], Query()] = [],
    category: Annotated[list[str], Query()] = [],
) -> list[str]:
    try:
        conds, params = [], {}
        if product:
            conds.append("product IN {products:Array(String)}")
            params["products"] = product
        if category:
            conds.append("test_category IN {categories:Array(String)}")
            params["categories"] = category
        where = f"WHERE {' AND '.join(conds)}" if conds else ""
        rows = ch.query(
            f"SELECT DISTINCT lot_id FROM lots {where} ORDER BY lot_id",
            parameters=params,
        ).result_rows
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/wafers")
def get_wafers(
    ch: CH,
    lot: Annotated[list[str], Query()] = [],
) -> dict:
    if not lot:
        return {"wafer_ids": [], "has_wafers": False}
    try:
        rows = ch.query(
            """SELECT DISTINCT wafer_id FROM wafers
               WHERE lot_id IN {lot_ids:Array(String)}
                 AND wafer_id != ''
               ORDER BY wafer_id""",
            parameters={"lot_ids": lot},
        ).result_rows
        ids = [r[0] for r in rows]
        return {"wafer_ids": ids, "has_wafers": len(ids) > 0}
    except Exception:
        return {"wafer_ids": [], "has_wafers": False}
