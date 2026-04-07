"""Filter option endpoints — products, lots, wafers."""

from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, Query

from .deps import get_db

router = APIRouter(tags=["filters"])

DB = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]


@router.get("/products")
def get_products(db: DB) -> list[str]:
    try:
        rows = db.execute(
            "SELECT DISTINCT product FROM lots WHERE product IS NOT NULL ORDER BY product"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/test-categories")
def get_test_categories(db: DB) -> list[str]:
    try:
        rows = db.execute(
            "SELECT DISTINCT test_category FROM lots WHERE test_category IS NOT NULL ORDER BY test_category"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/lots")
def get_lots(
    db: DB,
    product: Annotated[list[str], Query()] = [],
    category: Annotated[list[str], Query()] = [],
) -> list[str]:
    try:
        conditions, params = [], []
        if product:
            ph = ", ".join("?" * len(product))
            conditions.append(f"product IN ({ph})")
            params.extend(product)
        if category:
            ph = ", ".join("?" * len(category))
            conditions.append(f"test_category IN ({ph})")
            params.extend(category)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = db.execute(
            f"SELECT DISTINCT lot_id FROM lots {where} ORDER BY lot_id", params
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/wafers")
def get_wafers(
    db: DB,
    lot: Annotated[list[str], Query()] = [],
) -> dict:
    """Returns wafer list + whether this is CP (has_wafers) or FT data."""
    if not lot:
        return {"wafer_ids": [], "has_wafers": False}
    try:
        ph = ", ".join("?" * len(lot))
        rows = db.execute(
            f"""SELECT DISTINCT wafer_id FROM wafers
                WHERE lot_id IN ({ph}) AND wafer_id IS NOT NULL AND wafer_id != ''
                ORDER BY wafer_id""",
            lot,
        ).fetchall()
        ids = [r[0] for r in rows]
        return {"wafer_ids": ids, "has_wafers": len(ids) > 0}
    except Exception:
        return {"wafer_ids": [], "has_wafers": False}
