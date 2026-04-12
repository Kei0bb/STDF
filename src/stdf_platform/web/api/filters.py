"""Filter option endpoints — products, lots, wafers."""

from threading import Lock
from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, Query

from .deps import get_db

router = APIRouter(tags=["filters"])
DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]


@router.get("/products")
def get_products(db_tuple: DB) -> list[str]:
    db, lock = db_tuple
    try:
        with lock:
            rows = db.execute(
                "SELECT DISTINCT product FROM lots WHERE product != '' ORDER BY product"
            ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/test-categories")
def get_test_categories(db_tuple: DB) -> list[str]:
    db, lock = db_tuple
    try:
        with lock:
            rows = db.execute(
                "SELECT DISTINCT test_category FROM lots WHERE test_category != '' ORDER BY test_category"
            ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/lots")
def get_lots(
    db_tuple: DB,
    product: Annotated[list[str], Query()] = [],
    category: Annotated[list[str], Query()] = [],
) -> list[str]:
    db, lock = db_tuple
    try:
        conds, params = [], []
        if product:
            placeholders = ",".join(["?" for _ in product])
            conds.append(f"product IN ({placeholders})")
            params.extend(product)
        if category:
            placeholders = ",".join(["?" for _ in category])
            conds.append(f"test_category IN ({placeholders})")
            params.extend(category)
        where = f"WHERE {' AND '.join(conds)}" if conds else ""
        with lock:
            rows = db.execute(
                f"SELECT DISTINCT lot_id FROM lots {where} ORDER BY lot_id",
                params if params else None,
            ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/wafers")
def get_wafers(
    db_tuple: DB,
    lot: Annotated[list[str], Query()] = [],
) -> dict:
    if not lot:
        return {"wafer_ids": [], "has_wafers": False}
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            rows = db.execute(
                f"""SELECT DISTINCT wafer_id FROM wafers
                    WHERE lot_id IN ({placeholders})
                      AND wafer_id != ''
                    ORDER BY wafer_id""",
                list(lot),
            ).fetchall()
        ids = [r[0] for r in rows]
        return {"wafer_ids": ids, "has_wafers": len(ids) > 0}
    except Exception:
        return {"wafer_ids": [], "has_wafers": False}
