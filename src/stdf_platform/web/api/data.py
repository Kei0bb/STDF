"""Chart data endpoints."""

from threading import Lock
from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, Query

from .deps import get_db

router = APIRouter(tags=["data"])
DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]


@router.get("/summary")
def get_summary(db_tuple: DB, lot: Annotated[list[str], Query()] = []) -> list[dict]:
    if not lot:
        return []
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            rows = db.execute(f"""
                SELECT
                    l.lot_id, l.product, l.test_category, l.sub_process,
                    l.part_type, l.job_name, l.job_rev,
                    COUNT(DISTINCT w.wafer_id)                              AS wafer_count,
                    SUM(w.part_count)                                       AS total_parts,
                    SUM(w.good_count)                                       AS good_parts,
                    ROUND(100.0 * SUM(w.good_count)
                        / NULLIF(SUM(w.part_count), 0), 2)                 AS yield_pct
                FROM lots l
                LEFT JOIN (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
                    ) AS rn FROM wafers
                ) w ON l.lot_id = w.lot_id AND w.rn = 1
                WHERE l.lot_id IN ({placeholders})
                GROUP BY l.lot_id, l.product, l.test_category, l.sub_process,
                         l.part_type, l.job_name, l.job_rev
                ORDER BY l.product, l.test_category, l.lot_id
            """, list(lot)).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/wafer-yield")
def get_wafer_yield(db_tuple: DB, lot: str = "") -> list[dict]:
    if not lot:
        return []
    db, lock = db_tuple
    try:
        with lock:
            rows = db.execute("""
                SELECT wafer_id,
                       part_count AS total,
                       good_count AS good,
                       ROUND(100.0 * good_count / NULLIF(part_count, 0), 2) AS yield_pct,
                       retest_num
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
                    ) AS rn FROM wafers WHERE lot_id = ?
                ) WHERE rn = 1
                ORDER BY wafer_id
            """, [lot]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/wafermap")
def get_wafermap(db_tuple: DB, lot: str = "", wafer: str = "") -> list[dict]:
    if not lot or not wafer:
        return []
    db, lock = db_tuple
    try:
        with lock:
            rows = db.execute("""
                SELECT x_coord, y_coord, soft_bin, hard_bin, passed, part_id
                FROM parts
                WHERE lot_id = ? AND wafer_id = ?
                ORDER BY part_id
            """, [lot, wafer]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/tests")
def get_tests(db_tuple: DB, lot: Annotated[list[str], Query()] = []) -> list[dict]:
    if not lot:
        return []
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            rows = db.execute(f"""
                SELECT DISTINCT test_num, test_name, rec_type, units, lo_limit, hi_limit
                FROM test_data
                WHERE lot_id IN ({placeholders})
                ORDER BY test_num
            """, list(lot)).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/fails")
def get_fails(
    db_tuple: DB,
    lot: Annotated[list[str], Query()] = [],
    top_n: int = 20,
) -> list[dict]:
    if not lot:
        return []
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            rows = db.execute(f"""
                SELECT
                    test_num,
                    test_name,
                    COUNT(*)                                                    AS total,
                    SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)              AS fail_count,
                    ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
                        / COUNT(*), 2)                                          AS fail_rate
                FROM test_data
                WHERE lot_id IN ({placeholders})
                GROUP BY test_num, test_name
                HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
                ORDER BY fail_rate DESC
                LIMIT ?
            """, list(lot) + [top_n]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/distribution")
def get_distribution(
    db_tuple: DB,
    lot: Annotated[list[str], Query()] = [],
    test_num: int = 0,
    limit: int = 5000,
) -> dict:
    if not lot or not test_num:
        return {}
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            meta = db.execute(f"""
                SELECT FIRST(test_name), FIRST(units), FIRST(lo_limit), FIRST(hi_limit)
                FROM test_data
                WHERE lot_id IN ({placeholders}) AND test_num = ?
            """, list(lot) + [test_num]).fetchone()

            if not meta:
                return {}

            vals = db.execute(f"""
                SELECT result FROM test_data
                WHERE lot_id IN ({placeholders})
                  AND test_num = ?
                  AND result IS NOT NULL
                LIMIT ?
            """, list(lot) + [test_num, limit]).fetchall()

        return {
            "test_num": test_num,
            "test_name": meta[0] or "",
            "units": meta[1] or "",
            "lo_limit": meta[2],
            "hi_limit": meta[3],
            "values": [r[0] for r in vals],
        }
    except Exception:
        return {}
