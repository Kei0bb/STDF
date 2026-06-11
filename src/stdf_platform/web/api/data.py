"""Chart data endpoints."""

import logging
from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, Query

from .deps import get_db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["data"])
DB = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]


@router.get("/summary")
def get_summary(db: DB, lot: Annotated[list[str], Query()] = []) -> list[dict]:
    if not lot:
        return []
    try:
        placeholders = ",".join(["?" for _ in lot])
        rows = db.execute(f"""
            SELECT
                l.lot_id, l.product, l.test_category, l.sub_process,
                l.part_type, l.job_name, l.job_rev,
                MAX(p.wafer_count)                                      AS wafer_count,
                MAX(p.total_parts)                                      AS total_parts,
                MAX(p.good_parts)                                       AS good_parts,
                MAX(p.yield_pct)                                        AS yield_pct
            FROM lots l
            LEFT JOIN (
                -- Retest-aware, die/package-level yield from parts_final
                -- (WRR counts are partial on a retest and absent for FT).
                SELECT
                    lot_id,
                    COUNT(DISTINCT NULLIF(wafer_id, ''))               AS wafer_count,
                    COUNT(*)                                          AS total_parts,
                    SUM(CASE WHEN passed THEN 1 ELSE 0 END)           AS good_parts,
                    ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                     AS yield_pct
                FROM parts_final
                GROUP BY lot_id
            ) p ON l.lot_id = p.lot_id
            WHERE l.lot_id IN ({placeholders})
            GROUP BY l.lot_id, l.product, l.test_category, l.sub_process,
                     l.part_type, l.job_name, l.job_rev
            ORDER BY l.product, l.test_category, l.lot_id
        """, list(lot)).fetchdf()
        return rows.to_dict(orient="records")
    except Exception as e:
        logger.warning("%s failed: %s", __name__, e)
        return []


@router.get("/wafer-yield")
def get_wafer_yield(db: DB, lot: str = "") -> list[dict]:
    if not lot:
        return []
    try:
        rows = db.execute("""
            SELECT wafer_id,
                   COUNT(*)                                            AS total,
                   SUM(CASE WHEN passed THEN 1 ELSE 0 END)             AS good,
                   ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0), 2)                       AS yield_pct
            FROM parts_final
            WHERE lot_id = ?
            GROUP BY wafer_id
            ORDER BY wafer_id
        """, [lot]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception as e:
        logger.warning("%s failed: %s", __name__, e)
        return []


@router.get("/wafermap")
def get_wafermap(db: DB, lot: str = "", wafer: str = "") -> list[dict]:
    if not lot or not wafer:
        return []
    try:
        rows = db.execute("""
            SELECT x_coord, y_coord, soft_bin, hard_bin, passed, part_id
            FROM parts_final
            WHERE lot_id = ? AND wafer_id = ?
            ORDER BY part_id
        """, [lot, wafer]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception as e:
        logger.warning("%s failed: %s", __name__, e)
        return []


@router.get("/tests")
def get_tests(db: DB, lot: Annotated[list[str], Query()] = []) -> list[dict]:
    if not lot:
        return []
    try:
        placeholders = ",".join(["?" for _ in lot])
        rows = db.execute(f"""
            SELECT DISTINCT test_num, test_name, rec_type, units, lo_limit, hi_limit
            FROM test_data_final
            WHERE lot_id IN ({placeholders})
            ORDER BY test_num
        """, list(lot)).fetchdf()
        return rows.to_dict(orient="records")
    except Exception as e:
        logger.warning("%s failed: %s", __name__, e)
        return []


@router.get("/fails")
def get_fails(
    db: DB,
    lot: Annotated[list[str], Query()] = [],
    top_n: int = 20,
) -> list[dict]:
    if not lot:
        return []
    try:
        placeholders = ",".join(["?" for _ in lot])
        rows = db.execute(f"""
            SELECT
                test_num,
                test_name,
                COUNT(*)                                                    AS total,
                SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)              AS fail_count,
                ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
                    / COUNT(*), 2)                                          AS fail_rate
            FROM test_data_final
            WHERE lot_id IN ({placeholders})
            GROUP BY test_num, test_name
            HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
            ORDER BY fail_rate DESC
            LIMIT ?
        """, list(lot) + [top_n]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception as e:
        logger.warning("%s failed: %s", __name__, e)
        return []


@router.get("/distribution")
def get_distribution(
    db: DB,
    lot: Annotated[list[str], Query()] = [],
    test_num: int = 0,
    limit: int = 5000,
) -> dict:
    if not lot or not test_num:
        return {}
    try:
        placeholders = ",".join(["?" for _ in lot])
        meta = db.execute(f"""
            SELECT FIRST(test_name), FIRST(units), FIRST(lo_limit), FIRST(hi_limit)
            FROM test_data_final
            WHERE lot_id IN ({placeholders}) AND test_num = ?
        """, list(lot) + [test_num]).fetchone()

        if not meta or meta[0] is None:
            return {}

        vals = db.execute(f"""
            SELECT result FROM test_data_final
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
    except Exception as e:
        logger.warning("%s failed: %s", __name__, e)
        return {}
