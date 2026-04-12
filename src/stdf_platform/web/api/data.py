"""Chart data endpoints."""

from typing import Annotated

from clickhouse_connect.driver import Client
from fastapi import APIRouter, Depends, Query

from .deps import get_ch

router = APIRouter(tags=["data"])
CH = Annotated[Client, Depends(get_ch)]


@router.get("/summary")
def get_summary(ch: CH, lot: Annotated[list[str], Query()] = []) -> list[dict]:
    if not lot:
        return []
    try:
        rows = ch.query("""
            SELECT
                l.lot_id, l.product, l.test_category, l.sub_process,
                l.part_type, l.job_name, l.job_rev,
                countDistinct(w.wafer_id)                     AS wafer_count,
                sum(w.part_count)                             AS total_parts,
                sum(w.good_count)                             AS good_parts,
                round(100.0 * sum(w.good_count)
                    / nullIf(sum(w.part_count), 0), 2)        AS yield_pct
            FROM lots l
            LEFT JOIN (
                SELECT *, row_number() OVER (
                    PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
                ) AS rn FROM wafers
            ) w ON l.lot_id = w.lot_id AND w.rn = 1
            WHERE l.lot_id IN {lot_ids:Array(String)}
            GROUP BY l.lot_id, l.product, l.test_category, l.sub_process,
                     l.part_type, l.job_name, l.job_rev
            ORDER BY l.product, l.test_category, l.lot_id
        """, parameters={"lot_ids": lot}).named_results()
        return list(rows)
    except Exception:
        return []


@router.get("/wafer-yield")
def get_wafer_yield(ch: CH, lot: str = "") -> list[dict]:
    if not lot:
        return []
    try:
        rows = ch.query("""
            SELECT wafer_id,
                   part_count AS total,
                   good_count AS good,
                   round(100.0 * good_count / nullIf(part_count, 0), 2) AS yield_pct,
                   retest_num
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
                ) AS rn FROM wafers WHERE lot_id = {lot_id:String}
            ) WHERE rn = 1
            ORDER BY wafer_id
        """, parameters={"lot_id": lot}).named_results()
        return list(rows)
    except Exception:
        return []


@router.get("/wafermap")
def get_wafermap(ch: CH, lot: str = "", wafer: str = "") -> list[dict]:
    if not lot or not wafer:
        return []
    try:
        rows = ch.query("""
            SELECT x_coord, y_coord, soft_bin, hard_bin, passed, part_id
            FROM parts
            WHERE lot_id = {lot_id:String} AND wafer_id = {wafer_id:String}
            ORDER BY part_id
        """, parameters={"lot_id": lot, "wafer_id": wafer}).named_results()
        return list(rows)
    except Exception:
        return []


@router.get("/tests")
def get_tests(ch: CH, lot: Annotated[list[str], Query()] = []) -> list[dict]:
    if not lot:
        return []
    try:
        rows = ch.query("""
            SELECT DISTINCT test_num, test_name, rec_type, units, lo_limit, hi_limit
            FROM test_data
            WHERE lot_id IN {lot_ids:Array(String)}
            ORDER BY test_num
        """, parameters={"lot_ids": lot}).named_results()
        return list(rows)
    except Exception:
        return []


@router.get("/fails")
def get_fails(
    ch: CH,
    lot: Annotated[list[str], Query()] = [],
    top_n: int = 20,
) -> list[dict]:
    if not lot:
        return []
    try:
        rows = ch.query("""
            SELECT
                test_num,
                test_name,
                count()                                     AS total,
                countIf(passed = 'F')                       AS fail_count,
                round(100.0 * countIf(passed = 'F')
                    / count(), 2)                           AS fail_rate
            FROM test_data
            WHERE lot_id IN {lot_ids:Array(String)}
            GROUP BY test_num, test_name
            HAVING fail_count > 0
            ORDER BY fail_rate DESC
            LIMIT {top_n:UInt32}
        """, parameters={"lot_ids": lot, "top_n": top_n}).named_results()
        return list(rows)
    except Exception:
        return []


@router.get("/distribution")
def get_distribution(
    ch: CH,
    lot: Annotated[list[str], Query()] = [],
    test_num: int = 0,
    limit: int = 5000,
) -> dict:
    if not lot or not test_num:
        return {}
    try:
        meta = ch.query("""
            SELECT any(test_name) AS test_name,
                   any(units)     AS units,
                   any(lo_limit)  AS lo_limit,
                   any(hi_limit)  AS hi_limit
            FROM test_data
            WHERE lot_id IN {lot_ids:Array(String)} AND test_num = {test_num:Int32}
        """, parameters={"lot_ids": lot, "test_num": test_num}).first_row

        if not meta:
            return {}

        vals = ch.query("""
            SELECT result FROM test_data
            WHERE lot_id IN {lot_ids:Array(String)}
              AND test_num = {test_num:Int32}
              AND result IS NOT NULL
            LIMIT {lim:UInt32}
        """, parameters={"lot_ids": lot, "test_num": test_num, "lim": limit}).result_rows

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
