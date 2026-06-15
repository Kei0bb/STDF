"""Analysis queries for the report engine.

Every function receives a DuckDB connection that already has the Phase-1 views
(lots / parts_final / test_data_final / chipid_final, via setup_views) and the
lot identity (product, test_category, lot_id). Returns plain dict/list[dict].

Yield is always computed from parts_final (retest-aware, die/package level),
matching web/api/data.py and database.get_lot_summary.
"""

import duckdb


def lot_header(conn, product, test_category, lot_id) -> dict:
    """MIR-derived metadata for the lot (latest row if duplicated)."""
    row = conn.execute(
        """
        SELECT product, test_category, sub_process, part_type,
               job_name, job_rev, tester_type, operator,
               start_time, finish_time
        FROM lots
        WHERE product = ? AND test_category = ? AND lot_id = ?
        ORDER BY start_time DESC
        LIMIT 1
        """,
        [product, test_category, lot_id],
    ).fetchone()
    if not row:
        return {}
    cols = ["product", "test_category", "sub_process", "part_type",
            "job_name", "job_rev", "tester_type", "operator",
            "start_time", "finish_time"]
    out = dict(zip(cols, row))
    out["lot_id"] = lot_id
    return out


def max_retest(conn, product, test_category, lot_id) -> int:
    """Highest retest_num present for the lot (0 = single run, no retest)."""
    row = conn.execute(
        "SELECT COALESCE(MAX(retest_num), 0) FROM parts WHERE lot_id = ?",
        [lot_id],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def wafer_ids(conn, product, test_category, lot_id) -> list[str]:
    """Non-empty wafer ids in the lot (empty list for FT)."""
    rows = conn.execute(
        """
        SELECT DISTINCT wafer_id FROM parts_final
        WHERE lot_id = ? AND wafer_id != ''
        ORDER BY wafer_id
        """,
        [lot_id],
    ).fetchall()
    return [r[0] for r in rows]


def wafer_yield(conn, product, test_category, lot_id) -> list[dict]:
    """Per-wafer (or single FT group) yield from wafer_yield_final
    (gross-die denominator applied for CP)."""
    rows = conn.execute(
        """
        SELECT wafer_id, total, good, yield_pct
        FROM wafer_yield_final
        WHERE lot_id = ?
        ORDER BY wafer_id
        """,
        [lot_id],
    ).fetchdf()
    return rows.to_dict(orient="records")


def lot_yield_total(conn, product, test_category, lot_id) -> dict:
    """Lot-level totals from wafer_yield_final (gross-die aware)."""
    row = conn.execute(
        """
        SELECT SUM(total)                                        AS total,
               SUM(good)                                         AS good,
               ROUND(100.0 * SUM(good) / NULLIF(SUM(total), 0), 2) AS yield_pct
        FROM wafer_yield_final
        WHERE lot_id = ?
        """,
        [lot_id],
    ).fetchone()
    total, good, yield_pct = (row or (0, 0, None))
    return {"total": int(total or 0), "good": int(good or 0),
            "yield_pct": float(yield_pct) if yield_pct is not None else 0.0}


def bin_pareto(conn, product, test_category, lot_id) -> list[dict]:
    """Soft-bin counts per wafer (for a stacked pareto). One row per
    (soft_bin, wafer_id). FT collapses to a single wafer_id=''.

    Unprobed dies (gross die − probed) are added per wafer under the product's
    gd_fail_bin so the pareto totals match the gross-die denominator."""
    rows = conn.execute(
        """
        SELECT soft_bin, wafer_id, SUM(count) AS count
        FROM (
            SELECT soft_bin, wafer_id, COUNT(*) AS count
            FROM parts_final
            WHERE lot_id = ?
            GROUP BY soft_bin, wafer_id
            UNION ALL
            SELECT gd_fail_bin AS soft_bin, wafer_id, unprobed AS count
            FROM wafer_yield_final
            WHERE lot_id = ? AND gd_fail_bin IS NOT NULL AND unprobed > 0
        )
        GROUP BY soft_bin, wafer_id
        ORDER BY soft_bin, wafer_id
        """,
        [lot_id, lot_id],
    ).fetchdf()
    return rows.to_dict(orient="records")


def wafer_map(conn, product, test_category, lot_id, wafer_id) -> list[dict]:
    """Die coordinates + bin for one wafer (adapted from /wafermap)."""
    rows = conn.execute(
        """
        SELECT x_coord, y_coord, soft_bin, hard_bin, passed
        FROM parts_final
        WHERE lot_id = ? AND wafer_id = ?
        ORDER BY y_coord, x_coord
        """,
        [lot_id, wafer_id],
    ).fetchdf()
    return rows.to_dict(orient="records")


def top_fail_tests(conn, product, test_category, lot_id, top_n) -> list[dict]:
    """Top-N tests by fail rate (adapted from /fails). passed is 'P'/'F'."""
    try:
        rows = conn.execute(
            """
            SELECT test_num, FIRST(test_name) AS test_name,
                   COUNT(*)                                           AS total,
                   SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)      AS fail_count,
                   ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
                       / COUNT(*), 2)                                 AS fail_rate
            FROM test_data_final
            WHERE lot_id = ?
            GROUP BY test_num
            HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
            ORDER BY fail_rate DESC, test_num
            LIMIT ?
            """,
            [lot_id, top_n],
        ).fetchdf()
    except duckdb.CatalogException:
        return []
    return rows.to_dict(orient="records")


def test_values(conn, product, test_category, lot_id, test_num) -> dict:
    """Numeric results + limits for one test (for a histogram)."""
    try:
        meta = conn.execute(
            """
            SELECT FIRST(test_name), FIRST(units), FIRST(lo_limit), FIRST(hi_limit)
            FROM test_data_final
            WHERE lot_id = ? AND test_num = ?
            """,
            [lot_id, test_num],
        ).fetchone()
    except duckdb.CatalogException:
        return {}
    if not meta or meta[0] is None:
        return {}
    vals = conn.execute(
        """
        SELECT result FROM test_data_final
        WHERE lot_id = ? AND test_num = ? AND result IS NOT NULL
        """,
        [lot_id, test_num],
    ).fetchall()
    return {
        "test_num": test_num,
        "test_name": meta[0] or "",
        "units": meta[1] or "",
        "lo_limit": meta[2],
        "hi_limit": meta[3],
        "values": [r[0] for r in vals],
    }


def retest_history(conn, product, test_category, lot_id) -> list[dict]:
    """Per-retest part counts + yield (only meaningful when max_retest>0)."""
    rows = conn.execute(
        """
        SELECT retest_num,
               COUNT(*)                                          AS parts,
               SUM(CASE WHEN passed THEN 1 ELSE 0 END)           AS good,
               ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0), 2)                     AS yield_pct
        FROM parts
        WHERE lot_id = ?
        GROUP BY retest_num
        ORDER BY retest_num
        """,
        [lot_id],
    ).fetchdf()
    return rows.to_dict(orient="records")


def chipid_summary(conn, product, test_category, lot_id) -> list[dict]:
    """FT ChipID origin breakdown from chipid_final (latest retest per die)."""
    rows = conn.execute(
        """
        SELECT origin_fab,
               COUNT(*)                                           AS dies,
               SUM(CASE WHEN valid THEN 1 ELSE 0 END)             AS valid_dies,
               COUNT(DISTINCT origin_lot)                         AS origin_lots,
               COUNT(DISTINCT origin_wafer)                       AS origin_wafers
        FROM chipid_final
        WHERE lot_id = ?
        GROUP BY origin_fab
        ORDER BY dies DESC
        """,
        [lot_id],
    ).fetchdf()
    return rows.to_dict(orient="records")
