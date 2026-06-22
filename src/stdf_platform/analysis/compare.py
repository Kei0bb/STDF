"""Lot-to-lot comparison. All yields from parts_final (retest-aware)."""

from __future__ import annotations

import duckdb
import pandas as pd
import plotly.graph_objects as go

# Default top-N tests (by fail rate) auto-selected when test_nums is omitted.
_TOP_FAIL_TESTS = 20


def _in_clause(lot_ids):
    return ",".join("?" for _ in lot_ids)


def _top_fail_tests(conn, lot_id, top_n) -> list[int]:
    """test_nums for the top-N tests by fail rate in a lot (passed='P'/'F')."""
    try:
        rows = conn.execute(
            """
            SELECT test_num
            FROM test_data_final
            WHERE lot_id = ?
            GROUP BY test_num
            HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
            ORDER BY ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
                         / COUNT(*), 2) DESC, test_num
            LIMIT ?
            """,
            [lot_id, top_n],
        ).fetchall()
    except duckdb.CatalogException:
        return []
    return [int(r[0]) for r in rows]


def _test_values(conn, lot_id, test_num) -> dict:
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
        "test_name": meta[0] or "", "units": meta[1] or "",
        "lo_limit": meta[2], "hi_limit": meta[3],
        "values": [r[0] for r in vals],
    }


def yield_by_lot(s, product, lot_ids, test_category):
    ph = _in_clause(lot_ids)
    return s.conn.execute(
        f"""
        SELECT lot_id, wafer_id, total, good, yield_pct
        FROM wafer_yield_final
        WHERE lot_id IN ({ph})
        ORDER BY lot_id, wafer_id
        """,
        list(lot_ids),
    ).fetchdf()


def bin_pareto_by_lot(s, product, lot_ids, test_category):
    ph = _in_clause(lot_ids)
    # Probed dies + the gross-die QC-fail (unprobed) bucket so per-lot bin totals
    # match the gross-die denominator.
    return s.conn.execute(
        f"""
        WITH binned AS (
            SELECT lot_id, soft_bin, COUNT(*) AS count
            FROM parts_final
            WHERE lot_id IN ({ph})
            GROUP BY lot_id, soft_bin
            UNION ALL
            SELECT lot_id, gd_fail_bin AS soft_bin, SUM(unprobed) AS count
            FROM wafer_yield_final
            WHERE lot_id IN ({ph}) AND gd_fail_bin IS NOT NULL AND unprobed > 0
            GROUP BY lot_id, gd_fail_bin
        )
        SELECT lot_id, soft_bin, SUM(count) AS count,
               ROUND(100.0 * SUM(count)
                   / SUM(SUM(count)) OVER (PARTITION BY lot_id), 2) AS pct
        FROM binned
        GROUP BY lot_id, soft_bin
        ORDER BY lot_id, count DESC
        """,
        list(lot_ids) + list(lot_ids),
    ).fetchdf()


def test_stats_by_lot(s, product, lot_ids, test_category, test_nums=None):
    if test_nums is None:
        nums = set()
        for lot in lot_ids:
            nums.update(_top_fail_tests(s.conn, lot, _TOP_FAIL_TESTS))
        test_nums = sorted(nums)
    if not test_nums:
        # no failing tests anywhere → empty frame with the right columns
        return pd.DataFrame(
            columns=["lot_id", "test_num", "n", "mean", "std",
                     "cpk", "lo_limit", "hi_limit"])
    lp = _in_clause(lot_ids)
    tp = _in_clause(test_nums)
    return s.conn.execute(
        f"""
        SELECT lot_id, test_num,
               COUNT(*)                              AS n,
               ROUND(AVG(result), 6)                 AS mean,
               ROUND(stddev_pop(result), 6)          AS std,
               ROUND(LEAST(
                   (FIRST(hi_limit) - AVG(result)) / NULLIF(3*stddev_pop(result),0),
                   (AVG(result) - FIRST(lo_limit)) / NULLIF(3*stddev_pop(result),0)
               ), 3)                                 AS cpk,
               FIRST(lo_limit)                       AS lo_limit,
               FIRST(hi_limit)                       AS hi_limit
        FROM test_data_final
        WHERE lot_id IN ({lp}) AND test_num IN ({tp})
          AND result IS NOT NULL AND rec_type IN ('PTR','MPR')
          AND lo_limit IS NOT NULL AND hi_limit IS NOT NULL
        GROUP BY lot_id, test_num
        ORDER BY lot_id, test_num
        """,
        list(lot_ids) + list(test_nums),
    ).fetchdf()


def test_distribution_fig(s, product, lot_ids, test_category, test_num):
    fig = go.Figure()
    lo = hi = name = units = None
    for lot in lot_ids:
        v = _test_values(s.conn, lot, test_num)
        if not v or not v.get("values"):
            continue
        lo, hi = v["lo_limit"], v["hi_limit"]
        name, units = v["test_name"], v["units"]
        fig.add_histogram(x=v["values"], name=lot, opacity=0.6, nbinsx=40)
    for lim, lbl in ((lo, "LSL"), (hi, "USL")):
        if lim is not None:
            fig.add_vline(x=lim, line_color="#ef4444", line_dash="dash",
                          annotation_text=lbl)
    fig.update_layout(
        barmode="overlay",
        title=f"{name or ''} (#{test_num}) [{units or ''}] by lot",
        template="plotly_dark", margin=dict(l=40, r=20, t=40, b=40),
    )
    return fig
