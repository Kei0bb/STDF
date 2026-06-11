"""Time-series trend. Always ordered by MIR lots.start_time."""

from __future__ import annotations

from statistics import mean, pstdev

import plotly.graph_objects as go


def lot_trend(s, product, test_category, last_n=30):
    return s.conn.execute(
        """
        WITH agg AS (
            SELECT p.lot_id,
                   COUNT(*)                                    AS total,
                   SUM(CASE WHEN p.passed THEN 1 ELSE 0 END)   AS good
            FROM parts_final p
            GROUP BY p.lot_id
        ),
        j AS (
            SELECT a.lot_id, l.start_time AS start_t, a.total, a.good,
                   ROUND(100.0 * a.good / NULLIF(a.total, 0), 2) AS yield_pct
            FROM agg a
            JOIN (
                SELECT lot_id, product, test_category, start_time,
                       ROW_NUMBER() OVER (PARTITION BY product, test_category,
                           lot_id ORDER BY start_time DESC) AS rn
                FROM lots
            ) l ON a.lot_id = l.lot_id AND l.rn = 1
            WHERE l.product = ? AND l.test_category = ?
        )
        SELECT * FROM (
            SELECT * FROM j ORDER BY start_t DESC LIMIT ?
        ) ORDER BY start_t
        """,
        [product, test_category, last_n],
    ).fetchdf()


def test_trend(s, product, test_category, test_num, last_n=30):
    return s.conn.execute(
        """
        WITH agg AS (
            SELECT lot_id,
                   COUNT(*)                          AS n,
                   AVG(result)                       AS mean,
                   stddev_pop(result)                AS std,
                   FIRST(lo_limit)                   AS lo,
                   FIRST(hi_limit)                   AS hi
            FROM test_data_final
            WHERE test_num = ? AND result IS NOT NULL
              AND rec_type IN ('PTR','MPR')
            GROUP BY lot_id
        ),
        j AS (
            SELECT a.lot_id, l.start_time AS start_t, a.n,
                   ROUND(a.mean, 6) AS mean, ROUND(a.std, 6) AS std,
                   ROUND(LEAST((a.hi - a.mean)/NULLIF(3*a.std,0),
                               (a.mean - a.lo)/NULLIF(3*a.std,0)), 3) AS cpk
            FROM agg a
            JOIN (
                SELECT lot_id, product, test_category, start_time,
                       ROW_NUMBER() OVER (PARTITION BY product, test_category,
                           lot_id ORDER BY start_time DESC) AS rn
                FROM lots
            ) l ON a.lot_id = l.lot_id AND l.rn = 1
            WHERE l.product = ? AND l.test_category = ?
        )
        SELECT * FROM (
            SELECT * FROM j ORDER BY start_t DESC LIMIT ?
        ) ORDER BY start_t
        """,
        [test_num, product, test_category, last_n],
    ).fetchdf()


def trend_fig(df, y, control_limits=True):
    x = df["lot_id"].tolist() if "lot_id" in df else list(range(len(df)))
    yv = df[y].tolist()
    fig = go.Figure(go.Scatter(x=x, y=yv, mode="lines+markers", name=y))
    if control_limits:
        finite = [v for v in yv if v is not None]
        if len(finite) >= 2:
            mu, sigma = mean(finite), pstdev(finite)
            fig.add_hline(y=mu, line_color="#22c55e", annotation_text="mean")
            fig.add_hline(y=mu + 3 * sigma, line_color="#ef4444",
                          line_dash="dash", annotation_text="+3σ")
            fig.add_hline(y=mu - 3 * sigma, line_color="#ef4444",
                          line_dash="dash", annotation_text="-3σ")
    fig.update_layout(title=f"Trend: {y}", template="plotly_dark",
                      xaxis_title="lot", yaxis_title=y,
                      margin=dict(l=40, r=20, t=40, b=40))
    return fig
