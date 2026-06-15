"""CP wafer-plane analysis: radial zones, radial parametric profile, maps.

Radius is normalized against the die-coordinate extent of the lot:
center = midpoint of (min/max) x,y; r = hypot(x-cx, y-cy); rnorm = r / max(r).
"""

from __future__ import annotations

import plotly.graph_objects as go


_ZONE3 = {0: "center", 1: "mid", 2: "edge"}


def zone_yield(s, product, lot_id, n_zones=3):
    # Probed dies only: unprobed gross-die dies have no wafer coordinate and
    # cannot be assigned to a radial zone, so the gross-die denominator does not
    # apply here (unlike wafer_yield_final). Zone yield = good/probed per zone.
    df = s.conn.execute(
        """
        WITH ext AS (
            SELECT (MIN(x_coord)+MAX(x_coord))/2.0 AS cx,
                   (MIN(y_coord)+MAX(y_coord))/2.0 AS cy
            FROM parts_final WHERE lot_id = ?
        ),
        r AS (
            SELECT p.passed,
                   sqrt(pow(p.x_coord-ext.cx,2)+pow(p.y_coord-ext.cy,2)) AS rad
            FROM parts_final p, ext
            WHERE p.lot_id = ?
        ),
        rn AS (
            SELECT passed,
                   CASE WHEN MAX(rad) OVER () = 0 THEN 0
                        ELSE rad / MAX(rad) OVER () END AS rnorm
            FROM r
        ),
        z AS (
            SELECT passed,
                   LEAST(? - 1, CAST(floor(rnorm * ?) AS INTEGER)) AS zone_idx
            FROM rn
        )
        SELECT zone_idx,
               COUNT(*)                                    AS total,
               SUM(CASE WHEN passed THEN 1 ELSE 0 END)     AS good,
               ROUND(100.0*SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                     /NULLIF(COUNT(*),0),2)                AS yield_pct
        FROM z GROUP BY zone_idx ORDER BY zone_idx
        """,
        [lot_id, lot_id, n_zones, n_zones],
    ).fetchdf()
    if n_zones == 3:
        df.insert(0, "zone", df["zone_idx"].map(_ZONE3))
    else:
        df.insert(0, "zone", df["zone_idx"].map(lambda i: f"zone_{int(i)}"))
    return df


def radial_profile(s, product, lot_id, test_num):
    return s.conn.execute(
        """
        WITH ext AS (
            SELECT (MIN(x_coord)+MAX(x_coord))/2.0 AS cx,
                   (MIN(y_coord)+MAX(y_coord))/2.0 AS cy
            FROM parts_final WHERE lot_id = ?
        ),
        coords AS (
            SELECT part_id,
                   sqrt(pow(x_coord-ext.cx,2)+pow(y_coord-ext.cy,2)) AS rad
            FROM parts_final p, ext WHERE p.lot_id = ?
        ),
        rmax AS (SELECT MAX(rad) AS rm FROM coords),
        vals AS (
            SELECT c.rad, t.result,
                   CASE WHEN rmax.rm = 0 THEN 0
                        ELSE c.rad / rmax.rm END AS rnorm
            FROM test_data_final t
            JOIN coords c ON t.part_id = c.part_id
            CROSS JOIN rmax
            WHERE t.lot_id = ? AND t.test_num = ?
              AND t.result IS NOT NULL AND t.rec_type IN ('PTR','MPR')
        )
        SELECT LEAST(9, CAST(floor(rnorm*10) AS INTEGER)) AS radius_bin,
               COUNT(*)               AS n,
               ROUND(AVG(result),6)   AS mean,
               ROUND(stddev_pop(result),6) AS std
        FROM vals GROUP BY radius_bin ORDER BY radius_bin
        """,
        [lot_id, lot_id, lot_id, test_num],
    ).fetchdf()


def param_wafermap_fig(s, product, lot_id, wafer_id, test_num):
    df = s.conn.execute(
        """
        SELECT x_coord, y_coord, AVG(result) AS result
        FROM test_data_final
        WHERE lot_id = ? AND wafer_id = ? AND test_num = ?
          AND result IS NOT NULL
        GROUP BY x_coord, y_coord
        """,
        [lot_id, wafer_id, test_num],
    ).fetchdf()
    fig = go.Figure(go.Scatter(
        x=df["x_coord"], y=df["y_coord"], mode="markers",
        marker=dict(size=10, symbol="square", color=df["result"],
                    colorscale="Viridis", showscale=True,
                    colorbar=dict(title="value")),
        text=[f"{v:.3g}" for v in df["result"]],
    ))
    fig.update_layout(
        title=f"Wafer {wafer_id} test #{test_num}", template="plotly_dark",
        yaxis=dict(autorange="reversed", scaleanchor="x", scaleratio=1),
        margin=dict(l=30, r=20, t=40, b=30),
    )
    return fig
