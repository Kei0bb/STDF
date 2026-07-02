"""CP↔FT correlation (lot + die level) and test-to-test correlation."""

from __future__ import annotations



def cp_ft_yield(s, product, lot_ids=None):
    params = [product]
    lot_filter = ""
    if lot_ids is not None:
        ph = ",".join("?" for _ in lot_ids)
        lot_filter = f" AND lot_id IN ({ph})"
        params += list(lot_ids)
    # one params list is reused for both CP and FT scans below
    sql = f"""
        WITH cp AS (
            SELECT lot_id, COUNT(*) AS cp_total,
                   ROUND(100.0*SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                         /NULLIF(COUNT(*),0),2) AS cp_yield_pct
            FROM parts_final
            WHERE product = ? AND test_category = 'CP'{lot_filter}
            GROUP BY lot_id
        ),
        ft AS (
            SELECT lot_id, COUNT(*) AS ft_total,
                   ROUND(100.0*SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                         /NULLIF(COUNT(*),0),2) AS ft_yield_pct
            FROM parts_final
            WHERE product = ? AND test_category = 'FT'{lot_filter}
            GROUP BY lot_id
        )
        SELECT COALESCE(cp.lot_id, ft.lot_id) AS lot_id,
               cp.cp_total, cp.cp_yield_pct,
               ft.ft_total, ft.ft_yield_pct
        FROM cp FULL OUTER JOIN ft USING (lot_id)
        ORDER BY lot_id
    """
    return s.conn.execute(sql, params + params).fetchdf()


def die_cp_ft_join(s, product, lot_id):
    """Die-level CP↔FT join via decoded ChipID origin.

    FT lot `lot_id`'s chipid_final rows carry each die's decoded CP origin
    (origin_lot/origin_wafer/origin_x/origin_y). We join those to CP parts_final
    on (origin_lot=CP lot, origin_x=x_coord, origin_y=y_coord). The CP wafer_id
    is a free-form STRING while origin_wafer is an INT, so the wafer match is
    defensive: TRY_CAST(wafer_id AS BIGINT)=origin_wafer when numeric, else the
    (lot,x,y) match alone identifies the die. Only valid, TSMC-decoded dies join.
    """
    return s.conn.execute(
        """
        SELECT c.part_id            AS ft_part_id,
               c.part_txt           AS ft_part_txt,
               c.origin_fab,
               c.origin_lot         AS cp_lot_id,
               c.origin_wafer       AS cp_wafer,
               c.origin_x           AS cp_x,
               c.origin_y           AS cp_y,
               p.wafer_id           AS cp_wafer_id_str,
               p.soft_bin           AS cp_soft_bin,
               p.hard_bin           AS cp_hard_bin,
               p.passed             AS cp_passed
        FROM chipid_final c
        JOIN parts_final p
          ON p.product = ?
         AND p.test_category = 'CP'
         AND p.lot_id  = c.origin_lot
         AND p.x_coord = c.origin_x
         AND p.y_coord = c.origin_y
         AND (TRY_CAST(p.wafer_id AS BIGINT) = c.origin_wafer
              OR TRY_CAST(p.wafer_id AS BIGINT) IS NULL)
        WHERE c.lot_id = ? AND c.valid
        ORDER BY c.part_txt, c.origin_x, c.origin_y
        """,
        [product, lot_id],
    ).fetchdf()


def test_correlation(s, product, lot_id, test_category, test_nums):
    ph = ",".join("?" for _ in test_nums)
    long = s.conn.execute(
        f"""
        SELECT part_id, test_num, result
        FROM test_data_final
        WHERE lot_id = ? AND test_num IN ({ph})
          AND result IS NOT NULL AND rec_type IN ('PTR','MPR')
        """,
        [lot_id] + list(test_nums),
    ).fetchdf()
    wide = long.pivot_table(index="part_id", columns="test_num", values="result")
    return wide.corr()
