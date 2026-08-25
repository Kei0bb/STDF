-- database.py get_lot_summary:94-124 のフラット化(全ロット版)。
SELECT l.lot_id, l.product, l.test_category, l.sub_process, l.part_type,
       l.job_name, l.job_rev, l.job_variant_count, l.job_mixed,
       p.wafer_count, p.total_parts, p.good_parts, p.yield_pct,
       l.start_time, l.finish_time
FROM {{ ref('stg_lots') }} l
LEFT JOIN (
    SELECT lot_id,
           COUNT(*) FILTER (WHERE wafer_id <> '') AS wafer_count,
           SUM(total) AS total_parts,
           SUM(good)  AS good_parts,
           ROUND(100.0 * SUM(good) / NULLIF(SUM(total), 0), 2) AS yield_pct
    FROM {{ ref('stg_wafer_yield') }}
    GROUP BY lot_id
) p USING (lot_id)
