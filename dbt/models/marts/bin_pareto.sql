-- database.py get_bin_summary:187-207 の全ロット版 + hard_bin 追加。
WITH binned AS (
    SELECT lot_id, hard_bin, soft_bin, COUNT(*) AS count
    FROM {{ ref('stg_parts_final') }}
    GROUP BY lot_id, hard_bin, soft_bin
    UNION ALL
    SELECT lot_id, gd_fail_bin AS hard_bin, gd_fail_bin AS soft_bin,
           SUM(unprobed) AS count
    FROM {{ ref('stg_wafer_yield') }}
    WHERE gd_fail_bin IS NOT NULL AND unprobed > 0
    GROUP BY lot_id, gd_fail_bin
)
SELECT lot_id, hard_bin, soft_bin,
       SUM(count) AS count,
       ROUND(100.0 * SUM(count) / SUM(SUM(count)) OVER (PARTITION BY lot_id), 2) AS pct
FROM binned
GROUP BY lot_id, hard_bin, soft_bin
