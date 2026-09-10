-- ビン分布(ロット全体)。gross die 未測定ダイは gd_fail_bin に計上される。
SET VARIABLE lot = 'LOT001';

WITH binned AS (
    -- 実測されたダイ
    SELECT hard_bin, soft_bin, COUNT(*) AS count
    FROM parts_final
    WHERE lot_id = getvariable('lot')
    GROUP BY hard_bin, soft_bin
    UNION ALL
    -- gross die との差分(プローブされなかったダイ)。CP のみ・config.yaml の
    -- products.<P>.gross_die が設定されている製品でのみ行が出る。
    SELECT gd_fail_bin AS hard_bin, gd_fail_bin AS soft_bin, SUM(unprobed) AS count
    FROM wafer_yield_final
    WHERE lot_id = getvariable('lot')
      AND gd_fail_bin IS NOT NULL AND unprobed > 0
    GROUP BY gd_fail_bin
)
SELECT hard_bin, soft_bin,
       SUM(count) AS die_count,
       ROUND(100.0 * SUM(count) / SUM(SUM(count)) OVER (), 2) AS pct
FROM binned
GROUP BY hard_bin, soft_bin
ORDER BY die_count DESC
