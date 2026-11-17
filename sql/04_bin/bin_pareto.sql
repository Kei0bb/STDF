-- ビン分布(ロット全体)。サブプロセス(CP1/CP2…)ごとに別行。
-- gross die 未測定ダイは gd_fail_bin に計上される。
SET VARIABLE sub_process = NULL;

WITH binned AS (
    -- 実測されたダイ
    SELECT test_category, sub_process, hard_bin, soft_bin, COUNT(*) AS count
    FROM parts_final
    WHERE lot_id = getvariable('lot')
      AND opt_eq(sub_process, getvariable('sub_process'))
    GROUP BY test_category, sub_process, hard_bin, soft_bin
    UNION ALL
    -- gross die との差分(プローブされなかったダイ)。CP のみ・config.yaml の
    -- products.<P>.gross_die が設定されている製品でのみ行が出る。
    SELECT test_category, sub_process,
           gd_fail_bin AS hard_bin, gd_fail_bin AS soft_bin, SUM(unprobed) AS count
    FROM wafer_yield_final
    WHERE lot_id = getvariable('lot')
      AND opt_eq(sub_process, getvariable('sub_process'))
      AND gd_fail_bin IS NOT NULL AND unprobed > 0
    GROUP BY test_category, sub_process, gd_fail_bin
)
SELECT test_category, sub_process, hard_bin, soft_bin,
       SUM(count) AS die_count,
       ROUND(100.0 * SUM(count) / SUM(SUM(count)) OVER (PARTITION BY test_category, sub_process), 2) AS pct
FROM binned
GROUP BY test_category, sub_process, hard_bin, soft_bin
ORDER BY test_category, sub_process, die_count DESC
