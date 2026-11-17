-- ロット別 歩留まりサマリ(gross die 適用、FT ロットも含む)。サブプロセス(CP1/CP2…)ごとに別行。
-- 条件はすべて任意: 渡さなければ絞らない。s.run("lot_yield", product="P1")
SET VARIABLE product       = NULL;
SET VARIABLE test_category = NULL;
SET VARIABLE sub_process   = NULL;

SELECT l.lot_id, l.product, l.test_category, l.sub_process,
       l.job_name, l.job_rev, l.job_mixed,
       COUNT(*) FILTER (WHERE w.wafer_id <> '') AS wafer_count,
       SUM(w.total) AS total_parts,
       SUM(w.good)  AS good_parts,
       ROUND(100.0 * SUM(w.good) / NULLIF(SUM(w.total), 0), 2) AS yield_pct,
       l.start_time
FROM lots l
LEFT JOIN wafer_yield_final w
  ON  w.product       = l.product
  AND w.test_category = l.test_category
  AND w.sub_process   IS NOT DISTINCT FROM l.sub_process
  AND w.lot_id        = l.lot_id
WHERE opt_eq(l.product,       getvariable('product'))
  AND opt_eq(l.test_category, getvariable('test_category'))
  AND opt_eq(l.sub_process,   getvariable('sub_process'))
GROUP BY l.lot_id, l.product, l.test_category, l.sub_process,
         l.job_name, l.job_rev, l.job_mixed, l.start_time
ORDER BY l.start_time DESC
