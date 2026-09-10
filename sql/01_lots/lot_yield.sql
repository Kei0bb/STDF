-- ロット別 歩留まりサマリ(gross die 適用、FT ロットも含む)
SELECT l.lot_id, l.product, l.test_category, l.sub_process,
       l.job_name, l.job_rev, l.job_mixed,
       COUNT(*) FILTER (WHERE w.wafer_id <> '') AS wafer_count,
       SUM(w.total) AS total_parts,
       SUM(w.good)  AS good_parts,
       ROUND(100.0 * SUM(w.good) / NULLIF(SUM(w.total), 0), 2) AS yield_pct,
       l.start_time
FROM lots l
LEFT JOIN wafer_yield_final w USING (lot_id)
GROUP BY l.lot_id, l.product, l.test_category, l.sub_process,
         l.job_name, l.job_rev, l.job_mixed, l.start_time
ORDER BY l.start_time DESC
