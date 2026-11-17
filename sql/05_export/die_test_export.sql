-- ダイ×テストの明細(CSV 書き出し用。run(..., out="x.csv") を推奨)。
-- サブプロセス(CP1/CP2…)は混ぜず sub_process 列で区別する。
SET VARIABLE sub_process = NULL;

SELECT p.lot_id, p.test_category, p.sub_process, p.wafer_id, p.part_id, p.x_coord, p.y_coord,
       p.hard_bin, p.soft_bin, p.passed AS die_passed,
       td.test_num, td.test_name, td.result,
       td.lo_limit, td.hi_limit, td.units, td.passed AS test_passed
FROM parts_final p
JOIN test_data_final td
  ON  p.product       = td.product
  AND p.test_category = td.test_category
  AND p.sub_process   IS NOT DISTINCT FROM td.sub_process
  AND p.lot_id        = td.lot_id
  AND p.wafer_id      = td.wafer_id
  AND p.die_key       = td.die_key
WHERE p.lot_id = getvariable('lot')
  AND opt_eq(p.sub_process, getvariable('sub_process'))
ORDER BY p.test_category, p.sub_process, p.wafer_id, p.part_id, td.test_num
