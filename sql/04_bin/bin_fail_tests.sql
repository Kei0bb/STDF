-- ビンごとに、どのテストで落ちたか。サブプロセス(CP1/CP2…)は混ぜず別行。
SET VARIABLE sub_process = NULL;

SELECT p.test_category, p.sub_process, p.hard_bin, p.soft_bin, td.test_num, td.test_name,
       COUNT(*) AS fail_count
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
  AND p.passed = FALSE AND td.passed = 'F'
GROUP BY p.test_category, p.sub_process, p.hard_bin, p.soft_bin, td.test_num, td.test_name
ORDER BY fail_count DESC
