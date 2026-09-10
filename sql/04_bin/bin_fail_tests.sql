-- ビンごとに、どのテストで落ちたか
SET VARIABLE lot = 'LOT001';

SELECT p.hard_bin, p.soft_bin, td.test_num, td.test_name,
       COUNT(*) AS fail_count
FROM parts_final p
JOIN test_data_final td
  ON  p.lot_id   = td.lot_id
  AND p.wafer_id = td.wafer_id
  AND p.part_id  = td.part_id
WHERE p.lot_id = getvariable('lot')
  AND p.passed = FALSE AND td.passed = 'F'
GROUP BY p.hard_bin, p.soft_bin, td.test_num, td.test_name
ORDER BY fail_count DESC
