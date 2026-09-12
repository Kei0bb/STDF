-- ダイ×テストの明細(CSV 書き出し用。run(..., out="x.csv") を推奨)
SET VARIABLE lot = 'LOT001';

SELECT p.lot_id, p.wafer_id, p.part_id, p.x_coord, p.y_coord,
       p.hard_bin, p.soft_bin, p.passed AS die_passed,
       td.test_num, td.test_name, td.result,
       td.lo_limit, td.hi_limit, td.units, td.passed AS test_passed
FROM parts_final p
JOIN test_data_final td
  ON  p.lot_id   = td.lot_id
  AND p.wafer_id = td.wafer_id
  AND p.die_key  = td.die_key
WHERE p.lot_id = getvariable('lot')
ORDER BY p.wafer_id, p.part_id, td.test_num
