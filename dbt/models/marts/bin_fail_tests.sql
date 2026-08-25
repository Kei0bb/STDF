-- query.py ビン×Failテストセルの全ロット版。
SELECT p.lot_id, p.hard_bin, p.soft_bin, td.test_num, td.test_name,
       COUNT(*) AS fail_count
FROM {{ ref('stg_parts_final') }} p
JOIN {{ ref('stg_test_data_final') }} td
  ON  p.lot_id   = td.lot_id
  AND p.wafer_id = td.wafer_id
  AND p.part_id  = td.part_id
WHERE p.passed = FALSE AND td.passed = 'F'
GROUP BY p.lot_id, p.hard_bin, p.soft_bin, td.test_num, td.test_name
