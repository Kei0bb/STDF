-- Fail テストランキング(ワースト順)
SET VARIABLE lot = 'LOT001';

SELECT test_num, test_name,
       COUNT(*) AS total,
       SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) AS fail_cnt,
       ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) AS fail_pct
FROM test_data_final
WHERE lot_id = getvariable('lot')
GROUP BY test_num, test_name
HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
ORDER BY fail_pct DESC
