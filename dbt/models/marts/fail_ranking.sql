-- query.py Fail ランキングセル / database.py:163-177 の全ロット版。
SELECT lot_id, test_num, test_name,
       COUNT(*) AS total,
       SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) AS fails,
       ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) AS fail_pct
FROM {{ ref('stg_test_data_final') }}
GROUP BY lot_id, test_num, test_name
HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
