-- query.py Cp/Cpk セルの全ロット版。
-- query.py と違い ROUND しない — 表示丸めは利用側の責務。NULLIF(σ=0) 防御を追加。
SELECT lot_id, test_num, test_name, units, lo_limit, hi_limit,
       COUNT(*)                 AS n,
       AVG(result)              AS mean,
       STDDEV(result)           AS sigma,
       MIN(result)              AS min_val,
       MAX(result)              AS max_val,
       MEDIAN(result)           AS median,
       (hi_limit - lo_limit) / NULLIF(6 * STDDEV(result), 0) AS cp,
       LEAST(
           (hi_limit - AVG(result)) / NULLIF(3 * STDDEV(result), 0),
           (AVG(result) - lo_limit) / NULLIF(3 * STDDEV(result), 0)
       ) AS cpk
FROM {{ ref('stg_test_data_final') }}
WHERE lo_limit IS NOT NULL AND hi_limit IS NOT NULL
  AND rec_type IN ('PTR', 'MPR')
GROUP BY lot_id, test_num, test_name, units, lo_limit, hi_limit
