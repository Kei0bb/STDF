-- Cp / Cpk (test_name を '%' にすると全テスト)。サブプロセス(CP1/CP2…)ごとに別行。
SET VARIABLE test_name   = '%';
SET VARIABLE sub_process = NULL;

SELECT test_category, sub_process,
       test_num, test_name, units, lo_limit, hi_limit,
       COUNT(*)                 AS n,
       ROUND(AVG(result), 4)    AS mean,
       ROUND(STDDEV(result), 4) AS sigma,
       ROUND(MIN(result), 4)    AS min_val,
       ROUND(MAX(result), 4)    AS max_val,
       ROUND(MEDIAN(result), 4) AS median,
       ROUND((hi_limit - lo_limit) / NULLIF(6 * STDDEV(result), 0), 3) AS cp,
       ROUND(LEAST((hi_limit - AVG(result)) / NULLIF(3 * STDDEV(result), 0),
                   (AVG(result) - lo_limit) / NULLIF(3 * STDDEV(result), 0)), 3) AS cpk
FROM test_data_final
WHERE lot_id = getvariable('lot')
  AND test_name LIKE getvariable('test_name')
  AND opt_eq(sub_process, getvariable('sub_process'))
  AND lo_limit IS NOT NULL AND hi_limit IS NOT NULL
  AND result IS NOT NULL
  AND rec_type IN ('PTR', 'MPR')
GROUP BY test_category, sub_process, test_num, test_name, units, lo_limit, hi_limit
ORDER BY test_category, sub_process, cpk NULLS LAST
