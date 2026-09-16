-- ロット一覧(新しい順)。job_mixed はロット途中でテストプログラムが変わった印。
-- 条件はすべて任意: 渡さなければ絞らない。s.run("lot_list", product="P1", test_category="CP")
SET VARIABLE product       = NULL;
SET VARIABLE test_category = NULL;
SET VARIABLE sub_process   = NULL;

SELECT lot_id, product, test_category, sub_process,
       start_time, finish_time,
       job_name, job_rev, job_variant_count, job_mixed
FROM lots
WHERE opt_eq(product,       getvariable('product'))
  AND opt_eq(test_category, getvariable('test_category'))
  AND opt_eq(sub_process,   getvariable('sub_process'))
ORDER BY start_time DESC
