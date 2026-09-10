-- ロット一覧(新しい順)。job_mixed はロット途中でテストプログラムが変わった印。
SELECT lot_id, product, test_category, sub_process,
       start_time, finish_time,
       job_name, job_rev, job_variant_count, job_mixed
FROM lots
ORDER BY start_time DESC
