-- ロット内のテスト項目一覧(リミット付き)。サブプロセスごとに別行(プログラムが違えばリミットも違う)。
SET VARIABLE sub_process = NULL;

SELECT DISTINCT test_category, sub_process, test_num, test_name, rec_type, units, lo_limit, hi_limit
FROM test_data_final
WHERE lot_id = getvariable('lot')
  AND opt_eq(sub_process, getvariable('sub_process'))
ORDER BY test_category, sub_process, test_num
