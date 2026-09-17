-- ロット内のテスト項目一覧(リミット付き)

SELECT DISTINCT test_num, test_name, rec_type, units, lo_limit, hi_limit
FROM test_data_final
WHERE lot_id = getvariable('lot')
ORDER BY test_num
