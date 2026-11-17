-- ウェーハ別 歩留まり(1ロット)。probed=実測ダイ数, total=gross die 適用後の分母。
-- サブプロセス(CP1/CP2…)は混ぜず別行。sub_process を渡すとそれだけに絞る。
SET VARIABLE sub_process = NULL;

SELECT product, test_category, sub_process, wafer_id,
       probed, unprobed, total, good,
       total - good AS fail,
       ROUND(100.0 * good / NULLIF(total, 0), 2) AS yield_pct
FROM wafer_yield_final
WHERE lot_id = getvariable('lot')
  AND opt_eq(sub_process, getvariable('sub_process'))
ORDER BY test_category, sub_process, wafer_id
