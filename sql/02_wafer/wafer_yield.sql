-- ウェーハ別 歩留まり(1ロット)。probed=実測ダイ数, total=gross die 適用後の分母。
SET VARIABLE lot = 'LOT001';

SELECT wafer_id, probed, unprobed, total, good,
       total - good AS fail,
       ROUND(100.0 * good / NULLIF(total, 0), 2) AS yield_pct
FROM wafer_yield_final
WHERE lot_id = getvariable('lot')
ORDER BY wafer_id
