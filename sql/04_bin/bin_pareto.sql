-- ビン分布(ロット全体)
SET VARIABLE lot = 'LOT001';

SELECT hard_bin, soft_bin,
       COUNT(*) AS die_count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM parts_final
WHERE lot_id = getvariable('lot')
GROUP BY hard_bin, soft_bin
ORDER BY die_count DESC
