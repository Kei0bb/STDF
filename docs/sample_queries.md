# サンプル SQL クエリ集

`schema.md` で定義された 4 テーブル（`lots` / `wafers` / `parts` / `test_data`）を  
DuckDB ビュー経由で検索する際のリファレンスです。

> [!TIP]
> パーティション列（`product`, `test_category`, `sub_process`, `lot_id`）で絞ると高速です。

---

## 1. テストプログラム Check Out

### 1-1. テストプログラム名・リビジョン一覧

```sql
SELECT DISTINCT
    product,
    sub_process,
    job_name,
    job_rev
FROM lots
ORDER BY product, sub_process, job_name, job_rev;
```

### 1-2. 特定プログラムの最新リビジョンを確認

```sql
SELECT
    product,
    sub_process,
    job_name,
    job_rev,
    MAX(start_time) AS last_used
FROM lots
GROUP BY product, sub_process, job_name, job_rev
ORDER BY last_used DESC;
```

### 1-3. 特定ロットのテストプログラム情報

```sql
SELECT
    lot_id,
    product,
    sub_process,
    job_name,
    job_rev,
    tester_type,
    operator,
    start_time,
    finish_time
FROM lots
WHERE lot_id = 'YOUR_LOT_ID';
```

### 1-4. 特定ロットのテストプログラムと全データ一括取得

4 テーブルを JOIN し、テストプログラム情報から測定値まで 1 クエリで取得します。

```sql
SELECT
    -- ロット / テストプログラム情報
    l.lot_id,
    l.product,
    l.sub_process,
    l.job_name,
    l.job_rev,
    l.tester_type,
    l.operator,
    l.start_time       AS lot_start,
    l.finish_time      AS lot_finish,
    -- ウェーハ情報
    w.wafer_id,
    w.part_count,
    w.good_count,
    w.retest_num,
    -- ダイ情報
    p.part_id,
    p.x_coord,
    p.y_coord,
    p.hard_bin,
    p.soft_bin,
    p.passed           AS die_passed,
    p.test_time,
    -- テスト測定値
    td.test_num,
    td.test_name,
    td.rec_type,
    td.result,
    td.lo_limit,
    td.hi_limit,
    td.units,
    td.passed           AS test_passed
FROM lots l
JOIN (
    -- 最新リテストのウェーハのみ
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY lot_id, wafer_id
               ORDER BY retest_num DESC
           ) AS rn
    FROM wafers
) w  ON l.lot_id = w.lot_id AND w.rn = 1
JOIN parts p
    ON  w.lot_id   = p.lot_id
    AND w.wafer_id = p.wafer_id
JOIN test_data td
    ON  p.lot_id   = td.lot_id
    AND p.wafer_id = td.wafer_id
    AND p.part_id  = td.part_id
WHERE l.lot_id = 'YOUR_LOT_ID'
ORDER BY w.wafer_id, p.part_id, td.test_num;
```

> [!TIP]
> 行数が多い場合は `COPY (...) TO 'output.csv'` で CSV 出力するか、  
> `WHERE w.wafer_id = '...'` を追加してウェーハ単位で絞ると扱いやすくなります。

---

## 2. ロット検索

### 2-1. 製品 × 工程でロット一覧

```sql
SELECT
    lot_id,
    product,
    sub_process,
    start_time,
    finish_time,
    job_name,
    job_rev
FROM lots
WHERE product    = 'YOUR_PRODUCT'
  AND sub_process = 'CP1'
ORDER BY start_time DESC;
```

### 2-2. 期間指定でロット検索

```sql
SELECT *
FROM lots
WHERE start_time >= TIMESTAMP '2025-01-01'
  AND start_time <  TIMESTAMP '2025-02-01'
ORDER BY start_time;
```

---

## 3. ウェーハ検索

### 3-1. ロット内ウェーハ一覧（最新リテストのみ）

```sql
WITH latest AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY lot_id, wafer_id
               ORDER BY retest_num DESC
           ) AS rn
    FROM wafers
    WHERE lot_id = 'YOUR_LOT_ID'
)
SELECT
    wafer_id,
    part_count,
    good_count,
    rtst_count,
    abrt_count,
    retest_num,
    ROUND(good_count * 100.0 / part_count, 2) AS yield_pct
FROM latest
WHERE rn = 1
ORDER BY wafer_id;
```

### 3-2. 歩留まり低下ウェーハの抽出

```sql
WITH latest AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY lot_id, wafer_id
               ORDER BY retest_num DESC
           ) AS rn
    FROM wafers
)
SELECT
    lot_id,
    wafer_id,
    part_count,
    good_count,
    ROUND(good_count * 100.0 / part_count, 2) AS yield_pct
FROM latest
WHERE rn = 1
  AND good_count * 100.0 / part_count < 90   -- 歩留 90% 未満
ORDER BY yield_pct;
```

---

## 4. ダイ（パーツ）検索

### 4-1. ウェーハ内の Fail ダイ一覧

```sql
SELECT
    part_id,
    x_coord,
    y_coord,
    hard_bin,
    soft_bin,
    test_count,
    test_time
FROM parts
WHERE lot_id   = 'YOUR_LOT_ID'
  AND wafer_id = 'YOUR_WAFER_ID'
  AND passed   = FALSE
ORDER BY hard_bin, soft_bin;
```

### 4-2. ビン分布（ウェーハ単位）

```sql
SELECT
    hard_bin,
    soft_bin,
    COUNT(*)                                    AS die_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM parts
WHERE lot_id   = 'YOUR_LOT_ID'
  AND wafer_id = 'YOUR_WAFER_ID'
GROUP BY hard_bin, soft_bin
ORDER BY hard_bin, soft_bin;
```

### 4-3. ロット全体のビンサマリ

```sql
SELECT
    hard_bin,
    soft_bin,
    COUNT(*)  AS die_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM parts
WHERE lot_id = 'YOUR_LOT_ID'
GROUP BY hard_bin, soft_bin
ORDER BY die_count DESC;
```

---

## 5. テストデータ検索

### 5-1. 特定テスト項目の測定値分布

```sql
SELECT
    test_num,
    test_name,
    units,
    lo_limit,
    hi_limit,
    COUNT(*)                         AS cnt,
    ROUND(AVG(result), 4)            AS avg_val,
    ROUND(STDDEV(result), 4)         AS stddev_val,
    ROUND(MIN(result), 4)            AS min_val,
    ROUND(MAX(result), 4)            AS max_val,
    ROUND(MEDIAN(result), 4)         AS median_val
FROM test_data
WHERE lot_id    = 'YOUR_LOT_ID'
  AND test_name = 'YOUR_TEST_NAME'
GROUP BY test_num, test_name, units, lo_limit, hi_limit;
```

### 5-2. 規格外（Fail）テストの抽出

```sql
SELECT
    part_id,
    x_coord,
    y_coord,
    test_num,
    test_name,
    result,
    lo_limit,
    hi_limit,
    units
FROM test_data
WHERE lot_id   = 'YOUR_LOT_ID'
  AND wafer_id = 'YOUR_WAFER_ID'
  AND passed   = 'F'
ORDER BY test_num, part_id;
```

### 5-3. テスト項目ごとの Fail 率ワーストランキング

```sql
SELECT
    test_num,
    test_name,
    COUNT(*)                                            AS total,
    SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)      AS fail_cnt,
    ROUND(
        SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                                   AS fail_pct
FROM test_data
WHERE lot_id = 'YOUR_LOT_ID'
GROUP BY test_num, test_name
HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
ORDER BY fail_pct DESC;
```

### 5-4. 特定ダイの全テスト結果

```sql
SELECT
    test_num,
    test_name,
    rec_type,
    result,
    lo_limit,
    hi_limit,
    units,
    passed
FROM test_data
WHERE lot_id  = 'YOUR_LOT_ID'
  AND part_id = 'YOUR_PART_ID'
ORDER BY test_num;
```

---

## 6. クロステーブル分析

### 6-1. ロット歩留まりトレンド（日次）

```sql
SELECT
    DATE_TRUNC('day', l.start_time) AS test_date,
    l.product,
    l.sub_process,
    COUNT(DISTINCT l.lot_id)        AS lot_count,
    SUM(w.good_count)               AS total_good,
    SUM(w.part_count)               AS total_parts,
    ROUND(SUM(w.good_count) * 100.0 / SUM(w.part_count), 2) AS yield_pct
FROM lots l
JOIN (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY lot_id, wafer_id
               ORDER BY retest_num DESC
           ) AS rn
    FROM wafers
) w ON l.lot_id = w.lot_id AND w.rn = 1
WHERE l.product = 'YOUR_PRODUCT'
GROUP BY test_date, l.product, l.sub_process
ORDER BY test_date;
```

### 6-2. テスター × オペレータ別の歩留まり比較

```sql
SELECT
    l.tester_type,
    l.operator,
    COUNT(DISTINCT l.lot_id)  AS lot_count,
    SUM(w.good_count)         AS total_good,
    SUM(w.part_count)         AS total_parts,
    ROUND(SUM(w.good_count) * 100.0 / SUM(w.part_count), 2) AS yield_pct
FROM lots l
JOIN (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY lot_id, wafer_id
               ORDER BY retest_num DESC
           ) AS rn
    FROM wafers
) w ON l.lot_id = w.lot_id AND w.rn = 1
GROUP BY l.tester_type, l.operator
ORDER BY yield_pct;
```

### 6-3. Fail ビンとテスト項目の紐付け

```sql
SELECT
    p.hard_bin,
    p.soft_bin,
    td.test_num,
    td.test_name,
    COUNT(*) AS fail_count
FROM parts p
JOIN test_data td
    ON  p.lot_id   = td.lot_id
    AND p.wafer_id = td.wafer_id
    AND p.part_id  = td.part_id
WHERE p.lot_id  = 'YOUR_LOT_ID'
  AND p.passed  = FALSE
  AND td.passed = 'F'
GROUP BY p.hard_bin, p.soft_bin, td.test_num, td.test_name
ORDER BY fail_count DESC;
```

---

## 7. Cp / Cpk 算出

```sql
SELECT
    test_num,
    test_name,
    units,
    lo_limit,
    hi_limit,
    COUNT(*)                  AS n,
    ROUND(AVG(result), 6)     AS mean,
    ROUND(STDDEV(result), 6)  AS sigma,
    -- Cp = (USL - LSL) / (6σ)
    ROUND(
        (hi_limit - lo_limit) / (6 * STDDEV(result)), 3
    ) AS cp,
    -- Cpk = min((USL - μ) / 3σ, (μ - LSL) / 3σ)
    ROUND(
        LEAST(
            (hi_limit - AVG(result)) / (3 * STDDEV(result)),
            (AVG(result) - lo_limit) / (3 * STDDEV(result))
        ), 3
    ) AS cpk
FROM test_data
WHERE lot_id    = 'YOUR_LOT_ID'
  AND lo_limit IS NOT NULL
  AND hi_limit IS NOT NULL
  AND rec_type IN ('PTR', 'MPR')
GROUP BY test_num, test_name, units, lo_limit, hi_limit
HAVING COUNT(*) > 1
ORDER BY cpk;
```

---

## 注意事項

- プレースホルダ（`YOUR_LOT_ID` 等）は実際の値に置き換えてください。
- `wafers` テーブルにリテスト履歴がある場合、最新リテストのみ取得する  
  `ROW_NUMBER() ... ORDER BY retest_num DESC` パターンを使用してください。
- `parts.passed` は **BOOL** 型、`test_data.passed` は **STRING** 型（`'P'` / `'F'`）です。
