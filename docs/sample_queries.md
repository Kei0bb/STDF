# サンプル SQL クエリ集

`schema.md` で定義された 5 テーブル（`lots` / `wafers` / `parts` / `test_data` /
`chipid`）を DuckDB ビュー経由で検索する際のリファレンスです。

> [!IMPORTANT]
> **歩留り・解析は原則 `*_final` ビューを使ってください。**
> 生テーブル（`parts` / `test_data`）はリテストの**全試行**を含むため、そのまま
> 集計すると二重計上になります。`parts_final` / `test_data_final` /
> `chipid_final` は「ダイ/パッケージごとに最新リテストのみ」へ重複排除済みです。
> `wafers` の `part_count` / `good_count` は WRR の報告値（リテストは部分母集団・
> FT には存在しない）なので、**歩留りは `parts_final` から算出**します。

## 実行方法

### VS Code で対話的に実行（推奨）

プロジェクトルートの **`query.py`** を VS Code で開き、各セル (`# %%`) を Shift+Enter で実行します。
[Python 拡張](https://marketplace.visualstudio.com/items?itemName=ms-python.python) + Jupyter サポートが必要です。

```
# セットアップセルを実行後、LOT_ID を書き換えて各セルを実行
LOT_ID = "E6A773.00"
```

> [!TIP]
> **大量データで `*_final` クエリが遅いとき**：`*_final` は `ROW_NUMBER()` の重複
> 排除を毎回計算するため、巨大テーブルでは重くなります。1 ロットに絞って作業する
> なら `use_lot('LOT_ID')` を呼ぶと、そのロットの `parts_final` /
> `test_data_final` / `chipid_final` をメモリ上の表に materialize し、以降のクエリが
> 約 45 倍高速になります（実測）。全ロットに戻すには `use_all()`。
> 全件を Python に取り込む場合は `q(sql, limit=0, as_arrow=True)`（pandas 変換を
> 省いて約 3.5 倍）か、ファイル出力なら `to_csv(sql)` を使ってください。

### CLI から直接実行

```bash
# DuckDB シェル（対話モード）
stdf db shell

# 1 クエリを実行して表示
stdf db query "SELECT lot_id, product FROM lots ORDER BY start_time DESC LIMIT 10"
```

### Python スクリプトから（`*_final` ビューの定義込み）

`stdf db` / `query.py` は `*_final` を自動定義しますが、素の DuckDB から使う場合は
以下のように生ビューと派生ビューを作成します。

```python
import duckdb
con = duckdb.connect(":memory:")

for t in ["lots", "wafers", "parts", "test_data", "chipid"]:
    con.execute(f"""
        CREATE OR REPLACE VIEW {t} AS
        SELECT * FROM read_parquet('data/{t}/**/*.parquet', hive_partitioning=true)
    """)

# dedup identity: CP=ダイ座標 / FT=パッケージ 2D バーコード
DEDUP = ("CASE WHEN test_category = 'FT' THEN part_txt "
         "ELSE CONCAT(wafer_id, '|', x_coord, '|', y_coord) END")

con.execute(f"""
    CREATE OR REPLACE VIEW parts_final AS
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY lot_id, {DEDUP} ORDER BY retest_num DESC) AS rn
        FROM parts) WHERE rn = 1
""")
con.execute(f"""
    CREATE OR REPLACE VIEW test_data_final AS
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY lot_id, {DEDUP}, test_num, pin_num
            ORDER BY retest_num DESC) AS rn
        FROM test_data) WHERE rn = 1
""")
con.execute("""
    CREATE OR REPLACE VIEW chipid_final AS
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY lot_id, efuse_raw ORDER BY retest_num DESC) AS rn
        FROM chipid) WHERE rn = 1
""")
```

---

> [!TIP]
> パーティション列（`product`, `test_category`, `sub_process`, `lot_id`,
> `wafer_id`, `retest`）で絞ると高速です。

---

## 1. テストプログラム Check Out

### 1-1. テストプログラム名・リビジョン一覧

```sql
SELECT DISTINCT product, sub_process, job_name, job_rev
FROM lots
ORDER BY product, sub_process, job_name, job_rev;
```

### 1-2. 特定プログラムの最新リビジョンを確認

```sql
SELECT product, sub_process, job_name, job_rev,
       MAX(start_time) AS last_used
FROM lots
GROUP BY product, sub_process, job_name, job_rev
ORDER BY last_used DESC;
```

### 1-3. 特定ロットのテストプログラム情報

```sql
SELECT lot_id, product, sub_process, job_name, job_rev,
       tester_type, operator, start_time, finish_time
FROM lots
WHERE lot_id = 'YOUR_LOT_ID';
```

### 1-4. 特定ロットのテストプログラムと全データ一括取得

ロット → ダイ（最新リテスト）→ 測定値を 1 クエリで取得します。`parts_final` /
`test_data_final` を使うのでリテストは最新のみ。

```sql
SELECT
    l.lot_id, l.product, l.sub_process, l.job_name, l.job_rev,
    l.tester_type, l.operator,
    -- ダイ情報（最新リテスト）
    p.part_id, p.wafer_id, p.part_txt,
    p.x_coord, p.y_coord, p.hard_bin, p.soft_bin,
    p.passed AS die_passed, p.retest_num,
    -- テスト測定値（最新リテスト）
    td.test_num, td.test_name, td.rec_type,
    td.result, td.lo_limit, td.hi_limit, td.units,
    td.passed AS test_passed
FROM lots l
JOIN parts_final p      ON l.lot_id = p.lot_id
JOIN test_data_final td ON p.lot_id = td.lot_id AND p.part_id = td.part_id
WHERE l.lot_id = 'YOUR_LOT_ID'
ORDER BY p.wafer_id, p.part_id, td.test_num;
```

> [!TIP]
> 行数が多い場合は `COPY (...) TO 'output.csv' (HEADER)` で CSV 出力するか、
> `WHERE p.wafer_id = '...'` を追加して絞ると扱いやすくなります。

---

## 2. ロット検索

### 2-1. 製品 × 工程でロット一覧

```sql
SELECT lot_id, product, sub_process, start_time, finish_time, job_name, job_rev
FROM lots
WHERE product = 'YOUR_PRODUCT' AND sub_process = 'CP1'
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

### 2-3. ロット歩留りサマリ（CP / FT 両対応・retest 反映）

`parts_final` から算出するので、CP も FT も同じクエリで正しい最終歩留りが出ます。

```sql
SELECT
    l.lot_id, l.product, l.test_category, l.sub_process, l.job_name,
    COUNT(*)                                              AS dies,
    SUM(CASE WHEN p.passed THEN 1 ELSE 0 END)             AS good,
    ROUND(100.0 * SUM(CASE WHEN p.passed THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                       AS yield_pct
FROM lots l
JOIN parts_final p ON l.lot_id = p.lot_id
GROUP BY l.lot_id, l.product, l.test_category, l.sub_process, l.job_name
ORDER BY l.product, l.test_category, yield_pct;
```

---

## 3. ウェーハ / 歩留り（CP）

### 3-1. ロット内ウェーハ別歩留り（retest 反映 / parts_final 由来）

```sql
SELECT
    wafer_id,
    COUNT(*)                                             AS dies,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END)              AS good,
    ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                      AS yield_pct
FROM parts_final
WHERE lot_id = 'YOUR_LOT_ID'
GROUP BY wafer_id
ORDER BY yield_pct;
```

### 3-2. 歩留り低下ウェーハの抽出（90% 未満）

```sql
SELECT wafer_id, COUNT(*) AS dies,
       SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS good,
       ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0), 2) AS yield_pct
FROM parts_final
WHERE lot_id = 'YOUR_LOT_ID'
GROUP BY wafer_id
HAVING ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0), 2) < 90
ORDER BY yield_pct;
```

### 3-3. WRR 報告値 vs 実測（parts）歩留りの突合

`wafers`（WRR）と `parts_final` の歩留りが食い違う＝リテストや欠損の兆候。

```sql
SELECT
    w.wafer_id,
    w.good_count                                  AS wrr_good,
    w.part_count                                  AS wrr_total,
    p.good                                        AS parts_good,
    p.dies                                        AS parts_total,
    ROUND(100.0 * w.good_count / NULLIF(w.part_count, 0), 2) AS wrr_yield,
    ROUND(100.0 * p.good / NULLIF(p.dies, 0), 2)            AS parts_yield
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC) AS rn
    FROM wafers WHERE lot_id = 'YOUR_LOT_ID'
) w
JOIN (
    SELECT wafer_id, COUNT(*) AS dies,
           SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS good
    FROM parts_final WHERE lot_id = 'YOUR_LOT_ID'
    GROUP BY wafer_id
) p ON w.wafer_id = p.wafer_id
WHERE w.rn = 1
ORDER BY ABS(wrr_yield - parts_yield) DESC;
```

### 3-4. ウェーハマップ（die 座標 × bin）

ヒートマップ／ウェーハマップ描画用の素データ。

```sql
SELECT x_coord, y_coord, hard_bin, soft_bin, passed
FROM parts_final
WHERE lot_id = 'YOUR_LOT_ID' AND wafer_id = 'YOUR_WAFER_ID'
ORDER BY y_coord, x_coord;
```

### 3-5. ウェーハ中心 vs エッジの歩留り（ゾーン分析）

エッジ不良の傾向を見る。半径はウェーハ座標系に合わせて調整。

```sql
WITH d AS (
    SELECT *,
           SQRT(POWER(x_coord, 2) + POWER(y_coord, 2)) AS r
    FROM parts_final
    WHERE lot_id = 'YOUR_LOT_ID' AND wafer_id = 'YOUR_WAFER_ID'
)
SELECT
    CASE WHEN r <= 50 THEN '1_center'
         WHEN r <= 90 THEN '2_middle'
         ELSE '3_edge' END                          AS zone,
    COUNT(*)                                         AS dies,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END)          AS good,
    ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                  AS yield_pct
FROM d
GROUP BY zone
ORDER BY zone;
```

---

## 4. ダイ / ビン分析

### 4-1. ウェーハ内の Fail ダイ一覧

```sql
SELECT part_id, x_coord, y_coord, hard_bin, soft_bin, test_count, test_time
FROM parts_final
WHERE lot_id = 'YOUR_LOT_ID' AND wafer_id = 'YOUR_WAFER_ID' AND passed = FALSE
ORDER BY hard_bin, soft_bin;
```

### 4-2. ビン分布（ウェーハ単位）

```sql
SELECT hard_bin, soft_bin,
       COUNT(*)                                          AS die_count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM parts_final
WHERE lot_id = 'YOUR_LOT_ID' AND wafer_id = 'YOUR_WAFER_ID'
GROUP BY hard_bin, soft_bin
ORDER BY hard_bin, soft_bin;
```

### 4-3. ソフトビン・パレート（累積 % 付き）

不良ビンの寄与度を上位から。歩留り改善の優先順位付けに。

```sql
SELECT
    soft_bin,
    COUNT(*)                                                  AS die_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)        AS pct,
    ROUND(100.0 * SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC)
          / SUM(COUNT(*)) OVER (), 2)                         AS cumulative_pct
FROM parts_final
WHERE lot_id = 'YOUR_LOT_ID' AND passed = FALSE
GROUP BY soft_bin
ORDER BY die_count DESC;
```

### 4-4. 隣接 Fail のクラスタ検出（同一座標近傍の連続不良）

Fail die の上下左右いずれかに別の Fail がある＝クラスタ不良の候補。

```sql
WITH f AS (
    SELECT x_coord, y_coord
    FROM parts_final
    WHERE lot_id = 'YOUR_LOT_ID' AND wafer_id = 'YOUR_WAFER_ID'
      AND passed = FALSE
)
SELECT a.x_coord, a.y_coord,
       COUNT(b.x_coord) AS fail_neighbors
FROM f a
JOIN f b
  ON ABS(a.x_coord - b.x_coord) + ABS(a.y_coord - b.y_coord) = 1
GROUP BY a.x_coord, a.y_coord
ORDER BY fail_neighbors DESC, a.y_coord, a.x_coord;
```

---

## 5. パラメトリック測定（test_data）

### 5-1. 特定テスト項目の統計サマリ

```sql
SELECT
    test_num, test_name, units, lo_limit, hi_limit,
    COUNT(*)                  AS cnt,
    ROUND(AVG(result), 4)     AS avg_val,
    ROUND(STDDEV(result), 4)  AS stddev_val,
    ROUND(MIN(result), 4)     AS min_val,
    ROUND(MAX(result), 4)     AS max_val,
    ROUND(MEDIAN(result), 4)  AS median_val,
    ROUND(QUANTILE_CONT(result, 0.25), 4) AS q1,
    ROUND(QUANTILE_CONT(result, 0.75), 4) AS q3
FROM test_data_final
WHERE lot_id = 'YOUR_LOT_ID' AND test_name = 'YOUR_TEST_NAME'
GROUP BY test_num, test_name, units, lo_limit, hi_limit;
```

### 5-2. 規格外（Fail）テストの抽出

```sql
SELECT part_id, x_coord, y_coord, test_num, test_name,
       result, lo_limit, hi_limit, units
FROM test_data_final
WHERE lot_id = 'YOUR_LOT_ID' AND wafer_id = 'YOUR_WAFER_ID' AND passed = 'F'
ORDER BY test_num, part_id;
```

### 5-3. テスト項目ごとの Fail 率ワーストランキング

```sql
SELECT
    test_num, test_name,
    COUNT(*)                                       AS total,
    SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)  AS fail_cnt,
    ROUND(SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)                   AS fail_pct
FROM test_data_final
WHERE lot_id = 'YOUR_LOT_ID'
GROUP BY test_num, test_name
HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
ORDER BY fail_pct DESC;
```

### 5-4. 特定ダイの全テスト結果

```sql
SELECT test_num, test_name, rec_type, result, lo_limit, hi_limit, units, passed
FROM test_data_final
WHERE lot_id = 'YOUR_LOT_ID' AND part_id = 'YOUR_PART_ID'
ORDER BY test_num;
```

### 5-5. 規格マージン（限界に近いテスト＝歩留りリスク）

測定値が規格幅のどれだけ内側にあるか。`margin_pct` が小さいほど危険。

```sql
SELECT
    test_num, test_name, units, lo_limit, hi_limit,
    ROUND(AVG(result), 4)  AS mean,
    ROUND(100.0 * LEAST(AVG(result) - lo_limit, hi_limit - AVG(result))
          / NULLIF(hi_limit - lo_limit, 0), 2) AS margin_pct
FROM test_data_final
WHERE lot_id = 'YOUR_LOT_ID'
  AND lo_limit IS NOT NULL AND hi_limit IS NOT NULL
  AND rec_type IN ('PTR', 'MPR')
GROUP BY test_num, test_name, units, lo_limit, hi_limit
HAVING COUNT(*) > 1
ORDER BY margin_pct;
```

### 5-6. 外れ値検出（平均 ± 3σ を超えるダイ）

```sql
WITH stat AS (
    SELECT test_num, AVG(result) AS mu, STDDEV(result) AS sigma
    FROM test_data_final
    WHERE lot_id = 'YOUR_LOT_ID' AND test_name = 'YOUR_TEST_NAME'
    GROUP BY test_num
)
SELECT t.part_id, t.x_coord, t.y_coord, t.result,
       ROUND((t.result - s.mu) / NULLIF(s.sigma, 0), 2) AS z_score
FROM test_data_final t
JOIN stat s ON t.test_num = s.test_num
WHERE t.lot_id = 'YOUR_LOT_ID' AND t.test_name = 'YOUR_TEST_NAME'
  AND ABS(t.result - s.mu) > 3 * s.sigma
ORDER BY ABS(t.result - s.mu) DESC;
```

### 5-7. サイト間ばらつき（site-to-site）

`site_num`（parts 側）別にテスト平均を比較。テスター間差・ハンドラ差の検出。

```sql
SELECT
    p.site_num,
    td.test_name,
    COUNT(*)                 AS n,
    ROUND(AVG(td.result), 4) AS mean,
    ROUND(STDDEV(td.result), 4) AS sigma
FROM test_data_final td
JOIN parts_final p ON td.lot_id = p.lot_id AND td.part_id = p.part_id
WHERE td.lot_id = 'YOUR_LOT_ID' AND td.test_name = 'YOUR_TEST_NAME'
GROUP BY p.site_num, td.test_name
ORDER BY p.site_num;
```

### 5-8. 2 テスト項目間の相関

```sql
WITH pivoted AS (
    SELECT part_id,
           MAX(CASE WHEN test_name = 'TEST_A' THEN result END) AS a,
           MAX(CASE WHEN test_name = 'TEST_B' THEN result END) AS b
    FROM test_data_final
    WHERE lot_id = 'YOUR_LOT_ID' AND test_name IN ('TEST_A', 'TEST_B')
    GROUP BY part_id
)
SELECT
    COUNT(*)                       AS n,
    ROUND(CORR(a, b), 4)           AS pearson_r,
    ROUND(REGR_SLOPE(b, a), 6)     AS slope,
    ROUND(REGR_INTERCEPT(b, a), 6) AS intercept
FROM pivoted
WHERE a IS NOT NULL AND b IS NOT NULL;
```

### 5-9. MPR ピン別の分布（マルチピン測定）

```sql
SELECT pin_num, pin_name, units,
       COUNT(*)                 AS n,
       ROUND(AVG(result), 4)    AS mean,
       ROUND(STDDEV(result), 4) AS sigma
FROM test_data_final
WHERE lot_id = 'YOUR_LOT_ID' AND rec_type = 'MPR'
  AND test_name = 'YOUR_TEST_NAME'
GROUP BY pin_num, pin_name, units
ORDER BY pin_num;
```

---

## 6. リテスト分析

### 6-1. リテスト回数の分布（最終 retest_num 別ダイ数）

```sql
SELECT retest_num, COUNT(*) AS dies
FROM parts_final
WHERE lot_id = 'YOUR_LOT_ID'
GROUP BY retest_num
ORDER BY retest_num;
```

### 6-2. リテストによる歩留り改善量（初回 vs 最終）

`retest_num = 0`（初回）と最終（parts_final）の合否を比較。

```sql
WITH first AS (
    SELECT lot_id, wafer_id, x_coord, y_coord, part_txt, passed AS passed0
    FROM parts
    WHERE lot_id = 'YOUR_LOT_ID' AND retest_num = 0
),
final AS (
    SELECT lot_id, wafer_id, x_coord, y_coord, part_txt, passed AS passed_final
    FROM parts_final
    WHERE lot_id = 'YOUR_LOT_ID'
)
SELECT
    COUNT(*)                                                   AS dies,
    SUM(CASE WHEN passed0 THEN 1 ELSE 0 END)                   AS good_run0,
    SUM(CASE WHEN passed_final THEN 1 ELSE 0 END)              AS good_final,
    SUM(CASE WHEN NOT passed0 AND passed_final THEN 1 ELSE 0 END) AS recovered_by_retest
FROM first f
JOIN final fn USING (lot_id, wafer_id, x_coord, y_coord, part_txt);
```

> [!NOTE]
> FT は `wafer_id` / 座標が空のため、結合キーは実質 `part_txt` で効きます。
> CP は `wafer_id` + 座標で die を一意化します。

### 6-3. リテストでも Fail のままのダイ（恒久不良）

```sql
SELECT wafer_id, part_txt, x_coord, y_coord, hard_bin, soft_bin, retest_num
FROM parts_final
WHERE lot_id = 'YOUR_LOT_ID' AND passed = FALSE AND retest_num > 0
ORDER BY wafer_id, y_coord, x_coord;
```

---

## 7. クロステーブル / 工程横断

### 7-1. ロット歩留りトレンド（日次・parts_final 由来）

```sql
SELECT
    DATE_TRUNC('day', l.start_time) AS test_date,
    l.product, l.sub_process,
    COUNT(DISTINCT l.lot_id)        AS lot_count,
    COUNT(*)                        AS dies,
    SUM(CASE WHEN p.passed THEN 1 ELSE 0 END) AS good,
    ROUND(100.0 * SUM(CASE WHEN p.passed THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2) AS yield_pct
FROM lots l
JOIN parts_final p ON l.lot_id = p.lot_id
WHERE l.product = 'YOUR_PRODUCT'
GROUP BY test_date, l.product, l.sub_process
ORDER BY test_date;
```

### 7-2. テスター × オペレータ別の歩留り比較

```sql
SELECT
    l.tester_type, l.operator,
    COUNT(DISTINCT l.lot_id) AS lot_count,
    COUNT(*)                 AS dies,
    SUM(CASE WHEN p.passed THEN 1 ELSE 0 END) AS good,
    ROUND(100.0 * SUM(CASE WHEN p.passed THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2) AS yield_pct
FROM lots l
JOIN parts_final p ON l.lot_id = p.lot_id
GROUP BY l.tester_type, l.operator
ORDER BY yield_pct;
```

### 7-3. Fail ビンとテスト項目の紐付け

```sql
SELECT
    p.hard_bin, p.soft_bin, td.test_num, td.test_name,
    COUNT(*) AS fail_count
FROM parts_final p
JOIN test_data_final td ON p.lot_id = td.lot_id AND p.part_id = td.part_id
WHERE p.lot_id = 'YOUR_LOT_ID' AND p.passed = FALSE AND td.passed = 'F'
GROUP BY p.hard_bin, p.soft_bin, td.test_num, td.test_name
ORDER BY fail_count DESC;
```

### 7-4. 複数ロットの歩留り比較

```sql
SELECT
    l.lot_id, l.job_rev,
    COUNT(*)                 AS dies,
    SUM(CASE WHEN p.passed THEN 1 ELSE 0 END) AS good,
    ROUND(100.0 * SUM(CASE WHEN p.passed THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2) AS yield_pct
FROM lots l
JOIN parts_final p ON l.lot_id = p.lot_id
WHERE l.lot_id IN ('LOT_A', 'LOT_B', 'LOT_C')
GROUP BY l.lot_id, l.job_rev
ORDER BY yield_pct DESC;
```

---

## 8. Cp / Cpk 算出（工程能力）

```sql
SELECT
    test_num, test_name, units, lo_limit, hi_limit,
    COUNT(*)                  AS n,
    ROUND(AVG(result), 6)     AS mean,
    ROUND(STDDEV(result), 6)  AS sigma,
    -- Cp = (USL - LSL) / (6σ)
    ROUND((hi_limit - lo_limit) / (6 * STDDEV(result)), 3) AS cp,
    -- Cpk = min((USL - μ)/3σ, (μ - LSL)/3σ)
    ROUND(LEAST(
        (hi_limit - AVG(result)) / (3 * STDDEV(result)),
        (AVG(result) - lo_limit) / (3 * STDDEV(result))
    ), 3) AS cpk
FROM test_data_final
WHERE lot_id = 'YOUR_LOT_ID'
  AND lo_limit IS NOT NULL AND hi_limit IS NOT NULL
  AND rec_type IN ('PTR', 'MPR')
GROUP BY test_num, test_name, units, lo_limit, hi_limit
HAVING COUNT(*) > 1
ORDER BY cpk;
```

> Cpk < 1.33 はマージン不足の目安。`5-5`（規格マージン）と併せて確認します。

---

## 9. ChipID トレーサビリティ（EN-S0-CHIPID_R / TSMC）

FT の chiplet 製品は 1 パッケージに 2 die を含み、各 die の出自（fab / lot /
wafer / x / y）は GDR の `EN-S0-CHIPID_R`（**5 文字目は数字ゼロ**）をデコードした
`chipid` テーブルに入ります（**FT のみ生成**）。

- パッケージの一意キー = `part_txt`（2D バーコード）
- DUT 内の die 区別 = `chip_occurrence_index`（0 / 1）
- die の恒久 ID = `efuse_raw`（`chipid_final` はこれで最新リテストのみに重複排除）

### 9-1. パッケージ → 2 die の CP 出自を一覧

```sql
SELECT part_txt, chip_occurrence_index,
       origin_fab, origin_lot, origin_wafer, origin_x, origin_y
FROM chipid_final
WHERE lot_id = 'YOUR_FT_LOT'
ORDER BY part_txt, chip_occurrence_index;
```

### 9-2. 単一バーコードから全 die をたどる（現品トレース）

```sql
SELECT chip_occurrence_index, origin_fab, origin_lot,
       origin_wafer, origin_x, origin_y, efuse_raw
FROM chipid_final
WHERE part_txt = 'YOUR_2D_BARCODE'
ORDER BY chip_occurrence_index;
```

### 9-3. CP 出自（ロット / ウェハ）から FT パッケージを逆引き

```sql
SELECT origin_wafer, origin_x, origin_y, part_txt, chip_occurrence_index
FROM chipid_final
WHERE origin_lot = 'E6B156' AND origin_wafer = 11
ORDER BY origin_x, origin_y;
```

### 9-4. FT 不良を CP 出自ウェハ別に集計（CP↔FT 歩留り相関の起点）

```sql
SELECT
    c.origin_fab, c.origin_lot, c.origin_wafer,
    COUNT(*)                                   AS dies,
    SUM(CASE WHEN p.passed THEN 0 ELSE 1 END)  AS fail_dies,
    ROUND(100.0 * SUM(CASE WHEN p.passed THEN 0 ELSE 1 END)
          / COUNT(*), 2)                       AS fail_pct
FROM chipid_final c
JOIN parts_final p ON p.lot_id = c.lot_id AND p.part_txt = c.part_txt
WHERE c.lot_id = 'YOUR_FT_LOT' AND c.valid
GROUP BY c.origin_fab, c.origin_lot, c.origin_wafer
ORDER BY fail_pct DESC;
```

### 9-5. 出自ロット × ウェハの不良ヒートマップ素データ

```sql
SELECT
    c.origin_lot, c.origin_wafer, c.origin_x, c.origin_y,
    COUNT(*)                                  AS dies,
    SUM(CASE WHEN p.passed THEN 0 ELSE 1 END) AS fails
FROM chipid_final c
JOIN parts_final p ON p.lot_id = c.lot_id AND p.part_txt = c.part_txt
WHERE c.lot_id = 'YOUR_FT_LOT' AND c.valid
GROUP BY c.origin_lot, c.origin_wafer, c.origin_x, c.origin_y
ORDER BY fails DESC;
```

### 9-6. 不良パッケージの 2 die が「どの CP ウェハの組み合わせ」か

```sql
WITH d AS (
    SELECT c.part_txt, c.chip_occurrence_index,
           c.origin_lot || ':W' || c.origin_wafer AS origin, p.passed
    FROM chipid_final c
    JOIN parts_final p ON p.lot_id = c.lot_id AND p.part_txt = c.part_txt
    WHERE c.lot_id = 'YOUR_FT_LOT' AND c.valid
)
SELECT
    MAX(CASE WHEN chip_occurrence_index = 0 THEN origin END) AS die0_origin,
    MAX(CASE WHEN chip_occurrence_index = 1 THEN origin END) AS die1_origin,
    COUNT(*) FILTER (WHERE NOT passed) AS fail_count
FROM d
GROUP BY part_txt
HAVING fail_count > 0;
```

### 9-7. fab 組み合わせ別のパッケージ歩留り（TSMC1+2 vs 同一 fab）

```sql
WITH pkg AS (
    SELECT c.part_txt,
           MIN(c.origin_fab) || '+' || MAX(c.origin_fab) AS fab_combo,
           BOOL_AND(p.passed)                            AS pkg_pass
    FROM chipid_final c
    JOIN parts_final p ON p.lot_id = c.lot_id AND p.part_txt = c.part_txt
    WHERE c.lot_id = 'YOUR_FT_LOT' AND c.valid
    GROUP BY c.part_txt
)
SELECT fab_combo,
       COUNT(*)                                  AS packages,
       SUM(CASE WHEN pkg_pass THEN 1 ELSE 0 END) AS good,
       ROUND(100.0 * SUM(CASE WHEN pkg_pass THEN 1 ELSE 0 END)
             / COUNT(*), 2)                       AS yield_pct
FROM pkg
GROUP BY fab_combo
ORDER BY yield_pct;
```

### 9-8. CP 工程の die を FT 結果と突合（工程横断トレース）

FT の `chipid`（出自 lot/wafer/x/y）を、その CP ロットの `parts` に結合します。
**CP の `wafer_id` 表記**（例 `W11` / `11`）が `origin_wafer`（整数）と一致するよう
`LPAD` 等で整形してください（実データの命名規則に合わせる）。

```sql
SELECT
    c.lot_id           AS ft_lot,
    c.part_txt         AS ft_package,
    c.origin_lot       AS cp_lot,
    c.origin_wafer, c.origin_x, c.origin_y,
    cp.passed          AS cp_passed,   -- CP 工程での合否
    ft.passed          AS ft_passed    -- FT 工程での合否
FROM chipid_final c
JOIN parts_final ft
  ON ft.lot_id = c.lot_id AND ft.part_txt = c.part_txt
LEFT JOIN parts_final cp
  ON cp.lot_id  = c.origin_lot
 AND cp.wafer_id = 'W' || LPAD(CAST(c.origin_wafer AS VARCHAR), 2, '0')
 AND cp.x_coord = c.origin_x
 AND cp.y_coord = c.origin_y
WHERE c.lot_id = 'YOUR_FT_LOT' AND c.valid
  AND cp.passed = TRUE AND ft.passed = FALSE   -- CP 良 → FT 不良の die
ORDER BY c.origin_lot, c.origin_wafer;
```

### 9-9. デコード健全性チェック（取りこぼし / 非対応 fab）

```sql
SELECT
    COUNT(*)                                   AS total_chipids,
    SUM(CASE WHEN valid THEN 0 ELSE 1 END)     AS invalid_efuse,
    SUM(CASE WHEN origin_fab = 'UNSUPPORTED' THEN 1 ELSE 0 END) AS unsupported_fab
FROM chipid
WHERE lot_id = 'YOUR_FT_LOT';
```

> **注**: `chipid` は FT のみ。CP では die 出自＝プローブ座標（`parts` の
> `wafer_id` / `x_coord` / `y_coord`）で取得できるため生成されません。

---

## 注意事項

- プレースホルダ（`YOUR_LOT_ID` 等）は実際の値に置き換えてください。
- **歩留り・解析は `parts_final` / `test_data_final` / `chipid_final` を使用**。
  生テーブルはリテスト全試行を含み二重計上になります。
- `wafers` は **CP 専用**で `part_count` / `good_count` は WRR 報告値（リテストは
  部分母集団）。歩留りには使わず、時刻・`rtst_count` 等のメタ参照に留めます。
- `parts.passed` は **BOOL** 型、`test_data.passed` は **STRING** 型（`'P'` / `'F'`）。
- FT は `wafer_id` / 座標が無いため、`*_final` の重複排除は `part_txt`（2D バーコード）
  を identity に使います。`chipid_final` は `efuse_raw`（die 恒久 ID）で排除。
- `chipid` は **FT のみ**生成。CP には行がありません（出自はプローブ座標で代替）。
