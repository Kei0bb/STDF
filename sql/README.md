# 名前付きクエリライブラリ

**1ファイル1クエリ。** Python の `AnalysisSession.run()` から呼びます。

置き場所は**このフォルダ（リポジトリ直下 `sql/`）だけ**で、git 管理です。
本番機で直接編集して commit / push します。テストも全クエリを合成データで流すので、
壊れた SQL は開発機の `uv run pytest` で気付けます。

```
01_lots/    ロット単位  — 一覧 / 歩留まりサマリ
02_wafer/   ウェーハ単位 — 歩留まり
03_test/    テスト単位  — 項目一覧 / Fail ランキング / Cp・Cpk
04_bin/     ビン単位    — 分布 / ビン×Fail テスト
05_export/  CSV 書き出し前提の明細
```

## 動かし方

`workspace/query.py` で:

```python
s.queries()                                               # 一覧(params 列 = 受け取る変数)
s.run("fail_ranking", lot="LOT002")                       # → DataFrame
s.run("lot_list", product="P1", test_category="CP")       # 条件は複数同時に渡せる
s.run("die_test_export", lot="LOT002", out="lot002.csv")  # → CSV(行数を返す)
print(s.show("cpk"))                                      # SQL を表示(改造の出発点)
```

名前は `03_test/fail_ranking` でも、重複がなければ `fail_ranking` だけでも引けます。
ロット単位のクエリは `sub_process` 列を出力し（CP1 / CP2 は別行）、`sub_process=` で絞れます。

## 変数（条件）の書き方

| SQL の書き方 | 意味 | 渡さなかったら |
|---|---|---|
| `getvariable('lot')` だけ | **必須** | `TypeError`（0 行を黙って返さない） |
| `SET VARIABLE test_name = '%';` + `getvariable('test_name')` | 既定値つき | 既定値を使う |
| `SET VARIABLE product = NULL;` + `opt_eq(product, getvariable('product'))` | **任意条件** | 絞らない |

```sql
-- 条件付きロット一覧
SET VARIABLE product       = NULL;
SET VARIABLE test_category = NULL;

SELECT * FROM lots
WHERE opt_eq(product,       getvariable('product'))
  AND opt_eq(test_category, getvariable('test_category'))
```

- `opt_eq(col, v)` は `v` が NULL なら TRUE、そうでなければ `col = v`。`run()` が用意する
  マクロです。指定した条件は Parquet スキャンまで押し込まれ、未指定の条件は実行計画から
  消えるので、`test_data_final` のような大きいビューでも遅くなりません
- クエリが使わない名前を渡すと `TypeError`（`prodcut=` のような typo を黙って無視しない）
- 変数は `run()` のたびにリセットされます。前の `run()` で渡した値が次のクエリに残りません
- `run()` の `out` は出力先の指定に使うので、変数名にはできません

**値を SQL に文字列連結しないこと。** `run()` は値をバインド変数として渡すので、
クォートを含む値でも壊れません。f-string 埋め込みはそこが壊れます。

## 新しいクエリを足す

1. `sql/` の該当フォルダに `名前.sql` を作る
2. 1行目を `-- 説明` にする（`s.queries()` に出る説明文になります）
3. 条件は `getvariable('...')` で受け、任意にしたいものは `SET VARIABLE x = NULL;` と `opt_eq()` にする
4. commit / push

## どのビューを使うか

歩留りは `wafer_yield_final`（gross die 適用済み・FT ロットも含む）を使ってください。
WRR 値（`runs.part_count` / `good_count`）は CP のみ・リテストは部分母集団なので歩留りには使いません。
測定値は `test_data_final`、ダイは `parts_final` — いずれも retest 解決済み（`retest_flag = 0`）です。
FT の CHIP ID は `parts_final` の `c0_*` / `c1_*` / `chip_status` 列（別テーブルは無い）。

**sub_process を混ぜない。** 同じロットの CP1 / CP2（FT1 / FT2）は別の測定で、dedup も
`wafer_yield_final` も `(product, test_category, sub_process, lot_id, wafer_id)` ごとです。
ロット単位に集計・結合するクエリは `sub_process`（必要なら `test_category` / `product`）を
出力列・GROUP BY・JOIN キーに含めてください。同梱クエリは `sub_process` を出力列に持ち、
`SET VARIABLE sub_process = NULL;` + `opt_eq()` の任意条件で 1 つに絞れます
（例: `s.run("wafer_yield", lot="LOT002", sub_process="CP1")`）。

`parts_final` / `test_data_final` を結合するときは `part_id` を使わない
（ファイル内連番で、部分リテストでは別ダイに振り直される）。両ビューの
`die_key`（CP=座標 / FT=eFuse の `chip_key`、無ければ run 固有 ID）で結合する。
`sub_process` が空のストアでも結ばれるよう `IS NOT DISTINCT FROM` を使う:

    ON  p.product = td.product AND p.test_category = td.test_category
    AND p.sub_process IS NOT DISTINCT FROM td.sub_process
    AND p.lot_id = td.lot_id AND p.wafer_id = td.wafer_id AND p.die_key = td.die_key

合否は NULL があり得る（`parts.passed`: PART_FLG 0x10、`test_data.passed`: 未実行 / 合否無効、
`test_data.result`: 値無効、リミット: 「なし」指定）。`= FALSE` / `= 'F'` で数える側は NULL を
自然に除外するが、母数に含めたくない行は `IS NOT NULL` で明示する。
