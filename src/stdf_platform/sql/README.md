# 名前付きクエリライブラリ

**1ファイル1クエリ。** Python の `AnalysisSession.run()` から呼びます。

置き場所は2つあり、**同じ名前なら個人用が優先**されます:

| 場所 | git | 用途 |
|---|---|---|
| `sql/`（リポジトリ直下） | 管理しない（gitignore） | 個人用・本番機で手元改造するクエリ |
| `src/stdf_platform/sql/`（このフォルダ） | 管理する | 同梱の定番クエリ。テストとコードが使う |

同梱クエリを本番で改造したいときは、同じ相対パスで `sql/` にコピーして書き換えます
（例: `sql/04_bin/bin_pareto.sql`）。`s.queries()` の `source` 列で、どちらが使われて
いるか分かります。

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

1. `sql/` の該当フォルダに `名前.sql` を作る（みんなで使うなら `src/stdf_platform/sql/` に置いてコミット）
2. 1行目を `-- 説明` にする（`s.queries()` に出る説明文になります）
3. 条件は `getvariable('...')` で受け、任意にしたいものは `SET VARIABLE x = NULL;` と `opt_eq()` にする

## どのビューを使うか

歩留まりは `wafer_yield_final`（gross die 適用済み・FT ロットも含む）を使ってください。
`wafers` テーブルを自前で集計すると FT ロットが落ち、gross die も効きません。
測定値は `test_data_final`、ダイは `parts_final` — いずれも retest 解決済みです。

`parts_final` / `test_data_final` を結合するときは `part_id` を使わない
（ファイル内連番で、部分リテストでは別ダイに振り直される）。両ビューの
`die_key`（CP=座標 / FT=バーコード→PART_ID→合成ID）で結合する:

    AND p.lot_id = td.lot_id AND p.wafer_id = td.wafer_id AND p.die_key = td.die_key
