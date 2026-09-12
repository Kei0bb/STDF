# sql/ — 名前付きクエリライブラリ

**1ファイル1クエリ。** フォルダを開けばそれが一覧です。番号順に読めば一通り分かります。

```
01_lots/    ロット単位  — 一覧 / 歩留まりサマリ
02_wafer/   ウェーハ単位 — 歩留まり
03_test/    テスト単位  — 項目一覧 / Fail ランキング / Cp・Cpk
04_bin/     ビン単位    — 分布 / ビン×Fail テスト
05_export/  CSV 書き出し前提の明細
```

## 動かし方

**A. Python から(ふだんはこれ)** — `workspace/query.py` で:

```python
s.queries()                                    # 一覧
s.run("fail_ranking", lot="LOT002")            # → DataFrame
s.run("die_test_export", lot="LOT002", out="lot002.csv")   # → CSV(行数を返す)
print(s.show("cpk"))                           # SQL を表示(改造の出発点)
```

名前は `03_test/fail_ranking` でも、重複がなければ `fail_ranking` だけでも引けます。

**B. ファイルを開いてそのまま実行** — 各ファイルは自己完結しています。冒頭の

```sql
SET VARIABLE lot = 'LOT001';   -- ← 書き換えて実行
```

が既定値なので、エディタや DuckDB シェルにそのまま流しても動きます。

A と B は同じファイルを読みます。`run()` に渡したパラメータは、この既定値を**上書き**します。

## 新しいクエリを足す

1. 該当フォルダに `名前.sql` を作る
2. 1行目を `-- 説明` にする(`s.queries()` に出る説明文になります)
3. 絞り込みは `getvariable('lot')` を使い、冒頭に `SET VARIABLE lot = '...';` で既定値を置く

**値をSQLに文字列連結しないこと。** `run()` はパラメータをバインド変数として渡すので、
クォートを含む値でも壊れません。f-string 埋め込みはそこが壊れます。

## どのビューを使うか

歩留まりは `wafer_yield_final`(gross die 適用済み・FT ロットも含む)を使ってください。
`wafers` テーブルを自前で集計すると FT ロットが落ち、gross die も効きません。
測定値は `test_data_final`、ダイは `parts_final` — いずれも retest 解決済みです。

`parts_final` / `test_data_final` を結合するときは `part_id` を使わない
（ファイル内連番で、部分リテストでは別ダイに振り直される）。両ビューの
`die_key`（CP=座標 / FT=バーコード→PART_ID→合成ID）で結合する:

    AND p.lot_id = td.lot_id AND p.wafer_id = td.wafer_id AND p.die_key = td.die_key
