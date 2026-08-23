# per-run テストプログラム記録（`runs` テーブル）設計

- 日付: 2026-08-23
- 状態: 設計確定（実装前）

## 背景と問題

MIR 由来の `job_name` / `job_rev`（= テストプログラム、以下 TP）は `lots` テーブルに
しか無い。`lots` の書き込み先は `data/lots/.../lot_id={lot}/data.parquet` の 1 ファイル
固定で、同じ lot の 2 ファイル目以降は**上書き**される（`storage.py` `save_stdf_data`）。
その結果 `lots` に残るのは「最後に ingest したファイルの MIR」だけであり、

- 1 lot 内で wafer ごとに TP が変わっても検知できない
- `lots.job_name` / `job_rev` / `start_time` の値が ingest 順に依存する不定値になる

`wafers` / `parts` / `test_data` / `chipid` は `lot_id / wafer_id / retest` 単位で
残るため、**測定行の粒度と MIR の粒度がズレている**のが根本原因。

現状 per-wafer に近い唯一の TP 情報は `wafers.test_rev`（ファイル名の `Rev04` から抽出）
だが、CP 専用かつ MIR 由来ではない。

## 目的

wafer 単位（CP）/ FT lot 単位（FT）で TP を判別できるようにする。
主用途は **TP 混在の検知と一覧**（「この lot は wafer 1-12 が RevA、13-25 が RevB」）。

TP の判定キーは **MIR.JOB_NAM + MIR.JOB_REV** のみ。ファイル名由来の Rev や
node_nam / exec_typ / exec_ver / sblot_id は判定に使わない。

## 設計

### ① `runs` テーブル（新規・Parquet）

```
data/runs/product={p}/test_category={CP|FT}/sub_process={sp}/lot_id={lot}/wafer_id={w}/retest={n}/data.parquet
```

**1 STDF ファイル × wafer identity = 1 行**。CP は wafer ごと、FT は `wafer_id=''` +
`retest` ごと（= FT lot 単位）に 1 行。1 ファイルに複数 wafer が入っている場合は
同じ `source_file` を持つ複数行になる。

`RUNS_SCHEMA`:

| 列 | 型 | ソース |
|----|----|--------|
| lot_id | string | MIR.LOT_ID |
| wafer_id | string | WIR.WAFER_ID（FT は `''`） |
| product | string | CLI / パス |
| test_category | string | sub_process から導出 |
| sub_process | string | MIR.TEST_COD |
| retest_num | int64 | 自動算出（`wafer_retest_map`） |
| part_type | string | MIR.PART_TYP |
| job_name | string | MIR.JOB_NAM |
| job_rev | string | MIR.JOB_REV |
| start_time | timestamp(ms, UTC) | MIR.START_T |
| finish_time | timestamp(ms, UTC) | MRR.FINISH_T |
| tester_type | string | MIR.TSTR_TYP |
| operator | string | MIR.OPER_NAM |
| test_rev | string | ファイル名（`Rev04` 等） |
| source_file | string | CLI |

パーティション列と同名の実列（`lot_id` / `wafer_id` / `product` / `test_category` /
`sub_process`）を**あえて重複して持つ**。DuckDB は file 列と hive 列が衝突すると
file 側を採用する（v1.4.3 で実測）ため、これは既存 `lots` / `wafers` / `parts` と
同じ挙動——サニタイズ前の生値が読める——を保つための意図的な重複である。
hive 由来の `retest`（int）と実列 `retest_num` が並ぶのも `parts` / `wafers` と同じ。

`retest_num` は `save_stdf_data` が既に計算している `wafer_retest_map` をそのまま使う。
これにより `runs` は `parts` / `test_data` と `(lot_id, wafer_id, retest)` で
完全一致する join キーを持つ。

### ② `lots` は Parquet をやめ `runs` からの派生ビューにする

`runs` は `lots` の全列を細かい粒度で持つ上位互換であり、両方を Parquet に書くと
同じ MIR が二重に残る。よって **`lots` の Parquet 書き込みを廃止**し、
`views.py` でビューとして定義する。

```sql
CREATE OR REPLACE VIEW lots AS
SELECT lot_id, product, test_category, sub_process,
       arg_max(part_type,   start_time) AS part_type,
       arg_max(job_name,    start_time) AS job_name,
       arg_max(job_rev,     start_time) AS job_rev,
       MIN(start_time)  AS start_time,
       MAX(finish_time) AS finish_time,
       arg_max(tester_type, start_time) AS tester_type,
       arg_max(operator,    start_time) AS operator,
       COUNT(DISTINCT (job_name, job_rev))     AS job_variant_count,
       COUNT(DISTINCT (job_name, job_rev)) > 1 AS job_mixed
FROM runs
GROUP BY lot_id, product, test_category, sub_process
```

- 列順は現行 `LOTS_SCHEMA` と同一、末尾に `job_variant_count` / `job_mixed` を追加。
  既存の `lots` 消費者（`database.get_lot_summary` / `analysis/trend.py` /
  `analysis/session.py` `lots()` / `views.py` の `lot_product`）は**無改修で通る**。
- 1 lot = 1 行という契約も維持されるので、`lot_id` join の fan-out は起きない。
- **意味が変わる点**（要ドキュメント化）:
  - `start_time` = lot 内の最初の run、`finish_time` = 最後の run。
    現状は「最後に ingest したファイルの値」という不定値なので改善方向だが、
    `trend.py` のロット並び順に影響する。
  - `job_name` / `job_rev` / `part_type` / `tester_type` / `operator` は
    「最新 run の値」に定義が固定される（現状は ingest 順依存）。
    混在の有無は `job_mixed` で判定する。
- 消費者側に残っている `ROW_NUMBER() OVER (PARTITION BY product, test_category, lot_id
  ORDER BY start_time DESC)` の 1 行化は冗長になるが、無害なのでこの変更では触らない。

### ③ `wafers` から `test_rev` / `source_file` を削除

全コード・ドキュメントを grep した結果、この 2 列を**読んでいる箇所は存在しない**
（書き込み専用の死に列）。同じ内容が `runs` に入るため `WAFERS_SCHEMA` から削除する。

### ④ 参照手段

- `views.py` が `runs` を base ビューとして登録（`data/runs` が存在する場合）。
  per-wafer TP 一覧は `SELECT lot_id, wafer_id, retest_num, job_name, job_rev FROM runs`
  で直接引ける。`*_final` 系のビューは作らない（dedup の必要が無い）。
- `stdf db lots`: Job 列を、`job_mixed` の lot では
  `JOB (RevA→RevB) ⚠` の形式で表示する（新旧 2 版の場合。3 版以上なら
  `JOB (3 versions) ⚠`）。`get_lot_summary` の SELECT / GROUP BY に
  `job_variant_count` / `job_mixed` を追加する。
- `stdf db programs [--lot LOT_ID]`: lot / wafer / retest / job_name / job_rev /
  start_time / source_file の一覧を表示する新コマンド。
- `AnalysisSession.runs(lot_id=None, product=None, test_category=None)`:
  `session.lots()` と同じ形の DataFrame 返却メソッド。

### ⑤ 移行

旧ストアには `runs` が無いため、**WIPE → 全件再 ingest**（`retest_flag` 導入時と同じ方針、
マイグレーション経路は用意しない）。

中途半端な状態を検知するため、`setup_views()` は `data/lots/` ディレクトリが
存在したら `RuntimeError` を送出する:

```
Legacy store detected: data/lots/ is no longer written (MIR moved to data/runs/).
Wipe the data directory and re-ingest — there is no migration path.
```

`data/runs` が無い場合は `runs` / `lots` ビューを登録しない（`wafer_yield_final` は
`lots` 不在時の fallback を既に持つ）。

### ⑥ worker.py の直列化理由の更新

`run_ingest_pool` の docstring にある直列化理由 1（「lots テーブルが同じ
lot-level data.parquet を毎回書き換える」）は `lots` 廃止で消滅する。理由 2
（`_get_next_retest_num` の read-then-write）と理由 3（`_demote_superseded`）は
残るので**直列化そのものは維持**し、docstring から理由 1 を削除する。

## テスト

新規（`src/tests/test_runs_table.py`）:
- CP: 1 ファイル 2 wafer → `runs` が 2 行、それぞれ正しい wafer パーティションに入る
- FT: `wafer_id=''` / `retest=0` に 1 行
- リテスト: 同一 lot+wafer を再 ingest → `retest=1` に別行、`retest=0` の行は残る
- `runs` の `retest_num` が同じ ingest の `parts` / `test_data` と一致する
- `lots` ビュー: 2 run の lot で `start_time`=MIN / `finish_time`=MAX、
  `job_name` = 最新 run の値、1 lot 1 行
- `job_mixed`: 同一 TP 2 run → false、異なる `job_rev` 2 run → true / `job_variant_count`=2
- 旧ストア検知: `data/lots/` を作った状態で `setup_views()` が `RuntimeError`

改修:
- `src/tests/synth_data.py`、`test_analysis_compare.py`、`test_analysis_correlation.py`
  が `lots` の Parquet を直書きしているので `runs` 直書きに差し替える
- `wafers` の `test_rev` / `source_file` を検証しているテストがあれば削除

## ドキュメント

- `docs/schema.md`: `runs` セクション追加、`lots` を「`runs` 由来のビュー」に書き換え
  （`job_variant_count` / `job_mixed` を含む）、`wafers` から 2 列削除、
  適用範囲テーブルに `runs`（CP ✓ / FT ✓）を追加
- `docs/sample_queries.md`: 1092 行付近の「最後に ingest したファイルの job_name /
  job_rev が残る」注記を差し替え、TP 混在検出クエリを追加
- `query.py.example`: TP 混在確認セルを追加
- `CLAUDE.md`: Four Core Tables → 5 テーブル、storage layout、Key Design Decisions に
  「per-run MIR は runs、lots は派生ビュー」を追記
