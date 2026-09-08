# stdf テーブルスキーマ

STDFファイルをParquet形式に変換した際のテーブル定義。  
DuckDBビュー経由でSQLクエリ可能。

---

## パーティション構造

```
data/
└── {table}/
    └── product={product}/
        └── test_category={CP|FT}/
            └── sub_process={CP1|FT2|...}/
                └── lot_id={lot_id}/
                    └── (wafer_id={id}/retest={n}/)  ← runs / wafers / parts / test_data / chipid
                        └── data.parquet
```

> `lots` は Parquet を持たず `runs` から派生する DuckDB VIEW。`runs` / `wafers` /
> `parts` / `test_data` / `chipid` は `lot_id` の下にさらに `wafer_id` / `retest`
> まで切る（FT はウェーハ概念が無いため `wafer_id=`（空）になる。`runs` は FT では
> lot 単位 = `wafer_id=''` ごとに 1 行）。`product` / `test_category` /
> `sub_process` / `lot_id` / `wafer_id` / `retest` は Hive パーティション列として
> SELECT 可能。

---

## 適用範囲（CP / FT）

テーブルは工程によって生成有無・意味が変わります。**特に `wafers` は CP 専用**です。

| テーブル | CP | FT | 単位 / 備考 |
|---------|:--:|:--:|------|
| `runs` | ✓ | ✓ | CP は 1 wafer = 1 行、FT は 1 FT lot run（`wafer_id=''`）= 1 行。MIR メタデータの実体（Parquet） |
| `lots` | ✓ | ✓ | 1 lot = 1 行。`runs` から集約した VIEW（Parquet ではない） |
| `wafers` | ✓ | **✗** | **CP 専用**。FT は WIR/WRR が無いため **1 行も生成されない** |
| `parts` | ✓ | ✓ | CP = ダイ / FT = パッケージ（1 PRR）。真の単位 |
| `test_data` | ✓ | ✓ | パラメトリック測定（PTR/MPR/FTR） |
| `chipid` | ✗ | ✓ | **FT 専用**。die 出自トレース（CP は出自＝プローブ座標で冗長） |

> **歩留りの算出元**: `wafers.part_count` / `good_count` は WRR の**報告値**で、
> リテスト実行時は再測定した部分母集団しか含まず、FT には存在しません。
> このため **歩留りは `parts_final`（ダイ/パッケージ単位・最新リテスト）から
> 算出**します（`lot_yield_summary` マート / `AnalysisSession.lot_summary()` も同様）。
> `wafers` は時刻・`rtst_count` 等の WRR メタ情報用と位置づけます。

---

## runs

**1 STDF ファイル × wafer identity = 1 行**の MIR/MRR メタデータ。CP はファイル中の
wafer ごとに 1 行（1 ファイルに複数 wafer が入っていれば同じ `source_file` の複数行に
なる）、FT は `wafer_id=''` として **FT lot run 単位**（= ファイル単位）に 1 行。
`lots` はこのテーブルから集約した VIEW（下記参照）で、Parquet として書かれる実体は
`runs` だけ。

| 列名 | 型 | ソース | 説明 |
|------|----|--------|------|
| lot_id | STRING | MIR.LOT_ID | ロットID |
| wafer_id | STRING | WIR.WAFER_ID | ウェーハID（FT は空） |
| product | STRING | CLI / パス | 製品名 |
| test_category | STRING | sub_process から導出 | `CP` / `FT` / `OTHER` |
| sub_process | STRING | MIR.TEST_COD | 小工程（CP1, FT2 等） |
| retest_num | INT64 | 自動算出（`wafer_retest_map`） | リテスト番号（0=初回, 1,2...=リテスト）。`parts` / `test_data` と同じキーで join できる |
| part_type | STRING | MIR.PART_TYP | 品種名 |
| job_name | STRING | MIR.JOB_NAM | テストプログラム名 |
| job_rev | STRING | MIR.JOB_REV | テストプログラムリビジョン |
| start_time | TIMESTAMP(ms, UTC) | MIR.START_T | テスト開始時刻 |
| finish_time | TIMESTAMP(ms, UTC) | MRR.FINISH_T | テスト終了時刻 |
| tester_type | STRING | MIR.TSTR_TYP | テスター種別 |
| operator | STRING | MIR.OPER_NAM | オペレータ名 |
| node_name | STRING | MIR.NODE_NAM | テスター号機名（`tester_type` は機種） |
| handler_type | STRING | SDR.HAND_TYP | ハンドラ／プローバ機種 |
| handler_id | STRING | SDR.HAND_ID | ハンドラ／プローバ号機 |
| probe_card_type | STRING | SDR.CARD_TYP | プローブカード機種 |
| probe_card_id | STRING | SDR.CARD_ID | プローブカード ID |
| loadboard_type | STRING | SDR.LOAD_TYP | ロードボード機種 |
| loadboard_id | STRING | SDR.LOAD_ID | ロードボード ID |
| socket_type | STRING | SDR.CONT_TYP | ソケット（コンタクタ）機種 |
| socket_id | STRING | SDR.CONT_ID | ソケット（コンタクタ）ID |
| test_rev | STRING | ファイル名 (Rev04等) | テストプログラムリビジョン（ファイル名由来。TP 判定には使わない） |
| source_file | STRING | CLI | 元STDFファイル名 |

> **TP（テストプログラム）の判定キーは `job_name` + `job_rev`（MIR 由来）のみ**。
> `test_rev` はファイル名から抽出した参考情報で判定には使わない。
> per-wafer の TP 一覧は `SELECT lot_id, wafer_id, retest_num, job_name, job_rev FROM runs`
> で直接引ける（`stdf db programs` も参照）。

> **設備情報（SDR 由来）の畳み込みルール**: SDR (1/80) は「ヘッド × サイトグループ」ごとに
> 1 レコードで、1 ファイルに複数入ることがある。`runs` は 1 ファイル × wafer identity = 1 行
> なので、各列は **ファイル内の全 SDR の非空値を重複除去・ソートしてカンマ連結**した値になる。
> 全 SDR が同値なら単一値（`SKT-01`）、本当に混在した場合だけ `SKT-A,SKT-B` と見える。
> SDR が無いファイルや途中で切れた SDR では、取れなかった列は空文字 `''`。
> CP ではハンドラ欄にプローバ、カード欄にプローブカードが入り、
> `loadboard_*` / `socket_*` は空になるのが一般的（設備側の出力次第）。

---

## lots

**`runs` 由来の VIEW**（Parquet ではない）。1 lot = 1 行に集約される。

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

| 列名 | 型 | ソース | 説明 |
|------|----|--------|------|
| lot_id | STRING | MIR.LOT_ID | ロットID |
| product | STRING | CLI / パス | 製品名 |
| test_category | STRING | sub_process から導出 | `CP` / `FT` / `OTHER` |
| sub_process | STRING | MIR.TEST_COD | 小工程（CP1, FT2 等） |
| part_type | STRING | `runs` の最新 run の値 | 品種名 |
| job_name | STRING | `runs` の最新 run の値 | テストプログラム名 |
| job_rev | STRING | `runs` の最新 run の値 | テストプログラムリビジョン |
| start_time | TIMESTAMP(ms, UTC) | `MIN(runs.start_time)` | lot 内で最初の run の開始時刻 |
| finish_time | TIMESTAMP(ms, UTC) | `MAX(runs.finish_time)` | lot 内で最後の run の終了時刻 |
| tester_type | STRING | `runs` の最新 run の値 | テスター種別 |
| operator | STRING | `runs` の最新 run の値 | オペレータ名 |
| job_variant_count | INT64 | `COUNT(DISTINCT (job_name, job_rev))` | lot 内で異なる TP の版数 |
| job_mixed | BOOL | `job_variant_count > 1` | lot 内で TP が混在しているか |

> **意味が変わった点**（旧: 1 STDF ファイル = 1 行を上書きしていた頃との違い）:
> - `start_time` / `finish_time` は「lot 内の最初の run 〜 最後の run」になった
>   （旧: 最後に ingest したファイルの値という不定値）。`trend.py` のロット並び順に影響する。
> - `job_name` / `job_rev` / `part_type` / `tester_type` / `operator` は
>   **「最新 run（`start_time` が最も新しい run）の値」**に定義が固定される
>   （旧: ingest した順序に依存する不定値）。lot 内で TP が変わったかどうかは
>   `job_mixed` / `job_variant_count` で判定する。TP 混在の詳細（どの wafer が
>   どちらの版か）は `runs` を直接引く（[docs/sample_queries.md](./sample_queries.md)
>   の TP 混在検出クエリ参照）。
> - `lot_id` join の fan-out は起きない（1 lot = 1 行の契約は維持）。

---

## wafers

ウェーハ単位のサマリ。リテスト履歴を保持。**CP 専用**（FT は WIR/WRR が無く生成
されない）。`part_count` / `good_count` は WRR の報告値のため、歩留りは
`parts_final` から算出する（[適用範囲](#適用範囲cp--ft) 参照）。

> 旧スキーマにあった `test_rev` / `source_file` は書き込み専用の死に列だった
> （読んでいるコードが無かった）ため削除。同じ内容は `runs.test_rev` /
> `runs.source_file` で取得できる。

| 列名 | 型 | ソース | 説明 |
|------|----|--------|------|
| wafer_id | STRING | WIR.WAFER_ID | ウェーハID |
| lot_id | STRING | MIR.LOT_ID | ロットID |
| head_num | INT64 | WIR.HEAD_NUM | ヘッド番号 |
| start_time | TIMESTAMP(ms, UTC) | WIR.START_T | テスト開始時刻 |
| finish_time | TIMESTAMP(ms, UTC) | WRR.FINISH_T | テスト終了時刻 |
| part_count | INT64 | WRR.PART_CNT | 総ダイ数 |
| good_count | INT64 | WRR.GOOD_CNT | 良品数 |
| rtst_count | INT64 | WRR.RTST_CNT | リテスト数 |
| abrt_count | INT64 | WRR.ABRT_CNT | アボート数 |
| retest_num | INT64 | 自動算出 | リテスト番号（0=初回, 1,2...=リテスト） |

> **リテスト**: 同一 lot_id + wafer_id で再 ingest すると `retest_num` がインクリメント。  
> 分析クエリでは最新 `retest_num` のみを使用（`ROW_NUMBER() OVER(PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC)`）。

---

## parts

ダイ（チップ）単位の結果。

| 列名 | 型 | ソース | 説明 |
|------|----|--------|------|
| part_id | STRING | 自動生成 | `{lot_id}_{wafer_id}_{連番}`（ストリーム順） |
| part_txt | STRING | PRR.PART_TXT | 2D バーコード（FT パッケージの一意キー。CP は通常空） |
| lot_id | STRING | MIR.LOT_ID | ロットID |
| wafer_id | STRING | WIR.WAFER_ID | ウェーハID（FT は空） |
| head_num | INT64 | PIR.HEAD_NUM | ヘッド番号 |
| site_num | INT64 | PIR.SITE_NUM | サイト番号 |
| x_coord | INT64 | PRR.X_COORD | X座標（FT は -32768） |
| y_coord | INT64 | PRR.Y_COORD | Y座標（FT は -32768） |
| hard_bin | INT64 | PRR.HARD_BIN | ハードビン |
| soft_bin | INT64 | PRR.SOFT_BIN | ソフトビン |
| passed | BOOL | PRR.PART_FLG | 合否 |
| test_count | INT64 | PRR.NUM_TEST | テスト実行数 |
| test_time | INT64 | PRR.TEST_T | テスト時間 (ms) |
| retest_num | INT64 | 自動算出 | リテスト番号（0=初回, 1,2...） |

---

## test_data

テスト項目ごとの測定結果（PTR / MPR / FTR を統合）。

| 列名 | 型 | ソース | 説明 |
|------|----|--------|------|
| lot_id | STRING | MIR.LOT_ID | ロットID |
| wafer_id | STRING | WIR.WAFER_ID | ウェーハID |
| part_id | STRING | 自動生成 | ダイID |
| part_txt | STRING | PRR.PART_TXT | 2D バーコード（FT パッケージキー。CP は通常空） |
| x_coord | INT64 | PRR.X_COORD | X座標 |
| y_coord | INT64 | PRR.Y_COORD | Y座標 |
| test_num | INT64 | PTR/MPR/FTR.TEST_NUM | テスト番号 |
| test_name | STRING | PTR/MPR/FTR.TEST_TXT | テスト名 |
| rec_type | STRING | レコード種別 | `PTR` / `MPR` / `FTR` |
| lo_limit | FLOAT64 | PTR/MPR.LO_LIMIT | 下限規格値 |
| hi_limit | FLOAT64 | PTR/MPR.HI_LIMIT | 上限規格値 |
| units | STRING | PTR/MPR.UNITS | 単位 |
| result | FLOAT64 | PTR/MPR.RESULT | 測定値 |
| passed | STRING | PTR/MPR/FTR.TEST_FLG | 合否（`P` / `F`） |
| retest_num | INT64 | 自動算出 | リテスト番号 |
| pin_num | INT64 | MPR.PMR_INDX | ピン番号（MPR のみ。PTR/FTR は NULL） |
| pin_name | STRING | PMR から解決 | ピン名（MPR のみ） |
| exec_seq | INT64 | 自動算出（ingest 時） | 同一 run 内でのキー出現順（0始まり）。ループ計測（例: OTP ダンプが 1 test_num に 512 PTR を書く）の各回を区別 |
| retest_flag | INT64 | 自動算出（ingest 時） | キーごとの新しさ順位。0 = そのキーを含む最新 run。`test_data_final` は `retest_flag = 0` で絞り込み |

---

## chipid

FT chiplet 製品の die トレーサビリティ。GDR の `EN-S0-CHIPID_R`（**digit zero**）
をデコードした eFuse の出自情報。**FT のみ生成**（CP は出自＝プローブ座標で冗長な
ため書き込まない）。1 パッケージ（1 PRR）が 2 die を含むため 1 部品あたり複数行。

| 列名 | 型 | ソース | 説明 |
|------|----|--------|------|
| lot_id | STRING | MIR.LOT_ID | FT ロットID |
| part_id | STRING | 自動生成 | 部品ID（ストリーム順） |
| part_txt | STRING | PRR.PART_TXT | 2D バーコード（パッケージの一意キー） |
| chip_occurrence_index | INT64 | 出現順 | DUT 内の die 区別（0, 1, ...） |
| efuse_raw | STRING | GDR 値 | 正規化済み 64bit 文字列（**die の恒久ID**） |
| valid | BOOL | デコード結果 | 64bit デコード成功なら true |
| origin_fab_code | INT64 | EFUSE[0:4] | fab コード |
| origin_fab | STRING | デコード | `TSMC1` / `TSMC2` / `UNSUPPORTED` |
| origin_lot | STRING | デコード | CP ロット（6 文字） |
| origin_wafer | INT64 | デコード | 出自ウェーハ番号 |
| origin_x | INT64 | デコード | 出自 X 座標 |
| origin_y | INT64 | デコード | 出自 Y 座標 |
| reserved_bits | STRING | EFUSE[62:64] | 予備ビット |
| retest_num | INT64 | 自動算出 | リテスト番号 |

> ビット配置: `EFUSE[0:4]`=fab, `[4:13]`=Y(9bit), `[13:22]`=X(9bit),
> `[22:27]`=wafer(5bit, 値-3), `[27:62]`=lot(CHAR1×1 + CHAR2×5), `[62:64]`=予備。

---

## 派生 VIEW（`*_final`）

リテストの最新のみを残す重複排除ビュー。dedup の単位（identity）は工程で分岐：

| VIEW | dedup 方式 | 用途 |
|------|------------|------|
| `parts_final` | `ROW_NUMBER() OVER (PARTITION BY lot_id, <dedup 単位> ORDER BY retest_num DESC)` | 最新リテストのダイ/パッケージ |
| `test_data_final` | `WHERE retest_flag = 0`（ingest 時に付与済み。下記参照） | 最新リテストの測定値（ループ計測は全行保持） |
| `chipid_final` | `ROW_NUMBER() OVER (PARTITION BY lot_id, efuse_raw ORDER BY retest_num DESC)` | die（eFuse）単位の最新。occurrence 順入替えに堅牢 |

`parts_final` / `chipid_final` は小テーブルなのでウィンドウ計算コストが無視できる範囲。
CP は従来どおりウェーハ座標、FT は座標が無いため 2D バーコード（`part_txt`）/ eFuse を
identity に使う。

`test_data_final` は `retest_num` 順のウィンドウではなく、ingest 時に `storage.py` が
書き込む `retest_flag`（キーごとの新しさ順位。0 = 最新 run）を使った単純な述語フィルタ。
ウィンドウの `PARTITION BY` は述語ではないため、非パーティション列（`test_name` など）
への絞り込みがウィンドウの下までプッシュダウンされず、ロット絞り込みの `test_name LIKE`
検索が実データで 12 分以上かかる原因になっていた。`retest_flag = 0` は通常の述語なので
Parquet スキャンまでプッシュダウンされ、高速。

また、旧ウィンドウ版は (die, test, pin) ごとに 1 行だけを残す仕様だったため、ループ計測
（例: OTP ダンプが 1 test_num の下に 512 個の PTR を書く場合）の 511/512 行が無条件に
捨てられ、Fail した回だけが消えることもあった。フラグ方式は最新 run の全行を保持する
ため、ループ回を区別するには `exec_seq`（run 内 0 始まり出現順）を使う。

`retest_flag IS NULL`（旧スキーマ／フラグ未対応でストアされたファイル）の行は
`test_data_final` から除外される — そのストアは再取り込みが必要。`stdf build`
が実行する dbt テスト（`dbt/tests/assert_no_null_retest_flag.sql` 他）で検出できる。

---

## ER図

```mermaid
erDiagram
    lots ||--o{ wafers : "lot_id"
    lots ||--o{ parts : "lot_id"
    wafers ||--o{ parts : "lot_id, wafer_id"
    parts ||--o{ test_data : "lot_id, wafer_id, part_id"
    parts ||--o{ chipid : "lot_id, part_txt (FT)"

    lots {
        string lot_id PK
        string product
        string test_category
        string sub_process
        string part_type
        string job_name
    }
    wafers {
        string wafer_id PK
        string lot_id FK
        int part_count
        int good_count
        int retest_num
    }
    parts {
        string part_id PK
        string lot_id FK
        string wafer_id FK
        int x_coord
        int y_coord
        int hard_bin
        int soft_bin
        bool passed
    }
    test_data {
        string part_id FK
        string part_txt
        int test_num
        string test_name
        string rec_type
        float result
        string passed
    }
    chipid {
        string lot_id FK
        string part_txt
        int chip_occurrence_index
        string efuse_raw
        string origin_fab
        string origin_lot
        int origin_wafer
        int origin_x
        int origin_y
    }
```
