# stdf_dagster — STDF Data Pipeline

[Dagster](https://dagster.io/) ベースの STDF データパイプライン。
既存の `stdf_platform` モジュールを Dagster Asset として オーケストレーションします。

## クイックスタート

```bash
# 依存関係インストール
uv pip install "dagster>=1.9.0" "dagster-webserver>=1.9.0" "psycopg2-binary>=2.9.0"

# PostgreSQL 起動
docker compose up -d

# Dagster UI 起動
uv run dagster dev -m stdf_dagster
# → http://localhost:3000
```

## アーキテクチャ

```
FTP Server
    ↓  ftp_new_file_sensor (3時間間隔)
raw_stdf_files          ← FTP差分ダウンロード (config.yaml の接続情報を使用)
    ↓
parsed_stdf_data        ← Rust/Python パーサー
    ↓
stdf_parquet_tables     ← Hive-partitioned Parquet 書き出し
    ↓
duckdb_views            ← DuckDB ビュー更新
    ├→ yield_summary     ┐
    ├→ bin_distribution  ├ 分析 (CSV 出力)
    ├→ test_fail_ranking ┘
    └→ postgres_sync     ← PostgreSQL 全テーブル同期 (JMP/BI用)
```

## Assets

| グループ | Asset | 説明 |
|---------|-------|------|
| **ingestion** | `raw_stdf_files` | FTPから新規ファイルをDL (retry: 3回, 指数backoff) |
| **ingestion** | `parsed_stdf_data` | STDF バイナリ解析 (retry: 2回) |
| **tables** | `stdf_parquet_tables` | Parquet 書き出し (retry: 2回) |
| **tables** | `duckdb_views` | DuckDB ビューリフレッシュ |
| **analytics** | `yield_summary` | 歩留まりサマリ → `data/analytics/yield_summary.csv` |
| **analytics** | `bin_distribution` | Bin 分布 → `data/analytics/bin_distribution.csv` |
| **analytics** | `test_fail_ranking` | フェールテスト上位 → `data/analytics/test_fail_ranking.csv` |
| **postgres** | `postgres_sync` | DuckDB → PostgreSQL 全テーブル同期 (retry: 2回) |
| **local** | `local_ingest` | ローカルファイル直接 ingest (FTPバイパス) |

## Jobs

| Job | 説明 |
|-----|------|
| `full_pipeline` | FTP DL → Parse → Parquet → DuckDB → 分析 (全Asset) |
| `ingestion_only` | FTP DL → Parse → Parquet のみ |
| `refresh_views` | DuckDB ビュー更新のみ |

## 自動化

| 種類 | 名前 | 設定 | デフォルト |
|------|------|------|-----------|
| **Sensor** | `ftp_new_file_sensor` | 3時間間隔でFTPをポーリング | OFF |
| **Schedule** | `daily_refresh_schedule` | 毎朝 6:00 UTC にフルパイプライン実行 | OFF |

> Dagster UI の **Automation** ページからトグルで有効化できます。

## 設定

FTP接続情報や取得製品フィルタは **`config.yaml`** から自動読み込みされます（Dagster側での個別設定は不要）。

```yaml
# config.yaml の例
ftp:
  host: 192.168.1.100
  port: 21
  username: ${FTP_USER}      # ← .env の環境変数を参照
  password: ${FTP_PASSWORD}

filters:
  - product: SCT101A
    test_types: [CP, FT]
  - product: SCT202B
    test_types: [CP]
```

> `${FTP_USER}` のように書くと、環境変数 `FTP_USER` の値が自動で展開されます。
> `.env` ファイルに書いておくと便利です。

## Resources

既存の `stdf_platform` モジュールを `ConfigurableResource` でラップしています。

| Resource | ラップ元 |
|----------|---------|
| `STDFConfigResource` | `stdf_platform.config.Config` |
| `FTPResource` | `config.yaml` → `stdf_platform.ftp_client.FTPClient` |
| `STDFParserResource` | `stdf_platform.parser.parse_stdf` |
| `ParquetStorageResource` | `stdf_platform.storage.ParquetStorage` |
| `DuckDBResource` | `stdf_platform.database.Database` |
| `PostgresResource` | psycopg2 (PostgreSQL 接続 + bulk upsert) |

## ローカルファイル ingest

FTP を使わずにローカルファイルを直接 ingest するには、Dagster UI から `local_ingest` Asset を選択し、Config で以下を指定：

```yaml
file_path: "./downloads/sample.stdf"
product: "SCT101A"
test_type: "CP1"  # 空欄の場合はSTDFから自動検出
```

---

## PostgreSQL 連携ガイド

### PostgreSQL とは

PostgreSQL はオープンソースのリレーショナルデータベースです。
ネットワーク経由で複数ユーザーが同時にアクセスできるため、**JMP や Excel 等から社内共有データベースとして利用**できます。

### 全体の流れ

```
1. docker compose up -d        ← PostgreSQL サーバー起動
2. Dagster UI で postgres_sync  ← DuckDB のデータを PostgreSQL に同期
3. JMP / Excel から接続          ← SQL でデータ取得
```

### Step 1: PostgreSQL を起動する

```bash
# プロジェクトのルートディレクトリで実行
docker compose up -d
```

- 初回起動時にテーブルとインデックスが自動作成されます
- データは Docker ボリュームに永続化されるため、再起動してもデータは消えません
- 停止する場合: `docker compose down`（データは残ります）
- データごと完全に消す場合: `docker compose down -v`

### Step 2: データを PostgreSQL に同期する

Dagster UI (`http://localhost:3000`) で以下のいずれかの方法で同期：

- **手動**: Lineage ページで `postgres_sync` を選択 → **Materialize** をクリック
- **自動**: `daily_refresh_schedule` を有効化すると毎朝6時に自動同期

### Step 3: JMP / BI ツールから接続する

#### 接続情報

| 項目 | 値 | 備考 |
|------|-----|------|
| **Host** | サーバーのIPアドレス | 例: `192.168.1.50` |
| **Port** | `5432` | デフォルトの PostgreSQL ポート |
| **Database** | `stdf` | データベース名 |
| **User** | `stdf_reader` | 読み取り専用ユーザー（推奨） |
| **Password** | `stdf_read_only` | |

> ⚠️ ローカルで試す場合、Host は `localhost` で OK です。
> 社内の他のPCからアクセスする場合は、サーバーのIPアドレスを指定してください。

#### JMP での接続手順

1. JMP を開く
2. **ファイル** → **データベースを開く** (または **File → Open Database**)
3. 接続タイプで **ODBC** を選択
4. PostgreSQL の ODBC ドライバーを選択（要インストール: [psqlODBC](https://odbc.postgresql.org/)）
5. 上記の接続情報を入力
6. SQL を入力してデータ取得

#### Excel での接続手順 (Windows)

1. **データ** タブ → **データの取得** → **データベースから** → **PostgreSQL データベースから**
2. サーバー名: `サーバーIP:5432`、データベース: `stdf` を入力
3. 認証情報を入力

#### DBeaver での接続手順（無料 SQL クライアント）

1. [DBeaver](https://dbeaver.io/) をインストール
2. **新しい接続** → **PostgreSQL** を選択
3. 接続情報を入力 → **テスト接続** で確認

### テーブル構成（スキーマ）

PostgreSQL には以下の4テーブルがあり、全て生データが入っています。

#### `lots` — ロット情報

| カラム | 型 | 説明 |
|--------|-----|------|
| lot_id | TEXT | ロットID (例: `E6A773.00`) |
| product | TEXT | 製品名 (例: `SCT101A`) |
| test_category | TEXT | テストカテゴリ (`CP` or `FT`) |
| sub_process | TEXT | サブプロセス (例: `CP11`, `FT2`) |
| part_type | TEXT | 部品タイプ |
| job_name | TEXT | テストプログラム名 |
| job_rev | TEXT | テストプログラムバージョン |
| start_time | TIMESTAMPTZ | テスト開始日時 |
| finish_time | TIMESTAMPTZ | テスト終了日時 |
| tester_type | TEXT | テスター機種名 |
| operator | TEXT | オペレーター名 |

#### `wafers` — ウェハー歩留まり

| カラム | 型 | 説明 |
|--------|-----|------|
| wafer_id | TEXT | ウェハーID |
| lot_id | TEXT | ロットID |
| product | TEXT | 製品名 |
| part_count | INTEGER | 全ダイ数 |
| good_count | INTEGER | 良品ダイ数 |
| retest_num | INTEGER | リテスト番号（0=初回, 1,2...=リテスト） |
| test_rev | TEXT | テストRev (例: `Rev04`) |
| source_file | TEXT | 元STDFファイル名 |

#### `parts` — 個片結果

| カラム | 型 | 説明 |
|--------|-----|------|
| part_id | TEXT | ダイID |
| lot_id | TEXT | ロットID |
| wafer_id | TEXT | ウェハーID |
| product | TEXT | 製品名 |
| x_coord | INTEGER | X座標 |
| y_coord | INTEGER | Y座標 |
| hard_bin | INTEGER | ハードBin番号 |
| soft_bin | INTEGER | ソフトBin番号 |
| passed | BOOLEAN | 合否 (true=合格) |

#### `test_data` — テスト結果（最大テーブル）

| カラム | 型 | 説明 |
|--------|-----|------|
| lot_id | TEXT | ロットID |
| wafer_id | TEXT | ウェハーID |
| part_id | TEXT | ダイID |
| product | TEXT | 製品名 |
| test_num | INTEGER | テスト番号 |
| test_name | TEXT | テスト名 |
| lo_limit | DOUBLE | 下限規格値 |
| hi_limit | DOUBLE | 上限規格値 |
| units | TEXT | 単位 |
| result | DOUBLE | 測定値 |
| passed | TEXT | `P`=合格, `F`=不合格 |

### SQL クエリ例

PostgreSQL に接続した後、以下のような SQL でデータを取得できます。

```sql
-- ロット一覧を見る
SELECT lot_id, product, test_category, sub_process, job_name
FROM lots
ORDER BY product, lot_id;

-- 特定製品のウェハー歩留まりを確認（最新リテストのみ）
SELECT lot_id, wafer_id, part_count, good_count,
       ROUND(100.0 * good_count / NULLIF(part_count, 0), 2) AS yield_pct
FROM wafers
WHERE product = 'SCT101A'
  AND retest_num = (
      SELECT MAX(retest_num) FROM wafers w2
      WHERE w2.lot_id = wafers.lot_id AND w2.wafer_id = wafers.wafer_id
  )
ORDER BY lot_id, wafer_id;

-- 特定ロットの Bin 分布
SELECT soft_bin, COUNT(*) AS count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM parts
WHERE lot_id = 'E6A773.00'
GROUP BY soft_bin
ORDER BY count DESC;

-- 特定テスト項目の測定値（JMPに取り込んでヒストグラムなどに使用）
SELECT lot_id, wafer_id, part_id, x_coord, y_coord,
       result, lo_limit, hi_limit, passed
FROM test_data
WHERE product = 'SCT101A'
  AND test_name = 'Vth_N'
ORDER BY lot_id, wafer_id, part_id;

-- フェール率が高いテスト項目ランキング
SELECT test_num, test_name,
       COUNT(*) AS total,
       SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) AS fails,
       ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) AS fail_rate
FROM test_data
WHERE lot_id = 'E6A773.00'
GROUP BY test_num, test_name
HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
ORDER BY fail_rate DESC
LIMIT 20;
```

### ユーザーアカウント

| ユーザー | パスワード | 権限 | 用途 |
|---------|-----------|------|------|
| `stdf` | `stdf_dev_2024` | 読み書き | Dagster パイプライン（データ同期用） |
| `stdf_reader` | `stdf_read_only` | **読み取り専用** | JMP / Excel / BI ツール（推奨） |

> JMP 等からは `stdf_reader` を使ってください。
> 誤ってデータを変更してしまうリスクがありません。

### トラブルシューティング

| 問題 | 解決方法 |
|------|---------|
| `docker compose up` でエラー | Docker Desktop がインストール済み・起動中か確認 |
| 接続できない | `docker compose ps` で PostgreSQL が running か確認 |
| テーブルが空 | Dagster UI で `postgres_sync` をマテリアライズしたか確認 |
| 社内PCからアクセスできない | ファイアウォールでポート5432が開いているか確認 |

---

## ディレクトリ構成

```
src/stdf_dagster/
├── __init__.py              # Definitions エントリポイント
├── README.md                # ← このファイル
├── assets/
│   ├── ingestion.py         # raw_stdf_files, parsed_stdf_data
│   ├── tables.py            # stdf_parquet_tables, duckdb_views
│   ├── analytics.py         # yield_summary, bin_distribution, test_fail_ranking
│   ├── postgres_sync.py     # postgres_sync (DuckDB → PostgreSQL)
│   └── local_ingest.py      # local_ingest
├── resources/
│   ├── stdf_config.py       # STDFConfigResource
│   ├── ftp.py               # FTPResource (config.yaml から接続情報読込)
│   ├── stdf_parser.py       # STDFParserResource
│   ├── parquet.py            # ParquetStorageResource
│   ├── duckdb_resource.py   # DuckDBResource
│   └── postgres.py          # PostgresResource
├── sensors/
│   └── ftp_sensor.py        # ftp_new_file_sensor (3時間間隔)
├── schedules/
│   └── daily.py             # daily_refresh_schedule
└── jobs.py                  # full_pipeline, ingestion_only, refresh_views
```
