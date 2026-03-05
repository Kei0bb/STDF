# stdf_dagster — STDF Data Pipeline

[Dagster](https://dagster.io/) ベースの STDF データパイプライン。
既存の `stdf_platform` モジュールを Dagster Asset として オーケストレーションします。

## クイックスタート

```bash
# 依存関係インストール
uv pip install "dagster>=1.9.0" "dagster-webserver>=1.9.0"

# Dagster UI 起動
uv run dagster dev -m stdf_dagster
# → http://localhost:3000
```

## アーキテクチャ

```
FTP Server
    ↓  ftp_new_file_sensor (5分間隔)
raw_stdf_files          ← FTP差分ダウンロード
    ↓
parsed_stdf_data        ← Rust/Python パーサー
    ↓
stdf_parquet_tables     ← Hive-partitioned Parquet 書き出し
    ↓
duckdb_views            ← DuckDB ビュー更新
    ↓
yield_summary           ┐
bin_distribution        ├ 分析 (CSV 出力)
test_fail_ranking       ┘
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
| **Sensor** | `ftp_new_file_sensor` | 5分間隔でFTPをポーリング | OFF |
| **Schedule** | `daily_refresh_schedule` | 毎朝 6:00 UTC にフルパイプライン実行 | OFF |

> Dagster UI の **Automation** ページからトグルで有効化できます。

## Resources

既存の `stdf_platform` モジュールを `ConfigurableResource` でラップしています。

| Resource | ラップ元 |
|----------|---------|
| `STDFConfigResource` | `stdf_platform.config.Config` |
| `FTPResource` | `stdf_platform.ftp_client.FTPClient` |
| `STDFParserResource` | `stdf_platform.parser.parse_stdf` |
| `ParquetStorageResource` | `stdf_platform.storage.ParquetStorage` |
| `DuckDBResource` | `stdf_platform.database.Database` |

## ローカルファイル ingest

FTP を使わずにローカルファイルを直接 ingest するには、Dagster UI から `local_ingest` Asset を選択し、Config で以下を指定：

```yaml
file_path: "./downloads/sample.stdf"
product: "SCT101A"
test_type: "CP1"  # 空欄の場合はSTDFから自動検出
```

## ディレクトリ構成

```
src/stdf_dagster/
├── __init__.py              # Definitions エントリポイント
├── README.md                # ← このファイル
├── assets/
│   ├── ingestion.py         # raw_stdf_files, parsed_stdf_data
│   ├── tables.py            # stdf_parquet_tables, duckdb_views
│   ├── analytics.py         # yield_summary, bin_distribution, test_fail_ranking
│   └── local_ingest.py      # local_ingest
├── resources/
│   ├── stdf_config.py       # STDFConfigResource
│   ├── ftp.py               # FTPResource
│   ├── stdf_parser.py       # STDFParserResource
│   ├── parquet.py            # ParquetStorageResource
│   └── duckdb_resource.py   # DuckDBResource
├── sensors/
│   └── ftp_sensor.py        # ftp_new_file_sensor
├── schedules/
│   └── daily.py             # daily_refresh_schedule
└── jobs.py                  # full_pipeline, ingestion_only, refresh_views
```
