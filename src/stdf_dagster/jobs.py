"""Job definitions for the STDF Dagster pipeline."""

from dagster import (
    define_asset_job,
    AssetSelection,
)

# Full pipeline job: FTP download → parse → Parquet → DuckDB
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    description="FTPダウンロード → パース → Parquet保存 → DuckDBビュー更新 のフルパイプライン",
    selection=AssetSelection.all(),
)

# Ingestion only: FTP download → parse → Parquet (DuckDB更新なし)
ingestion_job = define_asset_job(
    name="ingestion_only",
    description="FTPダウンロード → パース → Parquet保存（DuckDBビュー更新なし）",
    selection=AssetSelection.assets("raw_stdf_files", "stdf_parquet_tables"),
)

# Refresh views only: DuckDB views update (no new data ingestion)
refresh_views_job = define_asset_job(
    name="refresh_views",
    description="DuckDBビューの再作成のみ（新規データ取り込みなし）",
    selection=AssetSelection.assets("duckdb_views"),
)
