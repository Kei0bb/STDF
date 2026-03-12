"""Dagster data pipeline for STDF Platform."""

from dagster import Definitions

from .assets.ingestion import raw_stdf_files, parsed_stdf_data
from .assets.tables import stdf_parquet_tables, duckdb_views
from .assets.analytics import yield_summary, bin_distribution, test_fail_ranking
from .assets.postgres_sync import postgres_sync
from .assets.local_ingest import local_ingest
from .jobs import full_pipeline_job, ingestion_job, refresh_views_job
from .sensors.ftp_sensor import ftp_new_file_sensor
from .schedules.daily import daily_refresh_schedule
from .resources import get_resources

defs = Definitions(
    assets=[
        # Ingestion pipeline
        raw_stdf_files,
        parsed_stdf_data,
        stdf_parquet_tables,
        duckdb_views,
        # Analytics
        yield_summary,
        bin_distribution,
        test_fail_ranking,
        # PostgreSQL sync
        postgres_sync,
        # Standalone
        local_ingest,
    ],
    jobs=[
        full_pipeline_job,
        ingestion_job,
        refresh_views_job,
    ],
    sensors=[
        ftp_new_file_sensor,
    ],
    schedules=[
        daily_refresh_schedule,
    ],
    resources=get_resources(),
)
