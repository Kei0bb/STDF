"""Dagster data pipeline for STDF Platform."""

from dagster import Definitions

from .assets.ingestion import raw_stdf_files, parsed_stdf_data
from .assets.tables import stdf_parquet_tables, duckdb_views
from .assets.local_ingest import local_ingest
from .jobs import full_pipeline_job, ingestion_job, refresh_views_job
from .sensors.ftp_sensor import ftp_new_file_sensor
from .schedules.daily import daily_refresh_schedule
from .resources import get_resources

defs = Definitions(
    assets=[
        raw_stdf_files,
        parsed_stdf_data,
        stdf_parquet_tables,
        duckdb_views,
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
