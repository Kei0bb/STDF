"""Dagster data pipeline for STDF Platform."""

from dagster import Definitions

from .assets.ingestion import raw_stdf_files, parsed_stdf_data
from .assets.tables import stdf_parquet_tables, duckdb_views
from .resources import get_resources

defs = Definitions(
    assets=[raw_stdf_files, parsed_stdf_data, stdf_parquet_tables, duckdb_views],
    resources=get_resources(),
)
