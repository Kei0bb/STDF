"""Dagster resources wrapping existing stdf_platform modules."""

from .ftp import FTPResource
from .stdf_parser import STDFParserResource
from .parquet import ParquetStorageResource
from .duckdb_resource import DuckDBResource
from .stdf_config import STDFConfigResource


def get_resources() -> dict:
    """Return the default resource configuration."""
    return {
        "stdf_config": STDFConfigResource(),
        "ftp": FTPResource(),
        "stdf_parser": STDFParserResource(),
        "parquet_storage": ParquetStorageResource(),
        "duckdb": DuckDBResource(),
    }


__all__ = [
    "FTPResource",
    "STDFParserResource",
    "ParquetStorageResource",
    "DuckDBResource",
    "STDFConfigResource",
    "get_resources",
]
