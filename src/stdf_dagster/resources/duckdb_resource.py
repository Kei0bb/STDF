"""Dagster resource wrapping stdf_platform.database.Database."""

from pathlib import Path

from dagster import ConfigurableResource

from stdf_platform.config import StorageConfig
from stdf_platform.database import Database


class DuckDBResource(ConfigurableResource):
    """DuckDB database resource for STDF analytics.

    Creates views over Hive-partitioned Parquet files and provides
    SQL query capabilities.
    """

    data_dir: str = "./data"
    database: str = "./data/stdf.duckdb"

    def _get_config(self) -> StorageConfig:
        """Create storage config."""
        return StorageConfig(
            data_dir=Path(self.data_dir),
            database=Path(self.database),
        )

    def get_database(self) -> Database:
        """Create and return a Database instance (caller must manage connection)."""
        return Database(self._get_config())

    def refresh_views(self) -> None:
        """Refresh DuckDB views to reflect latest Parquet data."""
        db = self.get_database()
        with db:
            db.refresh_views()

    def query(self, sql: str) -> list[dict]:
        """Execute a SQL query and return results.

        Args:
            sql: SQL query string.

        Returns:
            List of result rows as dictionaries.
        """
        db = self.get_database()
        with db:
            return db.query(sql)
