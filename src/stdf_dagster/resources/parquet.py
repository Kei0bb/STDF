"""Dagster resource wrapping stdf_platform.storage.ParquetStorage."""

from pathlib import Path

from dagster import ConfigurableResource

from stdf_platform.config import StorageConfig
from stdf_platform.storage import ParquetStorage
from stdf_platform.parser import STDFData


class ParquetStorageResource(ConfigurableResource):
    """Parquet storage resource for writing STDF data.

    Wraps ParquetStorage to handle Hive-partitioned Parquet output.
    """

    data_dir: str = "./data"
    compression: str = "gzip"

    def _get_storage(self) -> ParquetStorage:
        """Create a ParquetStorage instance."""
        config = StorageConfig(data_dir=Path(self.data_dir))
        return ParquetStorage(config)

    def save(
        self,
        data: STDFData,
        product: str,
        test_category: str,
        sub_process: str = "",
        source_file: str = "",
    ) -> dict:
        """Save parsed STDF data to Parquet files.

        Args:
            data: Parsed STDFData object.
            product: Product name.
            test_category: Test category (CP/FT).
            sub_process: Sub-process identifier.
            source_file: Original STDF filename.

        Returns:
            Dict with saved file paths and record counts.
        """
        storage = self._get_storage()
        result = storage.save_stdf_data(
            data=data,
            product=product,
            test_category=test_category,
            sub_process=sub_process,
            source_file=source_file,
            compression=self.compression,
        )
        return result
