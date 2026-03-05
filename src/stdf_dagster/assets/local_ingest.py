"""Local file ingestion asset: bypass FTP and ingest local STDF files."""

import gzip
import tempfile
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
)

from stdf_dagster.resources.stdf_parser import STDFParserResource
from stdf_dagster.resources.parquet import ParquetStorageResource
from stdf_dagster.resources.duckdb_resource import DuckDBResource
from stdf_platform.storage import _get_test_category


class LocalIngestConfig(Config):
    """Configuration for local STDF file ingestion."""

    file_path: str  # Path to STDF file
    product: str  # Product name
    test_type: str = ""  # Sub-process / test type (auto-detected from STDF if empty)


@asset(
    description="ローカルSTDFファイルを直接ingest（FTPバイパス）。パース → Parquet → DuckDBビュー更新を一括実行",
    group_name="local",
    kinds={"python", "rust"},
)
def local_ingest(
    context: AssetExecutionContext,
    config: LocalIngestConfig,
    stdf_parser: STDFParserResource,
    parquet_storage: ParquetStorageResource,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Ingest a local STDF file without FTP.

    Equivalent to `stdf2pq ingest <file>` but runs within Dagster.
    Parses the file, saves to Parquet, and refreshes DuckDB views.
    """
    file_path = Path(config.file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"STDF file not found: {file_path}")

    context.log.info(f"Ingesting: {file_path.name}")
    context.log.info(f"Parser: {'Rust' if stdf_parser.uses_rust else 'Python'}")

    # Parse
    if file_path.suffix == ".gz":
        with tempfile.NamedTemporaryFile(suffix=".stdf", delete=False) as tmp:
            with gzip.open(file_path, "rb") as gz:
                tmp.write(gz.read())
            tmp_path = Path(tmp.name)
        data = stdf_parser.parse(tmp_path)
        tmp_path.unlink(missing_ok=True)
    else:
        data = stdf_parser.parse(file_path)

    context.log.info(
        f"Parsed: lot={data.lot_id}, wafers={len(data.wafers)}, "
        f"parts={len(data.parts)}, tests={len(data.tests)}"
    )

    # Determine test_category
    sub_process = config.test_type or data.test_code or ""
    test_category = _get_test_category(sub_process) if sub_process else "UNKNOWN"

    # Save to Parquet
    result = parquet_storage.save(
        data=data,
        product=config.product,
        test_category=test_category,
        sub_process=sub_process,
        source_file=file_path.name,
    )

    context.log.info(f"Saved to Parquet: {result}")

    # Refresh DuckDB views
    duckdb.refresh_views()
    context.log.info("DuckDB views refreshed")

    return MaterializeResult(
        metadata={
            "lot_id": MetadataValue.text(data.lot_id),
            "product": MetadataValue.text(config.product),
            "test_category": MetadataValue.text(test_category),
            "sub_process": MetadataValue.text(sub_process),
            "wafer_count": MetadataValue.int(len(data.wafers)),
            "part_count": MetadataValue.int(len(data.parts)),
            "test_count": MetadataValue.int(len(data.tests)),
        }
    )
