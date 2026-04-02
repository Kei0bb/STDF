"""Local file ingestion asset: bypass FTP and ingest local STDF files."""

from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
)

from stdf_dagster.resources.stdf_config import STDFConfigResource
from stdf_dagster.resources.duckdb_resource import DuckDBResource


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
    stdf_config: STDFConfigResource,
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

    import subprocess
    import sys
    import json
    
    config_obj = stdf_config.load_config()

    try:
        # Run parse + save in a completely separate process (no timeout)
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "stdf_platform._ingest_worker",
                str(file_path),
                config.product,
                str(config_obj.storage.data_dir),
                config_obj.processing.compression,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stdout, stderr = proc.communicate()

        if proc.returncode != 0:
            error_msg = stderr.strip() if stderr else "unknown error"
            raise RuntimeError(f"Failed to ingest {file_path.name}: {error_msg}")

        # Parse worker output
        try:
            result = json.loads(stdout)
            sub_process = result.get("sub_process", "UNKNOWN")
            test_category = result.get("test_category", "OTHER")
            lot_id = result.get("lot_id", "UNKNOWN")
            wafer_count = result.get("wafer_count", 0)
            part_count = result.get("part_count", 0)
            test_count = result.get("test_count", 0)
        except json.JSONDecodeError:
            sub_process = "UNKNOWN"
            test_category = "OTHER"
            lot_id = "UNKNOWN"
            wafer_count = 0
            part_count = 0
            test_count = 0

        context.log.info(f"Saved to Parquet: {config.product}/{test_category}/{sub_process}")

    except Exception as e:
        context.log.error(f"✗ Failed {file_path.name}: {e}")
        raise e

    # Refresh DuckDB views
    duckdb.refresh_views()
    context.log.info("DuckDB views refreshed")

    return MaterializeResult(
        metadata={
            "lot_id": MetadataValue.text(lot_id),
            "product": MetadataValue.text(config.product),
            "test_category": MetadataValue.text(test_category),
            "sub_process": MetadataValue.text(sub_process),
            "wafer_count": MetadataValue.int(wafer_count),
            "part_count": MetadataValue.int(part_count),
            "test_count": MetadataValue.int(test_count),
        }
    )
