"""Table assets: Save parsed STDF data to Parquet and refresh DuckDB views."""

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
)

from stdf_dagster.resources.duckdb_resource import DuckDBResource
from stdf_dagster.resources.stdf_config import STDFConfigResource
from stdf_platform.sync_manager import SyncManager


@asset(
    description="パース済みSTDFデータをHive-partitioned Parquetテーブル (lots, wafers, parts, test_data) に書き出す",
    group_name="tables",
    kinds={"parquet"},
    retry_policy=RetryPolicy(max_retries=2, delay=5),
)
def stdf_parquet_tables(
    context: AssetExecutionContext,
    stdf_config: STDFConfigResource,
    raw_stdf_files: list[dict],
) -> MaterializeResult:
    """Save parsed STDF data to Hive-partitioned Parquet files.

    Writes lots, wafers, parts, and test_data tables, then marks
    each file as ingested in the sync history.
    """
    if not raw_stdf_files:
        context.log.info("No files to process")
        return MaterializeResult(
            metadata={"files_processed": MetadataValue.int(0)}
        )

    config = stdf_config.load_config()
    sync = SyncManager(config.storage.data_dir / "sync_history.json")

    import subprocess
    import sys
    import json
    from pathlib import Path

    success_count = 0
    failed_count = 0

    for file_info in raw_stdf_files:
        local_path = Path(file_info["local_path"])
        remote_path = file_info["remote_path"]
        product = file_info["product"]
        test_type = file_info["test_type"]
        filename = file_info["filename"]

        context.log.info(f"Processing: {filename}")

        try:
            # Run parse + save in a completely separate process (no timeout)
            proc = subprocess.Popen(
                [
                    sys.executable, "-m", "stdf_platform._ingest_worker",
                    str(local_path),
                    product,
                    str(config.storage.data_dir),
                    config.processing.compression,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            stdout, stderr = proc.communicate()

            if proc.returncode != 0:
                error_msg = stderr.strip() if stderr else "unknown error"
                context.log.error(f"  ✗ Failed {filename}: {error_msg}")
                failed_count += 1
                continue

            # Parse worker output
            try:
                result = json.loads(stdout)
                sub_process = result.get("sub_process", "UNKNOWN")
                test_category = result.get("test_category", "OTHER")
            except json.JSONDecodeError:
                sub_process = "UNKNOWN"
                test_category = "OTHER"

            sync.mark_ingested(remote_path)
            context.log.info(f"  ✓ Saved {filename} ({product}/{test_category}/{sub_process})")
            success_count += 1

        except Exception as e:
            context.log.error(f"  ✗ Failed {filename}: {e}")
            failed_count += 1

    context.log.info(
        f"Processed {len(raw_stdf_files)} files: "
        f"{success_count} success, {failed_count} failed"
    )

    return MaterializeResult(
        metadata={
            "files_saved": MetadataValue.int(success_count),
            "files_failed": MetadataValue.int(failed_count),
        }
    )


@asset(
    description="Parquetファイルの更新を反映してDuckDBビューをリフレッシュ",
    group_name="tables",
    kinds={"duckdb"},
)
def duckdb_views(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    stdf_parquet_tables: MaterializeResult,
) -> MaterializeResult:
    """Refresh DuckDB views after Parquet files are updated.

    Creates/updates views for lots, wafers, parts, and test_data
    pointing to the Hive-partitioned Parquet directory.
    """
    context.log.info("Refreshing DuckDB views...")
    duckdb.refresh_views()

    # Verify by querying lot count
    try:
        result = duckdb.query("SELECT COUNT(DISTINCT lot_id) as cnt FROM lots")
        lot_count = result[0]["cnt"] if result else 0
    except Exception:
        lot_count = 0

    context.log.info(f"DuckDB views refreshed. Total lots: {lot_count}")

    return MaterializeResult(
        metadata={
            "total_lots_in_db": MetadataValue.int(lot_count),
        }
    )
