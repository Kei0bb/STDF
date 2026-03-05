"""Table assets: Save parsed STDF data to Parquet and refresh DuckDB views."""

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from stdf_dagster.resources.parquet import ParquetStorageResource
from stdf_dagster.resources.duckdb_resource import DuckDBResource
from stdf_dagster.resources.stdf_config import STDFConfigResource
from stdf_platform.storage import _get_test_category
from stdf_platform.sync_manager import SyncManager


@asset(
    description="パース済みSTDFデータをHive-partitioned Parquetテーブル (lots, wafers, parts, test_data) に書き出す",
    group_name="tables",
    kinds={"parquet"},
)
def stdf_parquet_tables(
    context: AssetExecutionContext,
    parquet_storage: ParquetStorageResource,
    stdf_config: STDFConfigResource,
    parsed_stdf_data: list[dict],
) -> MaterializeResult:
    """Save parsed STDF data to Hive-partitioned Parquet files.

    Writes lots, wafers, parts, and test_data tables, then marks
    each file as ingested in the sync history.
    """
    if not parsed_stdf_data:
        context.log.info("No parsed data to save")
        return MaterializeResult(
            metadata={"files_saved": MetadataValue.int(0)}
        )

    config = stdf_config.load_config()
    sync = SyncManager(config.storage.data_dir / "sync_history.json")

    saved_count = 0
    total_lots = 0
    total_wafers = 0
    total_parts = 0
    total_tests = 0

    for item in parsed_stdf_data:
        data = item["data"]
        product = item["product"]
        test_type = item["test_type"]
        remote_path = item["remote_path"]
        filename = item["filename"]

        # Determine test_category from test_type (sub_process)
        sub_process = data.test_code if data.test_code else test_type
        test_category = _get_test_category(sub_process)

        context.log.info(
            f"Saving: {filename} (product={product}, "
            f"category={test_category}, sub={sub_process})"
        )

        try:
            result = parquet_storage.save(
                data=data,
                product=product,
                test_category=test_category,
                sub_process=sub_process,
                source_file=filename,
            )

            # Track counts
            total_lots += 1
            total_wafers += item.get("wafer_count", 0)
            total_parts += item.get("part_count", 0)
            total_tests += item.get("test_count", 0)
            saved_count += 1

            # Mark as ingested in sync history
            sync.mark_ingested(remote_path)

            context.log.info(f"  ✓ Saved {filename}")

        except Exception as e:
            context.log.error(f"  ✗ Failed to save {filename}: {e}")

    context.log.info(
        f"Saved {saved_count}/{len(parsed_stdf_data)} files: "
        f"lots={total_lots}, wafers={total_wafers}, "
        f"parts={total_parts}, tests={total_tests}"
    )

    return MaterializeResult(
        metadata={
            "files_saved": MetadataValue.int(saved_count),
            "total_lots": MetadataValue.int(total_lots),
            "total_wafers": MetadataValue.int(total_wafers),
            "total_parts": MetadataValue.int(total_parts),
            "total_tests": MetadataValue.int(total_tests),
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
