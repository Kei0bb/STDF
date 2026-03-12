"""PostgreSQL sync asset: sync all STDF data from DuckDB to PostgreSQL."""

from datetime import datetime, timezone

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
)

from stdf_dagster.resources.duckdb_resource import DuckDBResource
from stdf_dagster.resources.postgres import PostgresResource


def _convert_timestamps(rows: list[dict], ts_columns: list[str]) -> list[dict]:
    """Convert timestamp columns to Python datetime (or None)."""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    for row in rows:
        for col in ts_columns:
            val = row.get(col)
            if val is not None and val == epoch:
                row[col] = None
    return rows


@asset(
    description="DuckDBの全テーブルデータをPostgreSQLに同期（lots, wafers, parts, test_data）",
    group_name="postgres",
    deps=["duckdb_views"],
    kinds={"postgres"},
    retry_policy=RetryPolicy(max_retries=2, delay=10),
)
def postgres_sync(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    """Sync all STDF data from DuckDB/Parquet to PostgreSQL.

    Performs an incremental sync:
    1. Query DuckDB for all lot_ids
    2. Check which lots already exist in PostgreSQL
    3. Sync only new lots (delete + re-insert for existing if needed)
    """
    # Check PostgreSQL availability
    if not postgres.is_available():
        raise ConnectionError(
            "PostgreSQL is not reachable. "
            "Start it with: docker compose up -d"
        )

    db = duckdb.get_database()

    synced = {"lots": 0, "wafers": 0, "parts": 0, "test_data": 0}

    with db:
        # --- lots ---
        context.log.info("Syncing lots...")
        try:
            lots_df = db.query_df("""
                SELECT lot_id, product, test_category, sub_process,
                       part_type, job_name, job_rev,
                       start_time, finish_time, tester_type, operator
                FROM lots
            """)
            lots_rows = lots_df.to_dict("records")
            lots_rows = _convert_timestamps(lots_rows, ["start_time", "finish_time"])
            synced["lots"] = postgres.bulk_upsert(
                "lots", lots_rows,
                conflict_columns=["product", "test_category", "sub_process", "lot_id"],
            )
            context.log.info(f"  lots: {synced['lots']} rows")
        except Exception as e:
            context.log.warning(f"  lots sync skipped: {e}")

        # --- wafers ---
        context.log.info("Syncing wafers...")
        try:
            wafers_df = db.query_df("""
                SELECT wafer_id, lot_id, product, test_category, sub_process,
                       head_num, start_time, finish_time,
                       part_count, good_count, rtst_count, abrt_count,
                       test_rev, retest_num, source_file
                FROM wafers
            """)
            wafers_rows = wafers_df.to_dict("records")
            wafers_rows = _convert_timestamps(wafers_rows, ["start_time", "finish_time"])
            synced["wafers"] = postgres.bulk_upsert(
                "wafers", wafers_rows,
                conflict_columns=["product", "test_category", "sub_process", "lot_id", "wafer_id", "retest_num"],
            )
            context.log.info(f"  wafers: {synced['wafers']} rows")
        except Exception as e:
            context.log.warning(f"  wafers sync skipped: {e}")

        # --- parts ---
        context.log.info("Syncing parts...")
        try:
            parts_df = db.query_df("""
                SELECT part_id, lot_id, wafer_id, product, test_category, sub_process,
                       head_num, site_num, x_coord, y_coord,
                       hard_bin, soft_bin, passed, test_count, test_time
                FROM parts
            """)
            parts_rows = parts_df.to_dict("records")
            synced["parts"] = postgres.bulk_upsert(
                "parts", parts_rows,
                conflict_columns=["product", "test_category", "sub_process", "lot_id", "wafer_id", "part_id"],
            )
            context.log.info(f"  parts: {synced['parts']} rows")
        except Exception as e:
            context.log.warning(f"  parts sync skipped: {e}")

        # --- test_data (largest table, batch by lot) ---
        context.log.info("Syncing test_data...")
        try:
            # Get list of lots to sync in batches
            lot_list = db.query("SELECT DISTINCT lot_id, product FROM lots ORDER BY lot_id")

            total_test_rows = 0
            for lot_info in lot_list:
                lot_id = lot_info["lot_id"]
                product = lot_info["product"]

                test_df = db.query_df(f"""
                    SELECT lot_id, wafer_id, part_id, product, test_category, sub_process,
                           x_coord, y_coord, test_num, test_name, rec_type,
                           lo_limit, hi_limit, units, result, passed
                    FROM test_data
                    WHERE lot_id = '{lot_id}'
                """)

                if test_df.empty:
                    continue

                test_rows = test_df.to_dict("records")

                # Delete existing test_data for this lot before insert
                # (test_data has no PK, so we delete + insert)
                postgres.delete_lot_data("test_data", product, lot_id)

                rows_inserted = postgres.bulk_upsert(
                    "test_data", test_rows,
                    conflict_columns=[],  # No upsert, just insert after delete
                    batch_size=10000,
                )
                total_test_rows += rows_inserted

                context.log.info(f"  test_data lot={lot_id}: {rows_inserted} rows")

            synced["test_data"] = total_test_rows
        except Exception as e:
            context.log.warning(f"  test_data sync skipped: {e}")

    # Final counts from PostgreSQL
    try:
        pg_counts = postgres.get_row_counts()
    except Exception:
        pg_counts = {}

    context.log.info(
        f"PostgreSQL sync complete: "
        f"lots={synced['lots']}, wafers={synced['wafers']}, "
        f"parts={synced['parts']}, test_data={synced['test_data']}"
    )

    return MaterializeResult(
        metadata={
            "synced_lots": MetadataValue.int(synced["lots"]),
            "synced_wafers": MetadataValue.int(synced["wafers"]),
            "synced_parts": MetadataValue.int(synced["parts"]),
            "synced_test_data": MetadataValue.int(synced["test_data"]),
            "pg_total_lots": MetadataValue.int(pg_counts.get("lots", 0)),
            "pg_total_test_data": MetadataValue.int(pg_counts.get("test_data", 0)),
        }
    )
