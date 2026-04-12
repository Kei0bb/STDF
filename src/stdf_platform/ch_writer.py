"""ClickHouse writer for STDF data.

Called from _ingest_worker after Parquet write completes.
ClickHouse is optional — if STDF_CH_HOST is not set, this module is never imported.
"""

from __future__ import annotations

import pyarrow as pa
import clickhouse_connect
from clickhouse_connect.driver import Client


def get_client(host: str, port: int, database: str, username: str, password: str) -> Client:
    """Create a ClickHouse HTTP client."""
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connect_timeout=10,
        send_receive_timeout=300,
    )


# Map PyArrow column names to ClickHouse column names (same, but lets us
# drop partition-only columns that aren't stored in CH tables).
_CH_COLUMNS: dict[str, list[str]] = {
    "lots": [
        "lot_id", "product", "test_category", "sub_process",
        "part_type", "job_name", "job_rev",
        "start_time", "finish_time", "tester_type", "operator",
    ],
    "wafers": [
        "lot_id", "wafer_id", "head_num",
        "start_time", "finish_time",
        "part_count", "good_count", "rtst_count", "abrt_count",
        "test_rev", "retest_num", "source_file",
    ],
    "parts": [
        "part_id", "lot_id", "wafer_id",
        "head_num", "site_num",
        "x_coord", "y_coord",
        "hard_bin", "soft_bin",
        "passed", "test_count", "test_time",
    ],
    "test_data": [
        "lot_id", "wafer_id", "part_id",
        "x_coord", "y_coord",
        "test_num", "test_name", "rec_type",
        "lo_limit", "hi_limit", "units",
        "result", "passed",
    ],
}


def write_tables(client: Client, pa_tables: dict[str, pa.Table]) -> dict[str, int]:
    """Insert PyArrow tables into ClickHouse.

    Args:
        client: ClickHouse client
        pa_tables: {table_name: pa.Table} from storage.save_stdf_data()

    Returns:
        {table_name: rows_inserted}
    """
    inserted: dict[str, int] = {}

    for table_name, pa_table in pa_tables.items():
        cols = _CH_COLUMNS.get(table_name)
        if cols is None:
            continue

        # Keep only columns that exist in CH schema
        available = set(pa_table.schema.names)
        select_cols = [c for c in cols if c in available]
        df = pa_table.select(select_cols).to_pandas()

        if df.empty:
            continue

        # clickhouse-connect accepts pandas DataFrame directly
        client.insert_df(table_name, df, column_names=select_cols)
        inserted[table_name] = len(df)

    return inserted
