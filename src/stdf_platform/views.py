"""Single source of truth for DuckDB view definitions over the Parquet store.

Imported by database.py, web/api/deps.py and query.py so the dedup key and the
base/final view SQL exist in exactly one place. Paths use .as_posix() so the
generated SQL is valid on Windows as well as POSIX hosts.
"""

from pathlib import Path

import duckdb


# Dedup identity within a (lot, retest) group, expressed as native partition
# columns. CP rows have part_txt='' (so they group by wafer_id + x/y); FT rows
# have wafer_id='' and x=y=-32768 (so they group by part_txt). Listing all four
# columns is equivalent to the old CASE/CONCAT key for both categories — the
# "unused" columns are constant within a category — but avoids per-row string
# concatenation, making the ROW_NUMBER() window dedup ~25% faster.
_DEDUP_UNIT = "wafer_id, x_coord, y_coord, part_txt"


def setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> list[str]:
    """Register Parquet glob views and final-bin merge VIEWs.

    Returns the list of registered view names (base tables and the *_final
    dedup views that were created).
    """
    registered: list[str] = []
    for table in ["lots", "wafers", "parts", "test_data", "chipid"]:
        path = data_dir / table
        if path.exists():
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table} AS
                SELECT * FROM read_parquet(
                    '{path.as_posix()}/**/*.parquet', hive_partitioning=true
                )
            """)
            registered.append(table)

    if "parts" in registered:
        conn.execute(f"""
            CREATE OR REPLACE VIEW parts_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, {_DEDUP_UNIT}
                    ORDER BY retest_num DESC
                ) AS rn FROM parts
            ) WHERE rn = 1
        """)
        registered.append("parts_final")

    if "test_data" in registered:
        conn.execute(f"""
            CREATE OR REPLACE VIEW test_data_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, {_DEDUP_UNIT}, test_num, pin_num
                    ORDER BY retest_num DESC
                ) AS rn FROM test_data
            ) WHERE rn = 1
        """)
        registered.append("test_data_final")

    if "chipid" in registered:
        # die identity = decoded ChipID (efuse_raw), NOT positional
        # chip_occurrence_index (which can swap die0/die1 across retests).
        conn.execute("""
            CREATE OR REPLACE VIEW chipid_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, efuse_raw
                    ORDER BY retest_num DESC
                ) AS rn FROM chipid
            ) WHERE rn = 1
        """)
        registered.append("chipid_final")

    return registered
