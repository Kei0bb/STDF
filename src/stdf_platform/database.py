"""DuckDB database for STDF analytics."""

from pathlib import Path
from typing import Any

import duckdb

from .config import StorageConfig


class Database:
    """DuckDB database for STDF analytics."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.db_path = config.database
        self.data_dir = config.data_dir
        self._conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> None:
        """Connect to DuckDB database."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.db_path))
        self._create_views()

    def disconnect(self) -> None:
        """Disconnect from database."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get database connection."""
        if self._conn is None:
            raise RuntimeError("Database not connected")
        return self._conn

    def _create_views(self) -> None:
        """Create views for Parquet datasets."""
        tables = ["lots", "wafers", "parts", "test_data"]

        for table in tables:
            table_path = self.data_dir / table
            if table_path.exists():
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table} AS
                    SELECT * FROM read_parquet('{table_path}/**/*.parquet', hive_partitioning=true)
                """)

    def refresh_views(self) -> None:
        """Refresh views after new data is added."""
        self._create_views()

    def query(self, sql: str) -> list[dict]:
        """
        Execute SQL query and return results as list of dicts.

        Args:
            sql: SQL query string

        Returns:
            List of result rows as dictionaries
        """
        result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def query_df(self, sql: str):
        """
        Execute SQL query and return results as pandas DataFrame.

        Args:
            sql: SQL query string

        Returns:
            Pandas DataFrame with results
        """
        return self.conn.execute(sql).fetchdf()

    def get_lot_summary(self, lot_id: str | None = None) -> list[dict]:
        """Get lot summary with yield information (latest retest only)."""
        where_clause = f"WHERE l.lot_id = '{lot_id}'" if lot_id else ""

        sql = f"""
        SELECT 
            l.lot_id,
            l.product,
            l.test_category,
            l.sub_process,
            l.part_type,
            l.job_name,
            l.job_rev,
            COUNT(DISTINCT w.wafer_id) as wafer_count,
            SUM(w.part_count) as total_parts,
            SUM(w.good_count) as good_parts,
            ROUND(100.0 * SUM(w.good_count) / NULLIF(SUM(w.part_count), 0), 2) as yield_pct
        FROM lots l
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER(
                PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
            ) as rn
            FROM wafers
        ) w ON l.lot_id = w.lot_id AND w.rn = 1
        {where_clause}
        GROUP BY l.lot_id, l.product, l.test_category, l.sub_process, l.part_type, l.job_name, l.job_rev
        ORDER BY l.product, l.test_category, l.sub_process, l.lot_id
        """
        return self.query(sql)

    def get_wafer_yield(self, lot_id: str) -> list[dict]:
        """Get yield by wafer for a lot (latest retest only)."""
        sql = f"""
        SELECT 
            wafer_id,
            part_count as total,
            good_count as good,
            ROUND(100.0 * good_count / NULLIF(part_count, 0), 2) as yield_pct,
            test_rev,
            retest_num
        FROM (
            SELECT *, ROW_NUMBER() OVER(
                PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
            ) as rn
            FROM wafers
            WHERE lot_id = '{lot_id}'
        )
        WHERE rn = 1
        ORDER BY yield_pct DESC
        """
        return self.query(sql)

    def get_test_fail_rate(self, lot_id: str, top_n: int = 10) -> list[dict]:
        """Get top failing tests for a lot."""
        sql = f"""
        SELECT 
            test_num,
            test_name,
            COUNT(*) as total,
            SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as fails,
            ROUND(100.0 * SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) / COUNT(*), 2) as fail_rate
        FROM test_data
        WHERE lot_id = '{lot_id}'
        GROUP BY test_num, test_name
        HAVING SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) > 0
        ORDER BY fail_rate DESC
        LIMIT {top_n}
        """
        return self.query(sql)

    def get_bin_summary(self, lot_id: str) -> list[dict]:
        """Get bin distribution for a lot."""
        sql = f"""
        SELECT 
            soft_bin,
            COUNT(*) as count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
        FROM parts
        WHERE lot_id = '{lot_id}'
        GROUP BY soft_bin
        ORDER BY count DESC
        """
        return self.query(sql)

    def compare_lots(self, lot_ids: list[str]) -> list[dict]:
        """Compare yield across multiple lots (latest retest only)."""
        lot_list = ", ".join(f"'{lot}'" for lot in lot_ids)
        sql = f"""
        SELECT 
            l.lot_id,
            l.job_name,
            l.job_rev,
            COUNT(DISTINCT w.wafer_id) as wafers,
            SUM(w.part_count) as total,
            SUM(w.good_count) as good,
            ROUND(100.0 * SUM(w.good_count) / NULLIF(SUM(w.part_count), 0), 2) as yield_pct
        FROM lots l
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER(
                PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
            ) as rn
            FROM wafers
        ) w ON l.lot_id = w.lot_id AND w.rn = 1
        WHERE l.lot_id IN ({lot_list})
        GROUP BY l.lot_id, l.job_name, l.job_rev
        ORDER BY yield_pct DESC
        """
        return self.query(sql)
