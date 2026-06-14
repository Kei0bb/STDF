"""DuckDB database for STDF analytics."""

from typing import Any

import duckdb

from .config import StorageConfig
from .views import setup_views


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
        """Create views for Parquet datasets (delegates to stdf_platform.views)."""
        setup_views(self.conn, self.data_dir)

    def refresh_views(self) -> None:
        """Refresh views after new data is added."""
        self._create_views()

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        """
        Execute SQL query and return results as list of dicts.

        Args:
            sql: SQL query string (use ? or $1 for parameters)
            params: Query parameters

        Returns:
            List of result rows as dictionaries
        """
        result = self.conn.execute(sql, params or [])
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def query_df(self, sql: str, params: list | None = None):
        """
        Execute SQL query and return results as pandas DataFrame.

        Args:
            sql: SQL query string (use ? or $1 for parameters)
            params: Query parameters

        Returns:
            Pandas DataFrame with results
        """
        return self.conn.execute(sql, params or []).fetchdf()

    def get_lot_summary(self, lot_id: str | None = None) -> list[dict]:
        """Get lot summary with yield information (retest-aware).

        Yield is derived from parts_final (each die/package's latest retest),
        not from the WRR summary in `wafers`. The WRR of a retest run covers
        only the re-measured subset, and FT has no WRR at all — parts_final is
        correct for both CP and FT regardless of partial vs full retests.
        """
        where_clause = "WHERE l.lot_id = $1" if lot_id else ""
        params = [lot_id] if lot_id else []

        sql = f"""
        SELECT
            l.lot_id,
            l.product,
            l.test_category,
            l.sub_process,
            l.part_type,
            l.job_name,
            l.job_rev,
            MAX(p.wafer_count) as wafer_count,
            MAX(p.total_parts) as total_parts,
            MAX(p.good_parts) as good_parts,
            MAX(p.yield_pct) as yield_pct
        FROM lots l
        LEFT JOIN (
            SELECT
                lot_id,
                COUNT(DISTINCT NULLIF(wafer_id, '')) as wafer_count,
                COUNT(*) as total_parts,
                SUM(CASE WHEN passed THEN 1 ELSE 0 END) as good_parts,
                ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 2) as yield_pct
            FROM parts_final
            GROUP BY lot_id
        ) p ON l.lot_id = p.lot_id
        {where_clause}
        GROUP BY l.lot_id, l.product, l.test_category, l.sub_process, l.part_type, l.job_name, l.job_rev
        ORDER BY l.product, l.test_category, l.sub_process, l.lot_id
        """
        return self.query(sql, params)

    def get_wafer_yield(self, lot_id: str) -> list[dict]:
        """Get yield by wafer for a lot (retest-aware, die-level).

        Computed from parts_final so each die's final disposition is its latest
        retest. For FT (no wafer concept) wafer_id is empty, yielding a single
        package-level group instead of an empty result.
        """
        sql = """
        SELECT
            wafer_id,
            COUNT(*) as total,
            SUM(CASE WHEN passed THEN 1 ELSE 0 END) as good,
            ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*), 0), 2) as yield_pct
        FROM parts_final
        WHERE lot_id = $1
        GROUP BY wafer_id
        ORDER BY wafer_id
        """
        return self.query(sql, [lot_id])

    def get_test_fail_rate(self, lot_id: str, top_n: int = 10) -> list[dict]:
        """Get top failing tests for a lot."""
        sql = """
        SELECT
            test_num,
            test_name,
            COUNT(*) as total,
            SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) as fails,
            ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) as fail_rate
        FROM test_data_final
        WHERE lot_id = $1
        GROUP BY test_num, test_name
        HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
        ORDER BY fail_rate DESC
        LIMIT $2
        """
        return self.query(sql, [lot_id, top_n])

    def get_bin_summary(self, lot_id: str) -> list[dict]:
        """Get bin distribution for a lot."""
        sql = """
        SELECT
            soft_bin,
            COUNT(*) as count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
        FROM parts_final
        WHERE lot_id = $1
        GROUP BY soft_bin
        ORDER BY soft_bin
        """
        return self.query(sql, [lot_id])

    def compare_lots(self, lot_ids: list[str]) -> list[dict]:
        """Compare yield across multiple lots (retest-aware, die-level)."""
        placeholders = ", ".join(f"${i+1}" for i in range(len(lot_ids)))
        sql = f"""
        SELECT
            l.lot_id,
            l.job_name,
            l.job_rev,
            MAX(p.wafers) as wafers,
            MAX(p.total) as total,
            MAX(p.good) as good,
            MAX(p.yield_pct) as yield_pct
        FROM lots l
        LEFT JOIN (
            SELECT
                lot_id,
                COUNT(DISTINCT NULLIF(wafer_id, '')) as wafers,
                COUNT(*) as total,
                SUM(CASE WHEN passed THEN 1 ELSE 0 END) as good,
                ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 2) as yield_pct
            FROM parts_final
            GROUP BY lot_id
        ) p ON l.lot_id = p.lot_id
        WHERE l.lot_id IN ({placeholders})
        GROUP BY l.lot_id, l.job_name, l.job_rev
        ORDER BY yield_pct DESC
        """
        return self.query(sql, lot_ids)
