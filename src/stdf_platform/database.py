"""DuckDB database for STDF analytics."""

import duckdb

from .config import StorageConfig
from .views import setup_views


class Database:
    """DuckDB database for STDF analytics."""

    def __init__(self, config: StorageConfig,
                 gross_die_map: dict[str, tuple[int, int]] | None = None):
        self.config = config
        self.db_path = config.database
        self.data_dir = config.data_dir
        # {product: (gross_die, gd_fail_bin)} — applied at query time for the
        # gross-die yield denominator and QC-fail bin bucket (CP only).
        self.gross_die_map = gross_die_map
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
        setup_views(self.conn, self.data_dir, self.gross_die_map)

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

        Yield is derived from wafer_yield_final (each die/package's latest
        retest, with the gross-die denominator applied for CP). The WRR of a
        retest run covers only the re-measured subset, and FT has no WRR at all —
        wafer_yield_final is correct for both CP and FT regardless of partial vs
        full retests, and totals reflect gross die when configured.
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
                COUNT(*) FILTER (WHERE wafer_id <> '') as wafer_count,
                SUM(total) as total_parts,
                SUM(good) as good_parts,
                ROUND(100.0 * SUM(good) / NULLIF(SUM(total), 0), 2) as yield_pct
            FROM wafer_yield_final
            GROUP BY lot_id
        ) p ON l.lot_id = p.lot_id
        {where_clause}
        GROUP BY l.lot_id, l.product, l.test_category, l.sub_process, l.part_type, l.job_name, l.job_rev
        ORDER BY l.product, l.test_category, l.sub_process, l.lot_id
        """
        return self.query(sql, params)

    def get_wafer_yield(self, lot_id: str) -> list[dict]:
        """Get yield by wafer for a lot (retest-aware, die-level).

        Computed from wafer_yield_final so each die's final disposition is its
        latest retest and CP totals use the gross-die denominator when
        configured. For FT (no wafer concept) wafer_id is empty, yielding a
        single package-level group instead of an empty result.
        """
        sql = """
        SELECT wafer_id, total, good, yield_pct
        FROM wafer_yield_final
        WHERE lot_id = $1
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
        """Get bin distribution for a lot.

        Probed dies are counted from parts_final; dies lost to inline failure /
        aborted probe (gross die − probed) are added under the product's
        gd_fail_bin so the distribution totals match the gross-die denominator.
        Merges into an existing real bin if gd_fail_bin collides with one.
        """
        sql = """
        WITH binned AS (
            SELECT soft_bin, COUNT(*) AS count
            FROM parts_final
            WHERE lot_id = $1
            GROUP BY soft_bin
            UNION ALL
            SELECT gd_fail_bin AS soft_bin, SUM(unprobed) AS count
            FROM wafer_yield_final
            WHERE lot_id = $1 AND gd_fail_bin IS NOT NULL AND unprobed > 0
            GROUP BY gd_fail_bin
        )
        SELECT
            soft_bin,
            SUM(count) AS count,
            ROUND(100.0 * SUM(count) / SUM(SUM(count)) OVER (), 2) AS pct
        FROM binned
        GROUP BY soft_bin
        ORDER BY soft_bin
        """
        return self.query(sql, [lot_id])
