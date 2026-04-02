"""Dagster resource for PostgreSQL database."""

from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    """PostgreSQL connection resource for STDF data sharing.

    Provides connection management and bulk data operations
    for syncing STDF data to PostgreSQL.
    """

    host: str = "localhost"
    port: int = 5432
    database: str = "stdf"
    user: str = "stdf"
    password: str = "stdf_dev_2024"

    @contextmanager
    def get_connection(self) -> Generator:
        """Get a PostgreSQL connection (context-managed)."""
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql: str, params: tuple = None) -> None:
        """Execute a SQL statement."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()

    def query(self, sql: str, params: tuple = None) -> list[dict]:
        """Execute a query and return results as list of dicts."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def bulk_upsert(
        self,
        table: str,
        rows: list[dict],
        conflict_columns: list[str],
        batch_size: int = 5000,
    ) -> int:
        """Bulk upsert rows into a table.

        Uses COPY for initial load and INSERT ... ON CONFLICT for updates.

        Args:
            table: Target table name.
            rows: List of row dicts.
            conflict_columns: Columns for ON CONFLICT clause.
            batch_size: Number of rows per batch.

        Returns:
            Number of rows upserted.
        """
        if not rows:
            return 0

        columns = list(rows[0].keys())

        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(columns)

        if not conflict_columns:
            # Plain INSERT (no upsert) — used after delete_lot_data
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        else:
            conflict_cols = ", ".join(conflict_columns)
            update_cols = [c for c in columns if c not in conflict_columns]
            if update_cols:
                update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
                sql = f"""
                    INSERT INTO {table} ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_cols})
                    DO UPDATE SET {update_clause}
                """
            else:
                sql = f"""
                    INSERT INTO {table} ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_cols}) DO NOTHING
                """

        total = 0
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    values = [tuple(row[c] for c in columns) for row in batch]
                    psycopg2.extras.execute_batch(cur, sql, values, page_size=batch_size)
                    total += len(batch)
            conn.commit()

        return total

    def delete_lot_data(self, table: str, product: str, lot_id: str) -> None:
        """Delete all data for a specific lot (used before re-sync)."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {table} WHERE product = %s AND lot_id = %s",
                    (product, lot_id),
                )
            conn.commit()

    def get_row_counts(self) -> dict[str, int]:
        """Get row counts for all STDF tables."""
        tables = ["lots", "wafers", "parts", "test_data"]
        counts = {}
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cur.fetchone()[0]
        return counts

    def is_available(self) -> bool:
        """Check if PostgreSQL is reachable."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception:
            return False
