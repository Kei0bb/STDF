"""AnalysisSession: a read-only DuckDB session with Phase-1 views registered.

One object owns the :memory: connection, resolves the data dir via Config
(STDF_CONFIG-aware), registers the canonical views (setup_views), and exposes
small helpers (q/lots). Analysis modules take a session as their first arg and
read session.conn. Parquet is the source of truth; this layer never writes.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ..config import Config
from ..views import setup_views


class AnalysisSession:
    def __init__(self, data_dir: Path | None = None) -> None:
        config = Config.load()
        if data_dir is None:
            data_dir = config.storage.data_dir
        self.data_dir = Path(data_dir)
        self.conn = duckdb.connect(":memory:")
        self.registered = setup_views(self.conn, self.data_dir, config.gross_die_map)

    def q(self, sql: str, params: list | None = None) -> pd.DataFrame:
        """Run raw SQL (bound params) and return a DataFrame. Escape hatch."""
        return self.conn.execute(sql, params or []).fetchdf()

    def lots(self, product: str | None = None,
             test_category: str | None = None) -> pd.DataFrame:
        """MIR-derived lot list (latest row per lot), optionally filtered."""
        where, params = [], []
        if product is not None:
            where.append("product = ?")
            params.append(product)
        if test_category is not None:
            where.append("test_category = ?")
            params.append(test_category)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        return self.conn.execute(
            f"""
            SELECT lot_id, product, test_category, sub_process,
                   part_type, job_name, job_rev, tester_type, operator,
                   start_time, finish_time
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY product, test_category, lot_id
                    ORDER BY start_time DESC) AS rn
                FROM lots{clause}
            ) WHERE rn = 1
            ORDER BY start_time DESC, lot_id
            """,
            params,
        ).fetchdf()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "AnalysisSession":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
