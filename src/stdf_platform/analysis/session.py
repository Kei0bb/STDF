"""AnalysisSession: a read-only DuckDB session with Phase-1 views registered.

One object owns the :memory: connection, resolves the data dir via Config
(STDF_CONFIG-aware), registers the canonical views (setup_views), and exposes
small helpers (q/lots). Analysis modules take a session as their first arg and
read session.conn. Parquet is the source of truth; this layer never writes.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd

from ..config import Config
from ..mounts import setup_views


class AnalysisSession:
    def __init__(self, data_dir: Path | None = None, config: Config | None = None,
                 sql_dir: Path | None = None) -> None:
        # `config`, when given, is the caller's already-resolved Config (e.g.
        # the CLI's ctx.obj["config"], built from -c/--env) and is used as-is
        # — none of the Config.load()/fallback resolution below runs. This is
        # what lets a store's gross_die_map reach this session's `gross_die`
        # table as the caller intended, instead of this session silently
        # re-resolving its own Config.load() and picking up a different
        # (often empty) gross_die_map.
        if config is None:
            config = Config.load()
            # workspace/ (VSCode Jupytext cell scripts, sql/ query runs,
            # etc.) is not the repo root, so a bare Config.load() above
            # resolves against the wrong cwd and silently defaults to
            # ./var/data. Fall back to the repo-root config.yaml only when the
            # caller passed no data_dir, AND no STDF_CONFIG env var is set
            # (Config.load()'s own resolution order is explicit arg ->
            # STDF_CONFIG -> cwd config.yaml; this fallback must not override
            # an STDF_CONFIG resolution — see config.py:142), AND cwd has no
            # config.yaml of its own. Any of data_dir passed explicitly,
            # STDF_CONFIG set, or a cwd config.yaml existing keeps the
            # behavior above unchanged.
            if (data_dir is None and not os.environ.get("STDF_CONFIG")
                    and not Path("config.yaml").exists()):
                repo_cfg = Path(__file__).resolve().parents[3] / "config.yaml"
                if repo_cfg.exists():
                    config = Config.load(repo_cfg)
        if data_dir is None:
            data_dir = config.storage.data_dir
        self.data_dir = Path(data_dir)
        self.conn = duckdb.connect(":memory:")
        self.registered = setup_views(self.conn, self.data_dir, config.gross_die_map)
        # sql/ クエリライブラリの場所(run/queries/show が読む)。既定はリポジトリ同梱。
        self.sql_dir = sql_dir

    def q(self, sql: str, params: list | None = None) -> pd.DataFrame:
        """Run raw SQL (bound params) and return a DataFrame. Escape hatch."""
        return self.conn.execute(sql, params or []).fetchdf()

    # ── sql/ クエリライブラリ ──────────────────────────────────────

    def run(self, name: str, out: Path | str | None = None, **params):
        """sql/ の名前付きクエリを実行して DataFrame を返す。

        out を渡すと DuckDB が直接 CSV に書き(COPY TO)、書き出した行数を
        返す — Python 側にデータが載らないので大きなエクスポート向け。
        params はファイル冒頭の SET VARIABLE 既定値を上書きする。

            s.run("fail_ranking", lot="LOT002")
            s.run("die_test_export", lot="LOT002", out="lot002.csv")
        """
        from .library import run_query
        return run_query(self.conn, name, out=out, sql_dir=self.sql_dir, **params)

    def queries(self) -> pd.DataFrame:
        """sql/ にあるクエリの一覧(name / description)。"""
        from .library import list_queries
        return list_queries(self.sql_dir)

    def show(self, name: str) -> str:
        """クエリの SQL 本文を返す(改造の出発点に)。"""
        from .library import find_query
        return find_query(name, self.sql_dir).read_text(encoding="utf-8")

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
                    PARTITION BY product, test_category, lot_id, sub_process
                    ORDER BY start_time DESC) AS rn
                FROM lots{clause}
            ) WHERE rn = 1
            ORDER BY start_time DESC, lot_id
            """,
            params,
        ).fetchdf()

    def runs(self, lot_id: str | None = None, product: str | None = None,
              test_category: str | None = None) -> pd.DataFrame:
        """Per-run MIR rows from `runs` (one row per file x wafer identity).

        Unlike `lots()` (one row per lot, latest run's values collapsed via
        ROW_NUMBER), this returns every run verbatim — no dedup — since the
        whole point is inspecting a lot's wafer/retest-level test program
        history (job_mixed detection).
        """
        where, params = [], []
        if lot_id is not None:
            where.append("lot_id = ?")
            params.append(lot_id)
        if product is not None:
            where.append("product = ?")
            params.append(product)
        if test_category is not None:
            where.append("test_category = ?")
            params.append(test_category)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        return self.conn.execute(
            f"""
            SELECT lot_id, wafer_id, product, test_category, sub_process,
                   retest_num, part_type, job_name, job_rev,
                   start_time, finish_time, tester_type, operator,
                   node_name, handler_type, handler_id,
                   probe_card_type, probe_card_id,
                   loadboard_type, loadboard_id,
                   socket_type, socket_id,
                   test_rev, source_file
            FROM runs{clause}
            ORDER BY start_time, lot_id, wafer_id, retest_num
            """,
            params,
        ).fetchdf()

    def lot_summary(self, lot_id: str | None = None) -> pd.DataFrame:
        """Per-lot yield summary (gross-die aware), ex-Database.get_lot_summary."""
        where = "WHERE l.lot_id = ?" if lot_id else ""
        params = [lot_id] if lot_id else []
        return self.conn.execute(f"""
            SELECT l.lot_id, l.product, l.test_category, l.sub_process, l.part_type,
                   l.job_name, l.job_rev, l.job_variant_count, l.job_mixed,
                   MAX(p.wafer_count) AS wafer_count,
                   MAX(p.total_parts) AS total_parts,
                   MAX(p.good_parts)  AS good_parts,
                   MAX(p.yield_pct)   AS yield_pct
            FROM lots l
            LEFT JOIN (
                SELECT lot_id,
                       COUNT(*) FILTER (WHERE wafer_id <> '') AS wafer_count,
                       SUM(total) AS total_parts, SUM(good) AS good_parts,
                       ROUND(100.0 * SUM(good) / NULLIF(SUM(total), 0), 2) AS yield_pct
                FROM wafer_yield_final GROUP BY lot_id
            ) p ON l.lot_id = p.lot_id
            {where}
            GROUP BY ALL
            ORDER BY l.product, l.test_category, l.sub_process, l.lot_id
        """, params).fetchdf()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "AnalysisSession":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
