"""Yield analysis must be retest-aware and computed from die/package level.

Before the fix, get_wafer_yield/get_lot_summary/compare_lots read the WRR
summary counts in the `wafers` table and took the latest retest record. That is
wrong two ways:
  1. A retest run's WRR covers only the re-measured (failed) subset, so the
     "latest" WRR reports a partial population, not the final wafer yield.
  2. FT has no WIR/WRR at all, so the `wafers` table is empty for FT and yield
     came back blank.

The fix derives yield from parts_final (each die/package's latest retest),
which is correct for partial OR full retests and works uniformly for CP & FT.
"""

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_test_stdf import make_ft_stdf, make_stdf  # noqa: E402

from stdf_platform.parser import parse_stdf  # noqa: E402
from stdf_platform.storage import ParquetStorage  # noqa: E402
from stdf_platform.config import StorageConfig  # noqa: E402
from stdf_platform.database import Database  # noqa: E402


def _storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _db(tmp_path: Path) -> Database:
    return Database(StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb"))


def _ingest_ft(storage, ft_file):
    data = parse_stdf(ft_file)
    storage.save_stdf_data(
        data, product="CHIPLET2D", test_category="FT", sub_process="FT1",
        source_file=ft_file.name,
    )


# ── FT: yield must exist AND reflect the passing retest ──────────────────────

def test_ft_wafer_yield_reflects_latest_retest(tmp_path):
    ft = tmp_path / "ft.stdf"
    make_ft_stdf(ft, "FTY", parts=4, fail_part_ids={0, 1})  # run0: 2/4 fail
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft)

    ft_rt = tmp_path / "ft_rt.stdf"
    make_ft_stdf(ft_rt, "FTY", parts=4)  # retest run: all pass
    _ingest_ft(storage, ft_rt)

    with _db(tmp_path) as db:
        rows = db.get_wafer_yield("FTY")

    # FT has no wafer concept -> single group; final yield is 100% after retest,
    # NOT the run0 50% nor an empty result.
    assert len(rows) == 1
    assert rows[0]["total"] == 4
    assert rows[0]["good"] == 4
    assert rows[0]["yield_pct"] == 100.0


def test_ft_lot_summary_yield_from_parts(tmp_path):
    ft = tmp_path / "ft.stdf"
    make_ft_stdf(ft, "FTY", parts=4, fail_part_ids={0, 1})
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft)
    ft_rt = tmp_path / "ft_rt.stdf"
    make_ft_stdf(ft_rt, "FTY", parts=4)
    _ingest_ft(storage, ft_rt)

    with _db(tmp_path) as db:
        rows = db.get_lot_summary("FTY")

    assert len(rows) == 1
    assert rows[0]["total_parts"] == 4
    assert rows[0]["good_parts"] == 4
    assert rows[0]["yield_pct"] == 100.0


# ── CP: per-wafer grouping still works, yield from parts == good/total ────────

def test_cp_wafer_yield_grouped_by_wafer(tmp_path):
    random.seed(42)
    cp = tmp_path / "cp.stdf"
    make_stdf(cp, "CPLOT", num_wafers=2, parts_per_wafer=10)
    storage = _storage(tmp_path)
    data = parse_stdf(cp)
    storage.save_stdf_data(
        data, product="P", test_category="CP", sub_process="CP11",
        source_file=cp.name,
    )

    with _db(tmp_path) as db:
        rows = db.get_wafer_yield("CPLOT")

    assert len(rows) == 2  # two wafers, one group each
    for r in rows:
        assert r["total"] == 10
        assert 0 <= r["good"] <= 10
        assert r["yield_pct"] == round(100.0 * r["good"] / r["total"], 2)


def test_cp_reingest_does_not_double_count(tmp_path):
    """Re-ingesting a CP lot (retest) must not inflate totals — parts_final
    dedups by physical die, so total stays at the real die count."""
    random.seed(7)
    cp = tmp_path / "cp.stdf"
    make_stdf(cp, "CPLOT", num_wafers=1, parts_per_wafer=10)
    storage = _storage(tmp_path)

    data = parse_stdf(cp)
    storage.save_stdf_data(
        data, product="P", test_category="CP", sub_process="CP11",
        source_file=cp.name,
    )
    # second ingest = retest of the same wafer
    data2 = parse_stdf(cp)
    storage.save_stdf_data(
        data2, product="P", test_category="CP", sub_process="CP11",
        source_file=cp.name,
    )

    with _db(tmp_path) as db:
        rows = db.get_wafer_yield("CPLOT")

    assert len(rows) == 1
    assert rows[0]["total"] == 10  # not 20


# ── setup_views covers the parts_final yield path ─────────────────────────────

def test_setup_views_summary_and_wafer_yield_retest_aware(tmp_path):
    """setup_views must be retest-aware and work for FT (no wafers table)."""
    import duckdb
    from stdf_platform.views import setup_views

    ft = tmp_path / "ft.stdf"
    make_ft_stdf(ft, "FTY", parts=4, fail_part_ids={0, 1})
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft)
    ft_rt = tmp_path / "ft_rt.stdf"
    make_ft_stdf(ft_rt, "FTY", parts=4)
    _ingest_ft(storage, ft_rt)

    conn = duckdb.connect()
    setup_views(conn, tmp_path)

    # /summary
    summary = conn.execute("""
        SELECT MAX(p.total_parts) AS total_parts,
               MAX(p.good_parts)  AS good_parts,
               MAX(p.yield_pct)   AS yield_pct
        FROM lots l
        LEFT JOIN (
            SELECT lot_id, COUNT(*) AS total_parts,
                   SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS good_parts,
                   ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0), 2) AS yield_pct
            FROM parts_final GROUP BY lot_id
        ) p ON l.lot_id = p.lot_id
        WHERE l.lot_id = 'FTY'
    """).fetchone()
    assert summary == (4, 4, 100.0)

    # /wafer-yield (single FT group)
    wy = conn.execute("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS good
        FROM parts_final WHERE lot_id = 'FTY' GROUP BY wafer_id
    """).fetchall()
    assert wy == [(4, 4)]
