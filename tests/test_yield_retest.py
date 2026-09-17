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


sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_test_stdf import make_ft_stdf, make_stdf  # noqa: E402

from stdf_platform.parser import parse_stdf  # noqa: E402
from stdf_platform.storage import ParquetStorage  # noqa: E402
from stdf_platform.config import StorageConfig  # noqa: E402
from stdf_platform.analysis import AnalysisSession  # noqa: E402


def _storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _session(tmp_path: Path) -> AnalysisSession:
    return AnalysisSession(tmp_path)


def _wafer_yield(s: AnalysisSession, lot_id: str) -> list[dict]:
    """ex-Database.get_wafer_yield, via AnalysisSession.q()."""
    df = s.q(
        "SELECT wafer_id, total, good, yield_pct FROM wafer_yield_final "
        "WHERE lot_id = ? ORDER BY wafer_id",
        [lot_id],
    )
    return df.to_dict("records")


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

    with _session(tmp_path) as s:
        rows = _wafer_yield(s, "FTY")

    # FT has no wafer concept -> single group; final yield is 100% after retest,
    # NOT the run0 50% nor an empty result.
    assert len(rows) == 1
    assert rows[0]["total"] == 4
    assert rows[0]["good"] == 4
    assert rows[0]["yield_pct"] == 100.0


# ── CP: re-ingest must not double count ─────────────────────────────────────

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

    with _session(tmp_path) as s:
        rows = _wafer_yield(s, "CPLOT")

    assert len(rows) == 1
    assert rows[0]["total"] == 10  # not 20
