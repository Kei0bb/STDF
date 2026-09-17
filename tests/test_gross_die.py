"""Gross die: applied at QUERY time as the CP yield denominator.

No synthetic rows are written to Parquet. setup_views(gross_die_map=...) builds
the gross_die table + wafer_yield_final view; CP wafer total = max(probed, GD),
unprobed = total - probed sits in the denominator (QC fail). Robust to retests
and partial/aborted probes.
"""

import duckdb
import pytest
from pathlib import Path

from stdf_platform.storage import ParquetStorage
from stdf_platform.config import StorageConfig, Config
from stdf_platform.mounts import setup_views
from stdf_platform.parser import STDFData


def _storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _conn(tmp_path: Path, gross_die_map=None) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path, gross_die_map)
    return conn


def _cp_data(lot_id="LOT1", wafer_id="W1", n_parts=7, xy_start=0) -> STDFData:
    """Build a synthetic CP STDFData.

    Dies are placed at (i, i) for i in [xy_start, xy_start+n_parts) so separate
    ingests can simulate an aborted probe (run 0) continued by a retest (run 1)
    that probes a *different* set of coordinates.
    """
    data = STDFData()
    data.lot_id = lot_id
    data.part_type = "TEST"
    data.job_name = "JOB"
    data.job_rev = "A"
    data.start_time = 0
    data.finish_time = 0
    data.tester_type = "T"
    data.operator = "OP"
    data._current_wafer = wafer_id
    data.wafers = [{
        "wafer_id": wafer_id, "head_num": 1,
        "start_time": 0, "finish_time": 0,
        "part_count": n_parts, "good_count": n_parts - 1,
        "rtst_count": 0, "abrt_count": 0,
    }]
    data.parts = [
        {
            "part_id": f"{lot_id}_{wafer_id}_{i}",
            "part_txt": "",
            "lot_id": lot_id, "wafer_id": wafer_id,
            "head_num": 1, "site_num": 1,
            "x_coord": i, "y_coord": i,
            "hard_bin": 1, "soft_bin": 1,
            "passed": True, "test_count": 1, "test_time": 100,
        }
        for i in range(xy_start, xy_start + n_parts)
    ]
    data.tests = {}
    data.test_results = []
    return data


def _save(storage, data, product="P", category="CP", sub="CP1", src="f.stdf"):
    storage.save_stdf_data(data, product=product, test_category=category,
                           sub_process=sub, source_file=src)


# ── yield denominator = gross die ────────────────────────────────────────────

def test_gross_die_aborted_probe_then_retest(tmp_path):
    """The scenario the user asked about: CP test stops partway, then the wafer
    is retested. Dies probed in the continuation must be counted once (not as
    QC fail), and only the genuinely-never-probed dies inflate the denominator.

    GD=12; run0 probes dies 0..5 (abort), run1 probes dies 6..9 (continuation).
    10 distinct dies probed → total=12, unprobed=2 (NOT 6 phantom QC fails),
    and yield uses the GD denominator (10 good / 12).
    """
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 6, xy_start=0), src="run0.stdf")
    _save(storage, _cp_data("LOT1", "W1", 4, xy_start=6), src="run1.stdf")
    conn = _conn(tmp_path, {"P": (12, 200)})

    row = conn.execute(
        "SELECT probed, total, unprobed, good, yield_pct"
        " FROM wafer_yield_final WHERE lot_id='LOT1'"
    ).fetchone()
    assert row == (10, 12, 2, 10, 83.33)


def _ft_data(n_parts=5) -> STDFData:
    data = STDFData()
    data.lot_id = "FTLOT"
    data.part_type = "PKG"
    data.job_name = "JOB"
    data.job_rev = "A"
    data.start_time = 0
    data.finish_time = 0
    data.tester_type = "T"
    data.operator = "OP"
    data.wafers = []
    data.parts = [
        {
            "part_id": f"PKG{i}", "part_txt": f"PKG{i}",
            "lot_id": "FTLOT", "wafer_id": "",
            "head_num": 1, "site_num": 1,
            "x_coord": -32768, "y_coord": -32768,
            "hard_bin": 1, "soft_bin": 1,
            "passed": True, "test_count": 1, "test_time": 100,
        }
        for i in range(n_parts)
    ]
    data.tests = {}
    data.test_results = []
    return data


@pytest.mark.parametrize("case", ["no_gd_config", "probed_exceeds_gd", "ft"])
def test_total_falls_back_to_probed(tmp_path, case):
    """total == probed (unprobed 0) when there is no GD config, when probed
    somehow exceeds GD (never negative), and always for FT packages even if
    the product has a GD configured."""
    storage = _storage(tmp_path)
    if case == "no_gd_config":
        _save(storage, _cp_data("LOT1", "W1", 7))
        lot, gd_map, n = "LOT1", None, 7
    elif case == "probed_exceeds_gd":
        _save(storage, _cp_data("LOT1", "W1", 12))  # 12 probed > GD 10
        lot, gd_map, n = "LOT1", {"P": (10, 200)}, 12
    else:
        _save(storage, _ft_data(5), product="P", category="FT", sub="FT1", src="ft.stdf")
        lot, gd_map, n = "FTLOT", {"P": (10, 200)}, 5
    conn = _conn(tmp_path, gd_map)
    row = conn.execute(
        "SELECT probed, total, unprobed FROM wafer_yield_final WHERE lot_id = ?", [lot]
    ).fetchone()
    assert row == (n, n, 0)


# ── QC-fail bin bucket ───────────────────────────────────────────────────────

def _gd_session(tmp_path, monkeypatch, gross_die_map):
    """AnalysisSession pointed at a Config carrying the given gross_die_map
    (AnalysisSession resolves gross_die_map from Config.load(), not a ctor
    arg — unlike the old Database(config, gross_die_map))."""
    from stdf_platform import analysis as analysis_pkg
    from stdf_platform.config import Config, ProductConfig

    products = {
        prod: ProductConfig(gross_die=gd, gd_fail_bin=fail_bin)
        for prod, (gd, fail_bin) in gross_die_map.items()
    }
    cfg = Config(storage=StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb"),
                 products=products)
    monkeypatch.setattr(analysis_pkg.session.Config, "load", classmethod(lambda cls, p=None: cfg))
    return analysis_pkg.AnalysisSession(tmp_path)


def test_gross_die_qc_fail_bin_bucket(tmp_path, monkeypatch):
    """Unprobed dies appear in the bin distribution under gd_fail_bin, making
    the bin total equal the gross die.

    Runs the shipped sql/04_bin/bin_pareto.sql itself rather than re-deriving
    its SQL inline — a prior version asserted on SQL copy-pasted into the test
    body, which drifted from the real query and left its gd_fail_bin UNION
    branch with no coverage anywhere. (This used to build the dbt mart of the
    same name; dbt is gone, the query lives in sql/ now.)
    """
    from stdf_platform.config import ProductConfig

    storage = _storage(tmp_path)
    data = _cp_data("LOT1", "W1", 7)  # 7 probed (bin 1), GD 10 → 3 QC
    _save(storage, data)

    with _gd_session(tmp_path, monkeypatch, {"P": (10, 200)}) as s:
        df = s.run("bin_pareto", lot="LOT1")
    bins = dict(zip(df["soft_bin"], df["die_count"]))
    assert bins[1] == 7
    assert bins[200] == 3
    assert sum(bins.values()) == 10


# ── config parsing (unchanged) ───────────────────────────────────────────────

def test_gross_die_config_load(tmp_path):
    """Config.load() parses products.gross_die and gross_die_map property."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "products:\n"
        "  PROD_A:\n"
        "    gross_die: 1234\n"
        "    gd_fail_bin: 250\n"
        "  PROD_B:\n"
        "    gross_die: 800\n"
    )
    cfg = Config.load(cfg_file)
    assert cfg.products["PROD_A"].gross_die == 1234
    assert cfg.products["PROD_A"].gd_fail_bin == 250
    assert cfg.products["PROD_B"].gross_die == 800
    assert cfg.products["PROD_B"].gd_fail_bin == 200  # default
    assert cfg.gross_die_map == {"PROD_A": (1234, 250), "PROD_B": (800, 200)}
