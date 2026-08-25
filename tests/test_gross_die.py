"""Gross die: applied at QUERY time as the CP yield denominator.

No synthetic rows are written to Parquet. setup_views(gross_die_map=...) builds
the gross_die table + wafer_yield_final view; CP wafer total = max(probed, GD),
unprobed = total - probed sits in the denominator (QC fail). Robust to retests
and partial/aborted probes.
"""

import duckdb
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


def _cp_data(lot_id="LOT1", wafer_id="W1", n_parts=7, xy_start=0,
             part_txt=False) -> STDFData:
    """Build a synthetic CP STDFData.

    Dies are placed at (i, i) for i in [xy_start, xy_start+n_parts) so separate
    ingests can simulate an aborted probe (run 0) continued by a retest (run 1)
    that probes a *different* set of coordinates.

    part_txt=True populates a per-part-unique PRR.PART_TXT (serial / 2D barcode),
    which real CP testers may emit. Die identity must remain (wafer, x, y) so
    such part_txt values must NOT defeat retest dedup.
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
            "part_txt": f"SN-{lot_id}-{wafer_id}-{i:04d}" if part_txt else "",
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


def _wafer_total(conn, lot_id="LOT1"):
    return conn.execute(
        "SELECT total FROM wafer_yield_final WHERE lot_id = ?", [lot_id]
    ).fetchone()[0]


# ── yield denominator = gross die ────────────────────────────────────────────

def test_gross_die_wafer_total_is_gd(tmp_path):
    """CP wafer total uses the gross-die denominator when probed < GD."""
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 7))  # 7 probed
    conn = _conn(tmp_path, {"P": (10, 200)})

    row = conn.execute(
        "SELECT probed, total, unprobed FROM wafer_yield_final WHERE lot_id='LOT1'"
    ).fetchone()
    assert row == (7, 10, 3)


def test_gross_die_robust_to_selective_retest(tmp_path):
    """Full probe + a selective retest must NOT inflate the total past GD."""
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 7), src="f1.stdf")
    _save(storage, _cp_data("LOT1", "W1", 3), src="f2.stdf")  # retest subset
    conn = _conn(tmp_path, {"P": (10, 200)})
    assert _wafer_total(conn) == 10


def test_gross_die_aborted_probe_then_retest(tmp_path):
    """The scenario the user asked about: CP test stops partway, then the wafer
    is retested. Dies probed in the continuation must be counted once (not as
    QC fail), and only the genuinely-never-probed dies inflate the denominator.

    GD=12; run0 probes dies 0..5 (abort), run1 probes dies 6..9 (continuation).
    10 distinct dies probed → total=12, unprobed=2 (NOT 6 phantom QC fails).
    """
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 6, xy_start=0), src="run0.stdf")
    _save(storage, _cp_data("LOT1", "W1", 4, xy_start=6), src="run1.stdf")
    conn = _conn(tmp_path, {"P": (12, 200)})

    row = conn.execute(
        "SELECT probed, total, unprobed FROM wafer_yield_final WHERE lot_id='LOT1'"
    ).fetchone()
    assert row == (10, 12, 2)


def test_gross_die_part_txt_does_not_inflate(tmp_path):
    """Serials on CP parts must not defeat dedup → total stays at GD."""
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 7, part_txt=True), src="f1.stdf")
    _save(storage, _cp_data("LOT1", "W1", 3, part_txt=True), src="f2.stdf")
    conn = _conn(tmp_path, {"P": (10, 200)})
    assert _wafer_total(conn) == 10


def test_no_gross_die_config_falls_back_to_probed(tmp_path):
    """Without gross_die config, total == probed (no denominator change)."""
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 7))
    conn = _conn(tmp_path, None)
    row = conn.execute(
        "SELECT probed, total, unprobed FROM wafer_yield_final WHERE lot_id='LOT1'"
    ).fetchone()
    assert row == (7, 7, 0)


def test_gross_die_probed_exceeds_gd_no_negative(tmp_path):
    """If probed somehow exceeds GD, total = probed (unprobed never negative)."""
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 12))  # 12 probed > GD 10
    conn = _conn(tmp_path, {"P": (10, 200)})
    row = conn.execute(
        "SELECT probed, total, unprobed FROM wafer_yield_final WHERE lot_id='LOT1'"
    ).fetchone()
    assert row == (12, 12, 0)


def test_gross_die_ft_not_applied(tmp_path):
    """FT packages (wafer_id='') never get the gross-die denominator."""
    storage = _storage(tmp_path)
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
        for i in range(5)
    ]
    data.tests = {}
    data.test_results = []
    _save(storage, data, product="P", category="FT", sub="FT1", src="ft.stdf")
    # GD configured for product P, but FT must ignore it.
    conn = _conn(tmp_path, {"P": (10, 200)})
    row = conn.execute(
        "SELECT probed, total, unprobed FROM wafer_yield_final WHERE lot_id='FTLOT'"
    ).fetchone()
    assert row == (5, 5, 0)


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


def test_gross_die_database_wafer_yield(tmp_path, monkeypatch):
    """`stdf db query` over wafer_yield_final: GD applies to the CP total."""
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 8))  # 8 probed, all pass; GD 10

    with _gd_session(tmp_path, monkeypatch, {"P": (10, 200)}) as s:
        df = s.q(
            "SELECT wafer_id, total, good, yield_pct FROM wafer_yield_final "
            "WHERE lot_id = ? ORDER BY wafer_id",
            ["LOT1"],
        )
    assert len(df) == 1
    assert df.iloc[0]["total"] == 10           # GD denominator, not 8 probed
    assert df.iloc[0]["good"] == 8
    assert df.iloc[0]["yield_pct"] == 80.0     # 8 / 10


def test_gross_die_qc_fail_bin_bucket(tmp_path, monkeypatch):
    """Unprobed dies appear in the bin distribution under gd_fail_bin, making
    the bin total equal the gross die."""
    storage = _storage(tmp_path)
    _save(storage, _cp_data("LOT1", "W1", 7))  # 7 probed (bin 1), GD 10 → 3 QC

    with _gd_session(tmp_path, monkeypatch, {"P": (10, 200)}) as s:
        df = s.q(
            """
            WITH binned AS (
                SELECT soft_bin, COUNT(*) AS count
                FROM parts_final
                WHERE lot_id = ?
                GROUP BY soft_bin
                UNION ALL
                SELECT gd_fail_bin AS soft_bin, SUM(unprobed) AS count
                FROM wafer_yield_final
                WHERE lot_id = ? AND gd_fail_bin IS NOT NULL AND unprobed > 0
                GROUP BY gd_fail_bin
            )
            SELECT soft_bin, SUM(count) AS count,
                   ROUND(100.0 * SUM(count) / SUM(SUM(count)) OVER (), 2) AS pct
            FROM binned
            GROUP BY soft_bin
            ORDER BY soft_bin
            """,
            ["LOT1", "LOT1"],
        )
    bins = dict(zip(df["soft_bin"], df["count"]))
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
