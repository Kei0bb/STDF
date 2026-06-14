"""Gross die fill: synthetic parts injected at first ingest for CP wafers."""

import duckdb
from pathlib import Path

from stdf_platform.storage import ParquetStorage
from stdf_platform.config import StorageConfig, Config
from stdf_platform.views import setup_views
from stdf_platform.parser import STDFData


def _storage(tmp_path: Path, gross_die_map=None) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg, gross_die_map=gross_die_map)


def _conn(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path)
    return conn


def _cp_data(lot_id="LOT1", wafer_id="W1", n_parts=7) -> STDFData:
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
            "lot_id": lot_id, "wafer_id": wafer_id,
            "head_num": 1, "site_num": 1,
            "x_coord": i, "y_coord": i,
            "hard_bin": 1, "soft_bin": 1,
            "passed": True, "test_count": 1, "test_time": 100,
        }
        for i in range(n_parts)
    ]
    data.tests = {}
    data.test_results = []
    return data


def test_gross_die_fill_first_ingest(tmp_path):
    """Fill dies are visible in parts_final at the gross die count."""
    gross_die = 10
    n_probed = 7
    gd_fail_bin = 200
    storage = _storage(tmp_path, gross_die_map={"PROD": (gross_die, gd_fail_bin)})
    data = _cp_data("LOT1", "W1", n_probed)
    storage.save_stdf_data(data, product="PROD", test_category="CP",
                           sub_process="CP1", source_file="test.stdf")

    conn = _conn(tmp_path)
    total = conn.execute(
        "SELECT COUNT(*) FROM parts_final WHERE lot_id = 'LOT1'"
    ).fetchone()[0]
    assert total == gross_die, f"Expected {gross_die} total, got {total}"

    fill_count = conn.execute(
        "SELECT COUNT(*) FROM parts_final WHERE lot_id = 'LOT1'"
        " AND part_txt LIKE '__GDFILL_%'"
    ).fetchone()[0]
    assert fill_count == gross_die - n_probed

    bad_bin = conn.execute(
        "SELECT COUNT(*) FROM parts_final WHERE lot_id = 'LOT1'"
        " AND part_txt LIKE '__GDFILL_%' AND soft_bin != $1",
        [gd_fail_bin],
    ).fetchone()[0]
    assert bad_bin == 0

    passed_fill = conn.execute(
        "SELECT COUNT(*) FROM parts_final WHERE lot_id = 'LOT1'"
        " AND part_txt LIKE '__GDFILL_%' AND passed = true"
    ).fetchone()[0]
    assert passed_fill == 0


def test_gross_die_fill_not_added_on_retest(tmp_path):
    """Retest (retest_num > 0) does not add new fill dies.

    The dedup view keeps fill dies from retest 0 since part_txt is unique
    per fill slot and retest 1 (selective retest) does not overwrite them.
    Total in parts_final stays at gross_die.
    """
    gross_die = 10
    n_probed = 7
    storage = _storage(tmp_path, gross_die_map={"PROD": (gross_die, 200)})

    # First ingest
    storage.save_stdf_data(_cp_data("LOT1", "W1", n_probed),
                           product="PROD", test_category="CP",
                           sub_process="CP1", source_file="f1.stdf")
    # Second ingest (retest) — same lot/wafer, fewer dies (selective retest)
    storage.save_stdf_data(_cp_data("LOT1", "W1", 3),
                           product="PROD", test_category="CP",
                           sub_process="CP1", source_file="f2.stdf")

    conn = _conn(tmp_path)
    total = conn.execute(
        "SELECT COUNT(*) FROM parts_final WHERE lot_id = 'LOT1'"
    ).fetchone()[0]
    # Fill dies from retest 0 + latest-retest results for probed dies = gross_die
    assert total == gross_die


def test_no_gross_die_config_no_fill(tmp_path):
    """Without gross_die config, no fill dies are added."""
    storage = _storage(tmp_path, gross_die_map=None)
    storage.save_stdf_data(_cp_data("LOT1", "W1", 7),
                           product="PROD", test_category="CP",
                           sub_process="CP1", source_file="test.stdf")

    conn = _conn(tmp_path)
    total = conn.execute(
        "SELECT COUNT(*) FROM parts_final WHERE lot_id = 'LOT1'"
    ).fetchone()[0]
    assert total == 7


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

    gd_map = cfg.gross_die_map
    assert gd_map == {"PROD_A": (1234, 250), "PROD_B": (800, 200)}


def test_gross_die_ft_not_filled(tmp_path):
    """FT parts (wafer_id='') are never filled — gross die is a CP concept."""
    gross_die = 10
    storage = _storage(tmp_path, gross_die_map={"PROD": (gross_die, 200)})

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
    storage.save_stdf_data(data, product="PROD", test_category="FT",
                           sub_process="FT1", source_file="ft.stdf")

    conn = _conn(tmp_path)
    total = conn.execute(
        "SELECT COUNT(*) FROM parts_final WHERE lot_id = 'FTLOT'"
    ).fetchone()[0]
    assert total == 5  # no fill for FT
