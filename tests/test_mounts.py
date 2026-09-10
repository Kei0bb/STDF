"""Tests for the single-source DuckDB view module (stdf_platform.mounts)."""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from stdf_platform.mounts import setup_views


def _write_parts(data_dir: Path):
    path = (
        data_dir / "parts" / "product=PROD" / "test_category=CP"
        / "sub_process=" / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([
        ("lot_id", pa.string()), ("wafer_id", pa.string()), ("part_txt", pa.string()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("soft_bin", pa.int64()), ("passed", pa.bool_()), ("retest_num", pa.int64()),
    ])
    table = pa.table({
        "lot_id": ["LOT1", "LOT1"], "wafer_id": ["W1", "W1"], "part_txt": ["", ""],
        "x_coord": [1, 2], "y_coord": [1, 2], "soft_bin": [1, 0],
        "passed": [True, False], "retest_num": [0, 0],
    }, schema=schema)
    pq.write_table(table, path)


def _write_two_retests_same_die(data_dir: Path):
    """Same physical CP die (W1, x=1, y=1) probed twice with DIFFERENT part_txt
    serials — must collapse to one row in parts_final."""
    schema = pa.schema([
        ("lot_id", pa.string()), ("wafer_id", pa.string()), ("part_txt", pa.string()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("soft_bin", pa.int64()), ("passed", pa.bool_()), ("retest_num", pa.int64()),
    ])
    for retest, sn, passed in [(0, "SN-A", False), (1, "SN-B", True)]:
        path = (
            data_dir / "parts" / "product=PROD" / "test_category=CP"
            / "sub_process=" / "lot_id=LOT1" / "wafer_id=W1"
            / f"retest={retest}" / "data.parquet"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({
            "lot_id": ["LOT1"], "wafer_id": ["W1"], "part_txt": [sn],
            "x_coord": [1], "y_coord": [1], "soft_bin": [1],
            "passed": [passed], "retest_num": [retest],
        }, schema=schema), path)


def test_dedup_ignores_part_txt_for_cp_dies(tmp_path):
    """CP die identity is (wafer, x, y); a per-part part_txt must not defeat
    retest dedup. The latest retest (retest_num DESC) wins."""
    _write_two_retests_same_die(tmp_path)
    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path)
    rows = conn.execute(
        "SELECT COUNT(*) AS n, BOOL_OR(passed) AS passed"
        " FROM parts_final WHERE lot_id='LOT1'"
    ).fetchone()
    assert rows[0] == 1          # collapsed to one die, not summed
    assert rows[1] is True       # kept the latest retest (passing) result


def test_setup_views_registers_base_and_final(tmp_path):
    _write_parts(tmp_path)
    conn = duckdb.connect(":memory:")
    registered = setup_views(conn, tmp_path)
    assert "parts" in registered
    assert "parts_final" in registered
    n = conn.execute("SELECT COUNT(*) FROM parts_final WHERE lot_id='LOT1'").fetchone()[0]
    assert n == 2


def test_setup_views_raises_on_empty_dir(tmp_path):
    """Was: an empty data_dir returned []. That silence is exactly how a
    mistyped/cwd-relative data_dir went unnoticed until a later Catalog
    Error, so it now raises instead."""
    import pytest
    conn = duckdb.connect(":memory:")
    with pytest.raises(RuntimeError, match="No STDF store found"):
        setup_views(conn, tmp_path)


def test_marts_are_mounted(tmp_path):
    import pyarrow as pa, pyarrow.parquet as pq
    marts = tmp_path / "marts"
    marts.mkdir()
    pq.write_table(pa.table({"lot_id": ["L1"], "yield_pct": [99.0]}),
                   marts / "lot_yield_summary.parquet")
    conn = duckdb.connect(":memory:")
    registered = setup_views(conn, tmp_path)
    assert "lot_yield_summary" in registered
    assert conn.execute("SELECT yield_pct FROM lot_yield_summary").fetchone()[0] == 99.0


def test_mart_name_colliding_with_core_table_or_gross_die_is_skipped(tmp_path):
    """A mart file named after an already-registered canonical view (e.g.
    "parts") must not CREATE OR REPLACE that view — last-registration-wins
    would silently mask real data. A mart named "gross_die" must not raise
    (gross_die is a TABLE, not a VIEW; CREATE OR REPLACE VIEW over a table
    name raises duckdb.CatalogException)."""
    _write_parts(tmp_path)
    marts = tmp_path / "marts"
    marts.mkdir()
    # Colliding mart names: a fake "parts" mart with different data, and a
    # fake "gross_die" mart (gross_die is always created as a TABLE above).
    pq.write_table(pa.table({"lot_id": ["FAKE"]}), marts / "parts.parquet")
    pq.write_table(pa.table({"lot_id": ["FAKE"]}), marts / "gross_die.parquet")

    conn = duckdb.connect(":memory:")
    registered = setup_views(conn, tmp_path)  # must not raise

    # "parts" view still serves the real table, not the mart file.
    rows = conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0]
    assert rows == 2  # from _write_parts, not the 1-row FAKE mart

    # Neither collision name was double-registered.
    assert registered.count("parts") == 1
    assert registered.count("gross_die") == 0  # gross_die table is never
    # added to `registered` in the first place (see setup_views); the mart
    # of the same name must not add it either.


def test_setup_views_error_names_the_resolved_path(tmp_path):
    import pytest
    conn = duckdb.connect(":memory:")
    with pytest.raises(RuntimeError, match=str(tmp_path.resolve())):
        setup_views(conn, tmp_path)


def test_setup_views_still_works_with_only_one_table(tmp_path):
    """The guard must not reject a partially-populated store (a CP-only
    product has no chipid/, a fresh store may have only parts/)."""
    _write_parts(tmp_path)
    conn = duckdb.connect(":memory:")
    registered = setup_views(conn, tmp_path)
    assert "parts" in registered
