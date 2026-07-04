"""test_data_of macro: exact equivalence with test_data_final + fast-path shape.

The macro pre-filters by the set of matching test_num (a PARTITION BY column)
so dedup groups are kept or dropped whole. The critical edge case is a test
renamed across retests under the same test_num: naive pre-filtering by
test_name would resurface the superseded old-name row.
"""

import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stdf_platform.views import setup_views
from synth_data import _write_cp


def _write_renamed_retest(data_dir: Path):
    """One die measured twice; test 2002 renamed IDDQ_OLD → LEAK_NEW at retest 1."""
    base = (data_dir / "test_data" / "product=P" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=RLOT" / "wafer_id=W1")

    def w(retest, names, results):
        p = base / f"retest={retest}" / "data.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({
            "lot_id": ["RLOT"] * 3, "wafer_id": ["W1"] * 3, "part_txt": [""] * 3,
            "x_coord": [0] * 3, "y_coord": [0] * 3,
            "test_num": [1001, 2002, 3003], "pin_num": [0] * 3,
            "test_name": names, "result": results, "retest_num": [retest] * 3,
        }), p)

    w(0, ["IDDQ_A", "IDDQ_OLD", "Vth"], [1.0, 5.0, 0.5])
    w(1, ["IDDQ_A", "LEAK_NEW", "Vth"], [2.0, 6.0, 0.6])


def _conn(data_dir: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    setup_views(conn, data_dir)
    return conn


def test_macro_registered(tmp_path):
    _write_cp(tmp_path)
    conn = duckdb.connect(":memory:")
    assert "test_data_of" in setup_views(conn, tmp_path)


def test_macro_equals_view_without_filter(tmp_path):
    _write_cp(tmp_path)
    conn = _conn(tmp_path)
    view = conn.execute(
        "SELECT * FROM test_data_final WHERE lot_id='LOT1' ORDER BY test_num, wafer_id, x_coord"
    ).fetchall()
    macro = conn.execute(
        "SELECT * FROM test_data_of('LOT1') ORDER BY test_num, wafer_id, x_coord"
    ).fetchall()
    assert macro == view


def test_macro_equals_view_with_name_filter(tmp_path):
    _write_renamed_retest(tmp_path)
    conn = _conn(tmp_path)
    view = conn.execute("""
        SELECT test_num, test_name, result, retest_num FROM test_data_final
        WHERE lot_id='RLOT' AND test_name LIKE '%IDDQ%' ORDER BY test_num
    """).fetchall()
    macro = conn.execute("""
        SELECT test_num, test_name, result, retest_num
        FROM test_data_of('RLOT', name_like := '%IDDQ%') ORDER BY test_num
    """).fetchall()
    assert macro == view
    # the renamed test's retest-0 row must NOT resurface
    assert view == [(1001, "IDDQ_A", 2.0, 1)]


def test_macro_renamed_test_visible_under_new_name(tmp_path):
    _write_renamed_retest(tmp_path)
    conn = _conn(tmp_path)
    rows = conn.execute(
        "SELECT test_num, test_name, retest_num "
        "FROM test_data_of('RLOT', name_like := '%LEAK%')"
    ).fetchall()
    assert rows == [(2002, "LEAK_NEW", 1)]


def test_macro_filter_sits_below_window(tmp_path):
    """The semi join must feed the WINDOW, not follow it — that IS the fix."""
    _write_cp(tmp_path)
    conn = _conn(tmp_path)
    plan = "\n".join(
        r[1] for r in conn.execute(
            "EXPLAIN SELECT * FROM test_data_of('LOT1', name_like := '%Vth%')"
        ).fetchall()
    )
    assert "WINDOW" in plan
    assert plan.index("WINDOW") < plan.index("SEMI")
