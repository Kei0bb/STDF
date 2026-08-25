"""Numeric, hand-checked verification of the 5 analysis marts.

Every assertion below is computed by hand against tests/conftest.py's
`synth_store` fixture (see its docstring for the full data layout); the
comment above each assertion states the computation rationale.
"""
import math

import duckdb

from test_dbt_staging_parity import _run_dbt


def _mart(tmp_path, name):
    return (tmp_path / "marts" / f"{name}.parquet").as_posix()


def test_lot_yield_summary_values(tmp_path, synth_store):
    # LOT1/W1 after dedup: die(1,1) retest-1 passes, die(2,2) passes,
    # die(3,3) fails (never retested) -> 3 dies total, 2 good -> yield
    # 100.0*2/3 = 66.67. wafer_count=1 (one real wafer W1).
    _run_dbt(synth_store, tmp_path / "b.duckdb", tmp_path, select="staging marts")
    con = duckdb.connect(":memory:")
    row = con.execute(
        "SELECT wafer_count, total_parts, good_parts, yield_pct FROM "
        f"read_parquet('{_mart(tmp_path, 'lot_yield_summary')}') WHERE lot_id='LOT1'"
    ).fetchone()
    assert row == (1, 3, 2, 66.67)

    # TP-mixed case: LOT1 has 2 runs with different (job_name, job_rev)
    # (run0: CP_JOB/RevA, run1: CP_JOB/RevB) -> job_variant_count=2,
    # job_mixed=true. job_name/job_rev themselves are the *latest* run's
    # values (arg_max by start_time) -> run1's CP_JOB/RevB.
    row = con.execute(
        "SELECT job_name, job_rev, job_variant_count, job_mixed FROM "
        f"read_parquet('{_mart(tmp_path, 'lot_yield_summary')}') WHERE lot_id='LOT1'"
    ).fetchone()
    assert row == ("CP_JOB", "RevB", 2, True)

    # FT case: FTLOT1 has wafer_id='' only (no WIR) -> wafer_count=0, but
    # total/good are still probed-based (2 packages, 1 pass) -> yield 50.0.
    row = con.execute(
        "SELECT wafer_count, total_parts, good_parts, yield_pct FROM "
        f"read_parquet('{_mart(tmp_path, 'lot_yield_summary')}') WHERE lot_id='FTLOT1'"
    ).fetchone()
    assert row == (0, 2, 1, 50.0)


def test_fail_ranking_values(tmp_path, synth_store):
    # test_num=50 (FAIL_TEST) is measured only in run0, for die(1,1) (fails)
    # and die(2,2) (passes); neither die is re-measured on this test_num in
    # run1, so both rows keep retest_flag=0 from run0 -> after dedup:
    # total=2, fails=1 (die(1,1)), fail_pct = 100.0*1/2 = 50.0.
    _run_dbt(synth_store, tmp_path / "b.duckdb", tmp_path, select="staging marts")
    con = duckdb.connect(":memory:")
    row = con.execute(
        "SELECT total, fails, fail_pct FROM "
        f"read_parquet('{_mart(tmp_path, 'fail_ranking')}') "
        "WHERE lot_id='LOT1' AND test_num=50"
    ).fetchone()
    assert row == (2, 1, 50.0)

    # test_num=1 (VCC) ends up with 0 fails after dedup (die(1,1)'s retest
    # passes) -> HAVING fails>0 excludes it entirely from fail_ranking.
    row = con.execute(
        "SELECT COUNT(*) FROM "
        f"read_parquet('{_mart(tmp_path, 'fail_ranking')}') "
        "WHERE lot_id='LOT1' AND test_num=1"
    ).fetchone()
    assert row == (0,)


def test_cpk_stats_values(tmp_path, synth_store):
    # test_num=100 (CPK_TEST, lo=0, hi=10), measured only in run0:
    # results = [4.0, 6.0] -> n=2, mean=5.0,
    # sample stddev = sqrt(((4-5)^2 + (6-5)^2) / (2-1)) = sqrt(2),
    # cp   = (10-0) / (6*sqrt(2)),
    # cpk  = min((10-5)/(3*sqrt(2)), (5-0)/(3*sqrt(2))) = 5/(3*sqrt(2))
    #        (both branches equal since mean is centered between the limits).
    _run_dbt(synth_store, tmp_path / "b.duckdb", tmp_path, select="staging marts")
    con = duckdb.connect(":memory:")
    row = con.execute(
        "SELECT n, mean, sigma, cp, cpk FROM "
        f"read_parquet('{_mart(tmp_path, 'cpk_stats')}') "
        "WHERE lot_id='LOT1' AND test_num=100"
    ).fetchone()
    n, mean, sigma, cp, cpk = row
    assert n == 2
    assert mean == 5.0
    assert abs(sigma - math.sqrt(2)) < 1e-9
    assert abs(cp - (10.0 / (6 * math.sqrt(2)))) < 1e-9
    assert abs(cpk - (5.0 / (3 * math.sqrt(2)))) < 1e-9


def test_bin_pareto_values(tmp_path, synth_store):
    # LOT1 parts_final after dedup: die(1,1) hard_bin=1/soft_bin=1 (retest
    # pass), die(2,2) hard_bin=1/soft_bin=1 (never retested), die(3,3)
    # hard_bin=5/soft_bin=5 (fails, never retested). No gross_die_map is
    # configured for this test -> no gd_fail_bin rows.
    # bin(1,1): count=2, pct=100.0*2/3=66.67. bin(5,5): count=1, pct=33.33.
    _run_dbt(synth_store, tmp_path / "b.duckdb", tmp_path, select="staging marts")
    con = duckdb.connect(":memory:")
    rows = con.execute(
        "SELECT hard_bin, soft_bin, count, pct FROM "
        f"read_parquet('{_mart(tmp_path, 'bin_pareto')}') "
        "WHERE lot_id='LOT1' ORDER BY hard_bin"
    ).fetchall()
    assert rows == [(1, 1, 2, 66.67), (5, 5, 1, 33.33)]


def test_bin_fail_tests_values(tmp_path, synth_store):
    # Only die(3,3) is passed=FALSE in parts_final after dedup (die(1,1)'s
    # retest passes; die(2,2) always passed). die(3,3)'s only test_data row
    # is test_num=75 (BIN_FAIL_TEST), passed='F' -> a single joined row:
    # (hard_bin=5, soft_bin=5, test_num=75, fail_count=1).
    _run_dbt(synth_store, tmp_path / "b.duckdb", tmp_path, select="staging marts")
    con = duckdb.connect(":memory:")
    rows = con.execute(
        "SELECT hard_bin, soft_bin, test_num, test_name, fail_count FROM "
        f"read_parquet('{_mart(tmp_path, 'bin_fail_tests')}') "
        "WHERE lot_id='LOT1'"
    ).fetchall()
    assert rows == [(5, 5, 75, "BIN_FAIL_TEST", 1)]
