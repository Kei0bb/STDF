"""Retest dedup semantics of the ingest-time flags (storage._recompute_flags):
measurement time beats ingest order, a die measured twice in one file keeps its
later part, re-ingesting a file overwrites its own run, an interrupted ingest is
repaired by recover_pending(), and sub-processes of one lot are never mixed."""

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from conftest import _cp_run
from stdf_platform.analysis import AnalysisSession
from stdf_platform.config import StorageConfig
from stdf_platform.mounts import setup_views
from stdf_platform.storage import ParquetStorage


@pytest.fixture
def storage(tmp_path):
    return ParquetStorage(StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb"))


def _die(pid, x, y, passed, lot="LOT", wafer="W1"):
    return {"part_id": pid, "lot_id": lot, "wafer_id": wafer, "head_num": 1, "site_num": 1,
            "x_coord": x, "y_coord": y, "hard_bin": 1 if passed else 5,
            "soft_bin": 1 if passed else 5, "passed": passed, "test_count": 1, "test_time": 1}


def _meas(pid, value, passed, lot="LOT", wafer="W1"):
    return {"lot_id": lot, "wafer_id": wafer, "part_id": pid, "test_num": 1,
            "head_num": 1, "site_num": 1, "result": value, "passed": passed}


def _file(start, dies, lot="LOT", wafer="W1", rev="RevA"):
    """CP file whose dies are (part_id, x, y, passed); result = 1.0 pass / 0.0 fail."""
    return _cp_run(lot, wafer, "JOB", rev, start, start + 100,
                   parts=[_die(p, x, y, ok, lot, wafer) for p, x, y, ok in dies],
                   test_results=[_meas(p, 1.0 if ok else 0.0, ok, lot, wafer)
                                 for p, _, _, ok in dies])


def _save(storage, data, name, sub="CP1", category="CP"):
    storage.save_stdf_data(data, product="PROD", test_category=category,
                           sub_process=sub, source_file=name)


def _conn(tmp_path):
    conn = duckdb.connect()
    setup_views(conn, tmp_path)
    return conn


# (a) ----------------------------------------------------------------------

def test_late_arriving_older_file_does_not_win(tmp_path, storage):
    _save(storage, _file(3000, [("N0", 1, 1, True)]), "new.stdf")   # measured later
    _save(storage, _file(1000, [("O0", 1, 1, False)]), "old.stdf")  # ingested later
    conn = _conn(tmp_path)

    assert conn.execute("SELECT passed, retest_num FROM parts_final").fetchall() == [(True, 0)]
    assert conn.execute("SELECT result, retest_num FROM test_data_final").fetchall() == [(1.0, 0)]
    # the older file still exists in the raw tables, demoted
    assert conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0] == 2
    assert conn.execute(
        "SELECT retest_num, retest_flag FROM parts ORDER BY retest_num").fetchall() == [(0, 0), (1, 1)]


def test_newer_file_ingested_later_wins(tmp_path, storage):
    _save(storage, _file(1000, [("O0", 1, 1, False)]), "old.stdf")
    _save(storage, _file(3000, [("N0", 1, 1, True)]), "new.stdf")
    conn = _conn(tmp_path)
    assert conn.execute("SELECT passed, retest_num FROM parts_final").fetchall() == [(True, 1)]
    assert conn.execute("SELECT result FROM test_data_final").fetchall() == [(1.0,)]


# (b) ----------------------------------------------------------------------

def test_same_die_twice_in_one_file_keeps_later_part(tmp_path, storage):
    # P0 (first PIR) fails, P1 (later PIR) re-tests the same die and passes
    _save(storage, _file(1000, [("P0", 1, 1, False), ("P1", 1, 1, True), ("P2", 2, 2, True)]),
          "dup.stdf")
    conn = _conn(tmp_path)

    assert conn.execute(
        "SELECT part_id, passed FROM parts_final ORDER BY part_seq").fetchall() == [
        ("P1", True), ("P2", True)]
    assert conn.execute(
        "SELECT part_id, result FROM test_data_final ORDER BY part_seq").fetchall() == [
        ("P1", 1.0), ("P2", 1.0)]
    assert conn.execute(
        "SELECT COUNT(*) FROM wafer_yield_final WHERE probed = 2 AND good = 2").fetchone()[0] == 1


# (c) ----------------------------------------------------------------------

def test_same_source_file_twice_reuses_retest_number(tmp_path, storage):
    data = _file(1000, [("P0", 1, 1, True), ("P1", 2, 2, False)])
    _save(storage, data, "same.stdf")
    _save(storage, data, "same.stdf")
    conn = _conn(tmp_path)

    assert conn.execute("SELECT retest_num, source_file FROM runs").fetchall() == [(0, "same.stdf")]
    assert conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0] == 2        # not doubled
    assert conn.execute("SELECT COUNT(*) FROM test_data").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM parts_final").fetchone()[0] == 2
    # a different file still gets the next number
    _save(storage, _file(2000, [("P0", 1, 1, True)]), "other.stdf")
    assert sorted(r[0] for r in conn.execute("SELECT retest_num FROM runs").fetchall()) == [0, 1]


# (d) ----------------------------------------------------------------------

def test_recover_pending_repairs_stale_flags(tmp_path, storage):
    _save(storage, _file(1000, [("O0", 1, 1, False)]), "old.stdf")
    _save(storage, _file(3000, [("N0", 1, 1, True)]), "new.stdf")
    assert storage.recover_pending() == 0                       # nothing pending

    # simulate a crash after the files were written but before the flags were
    # recomputed: every flag stale at 0 (both dies look current)
    for table in ("parts", "test_data"):
        for f in (tmp_path / table).rglob("data.parquet"):
            t = pq.ParquetFile(f).read()
            i = t.schema.get_field_index("retest_flag")
            pq.write_table(t.set_column(i, "retest_flag", pa.array([0] * t.num_rows, pa.int64())), f)
    conn = _conn(tmp_path)
    assert conn.execute("SELECT COUNT(*) FROM parts_final").fetchone()[0] == 2  # broken

    pend = tmp_path / "_pending"
    pend.mkdir(exist_ok=True)
    (pend / "x.json").write_text(
        json.dumps(["PROD", "CP", "CP1", "LOT", "W1"]), encoding="utf-8")
    assert storage.recover_pending() == 1
    assert not list(pend.glob("*.json"))

    assert conn.execute("SELECT passed, retest_num FROM parts_final").fetchall() == [(True, 1)]
    assert conn.execute("SELECT result FROM test_data_final").fetchall() == [(1.0,)]


def test_pending_marker_is_removed_after_a_clean_ingest(tmp_path, storage):
    _save(storage, _file(1000, [("P0", 1, 1, True)]), "a.stdf")
    assert not list((tmp_path / "_pending").glob("*.json"))


def _interrupted_save(storage, data, name, monkeypatch):
    """save_stdf_data killed after writing parts/test_data, before the flags."""
    def boom(*a, **k):
        raise KeyboardInterrupt
    with monkeypatch.context() as m:
        m.setattr(storage, "_recompute_flags", boom)
        with pytest.raises(KeyboardInterrupt):
            _save(storage, data, name)


def test_interrupted_ingest_warns_in_views_and_batch_end_repairs_it(
        tmp_path, storage, monkeypatch):
    from types import SimpleNamespace

    from stdf_platform import cli, worker

    _save(storage, _file(1000, [("O0", 1, 1, False)]), "old.stdf")

    def pool(**kw):  # the batch's only file is killed mid-save (timeout)
        _interrupted_save(storage, _file(3000, [("N0", 1, 1, True)]), "new.stdf",
                          monkeypatch)
        with pytest.warns(UserWarning, match="_pending"):
            conn = _conn(tmp_path)
        assert conn.execute("SELECT COUNT(*) FROM parts_final").fetchone()[0] == 2
        return [], []

    monkeypatch.setattr(worker, "run_ingest_pool", pool)
    config = SimpleNamespace(storage=storage.config,
                             processing=SimpleNamespace(compression="zstd"))
    sync = SimpleNamespace(mark_ingested=lambda *a, **k: None, save=lambda: None)
    cli._run_ingest_batch(config, sync, [], cleanup=False, verbose=False)

    assert not list((tmp_path / "_pending").glob("*.json"))
    conn = _conn(tmp_path)  # no warning now (pytest -W error turns one into a failure)
    assert conn.execute("SELECT passed, retest_num FROM parts_final").fetchall() == [(True, 1)]


def test_ingest_lock_makes_a_second_batch_wait(storage):
    import threading

    order = []
    waited = threading.Event()
    held = storage.ingest_lock()
    held.__enter__()

    def second():
        with storage.ingest_lock(on_wait=waited.set):
            order.append("second")

    t = threading.Thread(target=second)
    t.start()
    assert waited.wait(5)
    order.append("first")
    held.__exit__(None, None, None)
    t.join(5)
    assert order == ["first", "second"]


def test_recover_pending_tolerates_a_marker_removed_meanwhile(tmp_path, storage, monkeypatch):
    _save(storage, _file(1000, [("P0", 1, 1, True)]), "a.stdf")
    pend = tmp_path / "_pending"
    (pend / "x.json").write_text(json.dumps(["PROD", "CP", "CP1", "LOT", "W1"]))
    real = storage._recompute_flags

    def and_remove(*group):
        real(*group)
        (pend / "x.json").unlink()  # another process finished it first
    monkeypatch.setattr(storage, "_recompute_flags", and_remove)
    assert storage.recover_pending() == 1


# (e) ----------------------------------------------------------------------

@pytest.fixture
def two_sub_processes(tmp_path, storage):
    """One lot/wafer tested at CP1 (1 of 2 pass) and CP2 (2 of 2 pass): the same
    dies appear in both and must not overlay each other."""
    _save(storage, _file(1000, [("A", 1, 1, True), ("B", 2, 2, False)], rev="RevA"),
          "cp1.stdf", sub="CP1")
    _save(storage, _file(2000, [("A", 1, 1, True), ("B", 2, 2, True)], rev="RevB"),
          "cp2.stdf", sub="CP2")
    return tmp_path


def test_cp1_and_cp2_of_one_lot_are_separate_yield_rows(two_sub_processes):
    conn = _conn(two_sub_processes)
    assert conn.execute("SELECT COUNT(*) FROM parts_final").fetchone()[0] == 4
    rows = conn.execute(
        "SELECT sub_process, probed, good, yield_pct FROM wafer_yield_final "
        "ORDER BY sub_process").fetchall()
    assert rows == [("CP1", 2, 1, 50.0), ("CP2", 2, 2, 100.0)]
    # each sub-process keeps its own retest numbering and flags
    assert conn.execute(
        "SELECT sub_process, retest_num FROM runs ORDER BY sub_process").fetchall() == [
        ("CP1", 0), ("CP2", 0)]


def test_named_queries_keep_sub_processes_apart(two_sub_processes):
    with AnalysisSession(two_sub_processes) as s:
        ly = s.run("lot_yield").sort_values("sub_process")
        assert list(ly["sub_process"]) == ["CP1", "CP2"]
        assert list(ly["total_parts"]) == [2, 2] and list(ly["good_parts"]) == [1, 2]

        wy = s.run("wafer_yield", lot="LOT")
        assert list(wy["sub_process"]) == ["CP1", "CP2"]
        assert list(s.run("wafer_yield", lot="LOT", sub_process="CP2")["good"]) == [2]

        fr = s.run("fail_ranking", lot="LOT")           # only CP1 has a failing test
        assert list(fr["sub_process"]) == ["CP1"] and list(fr["fail_cnt"]) == [1]

        bp = s.run("bin_pareto", lot="LOT").set_index(["sub_process", "hard_bin"])["pct"]
        assert bp[("CP1", 1)] == 50.0 and bp[("CP1", 5)] == 50.0 and bp[("CP2", 1)] == 100.0

        ex = s.run("die_test_export", lot="LOT")        # 4 dies x 1 test, no cross-join
        assert len(ex) == 4
        assert len(s.run("die_test_export", lot="LOT", sub_process="CP1")) == 2

        summary = s.lot_summary("LOT").sort_values("sub_process")
        assert list(summary["sub_process"]) == ["CP1", "CP2"]
        assert list(summary["total_parts"]) == [2, 2]
