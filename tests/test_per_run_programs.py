"""Per-run test-program metadata: `stdf db lots` mixed-TP marker,
`stdf db programs`, and AnalysisSession.runs(). See docs/schema.md (runs / lots).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest
from click.testing import CliRunner

from stdf_platform import cli
from stdf_platform.analysis import AnalysisSession
from stdf_platform.config import Config, StorageConfig
from stdf_platform.parser import STDFData
from stdf_platform.storage import ParquetStorage
from synth_data import _write_cp, _write_ft


def _patched_config(data_dir: Path) -> Config:
    return Config(storage=StorageConfig(data_dir=data_dir, database=data_dir / "stdf.duckdb"))


def _make_storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _make_cp_data(lot_id="LOT1", wafer_ids=("W1",), job_name="CP_JOB",
                   job_rev="RevA", start_time=1000, finish_time=2000) -> STDFData:
    """One CP file with one or more wafers (mirrors test_runs_table.py)."""
    data = STDFData()
    data.lot_id = lot_id
    data.part_type = "TEST"
    data.job_name = job_name
    data.job_rev = job_rev
    data.start_time = start_time
    data.finish_time = finish_time
    data.tester_type = "TESTER"
    data.operator = "OP"
    data.wafers = [
        {
            "wafer_id": wid, "head_num": 1,
            "start_time": start_time, "finish_time": finish_time,
            "part_count": 2, "good_count": 1,
            "rtst_count": 0, "abrt_count": 0,
        }
        for wid in wafer_ids
    ]
    data.parts = []
    data.tests = {1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"}}
    data.test_results = []
    for wid in wafer_ids:
        data.parts.append(
            {"part_id": f"{lot_id}_{wid}_0", "lot_id": lot_id, "wafer_id": wid,
             "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
             "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
        )
        data.test_results.append(
            {"lot_id": lot_id, "wafer_id": wid, "part_id": f"{lot_id}_{wid}_0",
             "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
        )
    return data


def _write_mixed_lot(tmp_path: Path):
    """LOT1: 2 runs (W1 RevA, W2 RevB) -> job_mixed=True, job_variant_count=2."""
    # sub_process="CP1" matches synth_data._write_cp/_write_ft's partitioning
    # convention (sub_process=CP1/FT1) so mixing these fixtures with theirs in
    # the same `runs` glob doesn't hit a Hive partition-key mismatch (an
    # omitted sub_process="" skips the partition segment entirely — see
    # storage.py _get_table_path).
    storage = _make_storage(tmp_path)
    storage.save_stdf_data(
        _make_cp_data(wafer_ids=("W1",), job_name="CP_JOB", job_rev="RevA", start_time=1000, finish_time=2000),
        product="PROD", test_category="CP", sub_process="CP1", source_file="run0.stdf",
    )
    storage.save_stdf_data(
        _make_cp_data(wafer_ids=("W2",), job_name="CP_JOB", job_rev="RevB", start_time=3000, finish_time=4000),
        product="PROD", test_category="CP", sub_process="CP1", source_file="run1.stdf",
    )



# ── CLI ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("writer, marked", [(_write_mixed_lot, True), (_write_cp, False)],
                         ids=["mixed", "clean"])
def test_cli_db_lots_marks_only_mixed_lots(tmp_path, monkeypatch, writer, marked):
    writer(tmp_path)
    monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))
    result = CliRunner().invoke(cli.main, ["db", "lots"])
    assert result.exit_code == 0, result.output
    if marked:
        assert "⚠×2" in result.output
    else:
        assert "⚠" not in result.output


def test_cli_db_programs_lists_runs_and_filters_by_lot(tmp_path, monkeypatch):
    _write_mixed_lot(tmp_path)
    _write_ft(tmp_path)
    monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))

    result = CliRunner().invoke(cli.main, ["db", "programs"])
    assert result.exit_code == 0, result.output
    for text in ("RevA", "RevB", "W1", "W2", "FT1"):
        assert text in result.output

    result = CliRunner().invoke(cli.main, ["db", "programs", "--lot", "FT1"])
    assert result.exit_code == 0, result.output
    assert "FT1" in result.output
    assert "RevA" not in result.output and "RevB" not in result.output
    assert "LOT1" not in result.output
    # FT rows have empty wafer_id -> rendered as "-"
    assert "-" in result.output


# ── AnalysisSession.runs ─────────────────────────────────────────────

def test_session_runs_filters(tmp_path):
    _write_mixed_lot(tmp_path)
    _write_ft(tmp_path)
    with AnalysisSession(tmp_path) as s:
        cp_only = s.runs(product="PROD", test_category="CP")
        assert len(cp_only) == 2
        assert set(cp_only["lot_id"]) == {"LOT1"}

        by_lot = s.runs(lot_id="FT1")
        assert len(by_lot) == 1
        assert by_lot.iloc[0]["lot_id"] == "FT1"
        assert by_lot.iloc[0]["wafer_id"] == ""

        # ordering: start_time then lot_id/wafer_id/retest_num
        all_runs = s.runs()
        assert list(all_runs["job_rev"])[:2] == ["RevA", "RevB"]  # start_time 1000 < 3000
        # SDR equipment columns are part of the runs() projection.
        for col in ("node_name", "handler_id", "probe_card_id", "loadboard_id", "socket_id"):
            assert col in all_runs.columns
