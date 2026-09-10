from pathlib import Path

import pytest

from stdf_platform.analysis import AnalysisSession
from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
from stdf_platform.storage import ParquetStorage


def _cp_data(lot="LOT", wafer="W1", parts=None, results=None) -> STDFData:
    data = STDFData()
    data.lot_id = lot
    data.part_type = "T"
    data.job_name = "JOB"
    data.job_rev = "A"
    data.tester_type = "T"
    data.operator = "OP"
    data.wafers = [{"wafer_id": wafer, "head_num": 1, "start_time": 0,
                    "finish_time": 0, "part_count": len(parts or []),
                    "good_count": 0, "rtst_count": 0, "abrt_count": 0}]
    data.parts = parts or []
    data.tests = {100: {"test_name": "T100", "rec_type": "PTR",
                        "lo_limit": 0.0, "hi_limit": 1.0, "units": "V"}}
    data.test_results = results or []
    return data


def _part(pid, x, y, passed, bin_):
    return {"part_id": pid, "part_txt": "", "part_serial": "",
            "lot_id": "LOT", "wafer_id": "W1", "head_num": 1, "site_num": 1,
            "x_coord": x, "y_coord": y, "hard_bin": bin_, "soft_bin": bin_,
            "passed": passed, "test_count": 1, "test_time": 0}


def _result(pid, x, y, passed):
    return {"lot_id": "LOT", "wafer_id": "W1", "part_id": pid, "test_num": 100,
            "head_num": 1, "site_num": 1, "result": 0.5,
            "passed": passed, "alarm_id": ""}


@pytest.fixture()
def partial_retest_store(tmp_path):
    storage = ParquetStorage(StorageConfig(data_dir=tmp_path))
    # retest=0: (0,0) fail / (1,0) pass。part_id はファイル内連番
    storage.save_stdf_data(
        _cp_data(parts=[_part("LOT_W1_0", 0, 0, False, 3),
                        _part("LOT_W1_1", 1, 0, True, 1)],
                 results=[_result("LOT_W1_0", 0, 0, False),
                          _result("LOT_W1_1", 1, 0, True)]),
        product="PROD", test_category="CP", sub_process="CP1",
        source_file="r0.stdf")
    # retest=1: (1,0) のみ再測定 → 連番が 0 に戻り part_id が衝突
    storage.save_stdf_data(
        _cp_data(parts=[_part("LOT_W1_0", 1, 0, True, 1)],
                 results=[_result("LOT_W1_0", 1, 0, True)]),
        product="PROD", test_category="CP", sub_process="CP1",
        source_file="r1.stdf")
    return tmp_path


def test_die_test_export_not_duplicated_by_partial_retest(partial_retest_store):
    s = AnalysisSession(partial_retest_store)
    df = s.run("05_export/die_test_export", lot="LOT")
    assert len(df) == 2
    assert set(zip(df.x_coord, df.y_coord)) == {(0, 0), (1, 0)}
    assert df.loc[df.x_coord == 0, "soft_bin"].tolist() == [3]


def test_bin_fail_tests_maps_fail_to_correct_die(partial_retest_store):
    s = AnalysisSession(partial_retest_store)
    df = s.run("04_bin/bin_fail_tests", lot="LOT")
    assert len(df) == 1
    assert (df.iloc[0]["hard_bin"], df.iloc[0]["soft_bin"]) == (3, 3)
    assert df.iloc[0]["fail_count"] == 1


@pytest.fixture()
def partial_retest_two_tests_store(tmp_path):
    """part_id が衝突する部分リテスト + テスト2項目。旧実装は pivot を
    part_id で行うため2ダイの測定値が平均で1行に潰れ、相関が NaN になる。"""
    storage = ParquetStorage(StorageConfig(data_dir=tmp_path))

    def data(parts, results):
        d = _cp_data(parts=parts, results=results)
        d.tests[101] = {"test_name": "T101", "rec_type": "PTR",
                        "lo_limit": 0.0, "hi_limit": 1.0, "units": "V"}
        return d

    def result(pid, x, test, val):
        return {"lot_id": "LOT", "wafer_id": "W1", "part_id": pid,
                "test_num": test, "head_num": 1, "site_num": 1,
                "result": val, "passed": True, "alarm_id": ""}

    storage.save_stdf_data(
        data([_part("LOT_W1_0", 0, 0, True, 1), _part("LOT_W1_1", 1, 0, True, 1)],
             [result("LOT_W1_0", 0, 100, 0.1), result("LOT_W1_0", 0, 101, 0.1),
              result("LOT_W1_1", 1, 100, 0.9), result("LOT_W1_1", 1, 101, 0.9)]),
        product="PROD", test_category="CP", sub_process="CP1", source_file="r0.stdf")
    # (1,0) のみ再測定 → part_id "LOT_W1_0" が旧 (0,0) と衝突
    storage.save_stdf_data(
        data([_part("LOT_W1_0", 1, 0, True, 1)],
             [result("LOT_W1_0", 1, 100, 0.9), result("LOT_W1_0", 1, 101, 0.9)]),
        product="PROD", test_category="CP", sub_process="CP1", source_file="r1.stdf")
    return tmp_path


def test_test_correlation_does_not_merge_colliding_dies(partial_retest_two_tests_store):
    from stdf_platform.analysis.correlation import test_correlation
    s = AnalysisSession(partial_retest_two_tests_store)
    corr = test_correlation(s, "PROD", "LOT", "CP", [100, 101])
    # 2ダイ × 2テスト。die_key で分離できていれば各ダイ内で完全相関する
    assert corr.shape == (2, 2)
    assert corr.loc[100, 101] == pytest.approx(1.0)


def test_radial_profile_does_not_duplicate_on_partial_retest(partial_retest_store):
    from stdf_platform.analysis.spatial import radial_profile
    s = AnalysisSession(partial_retest_store)
    df = radial_profile(s, "PROD", "LOT", 100)
    assert df["n"].sum() == 2
