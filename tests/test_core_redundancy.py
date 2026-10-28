"""workspace/_sample_core_redundancy.py(SQL はノートブック内)。

CP1 のダイごとの PSDN/HNS 判定値を、再検の最新結果でロット別に数える。
手計算の期待値で検証する(判定値が無いダイ、再検で値が変わるダイ、fail でも判定値が
0 のダイを含む)。
"""

import importlib.util
import time
from pathlib import Path

import pytest
from conftest import _cp_run

from stdf_platform.config import StorageConfig
from stdf_platform.storage import ParquetStorage

NOW = int(time.time())
DAY = 86400
PSDN, HNS = "PASS2_JUDGE_PSDN_CPLT", "PASS2_JUDGE_HNS_CPLT"
TESTS = {
    10: {"test_name": PSDN, "rec_type": "PTR", "lo_limit": None, "hi_limit": None, "units": ""},
    11: {"test_name": HNS, "rec_type": "PTR", "lo_limit": None, "hi_limit": None, "units": ""},
}


def _die(lot, i, passed):
    return {"part_id": f"{lot}_{i}", "lot_id": lot, "wafer_id": "W1", "head_num": 1,
            "site_num": 1, "x_coord": i, "y_coord": 0,
            "hard_bin": 1 if passed else 5, "soft_bin": 1 if passed else 5,
            "passed": passed, "test_count": 2, "test_time": 10}


def _judge(lot, i, psdn, hns):
    row = {"lot_id": lot, "wafer_id": "W1", "part_id": f"{lot}_{i}", "head_num": 1, "site_num": 1}
    return [{**row, "test_num": 10, "result": psdn, "passed": True},
            {**row, "test_num": 11, "result": hns, "passed": True}]


@pytest.fixture
def redundancy_store(tmp_path):
    st = ParquetStorage(StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb"))

    def ingest(data, name, sub="CP1", product="PROD"):
        st.save_stdf_data(data, product=product, test_category="CP", sub_process=sub,
                          source_file=name)

    # LOTA: d1 両方 0 / d2 (2,6) / d3 初回は (7,0) で fail / d4 判定値なし
    ingest(_cp_run(
        "LOTA", "W1", "JOB", "Rev", NOW - 30 * DAY, NOW - 30 * DAY + 100,
        parts=[_die("LOTA", 1, True), _die("LOTA", 2, True),
               _die("LOTA", 3, False), _die("LOTA", 4, True)],
        test_results=(_judge("LOTA", 1, 0, 0) + _judge("LOTA", 2, 2, 6)
                      + _judge("LOTA", 3, 7, 0)),
        tests=TESTS), "a0.stdf")
    # LOTA 再検: d3 を測り直して pass、判定値も (0,0) になる → 最新結果が効く
    ingest(_cp_run(
        "LOTA", "W1", "JOB", "Rev", NOW - 20 * DAY, NOW - 20 * DAY + 100,
        parts=[_die("LOTA", 3, True)],
        test_results=_judge("LOTA", 3, 0, 0), tests=TESTS), "a1.stdf")
    # LOTB: e1 は pass だが HNS=7 / e2 は fail だが両方 0
    ingest(_cp_run(
        "LOTB", "W1", "JOB", "Rev", NOW - 10 * DAY, NOW - 10 * DAY + 100,
        parts=[_die("LOTB", 1, True), _die("LOTB", 2, False)],
        test_results=_judge("LOTB", 1, 6, 7) + _judge("LOTB", 2, 0, 0),
        tests=TESTS), "b0.stdf")
    # 4 か月前に始まったロット: 既定の 3 か月には入らない
    ingest(_cp_run(
        "LOTOLD", "W1", "JOB", "Rev", NOW - 120 * DAY, NOW - 120 * DAY + 100,
        parts=[_die("LOTOLD", 1, True)], test_results=_judge("LOTOLD", 1, 0, 0),
        tests=TESTS), "old0.stdf")
    # 別工程(CP2)のロットは CP1 の集計に混ざらない
    ingest(_cp_run(
        "LOTC", "W1", "JOB", "Rev", NOW - 5 * DAY, NOW - 5 * DAY + 100,
        parts=[_die("LOTC", 1, True)], test_results=_judge("LOTC", 1, 0, 0),
        tests=TESTS), "c0.stdf", sub="CP2")
    return tmp_path



def _load_notebook(store, tmp_path, monkeypatch):
    pytest.importorskip("marimo")
    pytest.importorskip("altair")
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"storage:\n  data_dir: {store.as_posix()}\n"
                   f"notebook:\n  temp_directory: {(tmp_path / 'spill').as_posix()}\n",
                   encoding="utf-8")
    monkeypatch.setenv("STDF_CONFIG", str(cfg))
    path = Path(__file__).resolve().parents[1] / "workspace" / "_sample_core_redundancy.py"
    spec = importlib.util.spec_from_file_location("stdf_sample_core_redundancy", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rows(df):
    return {r.lot_id: (r.probed, r.pass_cnt, r.judged, r.both_zero, r.both_le_n)
            for r in df.itertuples()}


def test_notebook_counts_latest_retest_within_3_months(redundancy_store, tmp_path, monkeypatch):
    """既定(直近 3 か月)で、再検の最新結果をロット別に数え、古いロットと別工程は除く。"""
    module = _load_notebook(redundancy_store, tmp_path, monkeypatch)
    _outputs, defs = module.app.run()

    df = defs["df"]
    assert list(df["lot_id"]) == ["LOTA", "LOTB"]  # LOTOLD(4か月前)・LOTC(CP2) は除外、開始時刻順
    assert _rows(df) == {
        # d1(0,0) d2(2,6) d3 再検 pass (0,0) d4 判定なし
        "LOTA": (4, 4, 3, 2, 3),
        # e1 pass (6,7) / e2 fail (0,0)
        "LOTB": (2, 1, 2, 1, 1),
    }
    long = defs["long"]
    assert set(long["series"]) == {"pass(ハードビン)", "PSDN = HNS = 0", "PSDN, HNS ≤ 6"}
    assert len(long) == 6  # 2 ロット × 3 系列


def test_notebook_months_slider_widens_window(redundancy_store, tmp_path, monkeypatch):
    """スライダーを 6 か月にすると 4 か月前の LOTOLD も入る(範囲を決めているのは months だけ)。"""
    import marimo as mo
    module = _load_notebook(redundancy_store, tmp_path, monkeypatch)
    # 上書きは、そのセルの定義(コントロール 5 つ)をすべて渡す必要がある
    controls = {
        "products": ["PROD"],
        "product": mo.ui.dropdown(options=["(すべて)", "PROD"], value="(すべて)"),
        "sub_process": mo.ui.text(value="CP1"),
        "months": mo.ui.slider(1, 6, value=6),
        "mode": mo.ui.radio(options=["個数", "割合 (% of probed)"], value="個数"),
    }
    _outputs, defs = module.app.run(defs=controls)
    assert list(defs["df"]["lot_id"]) == ["LOTOLD", "LOTA", "LOTB"]
