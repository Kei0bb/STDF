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

    def ingest(data, name, sub="CP1", product="SCT101B"):
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
    # 別製品のロットは SCT101B の集計に混ざらない
    ingest(_cp_run(
        "LOTX", "W1", "JOB", "Rev", NOW - 5 * DAY, NOW - 5 * DAY + 100,
        parts=[_die("LOTX", 1, True)], test_results=_judge("LOTX", 1, 0, 0),
        tests=TESTS), "x0.stdf", product="OTHER")
    return tmp_path


def _load_notebook(store, tmp_path, monkeypatch, gross_die=None):
    pytest.importorskip("marimo")
    pytest.importorskip("plotly")
    products = (f"products:\n  SCT101B:\n    gross_die: {gross_die}\n" if gross_die else "")
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"storage:\n  data_dir: {store.as_posix()}\n{products}"
                   f"notebook:\n  temp_directory: {(tmp_path / 'spill').as_posix()}\n",
                   encoding="utf-8")
    monkeypatch.setenv("STDF_CONFIG", str(cfg))
    path = Path(__file__).resolve().parents[1] / "workspace" / "_sample_core_redundancy.py"
    spec = importlib.util.spec_from_file_location("stdf_sample_core_redundancy", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rows(lot):
    # (probed, 母数, yield %, 0 redun %, 6 redun %)
    return {row.lot_id: (row.probed, row.total, *row[-3:]) for _, row in lot.iterrows()}


def test_notebook_counts_latest_retest_within_3_months(redundancy_store, tmp_path, monkeypatch):
    """既定(直近 3 か月)で、再検の最新結果をロット別に数え、古いロット・別工程・別製品は除く。"""
    module = _load_notebook(redundancy_store, tmp_path, monkeypatch)
    _outputs, defs = module.app.run()

    die, lot = defs["die"], defs["lot"]
    # ダイ表: LOTA 4 + LOTB 2。LOTOLD(4か月前)・LOTC(CP2)・LOTX(別製品) は入らない。
    assert list(die["lot_id"].unique()) == ["LOTA", "LOTB"]  # 開始時刻順
    assert len(die) == 6
    # 再検で測り直した d3 は最新の (0,0) / pass になっている。判定値の無い d4 は NULL。
    a = die[die["lot_id"] == "LOTA"].set_index("x")
    assert (a.loc[3, "psdn"], a.loc[3, "hns"], bool(a.loc[3, "passed"])) == (0, 0, True)
    assert a.loc[4, ["psdn", "hns"]].isna().all()

    assert list(lot["lot_id"]) == ["LOTA", "LOTB"]
    assert list(lot.columns[-3:]) == ["yield %", "0 redun %", "6 redun %"]
    # gross_die 未設定: 母数 = probed
    assert _rows(lot) == {
        # LOTA: 4 ダイ中 pass 4 / d1(0,0) d3(0,0) が両方 0 / d1 d2(2,6) d3 が 6 以下
        "LOTA": (4, 4, 100.0, 50.0, 75.0),
        # LOTB: 2 ダイ中 pass 1(e1) / e2(0,0) だけ両方 0 / e1 は HNS=7 で 6 以下に入らず e2 のみ
        "LOTB": (2, 2, 50.0, 50.0, 50.0),
    }
    fig = defs["fig"]
    assert [tr.name for tr in fig.data] == ["yield %", "0 redun %", "6 redun %"]
    assert list(fig.data[0].x) == ["LOTA", "LOTB"]
    assert list(fig.data[1].y) == [50.0, 50.0]


def test_notebook_denominator_is_gross_die(redundancy_store, tmp_path, monkeypatch):
    """母数 = ウェハーごとの max(probed, gross_die)。測っていないダイは分子に入らない。"""
    module = _load_notebook(redundancy_store, tmp_path, monkeypatch, gross_die=8)
    _outputs, defs = module.app.run()
    assert _rows(defs["lot"]) == {
        "LOTA": (4, 8, 50.0, 25.0, 37.5),   # pass 4 / 8、両方 0 が 2 / 8、6 以下が 3 / 8
        "LOTB": (2, 8, 12.5, 12.5, 12.5),   # pass 1 / 8、両方 0 が 1 / 8、6 以下が 1 / 8
    }
    assert list(defs["fig"].data[0].y) == [50.0, 12.5]


def test_notebook_gross_die_below_probed_keeps_probed(redundancy_store, tmp_path, monkeypatch):
    """probed が gross_die を超えるウェハーは probed のまま(GREATEST)。"""
    module = _load_notebook(redundancy_store, tmp_path, monkeypatch, gross_die=3)
    _outputs, defs = module.app.run()
    rows = _rows(defs["lot"])
    assert rows["LOTA"] == (4, 4, 100.0, 50.0, 75.0)       # 4 > 3 → probed
    assert rows["LOTB"] == (2, 3, 33.33, 33.33, 33.33)     # 2 < 3 → gross_die


def test_notebook_months_constant_widens_window(redundancy_store, tmp_path, monkeypatch):
    """MONTHS を 6 にすると 4 か月前の LOTOLD も入る(範囲を決めているのは MONTHS だけ)。"""
    module = _load_notebook(redundancy_store, tmp_path, monkeypatch)
    # 上書きは、そのセルの定義(定数 6 つ)をすべて渡す必要がある
    consts = {"PRODUCT": "SCT101B", "SUB": "CP1", "MONTHS": 6, "MAX_N": 6,
              "PSDN": PSDN, "HNS": HNS}
    _outputs, defs = module.app.run(defs=consts)
    assert list(defs["lot"]["lot_id"]) == ["LOTOLD", "LOTA", "LOTB"]
