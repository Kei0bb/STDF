"""sql/ — 名前付きクエリライブラリ(AnalysisSession.run / queries / show)。

クエリファイルは自己完結(冒頭の SET VARIABLE が既定値)で、run() に渡した
パラメータがそれを上書きする。値はバインド変数として渡すので、SQL 断片や
クォートを含む値でも壊れない。
"""

from pathlib import Path

import pytest

from stdf_platform.analysis import AnalysisSession
from stdf_platform.analysis.library import DEFAULT_SQL_DIR, list_queries


@pytest.fixture
def session(synth_store):
    with AnalysisSession(synth_store) as s:
        yield s


def test_every_shipped_query_runs(session):
    """sql/ の全クエリが実際に実行できること(SQL の腐り検出)。"""
    names = list(session.queries()["name"])
    assert names, "sql/ が空"
    for name in names:
        df = session.run(name, lot="LOT1")
        assert df is not None


def test_queries_lists_name_and_description(session):
    df = session.queries()
    assert set(df.columns) == {"name", "description"}
    assert (df["description"].str.len() > 0).all(), "説明のないクエリがある"
    assert "03_test/fail_ranking" in set(df["name"])


def test_short_name_resolves(session):
    full = session.run("03_test/fail_ranking", lot="LOT1")
    short = session.run("fail_ranking", lot="LOT1")
    assert full.equals(short)


def test_param_overrides_file_default(session):
    """ファイル冒頭の SET VARIABLE より run() の引数が勝つ。"""
    default = session.run("wafer_yield")                    # ファイル既定 = LOT001
    other = session.run("wafer_yield", lot="FTLOT1")
    assert not default.equals(other)


def test_out_writes_csv_and_returns_rowcount(session, tmp_path):
    out = tmp_path / "e.csv"
    n = session.run("die_test_export", lot="LOT1", out=out)
    assert isinstance(n, int) and n > 0
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == n + 1                              # ヘッダ + 行数
    assert lines[0].startswith("lot_id,")


def test_params_are_bound_not_interpolated(session):
    """SQL 断片を値として渡しても注入にならない(結果が空になるだけ)。"""
    df = session.run("fail_ranking", lot="LOT001' OR '1'='1")
    assert df.empty


def test_unknown_query_lists_available(session):
    with pytest.raises(FileNotFoundError, match="fail_ranking"):
        session.run("no_such_query")


def test_bad_param_name_rejected(session):
    with pytest.raises(ValueError, match="パラメータ名"):
        session.run("fail_ranking", **{"lot; DROP TABLE x": "L"})


def test_show_returns_sql_text(session):
    text = session.show("cpk")
    assert "getvariable('lot')" in text and "SET VARIABLE" in text


def test_shipped_queries_use_canonical_views(session):
    """歩留まりクエリが wafers を自前集計していないこと。

    wafers ベースの集計は FT ロット(wafers に行が無い)を取りこぼし、
    gross die も効かない — wafer_yield_final が唯一の定義。
    """
    for name in ["01_lots/lot_yield", "02_wafer/wafer_yield"]:
        sql = (DEFAULT_SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
        assert "wafer_yield_final" in sql
        assert "FROM wafers" not in sql


def test_list_queries_on_missing_dir(tmp_path):
    with pytest.raises(FileNotFoundError):
        from stdf_platform.analysis.library import find_query
        find_query("x", tmp_path / "nope")
