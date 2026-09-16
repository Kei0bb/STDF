"""名前付きクエリライブラリ(AnalysisSession.run / queries / show)。

同梱クエリは src/stdf_platform/sql/(git 管理)、個人用はリポジトリ直下の
sql/(gitignore、同名なら個人用が優先)。

変数の規約:
  - クエリ内の getvariable('x') は呼び出し側が渡す。渡し忘れはエラー
  - ファイルに SET VARIABLE x = ...; があればそれが既定値(任意)
  - `SET VARIABLE x = NULL;` + opt_eq(col, getvariable('x')) で「渡さなければ絞らない」
  - クエリが使わない名前を渡したらエラー、run() ごとに変数はリセット
値はバインド変数として渡すので、SQL 断片やクォートを含む値でも壊れない。
"""

from pathlib import Path

import pytest

from stdf_platform.analysis import AnalysisSession, library
from stdf_platform.analysis.library import PACKAGE_SQL_DIR, find_query, query_params

# 同梱クエリを総当たりで流すときに、各変数へ渡す合成ストア上の値
_SAMPLE_VALUES = {"lot": "LOT1", "test_name": "%", "product": "PROD",
                  "test_category": "CP", "sub_process": "CP1"}


@pytest.fixture
def session(synth_store):
    with AnalysisSession(synth_store) as s:
        yield s


def _write(root: Path, name: str, sql: str) -> None:
    p = root / f"{name}.sql"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(sql, encoding="utf-8")


# ── 同梱クエリ ────────────────────────────────────────────────

def test_every_shipped_query_runs(session):
    """同梱の全クエリが、必須変数だけ渡して実行できること(SQL の腐り検出)。"""
    names = list(session.queries()["name"])
    assert names, "同梱クエリが空"
    for name in names:
        required = [k for k, v in query_params(find_query(name)).items() if v is None]
        df = session.run(name, **{k: _SAMPLE_VALUES[k] for k in required})
        assert df is not None


def test_queries_lists_name_description_params_source(session):
    df = session.queries()
    assert set(df.columns) == {"name", "description", "params", "source"}
    assert (df["description"].str.len() > 0).all(), "説明のないクエリがある"
    assert set(df["source"]) == {"package"}
    row = df.set_index("name").loc["03_test/cpk"]
    assert row["params"] == "lot, test_name='%'"


def test_short_name_resolves(session):
    full = session.run("03_test/fail_ranking", lot="LOT1")
    short = session.run("fail_ranking", lot="LOT1")
    assert full.equals(short)


def test_shipped_queries_use_canonical_views():
    """歩留まりクエリが wafers を自前集計していないこと。

    wafers ベースの集計は FT ロット(wafers に行が無い)を取りこぼし、
    gross die も効かない — wafer_yield_final が唯一の定義。
    """
    for name in ["01_lots/lot_yield", "02_wafer/wafer_yield"]:
        sql = (PACKAGE_SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
        assert "wafer_yield_final" in sql
        assert "FROM wafers" not in sql


# ── 複数条件 / 任意条件 ────────────────────────────────────────

def test_multiple_conditions_at_once(session):
    """lot 以外の条件を複数同時に渡せる。"""
    both = session.run("lot_list", product="PROD")
    assert set(both["lot_id"]) == {"LOT1", "FTLOT1"}
    cp = session.run("lot_list", product="PROD", test_category="CP")
    assert list(cp["lot_id"]) == ["LOT1"]
    ft = session.run("lot_list", test_category="FT", sub_process="FT2")
    assert list(ft["lot_id"]) == ["FTLOT1"]


def test_optional_condition_not_passed_does_not_filter(session):
    assert set(session.run("lot_list")["lot_id"]) == {"LOT1", "FTLOT1"}


def test_param_overrides_file_default(session):
    """ファイル冒頭の SET VARIABLE(既定値)より run() の引数が勝つ。"""
    all_tests = session.run("cpk", lot="LOT1")                 # test_name='%'
    one = session.run("cpk", lot="LOT1", test_name="CPK_TEST")
    assert len(one) == 1 and len(all_tests) > len(one)


# ── 変数の検証 ────────────────────────────────────────────────

def test_missing_required_variable_raises(session):
    """渡し忘れを 0 行で黙って返さない。"""
    with pytest.raises(TypeError, match="lot"):
        session.run("wafer_yield")


def test_unknown_variable_raises(session):
    """クエリが使わない名前(typo)を黙って無視しない。"""
    with pytest.raises(TypeError, match="prodcut") as e:
        session.run("lot_list", prodcut="PROD")
    assert "product" in str(e.value)          # 使える名前を示す


def test_variables_do_not_leak_between_runs(session):
    """前の run() で渡した値が、次の run() の条件に残らない。"""
    assert list(session.run("lot_list", test_category="FT")["lot_id"]) == ["FTLOT1"]
    assert set(session.run("lot_list")["lot_id"]) == {"LOT1", "FTLOT1"}


def test_variables_in_comments_and_strings_are_ignored(tmp_path):
    _write(tmp_path, "q", (
        "-- 説明 getvariable('in_comment')\n"
        "/* getvariable('in_block') */\n"
        "SELECT 'getvariable(''in_string'')' AS s, GETVARIABLE( 'real' ) AS v"
    ))
    assert query_params(tmp_path / "q.sql") == {"real": None}


def test_params_are_bound_not_interpolated(session):
    """SQL 断片を値として渡しても注入にならない(結果が空になるだけ)。"""
    df = session.run("fail_ranking", lot="LOT001' OR '1'='1")
    assert df.empty


def test_bad_param_name_rejected(session):
    with pytest.raises(ValueError, match="パラメータ名"):
        session.run("fail_ranking", **{"lot; DROP TABLE x": "L"})


# ── 出力 / 表示 ────────────────────────────────────────────────

def test_out_writes_csv_and_returns_rowcount(session, tmp_path):
    out = tmp_path / "e.csv"
    n = session.run("die_test_export", lot="LOT1", out=out)
    assert isinstance(n, int) and n > 0
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == n + 1                              # ヘッダ + 行数
    assert lines[0].startswith("lot_id,")


def test_run_out_path_with_single_quote(session, tmp_path):
    """出力パスは SQL リテラルとしてクォートされる(シングルクォートは '' 化)。"""
    out = tmp_path / "it's.csv"
    rows = session.run("01_lots/lot_list", out=out)
    assert rows >= 1
    assert out.exists()


def test_show_returns_sql_text(session):
    text = session.show("cpk")
    assert "getvariable('lot')" in text and "SET VARIABLE" in text


def test_unknown_query_lists_available(session):
    with pytest.raises(FileNotFoundError, match="fail_ranking"):
        session.run("no_such_query")


# ── 個人用 sql/ ───────────────────────────────────────────────

def test_personal_query_is_listed_and_runs(session, tmp_path, monkeypatch):
    personal = tmp_path / "personal"
    monkeypatch.setattr(library, "PERSONAL_SQL_DIR", personal)
    _write(personal, "10_mine/my_lots", "-- 自分用\nSELECT lot_id FROM lots ORDER BY lot_id")

    df = session.queries().set_index("name")
    assert df.loc["10_mine/my_lots", "source"] == "personal"
    assert df.loc["03_test/cpk", "source"] == "package"
    assert list(session.run("my_lots")["lot_id"]) == ["FTLOT1", "LOT1"]


def test_personal_query_overrides_package_query(session, tmp_path, monkeypatch):
    """同じ名前なら個人用が勝つ(本番で同梱クエリを手元で改造できる)。"""
    personal = tmp_path / "personal"
    monkeypatch.setattr(library, "PERSONAL_SQL_DIR", personal)
    _write(personal, "01_lots/lot_list", "-- 改造版\nSELECT 'mine' AS who")

    assert list(session.run("lot_list")["who"]) == ["mine"]
    listed = session.queries()
    assert (listed["name"] == "01_lots/lot_list").sum() == 1   # 重複表示しない
    assert listed.set_index("name").loc["01_lots/lot_list", "source"] == "personal"


def test_explicit_sql_dir_uses_only_that_dir(synth_store, tmp_path):
    only = tmp_path / "only"
    _write(only, "x", "-- だけ\nSELECT 1 AS one")
    with AnalysisSession(synth_store, sql_dir=only) as s:
        assert list(s.queries()["name"]) == ["x"]
        with pytest.raises(FileNotFoundError):
            s.run("cpk", lot="LOT1")


def test_find_query_on_missing_dir(tmp_path):
    with pytest.raises(FileNotFoundError):
        find_query("x", tmp_path / "nope")
