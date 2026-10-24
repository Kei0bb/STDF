"""共有 marimo サーバ用: notebook セクションの config と AnalysisSession.for_notebook()。"""

import duckdb
import pytest

from stdf_platform.analysis import AnalysisSession
from stdf_platform.config import Config, NotebookConfig


def test_notebook_config_defaults():
    nb = Config().notebook
    assert nb == NotebookConfig()
    assert nb.memory_limit == "4GB"
    assert nb.threads == 4


def test_notebook_config_loaded_and_temp_dir_anchored_to_config_dir(tmp_path, monkeypatch):
    cfg = tmp_path / "cfg" / "config.yaml"
    cfg.parent.mkdir()
    cfg.write_text(
        "notebook:\n  memory_limit: 1GB\n  threads: 2\n  temp_directory: ./var/tmp-nb\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("STDF_CONFIG", raising=False)
    nb = Config.load(cfg).notebook
    assert (nb.memory_limit, nb.threads) == ("1GB", 2)
    # 相対パスは cwd ではなく config.yaml のあるディレクトリ基準
    assert nb.temp_directory == (cfg.parent.resolve() / "var" / "tmp-nb")


def test_for_notebook_applies_limits_and_views_on_given_connection(synth_store, tmp_path):
    config = Config()
    config.notebook = NotebookConfig(
        memory_limit="1GB", threads=2, temp_directory=tmp_path / "spill")
    conn = duckdb.connect(":memory:")
    s = AnalysisSession.for_notebook(data_dir=synth_store, config=config, conn=conn)

    assert s.conn is conn
    mem, threads, tmp = conn.execute(
        "SELECT current_setting('memory_limit'), current_setting('threads'),"
        " current_setting('temp_directory')").fetchone()
    assert threads == 2
    assert tmp == str(tmp_path / "spill")
    assert mem.startswith(("953", "1.0 GiB", "1 GiB", "1000"))  # 1GB → ~953.6 MiB
    # ビューが同じ接続で読める(= SQL セルが engine 指定なしでも *_final を読める)
    assert conn.execute("SELECT count(*) FROM lots").fetchone()[0] > 0


def test_for_notebook_uses_duckdb_default_connection(synth_store, monkeypatch):
    """mo.sql(engine 無し)は duckdb.sql = 既定接続を使う。セッションをそこに載せる。"""
    fresh = duckdb.connect(":memory:")
    monkeypatch.setattr(duckdb, "default_connection", lambda: fresh)
    s = AnalysisSession.for_notebook(data_dir=synth_store, config=Config())
    assert s.conn is fresh


def test_plain_session_still_gets_private_memory_connection(synth_store):
    a = AnalysisSession(data_dir=synth_store, config=Config())
    b = AnalysisSession(data_dir=synth_store, config=Config())
    assert a.conn is not b.conn


def test_invalid_memory_limit_fails_loudly(synth_store):
    config = Config()
    config.notebook = NotebookConfig(memory_limit="lots")
    with pytest.raises(duckdb.Error):
        AnalysisSession.for_notebook(
            data_dir=synth_store, config=config, conn=duckdb.connect(":memory:"))


def test_template_notebook_runs_end_to_end(synth_store, tmp_path, monkeypatch):
    """workspace/_template.py の全セルが合成データで通る(bind 変数のクエリ、engine 無しの
    mo.sql が既定接続のビューを読む経路を含む)。marimo が無い環境ではスキップ。

    既定接続はプロセス全域なので、このテストの後もビューが残る。他のテストは
    duckdb の既定接続を使わないので影響しない。
    """
    pytest.importorskip("marimo")
    pytest.importorskip("sqlglot")
    import importlib.util
    from pathlib import Path

    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"storage:\n  data_dir: {synth_store.as_posix()}\n"
        f"notebook:\n  memory_limit: 1GB\n  threads: 2\n"
        f"  temp_directory: {(tmp_path / 'spill').as_posix()}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("STDF_CONFIG", str(cfg))

    template = Path(__file__).resolve().parents[1] / "workspace" / "_template.py"
    spec = importlib.util.spec_from_file_location("stdf_notebook_template", template)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    outputs, defs = module.app.run()

    assert defs["s"].conn is duckdb.default_connection()
    assert len(defs["lot_picker"].options) == len(set(defs["lot_picker"].options)) > 0
    # 最後の2つの出力: SQL セル(lots の先頭10件)と s.queries() の一覧 — 空ではない
    assert len(outputs[-1]) > 0
