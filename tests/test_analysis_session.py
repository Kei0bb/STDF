"""AnalysisSession lifecycle + helpers over synthetic Parquet."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import stdf_platform.analysis.session as session_mod
from stdf_platform.analysis import AnalysisSession
from stdf_platform.config import Config, ProductConfig, StorageConfig
from synth_data import _write_cp, _write_ft


def test_session_registers_views(tmp_path):
    _write_cp(tmp_path)
    with AnalysisSession(tmp_path) as s:
        assert "parts_final" in s.registered
        assert "lots" in s.registered


def test_session_q_binds_params(tmp_path):
    _write_cp(tmp_path)
    with AnalysisSession(tmp_path) as s:
        df = s.q("SELECT COUNT(*) AS n FROM parts_final WHERE lot_id = ?", ["LOT1"])
        assert int(df.iloc[0]["n"]) == 4   # 4 distinct dies, retest-aware


def test_session_lots_filter(tmp_path):
    _write_cp(tmp_path)
    _write_ft(tmp_path)
    with AnalysisSession(tmp_path) as s:
        allrows = s.lots()
        assert set(allrows["lot_id"]) == {"LOT1", "FT1"}
        cp = s.lots(product="PROD", test_category="CP")
        assert list(cp["lot_id"]) == ["LOT1"]


def test_session_lots_dedup_latest(tmp_path):
    _write_cp(tmp_path)
    with AnalysisSession(tmp_path) as s:
        rows = s.lots(product="PROD")
        assert len(rows) == 1   # single MIR row, not duplicated


def test_session_default_data_dir(monkeypatch, tmp_path):
    _write_cp(tmp_path)
    # Config.load() with no config.yaml → defaults; point STDF_CONFIG at a yaml
    cfg = tmp_path / "c.yaml"
    cfg.write_text(f"storage:\n  data_dir: {tmp_path.as_posix()}\n")
    monkeypatch.setenv("STDF_CONFIG", str(cfg))
    with AnalysisSession() as s:    # data_dir=None → resolved from config
        assert "parts_final" in s.registered


def test_session_config_param_reaches_gross_die_table(tmp_path, monkeypatch):
    """A Config passed explicitly via config= (as the CLI / server now do,
    using their already-resolved ctx.obj["config"]) must be used as-is —
    its gross_die_map must reach the session's `gross_die` table — instead
    of AnalysisSession silently re-resolving its own Config.load() and
    picking up a different (often empty) gross_die_map. Regression test for
    the mart-vs-runtime gross-die divergence finding.
    """
    _write_cp(tmp_path)
    # If AnalysisSession ignored `config` and called Config.load() itself,
    # this monkeypatch would make that call return an empty-products Config,
    # so the assertion below would fail loudly rather than passing by luck.
    monkeypatch.setattr(session_mod.Config, "load", classmethod(lambda cls, p=None: Config()))

    cfg = Config(
        storage=StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb"),
        products={"PROD": ProductConfig(gross_die=500, gd_fail_bin=200)},
    )
    with AnalysisSession(tmp_path, config=cfg) as s:
        rows = s.q("SELECT product, gross_die, gd_fail_bin FROM gross_die")
    assert len(rows) == 1
    assert rows.iloc[0]["product"] == "PROD"
    assert int(rows.iloc[0]["gross_die"]) == 500


def test_session_falls_back_to_repo_config_from_other_cwd(monkeypatch, tmp_path):
    """AnalysisSession() run with no explicit data_dir, no STDF_CONFIG, and no
    config.yaml in cwd (e.g. launched from workspace/ in VSCode) falls back to
    the repo-root config.yaml (session.py Step 2) instead of silently
    defaulting to ./var/data relative to the wrong cwd.

    Never touches the real repo config.yaml: instead of writing to the real
    file, this fakes the module's own __file__ so `parents[3] / "config.yaml"`
    resolves into a scratch "repo root" under tmp_path. Path.resolve() does
    not require the intermediate directories to exist for this to work.
    """
    _write_cp(tmp_path)
    fake_repo = tmp_path / "fake_repo"
    fake_session_file = fake_repo / "src" / "stdf_platform" / "analysis" / "session.py"
    monkeypatch.setattr(session_mod, "__file__", str(fake_session_file))
    repo_cfg = fake_repo / "config.yaml"
    repo_cfg.parent.mkdir(parents=True, exist_ok=True)
    repo_cfg.write_text(f"storage:\n  data_dir: {tmp_path.as_posix()}\n", encoding="utf-8")

    cwd = tmp_path / "workdir"   # cwd has no config.yaml of its own
    cwd.mkdir()
    monkeypatch.delenv("STDF_CONFIG", raising=False)
    monkeypatch.chdir(cwd)
    with session_mod.AnalysisSession() as s:   # data_dir=None → falls back to repo_cfg
        assert s.data_dir == tmp_path
        assert "parts_final" in s.registered


def test_session_stdf_config_wins_over_repo_config_fallback(monkeypatch, tmp_path):
    """STDF_CONFIG must take priority over the repo-root fallback: the
    fallback in session.py only applies when no STDF_CONFIG is set, matching
    Config.load()'s documented resolution order (explicit arg -> STDF_CONFIG
    -> cwd config.yaml; config.py:142). A cwd lacking config.yaml must not
    make the repo-root config.yaml clobber a valid STDF_CONFIG resolution.
    """
    _write_cp(tmp_path)
    fake_repo = tmp_path / "fake_repo"
    fake_session_file = fake_repo / "src" / "stdf_platform" / "analysis" / "session.py"
    monkeypatch.setattr(session_mod, "__file__", str(fake_session_file))
    repo_cfg = fake_repo / "config.yaml"
    repo_cfg.parent.mkdir(parents=True, exist_ok=True)
    # Repo-root config points at a directory with no data at all — if the
    # fallback wrongly wins, s.registered would be empty (no tables found).
    wrong_dir = tmp_path / "wrong_data"
    wrong_dir.mkdir()
    repo_cfg.write_text(f"storage:\n  data_dir: {wrong_dir.as_posix()}\n", encoding="utf-8")

    stdf_cfg = tmp_path / "stdf_config.yaml"
    stdf_cfg.write_text(f"storage:\n  data_dir: {tmp_path.as_posix()}\n", encoding="utf-8")

    cwd = tmp_path / "workdir"   # cwd has no config.yaml of its own either
    cwd.mkdir()
    monkeypatch.setenv("STDF_CONFIG", str(stdf_cfg))
    monkeypatch.chdir(cwd)
    with session_mod.AnalysisSession() as s:   # data_dir=None → STDF_CONFIG must win
        assert s.data_dir == tmp_path
        assert "parts_final" in s.registered


def test_lots_keeps_sub_process_distinct(tmp_path):
    """lots ビューは (lot, product, test_category, sub_process) ごとに1行。
    lots() が sub_process を PARTITION に含めないと片方が黙って落ちる。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    for sub in ("CP1", "CP2"):
        p = (tmp_path / "runs" / "product=P" / "test_category=CP"
             / f"sub_process={sub}" / "lot_id=LOT" / "wafer_id=W1"
             / "retest=0" / "data.parquet")
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({
            "lot_id": ["LOT"], "wafer_id": ["W1"], "product": ["P"],
            "test_category": ["CP"], "sub_process": [sub], "retest_num": [0],
            "part_type": ["T"], "job_name": ["J"], "job_rev": ["A"],
            "tester_type": ["T"], "operator": ["OP"],
            "start_time": [pa.scalar(1_700_000_000_000, pa.timestamp("ms", tz="UTC"))],
            "finish_time": [pa.scalar(1_700_000_100_000, pa.timestamp("ms", tz="UTC"))],
            "test_rev": [""], "source_file": [""],
        }), p)

    with AnalysisSession(tmp_path) as s:
        df = s.lots()
    assert sorted(df["sub_process"]) == ["CP1", "CP2"]
