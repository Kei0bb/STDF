"""AnalysisSession config resolution when no data_dir is given."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import stdf_platform.analysis.session as session_mod
from synth_data import _write_cp


@pytest.mark.parametrize("use_stdf_config", [False, True],
                         ids=["repo_root_fallback", "stdf_config_wins"])
def test_session_config_resolution_from_other_cwd(monkeypatch, tmp_path, use_stdf_config):
    """AnalysisSession() with no data_dir, launched from a cwd without its own
    config.yaml (e.g. workspace/ in VSCode):
      - no STDF_CONFIG -> falls back to the repo-root config.yaml instead of
        silently defaulting to ./var/data relative to the wrong cwd;
      - STDF_CONFIG set -> it wins over that repo-root fallback.

    Never touches the real repo config.yaml: the module's __file__ is faked so
    `parents[3] / "config.yaml"` resolves into a scratch "repo root".
    """
    _write_cp(tmp_path)
    fake_repo = tmp_path / "fake_repo"
    fake_session_file = fake_repo / "src" / "stdf_platform" / "analysis" / "session.py"
    monkeypatch.setattr(session_mod, "__file__", str(fake_session_file))
    repo_cfg = fake_repo / "config.yaml"
    repo_cfg.parent.mkdir(parents=True, exist_ok=True)

    if use_stdf_config:
        # Repo-root config points at an empty dir — if the fallback wrongly
        # wins, the session sees no store.
        wrong_dir = tmp_path / "wrong_data"
        wrong_dir.mkdir()
        repo_cfg.write_text(f"storage:\n  data_dir: {wrong_dir.as_posix()}\n", encoding="utf-8")
        stdf_cfg = tmp_path / "stdf_config.yaml"
        stdf_cfg.write_text(f"storage:\n  data_dir: {tmp_path.as_posix()}\n", encoding="utf-8")
        monkeypatch.setenv("STDF_CONFIG", str(stdf_cfg))
    else:
        repo_cfg.write_text(f"storage:\n  data_dir: {tmp_path.as_posix()}\n", encoding="utf-8")
        monkeypatch.delenv("STDF_CONFIG", raising=False)

    cwd = tmp_path / "workdir"   # cwd has no config.yaml of its own
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    with session_mod.AnalysisSession() as s:   # data_dir=None → resolved from config
        assert s.data_dir == tmp_path
        assert "parts_final" in s.registered
