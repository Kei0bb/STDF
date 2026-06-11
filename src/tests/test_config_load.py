"""Config.load resolution order: explicit arg → STDF_CONFIG → cwd config.yaml."""

from pathlib import Path

from stdf_platform.config import Config


def _write_cfg(path: Path, data_dir: str):
    path.write_text(f"storage:\n  data_dir: {data_dir}\n", encoding="utf-8")


def test_explicit_arg_wins(tmp_path, monkeypatch):
    explicit = tmp_path / "explicit.yaml"
    _write_cfg(explicit, "/from/explicit")
    env_cfg = tmp_path / "env.yaml"
    _write_cfg(env_cfg, "/from/env")
    monkeypatch.setenv("STDF_CONFIG", str(env_cfg))
    cfg = Config.load(explicit)
    assert cfg.storage.data_dir == Path("/from/explicit")


def test_env_var_used_when_no_arg(tmp_path, monkeypatch):
    env_cfg = tmp_path / "env.yaml"
    _write_cfg(env_cfg, "/from/env")
    monkeypatch.setenv("STDF_CONFIG", str(env_cfg))
    monkeypatch.chdir(tmp_path)  # cwd config.yaml absent
    cfg = Config.load()
    assert cfg.storage.data_dir == Path("/from/env")


def test_cwd_config_used_when_no_arg_no_env(tmp_path, monkeypatch):
    monkeypatch.delenv("STDF_CONFIG", raising=False)
    _write_cfg(tmp_path / "config.yaml", "/from/cwd")
    monkeypatch.chdir(tmp_path)
    cfg = Config.load()
    assert cfg.storage.data_dir == Path("/from/cwd")


def test_missing_everything_returns_defaults(tmp_path, monkeypatch):
    monkeypatch.delenv("STDF_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)  # no config.yaml here
    cfg = Config.load()
    assert cfg.storage.data_dir == Path("./data")
