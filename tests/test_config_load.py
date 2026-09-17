"""Config.load resolution order: explicit arg → STDF_CONFIG → cwd config.yaml."""

from pathlib import Path

import pytest

from stdf_platform.config import Config, StorageConfig


def _write_cfg(path: Path, data_dir: str):
    path.write_text(f"storage:\n  data_dir: {data_dir}\n", encoding="utf-8")


@pytest.mark.parametrize("explicit, env, cwd, expected", [
    (True, True, True, "/from/explicit"),
    (False, True, True, "/from/env"),
    (False, False, True, "/from/cwd"),
    (False, False, False, "./var/data"),     # defaults stay under var/
], ids=["explicit_arg_wins", "env_over_cwd", "cwd", "defaults"])
def test_config_resolution_order(tmp_path, monkeypatch, explicit, env, cwd, expected):
    (tmp_path / "other").mkdir()
    explicit_cfg = tmp_path / "other" / "explicit.yaml"
    _write_cfg(explicit_cfg, "/from/explicit")
    if env:
        env_cfg = tmp_path / "other" / "env.yaml"
        _write_cfg(env_cfg, "/from/env")
        monkeypatch.setenv("STDF_CONFIG", str(env_cfg))
    else:
        monkeypatch.delenv("STDF_CONFIG", raising=False)
    if cwd:
        _write_cfg(tmp_path / "config.yaml", "/from/cwd")
    monkeypatch.chdir(tmp_path)

    cfg = Config.load(explicit_cfg if explicit else None)
    assert cfg.storage.data_dir == Path(expected)


def test_with_env_derives_from_data_dir():
    """--env dev must stay inside whatever runtime root data_dir points at.

    A hardcoded "./data-{env}" would recreate a store at the project root
    even when data_dir was moved under var/, which is exactly what the
    single-runtime-root layout exists to prevent.
    """
    cfg = StorageConfig(
        data_dir=Path("./var/data"),
        database=Path("./var/data/stdf.duckdb"),
        download_dir=Path("./var/downloads"),
    )
    dev = cfg.with_env("dev")
    assert dev.data_dir == Path("var/data-dev")
    assert dev.database == Path("var/data-dev/stdf.duckdb")
    # download_dir is shared across envs (dev reuses the same source files).
    assert dev.download_dir == Path("./var/downloads")


def test_relative_data_dir_anchors_to_config_dir(tmp_path, monkeypatch):
    """A relative data_dir must mean the same store from any cwd.

    Before this, data_dir resolved against the process cwd, so a VSCode
    Interactive Window with cwd=workspace/ resolved ./var/data to
    workspace/var/data and silently came up with no views.
    """
    (tmp_path / "config.yaml").write_text(
        "storage:\n"
        "  data_dir: ./var/data\n"
        "  database: ./var/data/stdf.duckdb\n"
        "  download_dir: ./var/downloads\n",
        encoding="utf-8",
    )
    elsewhere = tmp_path / "sub" / "dir"
    elsewhere.mkdir(parents=True)
    monkeypatch.chdir(elsewhere)          # cwd deliberately not the config's dir

    cfg = Config.load(tmp_path / "config.yaml")
    assert cfg.storage.data_dir == tmp_path.resolve() / "var" / "data"
    assert cfg.storage.database == tmp_path.resolve() / "var" / "data" / "stdf.duckdb"
    assert cfg.storage.download_dir == tmp_path.resolve() / "var" / "downloads"


def test_server_resource_limits_are_loaded(tmp_path):
    (tmp_path / "config.yaml").write_text(
        "server:\n"
        "  max_rows: 500\n"
        "  memory_limit: 4GB\n"
        "  threads: 3\n"
        "  query_timeout_seconds: 15\n",
        encoding="utf-8",
    )
    cfg = Config.load(tmp_path / "config.yaml")
    assert cfg.server.max_rows == 500
    assert cfg.server.memory_limit == "4GB"
    assert cfg.server.threads == 3
    assert cfg.server.query_timeout_seconds == 15
