"""Config.load resolution order: explicit arg → STDF_CONFIG → cwd config.yaml."""

from pathlib import Path

from stdf_platform.config import Config, StorageConfig


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
    assert cfg.storage.data_dir == Path("./var/data")


def test_load_tolerates_legacy_batch_size(tmp_path, monkeypatch):
    monkeypatch.delenv("STDF_CONFIG", raising=False)
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "processing:\n  batch_size: 500\n  compression: gzip\n", encoding="utf-8"
    )
    cfg = Config.load(cfg_file)
    assert cfg.processing.compression == "gzip"
    assert not hasattr(cfg.processing, "batch_size")


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


def test_with_env_none_returns_self():
    cfg = StorageConfig()
    assert cfg.with_env(None) is cfg


def test_relative_data_dir_anchors_to_config_dir(tmp_path, monkeypatch):
    """A relative data_dir must mean the same store from any cwd.

    Before this, data_dir resolved against the process cwd, so one
    config.yaml pointed at different directories depending on where the
    interpreter started — the CLI (always run from the repo root) found the
    store, while a VSCode Interactive Window with cwd=workspace/ resolved
    ./var/data to workspace/var/data and silently came up with no views.
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


def test_absolute_data_dir_is_left_alone(tmp_path):
    abs_store = (tmp_path / "elsewhere" / "store").resolve()
    (tmp_path / "config.yaml").write_text(
        f"storage:\n  data_dir: {abs_store.as_posix()}\n", encoding="utf-8")
    cfg = Config.load(tmp_path / "config.yaml")
    assert cfg.storage.data_dir == abs_store
