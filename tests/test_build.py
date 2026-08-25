"""stdf build (run_build) — dbt run + dbt test wrapped with an atomic
data/marts/ swap. See src/stdf_platform/build.py.
"""
from stdf_platform.build import run_build, BuildError
from stdf_platform.config import Config, StorageConfig


def _config_for(tmp_path, data_dir):
    c = Config()
    c.storage = StorageConfig(data_dir=data_dir,
                              database=data_dir / "stdf.duckdb",
                              download_dir=tmp_path / "dl")
    return c


def test_run_build_produces_marts(tmp_path, synth_store):
    run_build(_config_for(tmp_path, synth_store))
    assert (synth_store / "marts" / "lot_yield_summary.parquet").exists()
    assert not (synth_store / ".marts_build").exists()   # 一時ディレクトリ掃除済み


def test_run_build_swaps_atomically_on_rerun(tmp_path, synth_store):
    run_build(_config_for(tmp_path, synth_store))
    first = (synth_store / "marts" / "lot_yield_summary.parquet").stat().st_mtime_ns
    run_build(_config_for(tmp_path, synth_store))
    second = (synth_store / "marts" / "lot_yield_summary.parquet").stat().st_mtime_ns
    assert second != first                                # 差し替わっている


def test_run_build_raises_on_invariant_violation(tmp_path, corrupt_store):
    # corrupt_store: retest_flag=NULL 行を含む(test_dbt_invariant_tests.py と共用 fixture)
    import pytest
    with pytest.raises(BuildError):
        run_build(_config_for(tmp_path, corrupt_store))
