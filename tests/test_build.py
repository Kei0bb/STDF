"""stdf build (run_build) — dbt run + dbt test wrapped with an atomic
data/marts/ swap. See src/stdf_platform/build.py.
"""
from pathlib import Path

import pytest

from conftest import _write_null_flag_row
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
    with pytest.raises(BuildError):
        run_build(_config_for(tmp_path, corrupt_store))


def test_run_build_preserves_old_marts_on_dbt_failure(tmp_path, synth_store):
    """A later failed build (dbt test rejects an invariant violation) must
    never touch the previous good marts/ — the swap only happens after
    `dbt run` + `dbt test` both succeed, so a failure here never even
    reaches `_replace_dir_with_retry`.
    """
    # (a) a successful build establishes a known-good marts/.
    run_build(_config_for(tmp_path, synth_store))
    target = synth_store / "marts" / "lot_yield_summary.parquet"
    content_before = target.read_bytes()
    mtime_before = target.stat().st_mtime_ns

    # (b) corrupt the same store in place and rebuild -> dbt test fails.
    _write_null_flag_row(synth_store)
    with pytest.raises(BuildError):
        run_build(_config_for(tmp_path, synth_store))

    # (c) marts/ content and mtime are untouched.
    assert target.read_bytes() == content_before
    assert target.stat().st_mtime_ns == mtime_before


def test_replace_dir_with_retry_succeeds_after_transient_failure(tmp_path, synth_store, monkeypatch):
    """A rename that fails once (simulating a Windows reader holding the
    file open) then succeeds must still produce a working marts/ — this
    exercises the retry loop's happy path.
    """
    run_build(_config_for(tmp_path, synth_store))  # seed an existing marts/ to swap over
    marts_dir = synth_store / "marts"

    real_rename = Path.rename
    calls = {"n": 0}

    def flaky_rename(self, target):
        target_path = Path(target)
        if self.name == ".marts_build" and target_path.name == "marts":
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("simulated transient rename failure")
        return real_rename(self, target)

    monkeypatch.setattr(Path, "rename", flaky_rename)
    monkeypatch.setattr("stdf_platform.build.time.sleep", lambda s: None)

    run_build(_config_for(tmp_path, synth_store))  # must recover via retry

    assert calls["n"] >= 2  # confirms the retry path actually ran
    assert (marts_dir / "lot_yield_summary.parquet").exists()


def test_replace_dir_with_retry_restores_old_marts_when_exhausted(tmp_path, synth_store, monkeypatch):
    """Regression test for the atomicity bug: if every src -> dst rename
    attempt fails, the previous good marts/ must be restored (old -> dst)
    before re-raising — NOT deleted by a premature `shutil.rmtree(old)` on
    a later retry iteration, which would leave marts/ missing entirely.
    """
    run_build(_config_for(tmp_path, synth_store))  # seed an existing, known-good marts/
    marts_dir = synth_store / "marts"
    target = marts_dir / "lot_yield_summary.parquet"
    content_before = target.read_bytes()

    real_rename = Path.rename

    def always_fail_rename(self, target_arg):
        target_path = Path(target_arg)
        if self.name == ".marts_build" and target_path.name == "marts":
            raise OSError("simulated permanent rename failure")
        return real_rename(self, target_arg)

    monkeypatch.setattr(Path, "rename", always_fail_rename)
    monkeypatch.setattr("stdf_platform.build.time.sleep", lambda s: None)

    with pytest.raises(OSError):
        run_build(_config_for(tmp_path, synth_store))

    assert marts_dir.exists()
    assert target.read_bytes() == content_before
