"""dbt singular tests (Step 3 of the brief) reproduce cli.py `verify-flags`.

A corrupt store (a test_data row with retest_flag IS NULL, from a pre-flag
file — see storage.py / views.py's `test_data_final` docstring) must fail
`dbt test`; a clean store must pass. This is the dbt-side counterpart to
tests/test_verify_flags.py's CLI-level checks.

The corrupt-store fixture lives in tests/conftest.py as `corrupt_store`
(shared with tests/test_build.py, Task 5).
"""
import os
import subprocess
from pathlib import Path

from test_dbt_staging_parity import _run_dbt

REPO = Path(__file__).resolve().parent.parent


def _run_dbt_test(data_dir: Path, build_db: Path, tmp_path: Path) -> subprocess.CompletedProcess:
    marts_dir = tmp_path / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update({
        "STDF_DATA_DIR": data_dir.as_posix(),
        "STDF_BUILD_DB": build_db.as_posix(),
        "STDF_MARTS_DIR": marts_dir.as_posix(),
    })
    cmd = [
        "uv", "run", "dbt", "test",
        "--project-dir", str(REPO / "dbt"), "--profiles-dir", str(REPO / "dbt"),
    ]
    return subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=REPO)


def test_dbt_test_fails_on_corrupt_store(tmp_path, corrupt_store):
    build_db = tmp_path / "b.duckdb"
    _run_dbt(corrupt_store, build_db, tmp_path, select="staging marts")

    r = _run_dbt_test(corrupt_store, build_db, tmp_path)
    assert r.returncode != 0, r.stdout + r.stderr
    assert "assert_no_null_retest_flag" in (r.stdout + r.stderr)


def test_dbt_test_passes_on_clean_store(tmp_path, synth_store):
    build_db = tmp_path / "b.duckdb"
    _run_dbt(synth_store, build_db, tmp_path, select="staging marts")

    r = _run_dbt_test(synth_store, build_db, tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
