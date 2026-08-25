"""dbt singular tests (Step 3 of the brief) reproduce cli.py `verify-flags`.

A corrupt store (a test_data row with retest_flag IS NULL, from a pre-flag
file — see storage.py / views.py's `test_data_final` docstring) must fail
`dbt test`; a clean store must pass. This is the dbt-side counterpart to
tests/test_verify_flags.py's CLI-level checks.
"""
import os
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from stdf_platform.storage import TEST_DATA_SCHEMA
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


def _write_null_flag_row(data_dir: Path) -> None:
    """Write one test_data row from a pre-retest_flag file: same shape as
    test_verify_flags.py's `old_schema` case (exec_seq/retest_flag columns
    entirely absent, not just NULL-valued) for a lot/wafer/die not otherwise
    in synth_store, so it doesn't disturb the other marts fixtures.
    """
    old_schema = pa.schema([f for f in TEST_DATA_SCHEMA if f.name not in ("exec_seq", "retest_flag")])
    row = {
        "lot_id": ["LOTCORRUPT"], "wafer_id": ["WBAD"], "part_id": ["PBAD"], "part_txt": [""],
        "x_coord": [9], "y_coord": [9], "test_num": [1], "test_name": ["VCC"],
        "rec_type": ["PTR"], "lo_limit": [0.9], "hi_limit": [1.1], "units": ["V"],
        "result": [1.0], "passed": ["P"], "retest_num": [0],
        "pin_num": pa.array([None], type=pa.int64()), "pin_name": [None],
    }
    path = (
        data_dir / "test_data" / "product=PROD" / "test_category=CP" / "sub_process=CP1"
        / "lot_id=LOTCORRUPT" / "wafer_id=WBAD" / "retest=0" / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(row, schema=old_schema), path)


def test_dbt_test_fails_on_corrupt_store(tmp_path, synth_store):
    _write_null_flag_row(synth_store)
    build_db = tmp_path / "b.duckdb"
    _run_dbt(synth_store, build_db, tmp_path, select="staging marts")

    r = _run_dbt_test(synth_store, build_db, tmp_path)
    assert r.returncode != 0, r.stdout + r.stderr
    assert "assert_no_null_retest_flag" in (r.stdout + r.stderr)


def test_dbt_test_passes_on_clean_store(tmp_path, synth_store):
    build_db = tmp_path / "b.duckdb"
    _run_dbt(synth_store, build_db, tmp_path, select="staging marts")

    r = _run_dbt_test(synth_store, build_db, tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
