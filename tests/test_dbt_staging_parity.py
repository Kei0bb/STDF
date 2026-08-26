"""dbt staging と mounts.py(runtime views)のセマンティクス一致を機械検証する。"""
import os
import subprocess
from pathlib import Path

import duckdb

from stdf_platform.mounts import setup_views

REPO = Path(__file__).resolve().parent.parent


def _run_dbt(data_dir: Path, build_db: Path, tmp_path: Path,
             select: str = "staging", extra_args: list[str] | None = None) -> None:
    marts_dir = tmp_path / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update({
        "STDF_DATA_DIR": data_dir.as_posix(),
        "STDF_BUILD_DB": build_db.as_posix(),
        "STDF_MARTS_DIR": marts_dir.as_posix(),
    })
    cmd = [
        "uv", "run", "dbt", "run", "--select", select,
        "--project-dir", str(REPO / "dbt"), "--profiles-dir", str(REPO / "dbt"),
    ]
    if extra_args:
        cmd += extra_args
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=REPO)
    assert r.returncode == 0, r.stdout + r.stderr


PAIRS = [  # (mounts.py 側ビュー, dbt staging モデル, ソート列)
    # x_coord/y_coord だけだと FT(x=y=-32768 センチネル)の複数パッケージが
    # 同着になり順序が不定になるため、part_id まで含めてソートキーを一意に
    # 近づける。
    ("parts_final", "stg_parts_final", "lot_id, wafer_id, x_coord, y_coord, part_id"),
    # test_num だけだと同一キーで複数行(loop測定/PTR+MPR混在)が起きうり同着で
    # 順序が不定になるため、part_id まで含めてソートキーを一意に近づける。
    ("test_data_final", "stg_test_data_final", "lot_id, wafer_id, test_num, part_id"),
    ("lots", "stg_lots", "lot_id"),
    ("wafer_yield_final", "stg_wafer_yield", "lot_id, wafer_id"),
    ("chipid_final", "stg_chipid_final", "lot_id, efuse_raw"),
]


def test_staging_matches_runtime_views(tmp_path, synth_store):
    # synth_store: parts(2リテスト) + runs + test_data(retest_flag付き)を書く
    #              fixture(tests/conftest.py)。
    build_db = tmp_path / "build.duckdb"
    _run_dbt(synth_store, build_db, tmp_path)

    runtime = duckdb.connect(":memory:")
    setup_views(runtime, synth_store)
    build = duckdb.connect(str(build_db), read_only=True)

    for view, model, order in PAIRS:
        a = runtime.execute(f"SELECT * FROM {view} ORDER BY {order}").fetchall()
        b = build.execute(f"SELECT * FROM {model} ORDER BY {order}").fetchall()
        assert a == b, f"{view} != {model}"


def test_staging_matches_runtime_views_with_gross_die(tmp_path, synth_store):
    """gross_die_map 設定時、total/unprobed の一致を確認する。"""
    build_db = tmp_path / "build.duckdb"
    _run_dbt(
        synth_store, build_db, tmp_path, select="staging",
        extra_args=["--vars", "{gross_die_map: {PROD: {gross_die: 5, gd_fail_bin: 200}}}"],
    )

    runtime = duckdb.connect(":memory:")
    setup_views(runtime, synth_store, gross_die_map={"PROD": (5, 200)})
    build = duckdb.connect(str(build_db), read_only=True)

    a = runtime.execute(
        "SELECT * FROM wafer_yield_final ORDER BY lot_id, wafer_id"
    ).fetchall()
    b = build.execute(
        "SELECT * FROM stg_wafer_yield ORDER BY lot_id, wafer_id"
    ).fetchall()
    assert a == b, "wafer_yield_final != stg_wafer_yield (gross_die case)"
