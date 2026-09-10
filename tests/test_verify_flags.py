"""`stdf db verify` — test_data の retest_flag 不変条件。

storage.py が ingest 時に確定させるフラグが壊れていると、test_data_final
(= retest_flag = 0)が黙って誤った行集合を返す。この4条件は元々
`stdf db verify-flags` にあり、dbt 導入時に dbt/tests/assert_*.sql へ移り、
dbt 撤去で再び CLI に戻ってきたもの。SQL は mounts.FLAG_INVARIANTS が単一の
定義を持つ。
"""

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from stdf_platform.cli import main
from stdf_platform.mounts import FLAG_INVARIANTS, setup_views


def _config(tmp_path: Path, data_dir: Path) -> Path:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"storage:\n  data_dir: {data_dir.as_posix()}\n")
    return cfg


def test_verify_passes_on_clean_store(tmp_path, synth_store):
    r = CliRunner().invoke(main, ["db", "verify"],
                           env={"STDF_CONFIG": str(_config(tmp_path, synth_store))})
    assert r.exit_code == 0, r.output
    assert "すべての不変条件を満たしています" in r.output
    for name, _, _ in FLAG_INVARIANTS:
        assert name in r.output


def test_verify_fails_on_null_retest_flag(tmp_path, corrupt_store):
    r = CliRunner().invoke(main, ["db", "verify"],
                           env={"STDF_CONFIG": str(_config(tmp_path, corrupt_store))})
    assert r.exit_code == 1, r.output
    assert "null_flags" in r.output
    assert "再 ingest" in r.output


def test_verify_detects_orphaned_key(tmp_path, synth_store):
    """あるキーについて flag=0 の行がどこにも無い状態を検出する。

    conftest の _write_null_flag_row と同じ書き方で、synth_store の他の行に
    干渉しない独立したキーを1本だけ足す(フラグは 1 のみ = 最新 run が
    flag=0 になっていない)。
    """
    from conftest import TEST_DATA_SCHEMA

    row = {
        "lot_id": ["LOTORPHAN"], "wafer_id": ["W9"], "part_id": ["P9"], "part_txt": [""],
        "x_coord": [9], "y_coord": [9], "test_num": [1], "test_name": ["VCC"],
        "rec_type": ["PTR"], "lo_limit": [0.9], "hi_limit": [1.1], "units": ["V"],
        "result": [1.0], "passed": ["P"], "retest_num": [0],
        "pin_num": pa.array([None], type=pa.int64()), "pin_name": [None],
        "exec_seq": [0], "retest_flag": [1],          # ← flag=0 の行が無い
    }
    path = (
        synth_store / "test_data" / "product=PROD" / "test_category=CP"
        / "sub_process=CP1" / "lot_id=LOTORPHAN" / "wafer_id=W9" / "retest=0"
        / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(row, schema=TEST_DATA_SCHEMA), path)

    r = CliRunner().invoke(main, ["db", "verify"],
                           env={"STDF_CONFIG": str(_config(tmp_path, synth_store))})
    assert r.exit_code == 1, r.output
    assert "orphaned_keys" in r.output


def test_invariant_sql_is_valid_against_a_real_store(synth_store):
    """4本の SQL が実際に実行できること(構文・列名の腐り検出)。"""
    conn = duckdb.connect(":memory:")
    setup_views(conn, synth_store)
    for name, _, sql in FLAG_INVARIANTS:
        conn.execute(sql).fetchall()      # 例外が出なければよい
