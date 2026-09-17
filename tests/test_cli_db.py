"""`stdf db query` / `db lots` / `db shell` CLI behavior."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from stdf_platform.cli import main


def _write_config(tmp_path: Path, data_dir: Path) -> Path:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"storage:\n  data_dir: {data_dir.as_posix()}\n")
    return cfg


def test_db_query_writes_csv(tmp_path, synth_store):
    out = tmp_path / "o.csv"
    r = CliRunner().invoke(main, [
        "db", "query", "SELECT lot_id FROM lots ORDER BY lot_id",
        "-o", str(out)],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code == 0, r.output
    assert out.read_text().splitlines()[0] == "lot_id"


@pytest.mark.parametrize("sql_arg, use_file, ok", [
    (False, True, True),     # -f FILE
    (True, True, False),     # both SQL and -f -> usage error
    (False, False, False),   # neither -> usage error
], ids=["file_only", "both", "neither"])
def test_db_query_sql_or_file_argument(tmp_path, synth_store, sql_arg, use_file, ok):
    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT COUNT(*) AS n FROM lots")
    args = ["db", "query"]
    if sql_arg:
        args.append("SELECT 1")
    if use_file:
        args += ["-f", str(sql_file)]
    r = CliRunner().invoke(main, args,
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert (r.exit_code == 0) is ok, r.output


def test_db_lots_lot_with_no_parts_renders_zero(tmp_path, synth_store):
    """A lot with a `runs` row but zero rows in `wafer_yield_final` (e.g. an
    MIR/WIR-only run, no probed dies) must render 0 / 0.00%, not crash.

    lot_summary()'s LEFT JOIN against wafer_yield_final leaves
    wafer_count/total_parts/good_parts/yield_pct NULL for such a lot;
    .fetchdf() surfaces that as pd.NA, which used to blow up `v or 0`
    with `TypeError: boolean value of NA is ambiguous`.
    """
    from conftest import _cp_run
    from stdf_platform.config import StorageConfig
    from stdf_platform.storage import ParquetStorage

    storage = ParquetStorage(StorageConfig(data_dir=synth_store, database=synth_store / "db.duckdb"))
    storage.save_stdf_data(
        _cp_run("LOTNOPARTS", "W1", "JOB", "RevA", 1000, 2000, parts=[], test_results=[]),
        product="PROD", test_category="CP", sub_process="CP1", source_file="norun.stdf",
    )

    r = CliRunner().invoke(main, ["db", "lots"],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code == 0, r.output
    assert "LOTNOPARTS" in r.output
    assert "ambiguous" not in r.output
    assert "0.00%" in r.output


def test_db_query_renders_nulls_and_array_cells(tmp_path, synth_store):
    """NULLs render blank (not pandas' "<NA>" / "nan"), and LIST cells render
    their contents: pd.isna() on a multi-element numpy array raises
    ValueError, which a size-1 array happens not to trigger."""
    r = CliRunner().invoke(main, [
        "db", "query",
        "SELECT [1,2,3] AS arr, [1] AS single, NULL::BIGINT AS n, NULL::DOUBLE AS f"],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code == 0, r.output
    assert "1" in r.output and "2" in r.output and "3" in r.output
    assert "<NA>" not in r.output
    assert "nan" not in r.output.lower()


def test_db_query_honors_gross_die_via_dash_c(tmp_path, synth_store):
    """The CLI's -c config (not STDF_CONFIG/cwd) must reach AnalysisSession's
    gross_die table. Regression test: AnalysisSession used to silently
    re-resolve its own Config.load() instead of the CLI's already-resolved
    ctx.obj["config"], so CLI/serve computed GD-less yields.
    """
    cfg_path = tmp_path / "gd_config.yaml"
    cfg_path.write_text(
        f"storage:\n  data_dir: {synth_store.as_posix()}\n"
        "products:\n  PROD:\n    gross_die: 500\n    gd_fail_bin: 200\n"
    )
    r = CliRunner().invoke(main, [
        "-c", str(cfg_path), "db", "query", "SELECT product, gross_die FROM gross_die"])
    assert r.exit_code == 0, r.output
    assert "PROD" in r.output
    assert "500" in r.output


# ── db shell: 永続カタログの再利用 ────────────────────────────────────

def test_db_shell_reuses_registered_views(tmp_path, synth_store, monkeypatch):
    """2回目以降の `stdf db shell` はビューを再登録しない。

    登録はストアの Parquet ファイル数に比例する。ビュー自体が glob なので、
    登録後に ingest したデータも再登録なしで見える — 再登録が要るのは
    「あるべきビューの集合」が変わったときだけ。
    """
    launched = []
    monkeypatch.setattr("subprocess.run", lambda *a, **k: launched.append(a))
    env = {"STDF_CONFIG": str(_write_config(tmp_path, synth_store))}

    r1 = CliRunner().invoke(main, ["db", "shell"], env=env)
    assert r1.exit_code == 0, r1.output
    assert "Registered" in r1.output

    r2 = CliRunner().invoke(main, ["db", "shell"], env=env)
    assert r2.exit_code == 0, r2.output
    assert "Reusing" in r2.output
    assert len(launched) == 2                      # どちらも duckdb CLI を起動している


def test_db_shell_reregisters_when_gross_die_changes(tmp_path, synth_store, monkeypatch):
    """あるべきビューの中身が変わったら、キャッシュを使わず登録し直す。

    gross_die は `gross_die` テーブルと `wafer_yield_final` の定義を変えるので、
    fingerprint に含まれる。
    """
    monkeypatch.setattr("subprocess.run", lambda *a, **k: None)
    plain = _write_config(tmp_path, synth_store)
    CliRunner().invoke(main, ["db", "shell"], env={"STDF_CONFIG": str(plain)})

    gd = tmp_path / "gd.yaml"
    gd.write_text(
        f"storage:\n  data_dir: {synth_store.as_posix()}\n"
        f"  database: {(synth_store / 'stdf.duckdb').as_posix()}\n"
        "products:\n  PROD:\n    gross_die: 500\n    gd_fail_bin: 200\n"
    )
    r = CliRunner().invoke(main, ["db", "shell"], env={"STDF_CONFIG": str(gd)})
    assert "Registered" in r.output, r.output        # 再利用ではなく登録し直す


def test_db_shell_drops_stale_views(tmp_path, synth_store, monkeypatch):
    """永続カタログに残った、いまは定義されていないビューを落とす。

    setup_views() は CREATE OR REPLACE しかしないので、一度でも作られたビューは
    storage.database に残り続ける。参照先の Parquet が消えると引いた瞬間に
    IOException になる(残っていれば古いデータを黙って返す)。
    """
    import duckdb
    monkeypatch.setattr("subprocess.run", lambda *a, **k: None)
    db_path = tmp_path / "shell.duckdb"
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"storage:\n  data_dir: {synth_store.as_posix()}\n"
                   f"  database: {db_path.as_posix()}\n")
    env = {"STDF_CONFIG": str(cfg)}
    CliRunner().invoke(main, ["db", "shell"], env=env)

    con = duckdb.connect(str(db_path))
    con.execute("CREATE VIEW leftover_mart AS SELECT 1 AS x")   # 過去の登録を模す
    # 過去の登録一覧に含めておく。一覧があるときは「我々が作った残骸」だけを
    # 落とし、ユーザーが shell で作ったビューを巻き込まない。
    con.execute(
        "UPDATE _stdf_mount_state "
        "SET fingerprint = 'stale', "
        "    registered = registered || ',leftover_mart'"
    )
    con.close()

    r = CliRunner().invoke(main, ["db", "shell", "--refresh"], env=env)
    assert r.exit_code == 0, r.output
    assert "leftover_mart" in r.output and "stale" in r.output

    con = duckdb.connect(str(db_path))
    names = {row[0] for row in
             con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()}
    con.close()
    assert "leftover_mart" not in names
    assert "parts_final" in names        # 現行のビューは残っている
