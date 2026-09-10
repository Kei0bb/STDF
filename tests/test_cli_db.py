"""`stdf db query` -o/-f options + the `analyze` command group removal.

See task-7-brief.md (CLI スリム化 + database.py 削除).
"""

from pathlib import Path

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


def test_db_query_from_file(tmp_path, synth_store):
    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT COUNT(*) AS n FROM lots")
    r = CliRunner().invoke(main, ["db", "query", "-f", str(sql_file)],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code == 0, r.output


def test_analyze_group_removed():
    r = CliRunner().invoke(main, ["analyze", "yield", "LOT1"])
    assert r.exit_code != 0   # コマンドが存在しない


# ── review round 1: pd.NA / NaN rendering fixes ────────────────────────

def test_db_lots_lot_with_no_parts_renders_zero(tmp_path, synth_store):
    """A lot with a `runs` row but zero rows in `wafer_yield_final` (e.g. an
    MIR/WIR-only run, no probed dies) must render 0 / 0.00%, not crash.

    lot_summary()'s LEFT JOIN against wafer_yield_final leaves
    wafer_count/total_parts/good_parts/yield_pct NULL for such a lot;
    .fetchdf() surfaces that as pd.NA (not the plain None the old
    dict-based Database.query() gave), which used to blow up `v or 0`
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


def test_db_query_prints_null_without_na_literal(tmp_path, synth_store):
    """A NULL in an integer result column must render blank, not the pandas
    nullable-dtype repr "<NA>" (or "nan" for a float column)."""
    r = CliRunner().invoke(main, [
        "db", "query", "SELECT NULL::BIGINT AS n, 1 AS x"],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code == 0, r.output
    assert "<NA>" not in r.output
    assert "nan" not in r.output.lower()


def test_db_query_prints_array_cell_and_null_together(tmp_path, synth_store):
    """A DuckDB LIST/ARRAY result column comes back from .fetchdf() as a
    multi-element numpy array; pd.isna() on that raises ValueError ("The
    truth value of an array with more than one element is ambiguous"),
    unlike a single-element array (numpy allows bool() on size-1 arrays,
    so that case "coincidentally" worked before the ndim guard). Pins both:
    the 3-element array must render its contents, the 1-element array too,
    and the NULL column alongside them must still render blank.
    """
    r = CliRunner().invoke(main, [
        "db", "query",
        "SELECT [1,2,3] AS arr, [1] AS single, NULL::INTEGER AS n"],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code == 0, r.output
    assert "1" in r.output and "2" in r.output and "3" in r.output
    assert "<NA>" not in r.output


def test_db_query_usage_error_both_sql_and_file(tmp_path, synth_store):
    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT 1")
    r = CliRunner().invoke(main, ["db", "query", "SELECT 1", "-f", str(sql_file)],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code != 0


def test_db_query_honors_gross_die_via_dash_c(tmp_path, synth_store):
    """The CLI's -c config (not STDF_CONFIG/cwd) must reach AnalysisSession's
    gross_die table. Regression test: AnalysisSession used to silently
    re-resolve its own Config.load() instead of the CLI's already-resolved
    ctx.obj["config"], so a -c config's gross_die_map never reached the
    session — CLI/serve computed GD-less yields while `stdf build` built the
    marts WITH gross die.
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


def test_db_query_usage_error_neither_sql_nor_file(tmp_path, synth_store):
    r = CliRunner().invoke(main, ["db", "query"],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code != 0


# ── db shell: 永続カタログの再利用 ────────────────────────────────────

def test_db_shell_reuses_registered_views(tmp_path, synth_store, monkeypatch):
    """2回目以降の `stdf db shell` はビューを再登録しない。

    登録はストアの Parquet ファイル数に比例し、*_final / lots /
    wafer_yield_final がベースビューの glob を再バインドするため実質2周ぶん
    かかる。ビュー自体が glob なので、登録後に ingest したデータも再登録なし
    で見える — 再登録が要るのは「あるべきビューの集合」が変わったときだけ。
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


def test_db_shell_refresh_forces_reregistration(tmp_path, synth_store, monkeypatch):
    monkeypatch.setattr("subprocess.run", lambda *a, **k: None)
    env = {"STDF_CONFIG": str(_write_config(tmp_path, synth_store))}
    CliRunner().invoke(main, ["db", "shell"], env=env)
    r = CliRunner().invoke(main, ["db", "shell", "--refresh"], env=env)
    assert r.exit_code == 0, r.output
    assert "Registered" in r.output and "Reusing" not in r.output


def test_db_shell_reregisters_when_a_mart_appears(tmp_path, synth_store, monkeypatch):
    """新しいマートが増えたら、キャッシュを使わず登録し直す。"""
    import pyarrow as pa
    import pyarrow.parquet as pq
    monkeypatch.setattr("subprocess.run", lambda *a, **k: None)
    env = {"STDF_CONFIG": str(_write_config(tmp_path, synth_store))}
    CliRunner().invoke(main, ["db", "shell"], env=env)

    marts = synth_store / "marts"
    marts.mkdir(exist_ok=True)
    pq.write_table(pa.table({"lot_id": ["L1"]}), marts / "brand_new_mart.parquet")

    r = CliRunner().invoke(main, ["db", "shell"], env=env)
    assert "Registered" in r.output, r.output
    assert "brand_new_mart" in r.output


def test_store_fingerprint_tracks_tables_marts_and_gross_die(tmp_path, synth_store):
    from stdf_platform.mounts import store_fingerprint
    base = store_fingerprint(synth_store, {})
    assert base == store_fingerprint(synth_store, {})            # 安定
    assert base != store_fingerprint(synth_store, {"P": (100, 200)})   # GD 変化を検出
