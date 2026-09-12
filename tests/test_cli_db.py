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
    session — CLI/serve then computed GD-less yields, silently disagreeing
    with any other caller that passed the config properly.
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


def test_store_fingerprint_tracks_tables_and_gross_die(tmp_path, synth_store):
    from stdf_platform.mounts import store_fingerprint
    base = store_fingerprint(synth_store, {})
    assert base == store_fingerprint(synth_store, {})            # 安定
    assert base != store_fingerprint(synth_store, {"P": (100, 200)})   # GD 変化を検出


def test_db_shell_drops_stale_views(tmp_path, synth_store, monkeypatch):
    """永続カタログに残った、いまは定義されていないビューを落とす。

    setup_views() は CREATE OR REPLACE しかしないので、一度でも作られたビューは
    storage.database に残り続ける。dbt 時代のマートビューがこれで、参照先の
    Parquet が消えると引いた瞬間に IOException になる(残っていれば古いデータを
    黙って返す、というもっと悪い挙動もある)。
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
    # 過去の登録一覧に含めておく(dbt 時代のマート等)。一覧が無い旧 DB では
    # 非登録ビュー全部を落とすが、一覧があるときは「我々が作った残骸」だけを
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


def test_stale_views_preserves_user_views(tmp_path):
    import duckdb
    from stdf_platform.cli import _stale_views

    conn = duckdb.connect()
    conn.execute("CREATE VIEW parts_final AS SELECT 1 AS x")
    conn.execute("CREATE VIEW user_view AS SELECT 1 AS y")

    # 前回登録が分かる場合: 我々が作った previous の残骸だけ落とす
    assert _stale_views(conn, ["parts_final"],
                        ["parts_final", "lot_summary"]) == ["lot_summary"]
    # 前回状態が無い旧 DB: 従来通り非登録ビューを落とす
    assert _stale_views(conn, ["parts_final"], None) == ["user_view"]
    conn.close()


def test_db_shell_explains_how_to_recover_from_a_broken_db_file(tmp_path, synth_store):
    """開けない永続DBは、トレースバックではなく復旧手順を出す。

    このファイルはビューのカタログを持つだけのキャッシュ(中身は data_dir から
    再生成できる)なので、正しい復旧は「消して再実行」。
    """
    db_path = tmp_path / "broken.duckdb"
    db_path.write_bytes(b"not a duckdb file" * 100)
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"storage:\n  data_dir: {synth_store.as_posix()}\n"
                   f"  database: {db_path.as_posix()}\n")

    r = CliRunner().invoke(main, ["db", "shell"], env={"STDF_CONFIG": str(cfg)})
    assert r.exit_code == 1
    assert "再生成可能なキャッシュ" in r.output
    assert str(db_path) in r.output
