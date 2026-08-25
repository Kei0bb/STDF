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


def test_db_query_usage_error_neither_sql_nor_file(tmp_path, synth_store):
    r = CliRunner().invoke(main, ["db", "query"],
        env={"STDF_CONFIG": str(_write_config(tmp_path, synth_store))})
    assert r.exit_code != 0
