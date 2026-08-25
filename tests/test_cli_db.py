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
