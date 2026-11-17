"""`stdf ingest` / `stdf ingest-all` entry behavior: file selection, .gz,
bad files, --allow-truncated, exit codes, per-file ingest history."""

import gzip
import json
import shutil
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from make_test_stdf import make_stdf
from stdf_platform import worker
from stdf_platform.cli import _collect_ingest_files, main


@pytest.fixture
def env(tmp_path):
    data = tmp_path / "data"
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"storage:\n  data_dir: {data.as_posix()}\n"
        f"  database: {(data / 'stdf.duckdb').as_posix()}\n"
        f"  download_dir: {(tmp_path / 'dl').as_posix()}\n"
    )
    src = tmp_path / "src"
    src.mkdir()
    return {"cfg": cfg, "data": data, "src": src}


def _mk(path: Path, lot: str) -> Path:
    make_stdf(path, lot, num_wafers=1, parts_per_wafer=5)
    return path


def _gz(path: Path) -> Path:
    out = path.with_name(path.name + ".gz")
    with open(path, "rb") as f, gzip.open(out, "wb") as g:
        shutil.copyfileobj(f, g)
    path.unlink()
    return out


def _run(env, *args):
    return CliRunner().invoke(main, ["--config", str(env["cfg"]), *args])


def _history(env) -> set[str]:
    f = env["data"] / "ingest_history.json"
    if not f.exists():
        return set()
    return {Path(k).name for k in json.loads(f.read_text())["ingested"]}


def _runs(env) -> list[tuple[str, str]]:
    glob = (env["data"] / "runs" / "**" / "*.parquet").as_posix()
    rows = duckdb.sql(
        f"SELECT lot_id, source_file FROM read_parquet('{glob}', union_by_name=true) "
        "ORDER BY lot_id").fetchall()
    return rows


def _truncate(path: Path, cut: int = 5) -> None:
    b = path.read_bytes()
    path.write_bytes(b[:-cut])


def test_default_selection_and_recursive(tmp_path):
    root = tmp_path / "d"
    (root / "sub").mkdir(parents=True)
    (root / "_corrupt").mkdir()
    (root / "sub" / "_x").mkdir()
    for n in ["a.stdf", "b.STD", "c.stdf.gz", "d.Std.GZ", "e.txt", "f.stdf.bak"]:
        (root / n).write_bytes(b"x")
    (root / "sub" / "g.std").write_bytes(b"x")
    (root / "sub" / "_x" / "h.std").write_bytes(b"x")
    (root / "_corrupt" / "i.stdf.gz").write_bytes(b"x")

    flat = [p.name for p in _collect_ingest_files(root, None, False)]
    assert flat == ["a.stdf", "b.STD", "c.stdf.gz", "d.Std.GZ"]

    rec = [p.name for p in _collect_ingest_files(root, None, True)]
    assert sorted(rec) == ["a.stdf", "b.STD", "c.stdf.gz", "d.Std.GZ", "g.std"]

    # --glob override keeps working, and recursion still skips `_` dirs
    assert [p.name for p in _collect_ingest_files(root, "*.stdf.bak", False)] == ["f.stdf.bak"]
    assert sorted(p.name for p in _collect_ingest_files(root, "*.std*", True)) == [
        "a.stdf", "c.stdf.gz", "f.stdf.bak", "g.std"]


def test_recursive_flag_skips_underscore_dirs(env):
    src = env["src"]
    (src / "_corrupt").mkdir()
    (src / "sub").mkdir()
    _mk(src / "sub" / "LOTA_1@CP11_1#202601010000.stdf", "LOTA")
    _mk(src / "_corrupt" / "LOTB_1@CP11_1#202601010000.stdf", "LOTB")

    r = _run(env, "ingest-all", str(src), "-p", "P", "-r")
    assert r.exit_code == 0, r.output
    assert [lot for lot, _ in _runs(env)] == ["LOTA"]

    # without -r the subdirectories are not visited at all
    env["data"].joinpath("ingest_history.json").unlink()
    r = _run(env, "ingest-all", str(src), "-p", "P")
    assert "No files matching" in r.output


def test_ingest_all_gz_and_std_source_file_without_gz(env):
    src = env["src"]
    _gz(_mk(src / "LOTA_1@CP11_1#202601010000.stdf", "LOTA"))
    _mk(src / "LOTB_1@CP11_1#202601010000.std", "LOTB")
    _gz(_mk(src / "LOTC_1@CP11_1#202601010000.std", "LOTC"))

    r = _run(env, "ingest-all", str(src), "-p", "P")
    assert r.exit_code == 0, r.output
    # source_file drops .gz: the same name fetch stores for its decompressed copy
    assert _runs(env) == [
        ("LOTA", "LOTA_1@CP11_1#202601010000.stdf"),
        ("LOTB", "LOTB_1@CP11_1#202601010000.std"),
        ("LOTC", "LOTC_1@CP11_1#202601010000.std"),
    ]
    assert _history(env) == {
        "LOTA_1@CP11_1#202601010000.stdf.gz",
        "LOTB_1@CP11_1#202601010000.std",
        "LOTC_1@CP11_1#202601010000.std.gz",
    }


def test_garbage_and_corrupt_gz_fail_but_batch_continues(env):
    src = env["src"]
    (src / "BAD1_1@CP11_1#202601010000.stdf").write_bytes(b"this is not an stdf file" * 20)
    good = _gz(_mk(src / "BAD2_1@CP11_1#202601010000.stdf", "BAD2"))
    good.write_bytes(good.read_bytes()[:-20])           # truncated gzip stream
    (src / "BAD3_1@CP11_1#202601010000.std.gz").write_bytes(b"not gzip at all")
    _mk(src / "OK_1@CP11_1#202601010000.stdf", "OKLOT")

    r = _run(env, "ingest-all", str(src), "-p", "P")
    assert r.exit_code == 1, r.output
    assert _runs(env) == [("OKLOT", "OK_1@CP11_1#202601010000.stdf")]
    assert _history(env) == {"OK_1@CP11_1#202601010000.stdf"}
    failed = json.loads((env["data"] / "ingest_failures.json").read_text())["failures"]
    assert len(failed) == 3
    assert not list(Path(__import__("tempfile").gettempdir()).glob("stdf_gz_*"))


def test_truncated_rejected_then_accepted_with_flag(env):
    src = env["src"]
    f = _mk(src / "LOTT_1@CP11_1#202601010000.stdf", "LOTT")
    _truncate(f)

    r = _run(env, "ingest-all", str(src), "-p", "P")
    assert r.exit_code == 1, r.output
    assert _history(env) == set()
    assert not (env["data"] / "runs").exists() or _runs(env) == []

    r = _run(env, "ingest-all", str(src), "-p", "P", "--allow-truncated")
    assert r.exit_code == 0, r.output
    assert [lot for lot, _ in _runs(env)] == ["LOTT"]
    assert _history(env) == {f.name}


def test_single_ingest_gz_stores_name_without_gz(env):
    gz = _gz(_mk(env["src"] / "LOTS_1@CP11_1#202601010000.stdf", "LOTS"))
    r = _run(env, "ingest", str(gz), "-p", "P")
    assert r.exit_code == 0, r.output
    assert _runs(env) == [("LOTS", gz.name[:-3])]


def test_single_ingest_failure_exits_1(env):
    bad = env["src"] / "junk.stdf"
    bad.write_bytes(b"\x00\x01\x02" * 50)
    r = _run(env, "ingest", str(bad), "-p", "P")
    assert r.exit_code == 1, r.output

    trunc = _mk(env["src"] / "LOTX_1@CP11_1#202601010000.stdf", "LOTX")
    _truncate(trunc)
    assert _run(env, "ingest", str(trunc), "-p", "P").exit_code == 1
    assert _run(env, "ingest", str(trunc), "-p", "P", "--allow-truncated").exit_code == 0


def test_history_is_written_per_file_before_batch_ends(env, monkeypatch):
    src = env["src"]
    a = src / "A_1@CP11_1#202601010000.stdf"
    b = src / "B_1@CP11_1#202601010000.stdf"
    a.write_bytes(b"x")
    b.write_bytes(b"x")
    calls = []

    def fake_single(local_path, *a_, **kw):
        calls.append(local_path.name)
        if len(calls) == 1:
            return worker.IngestResult(local_path=local_path, remote_path=None, success=True)
        # second file: the whole run dies mid-batch
        assert _history(env) == {a.name}, "first success must already be on disk"
        raise RuntimeError("crash")

    monkeypatch.setattr(worker, "_run_single", fake_single)
    r = _run(env, "ingest-all", str(src), "-p", "P", "-w", "1")
    assert r.exit_code != 0
    assert _history(env) == {a.name}


def test_single_ingest_sub_process_override(env):
    f = _mk(env["src"] / "LOTO_1@CP11_1#202601010000.stdf", "LOTO")
    r = _run(env, "ingest", str(f), "-p", "P", "-s", "CP2")
    assert r.exit_code == 0, r.output
    glob = (env["data"] / "runs" / "**" / "*.parquet").as_posix()
    assert duckdb.sql(
        f"SELECT DISTINCT sub_process FROM read_parquet('{glob}')").fetchall() == [("CP2",)]


def test_gz_and_plain_copy_of_one_file_are_one_run(env):
    plain = _mk(env["src"] / "LOTA_1@CP11_1#202601010000.stdf", "LOTA")
    assert _run(env, "ingest", str(plain), "-p", "P").exit_code == 0
    gz = _gz(plain)
    assert _run(env, "ingest", str(gz), "-p", "P").exit_code == 0
    assert _runs(env) == [("LOTA", "LOTA_1@CP11_1#202601010000.stdf")]
    retests = {p.name for p in (env["data"] / "runs").rglob("retest=*")}
    assert retests == {"retest=0"}
