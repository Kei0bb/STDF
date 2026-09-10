"""stdf build — dbt run + dbt test をラップし、marts をアトミックに差し替える。

出力は data/.marts_build/ に書き、dbt test まで通ってから data/marts/ と
入れ替える(旧 marts は .marts_old に退避して削除)。Windows では読み手が
ファイルを開いていると rename が失敗しうるため短いリトライを挟む。
"""
import json
import shutil
import subprocess
import time
from pathlib import Path

from .config import Config

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DBT_DIR = REPO_ROOT / "dbt"


class BuildError(RuntimeError):
    pass


def _dbt(cmd: list[str], env_extra: dict[str, str], echo: bool = True) -> None:
    """Run dbt, streaming its output as it arrives.

    Output used to be swallowed by capture_output=True and shown only on
    failure. On a real store a full-refresh build runs for a long time, so
    that gave no way to tell a slow model from a hung one — and if the run
    was interrupted, nothing had been printed at all. Lines are echoed live
    AND kept, so BuildError still carries the full log.
    """
    import os
    env = os.environ.copy()
    env.update(env_extra)
    proc = subprocess.Popen(
        ["uv", "run", "dbt", *cmd,
         "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        env=env, cwd=REPO_ROOT, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        lines.append(line)
        if echo:
            print(line, end="", flush=True)
    if proc.wait() != 0:
        raise BuildError(f"dbt {cmd[0]} failed:\n{''.join(lines)}")


def _replace_dir_with_retry(src: Path, dst: Path, attempts: int = 5) -> None:
    """Rename src -> dst, preserving the previous dst as a fallback.

    The dst -> old backup rename happens exactly ONCE (not inside the retry
    loop): only the src -> dst rename is retried. If every retry of
    src -> dst fails, the backup is restored to dst (old -> dst) before
    re-raising, so a failed build never leaves dst missing or in a
    half-swapped state — the previous good marts/ survives.
    """
    old = dst.with_name(dst.name + "_old")
    if old.exists():
        shutil.rmtree(old)
    moved_old = False
    if dst.exists():
        dst.rename(old)
        moved_old = True
    for i in range(attempts):
        try:
            src.rename(dst)
            if moved_old:
                shutil.rmtree(old, ignore_errors=True)
            return
        except OSError:
            if i == attempts - 1:
                if moved_old:
                    old.rename(dst)
                raise
            time.sleep(0.5 * (i + 1))


def run_build(config: Config, skip_tests: bool = False,
              threads: int | None = None, echo: bool = True) -> None:
    """Build the dbt marts and swap them into data/marts/.

    skip_tests drops the `dbt test` phase. Those four invariant tests
    (dbt/tests/assert_*.sql) each scan the whole test_data store — three of
    them without the retest_flag filter, so they read every retest
    generation — and group by the die-identity key. On a large store they
    dominate the build. They re-verify flags that storage.py already
    established at ingest time, so running them on every build is a choice,
    not a requirement: skip them for a routine mart refresh and run a full
    `stdf build` when the store has changed in ways worth re-validating.

    threads overrides the dbt thread count (profiles.yml sets 4). DuckDB
    already parallelizes within a single query, so concurrent models mostly
    compete for memory and can push large aggregations into spilling to
    disk; --threads 1 is often faster on a big store.
    """
    data_dir = config.storage.data_dir.resolve()
    build_dir = data_dir / ".marts_build"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)

    gd_vars = {
        p: {"gross_die": gd, "gd_fail_bin": fb}
        for p, (gd, fb) in (config.gross_die_map or {}).items()
    }
    # NOTE: filename must NOT start with "." — dbt-duckdb derives the
    # catalog name from the file stem, and a leading dot resolves to a
    # catalog dbt itself can't then bind to ("Binder Error: Catalog ".build"
    # does not exist!"), confirmed by manual `dbt run` repro. build_dir
    # itself (.marts_build) is already hidden, so this file doesn't need to
    # be too.
    build_db = build_dir / "build.duckdb"
    env = {
        "STDF_DATA_DIR": data_dir.as_posix(),
        "STDF_MARTS_DIR": build_dir.as_posix(),
        "STDF_BUILD_DB": build_db.as_posix(),
    }
    vars_arg = ["--vars", json.dumps({"gross_die_map": gd_vars})]
    # "+marts": marts and everything they depend on (staging models, tests
    # on those sources) — NOT "everything". This excludes stg_chipid_final,
    # which no mart references and which errors out on any store with no
    # chipid/ directory (CP-only products, fresh --env dev stores). Applying
    # the same selector to `dbt test` keeps tests consistent with what was
    # built; the four dbt/tests/assert_*.sql singular tests still run under
    # it because they depend on source('stdf','test_data'), which IS an
    # ancestor of the marts (via stg_test_data_final) — confirmed via
    # `dbt ls --select "+marts" --resource-type test`.
    sel = ["--select", "+marts"]
    if threads is not None:
        sel += ["--threads", str(threads)]

    _dbt(["run", *sel, *vars_arg], env, echo=echo)
    if not skip_tests:
        _dbt(["test", *sel, *vars_arg], env, echo=echo)

    build_db.unlink(missing_ok=True)  # ビルドDBは出荷しない
    _replace_dir_with_retry(build_dir, data_dir / "marts")
