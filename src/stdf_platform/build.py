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


def _dbt(cmd: list[str], env_extra: dict[str, str]) -> None:
    import os
    env = os.environ.copy()
    env.update(env_extra)
    r = subprocess.run(
        ["uv", "run", "dbt", *cmd,
         "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        env=env, capture_output=True, text=True, cwd=REPO_ROOT,
    )
    if r.returncode != 0:
        raise BuildError(f"dbt {cmd[0]} failed:\n{r.stdout}\n{r.stderr}")


def _replace_dir_with_retry(src: Path, dst: Path, attempts: int = 5) -> None:
    old = dst.with_name(dst.name + "_old")
    for i in range(attempts):
        try:
            if old.exists():
                shutil.rmtree(old)
            if dst.exists():
                dst.rename(old)
            src.rename(dst)
            if old.exists():
                shutil.rmtree(old, ignore_errors=True)
            return
        except OSError:
            if i == attempts - 1:
                raise
            time.sleep(0.5 * (i + 1))


def run_build(config: Config, *, select: str | None = None) -> None:
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
    sel = ["--select", select] if select else []

    _dbt(["run", *sel, *vars_arg], env)
    _dbt(["test", *vars_arg], env)

    build_db.unlink(missing_ok=True)  # ビルドDBは出荷しない
    _replace_dir_with_retry(build_dir, data_dir / "marts")
