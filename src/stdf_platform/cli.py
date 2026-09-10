"""CLI interface for STDF Platform."""

import sys
import gzip
import tempfile
import shutil
from pathlib import Path

import click
import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from . import __version__
from .config import Config
from .analysis import AnalysisSession
from .sync_manager import SyncManager


console = Console()


def _cell(v, default: str = "", fmt=str) -> str:
    """Render one DataFrame scalar for table display.

    `.fetchdf()` surfaces SQL NULL as `None`, float NaN, or pandas' nullable
    `pd.NA` depending on dtype — unlike the old dict-based `Database.query()`,
    which always gave plain `None`. `v or 0` / `v is not None` break on
    `pd.NA` (`TypeError: boolean value of NA is ambiguous`) or silently print
    "<NA>"/"nan". This is the one null check every itertuples()-based table
    renderer in this module goes through.

    `db query` runs arbitrary user SQL, so `v` may also be a DuckDB
    LIST/ARRAY result, which `.fetchdf()` hands back as a multi-element
    numpy array — `pd.isna()` on THAT raises `ValueError: The truth value
    of an array with more than one element is ambiguous`. `np.ndim(v) != 0`
    (true for any array/list, regardless of length) routes those straight
    to `fmt(v)` without ever reaching `pd.isna()`.
    """
    if np.ndim(v) != 0:
        return fmt(v)
    if v is None or pd.isna(v):
        return default
    return fmt(v)


@click.group()
@click.version_option(version=__version__, prog_name="stdf")
@click.option("--config", "-c", type=click.Path(path_type=Path), help="Config file path")
@click.option("--env", "-e", default=None, help="Environment name (e.g. dev). Isolates data to <data_dir>-{env}/ (e.g. var/data-dev/)")
@click.pass_context
def main(ctx, config: Path | None, env: str | None):
    """stdf - STDF to Parquet converter and analysis DB."""
    ctx.ensure_object(dict)
    cfg = Config.load(config)
    if env:
        cfg.storage = cfg.storage.with_env(env)
    ctx.obj["config"] = cfg
    ctx.obj["env"] = env


@main.command()
@click.argument("stdf_file", type=click.Path(exists=True, path_type=Path))
@click.option("--product", "-p", help="Product name (required unless --from-path)")
@click.option("--sub-process", "-s", help="Sub-process (CP11, FT2, etc. - default: from STDF MIR.TEST_COD)")
@click.option("--from-path", is_flag=True, help="Auto-detect product from file path (e.g. .../SCT101A/CP/...)")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def ingest(ctx, stdf_file: Path, product: str | None, sub_process: str | None, from_path: bool, verbose: bool):
    """
    Ingest an STDF file into the data platform.

    STDF_FILE: Path to the STDF file to ingest

    Product is specified via -p or auto-detected with --from-path.
    Sub-process is determined from STDF MIR.TEST_COD (e.g. CP1, FT2).
    """
    config: Config = ctx.obj["config"]
    config.ensure_directories()

    # Extract product from path (only when --from-path is specified)
    if product is None and from_path:
        parts = stdf_file.resolve().parts
        for i, part in enumerate(parts):
            part_upper = part.upper()
            if part_upper.startswith("CP") or part_upper.startswith("FT") or part_upper.startswith("PT"):
                if i > 0:
                    product = parts[i - 1]
                break

    product = product or "UNKNOWN"

    console.print(f"\n[bold]stdf - Ingest[/bold]")
    if ctx.obj.get("env"):
        console.print(f"  [yellow]Environment: {ctx.obj['env']}[/yellow]  (data → data-{ctx.obj['env']}/)")
    console.print(f"  File: {stdf_file}")
    console.print(f"  Product: {product}")
    console.print()

    temp_file = None
    try:
        # Handle gzip files
        file_to_parse = stdf_file
        if stdf_file.suffix.lower() == ".gz":
            console.print("  [dim]Decompressing .gz file...[/dim]")
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".stdf", mode="wb")
            with gzip.open(stdf_file, "rb") as f_in:
                shutil.copyfileobj(f_in, temp_file)
            temp_file.close()
            file_to_parse = Path(temp_file.name)
            console.print("  [green]✓[/green] Decompressed")

        # Using isolated subprocess for parsing and saving
        # 4th element (ttype) is not used by the worker; it determines sub_process internally from the STDF file
        to_ingest = [(None, file_to_parse, product, "")]
        sync_manager = SyncManager(config.storage.data_dir / "sync_history.json")
        _run_ingest_batch(config, sync_manager, to_ingest, cleanup=False, verbose=verbose)
        console.print(f"\n[green]✓[/green] Successfully ingested {stdf_file.name}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if verbose:
            console.print_exception()
        sys.exit(1)
    finally:
        # Clean up temp file
        if temp_file:
            try:
                Path(temp_file.name).unlink()
            except Exception:
                pass


@main.command("ingest-all")
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--product", "-p", required=True, help="Product name")
@click.option("--glob", "-g", default="*.stdf*", show_default=True, help="File glob pattern")
@click.option("--workers", "-w", default=4, show_default=True, help="Concurrent worker count")
@click.option("--timeout", default=300, show_default=True, help="Per-file timeout (seconds)")
@click.option("--force", "-f", is_flag=True, help="Re-ingest files even if already ingested")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def ingest_all(ctx, directory: Path, product: str, glob: str, workers: int, timeout: int, force: bool, verbose: bool):
    """Ingest all STDF files in a directory using a concurrent worker pool.

    Already-ingested files are skipped automatically — safe to re-run after
    an interrupted ingest. Use --force to re-ingest everything.

    DIRECTORY: Path to directory containing STDF files.

    Example: stdf ingest-all ./var/downloads -p SCT101A --workers 8
    """
    from .ingest_history import IngestHistory

    config: Config = ctx.obj["config"]
    config.ensure_directories()

    all_files = sorted(directory.glob(glob))
    if not all_files:
        console.print(f"[yellow]No files matching '{glob}' in {directory}[/yellow]")
        return

    history = IngestHistory(config.storage.data_dir / "ingest_history.json")

    if force:
        stdf_files = all_files
        skipped_count = 0
    else:
        stdf_files = [f for f in all_files if not history.is_done(f)]
        skipped_count = len(all_files) - len(stdf_files)

    excluded_count = 0
    if config.exclude:
        before = len(stdf_files)
        stdf_files = [f for f in stdf_files if not config.should_exclude(str(f))]
        excluded_count = before - len(stdf_files)

    console.print(f"\n[bold]stdf - Ingest All[/bold]")
    console.print(f"  Directory : {directory}")
    console.print(f"  Files     : {len(all_files)} found")
    if skipped_count:
        console.print(f"  Skipped   : [dim]{skipped_count} already ingested[/dim]")
    if excluded_count:
        console.print(f"  Excluded  : [dim]{excluded_count} matching exclude patterns[/dim]")
    console.print(f"  To ingest : {len(stdf_files)}")
    console.print(f"  Product   : {product}")
    console.print(f"  Workers   : {workers}")
    if force:
        console.print(f"  [yellow]Force mode: re-ingesting all files[/yellow]")
    console.print()

    if not stdf_files:
        console.print("[green]✓[/green] Nothing to do — all files already ingested.")
        console.print("[dim]Use --force to re-ingest.[/dim]")
        return

    to_ingest = [(None, f, product, "") for f in stdf_files]
    sync_manager = SyncManager(config.storage.data_dir / "sync_history.json")
    successes, failures = _run_ingest_batch(
        config, sync_manager, to_ingest,
        cleanup=False, verbose=verbose,
        timeout=timeout, max_workers=workers,
    )

    # Record successes so they are skipped on next run
    if successes:
        history.mark_done_batch([r.local_path for r in successes])


@main.command()
@click.option("--host", default=None, help="Bind address (default: config server.host)")
@click.option("--port", default=None, type=int, help="Port (default: config server.port)")
@click.pass_context
def serve(ctx, host: str | None, port: int | None):
    """Start the read-only HTTP query server (multi-user access).

    One request = one in-memory DuckDB session over the Parquet store; user
    SQL is restricted to a single SELECT with filesystem access locked to
    data_dir. See docs/multi-user-server.md.
    """
    import uvicorn

    from .server import create_app
    from .server.app import validate_memory_limit

    config: Config = ctx.obj["config"]
    # 設定ミスは起動時に落とす(以前は毎リクエスト 500 になっていた)。
    try:
        validate_memory_limit(config.server.memory_limit)
    except ValueError as e:
        console.print(f"[red]Config error:[/red] {e}")
        sys.exit(1)
    if int(config.server.threads) < 1:
        console.print("[red]Config error:[/red] server.threads は 1 以上")
        sys.exit(1)
    host = host or config.server.host
    port = port or config.server.port
    console.print(f"[bold]stdf query server[/bold] → http://{host}:{port}")
    console.print(f"  data_dir: {config.storage.data_dir}")
    uvicorn.run(create_app(config), host=host, port=port)


# ── db group ──────────────────────────────────────────────────────

@main.group()
def db():
    """Database operations (lots, query, shell)."""
    pass


@db.command()
@click.option("--lot", "-l", help="Filter by lot ID")
@click.pass_context
def lots(ctx, lot: str | None):
    """List ingested lots.

    The Job column shows the latest run's test program as `name (rev)`. If
    the lot ran under more than one distinct (job_name, job_rev) — i.e. the
    test program changed mid-lot — a `⚠×N` marker is appended (N = number of
    distinct program versions). Use `stdf db programs --lot <ID>` to see the
    per-wafer/per-run breakdown behind that marker.
    """
    config: Config = ctx.obj["config"]

    try:
        with AnalysisSession(config.storage.data_dir, config=config) as s:
            df = s.lot_summary(lot)

            if df.empty:
                console.print("[yellow]No lots found[/yellow]")
                return

            table = Table(title="Lot Summary")
            table.add_column("Lot ID", style="cyan")
            table.add_column("Part Type")
            table.add_column("Job")
            table.add_column("Wafers", justify="right")
            table.add_column("Parts", justify="right")
            table.add_column("Good", justify="right")
            table.add_column("Yield %", justify="right", style="green")

            for row in df.itertuples():
                job_cell = f"{row.job_name} ({row.job_rev})" if row.job_name else ""
                if row.job_mixed:
                    job_cell += f" [red]⚠×{row.job_variant_count}[/red]"
                table.add_row(
                    row.lot_id,
                    row.part_type or "",
                    job_cell,
                    _cell(row.wafer_count, "0"),
                    _cell(row.total_parts, "0", lambda v: f"{v:,}"),
                    _cell(row.good_parts, "0", lambda v: f"{v:,}"),
                    _cell(row.yield_pct, "0.00%", lambda v: f"{v:.2f}%"),
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@db.command()
@click.option("--lot", "-l", help="Filter by lot ID")
@click.pass_context
def programs(ctx, lot: str | None):
    """List per-run test program (MIR job_name/job_rev) history.

    One row per (lot, wafer, retest) run — the same granularity as the
    `runs` table. Use this to see the wafer/retest breakdown behind a
    `⚠` marker in `stdf db lots`'s Job column.
    """
    config: Config = ctx.obj["config"]

    try:
        with AnalysisSession(config.storage.data_dir, config=config) as s:
            df = s.runs(lot_id=lot)

            if df.empty:
                console.print("[yellow]No runs found[/yellow]")
                return

            table = Table(title="Test Program History")
            table.add_column("Lot ID", style="cyan")
            table.add_column("Wafer")
            table.add_column("Retest", justify="right")
            table.add_column("Job")
            table.add_column("Rev")
            table.add_column("Start")
            table.add_column("Source File")

            for row in df.itertuples():
                table.add_row(
                    row.lot_id,
                    row.wafer_id or "-",
                    str(row.retest_num),
                    row.job_name or "",
                    row.job_rev or "",
                    _cell(row.start_time),
                    row.source_file or "",
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@db.command()
@click.argument("sql", required=False)
@click.option("--output", "-o", type=click.Path(path_type=Path), help="Write result to CSV instead of printing")
@click.option("--file", "-f", "sql_file", type=click.Path(exists=True, path_type=Path), help="Read SQL from file (single SELECT). 名前付きクエリは sql/ + AnalysisSession.run()")
@click.pass_context
def query(ctx, sql: str | None, output: Path | None, sql_file: Path | None):
    """Execute SQL (inline or from -f FILE) against the store."""
    config: Config = ctx.obj["config"]
    if (sql is None) == (sql_file is None):
        raise click.UsageError("give SQL inline or via -f, not both/neither")
    if sql_file is not None:
        sql = sql_file.read_text(encoding="utf-8")
    try:
        with AnalysisSession(config.storage.data_dir, config=config) as s:
            if output is not None:
                from .analysis.library import sql_literal
                n = s.conn.execute(
                    f"COPY ({sql.rstrip('; ')}) TO '{sql_literal(output.as_posix())}'"
                    f" (HEADER, DELIMITER ',')"
                ).fetchone()[0]
                console.print(f"Exported {n:,} rows → {output}")
            else:
                df = s.q(sql)
                if df.empty:
                    console.print("[yellow]No results[/yellow]")
                    return
                table = Table(title="Query Results")
                for col in df.columns:
                    table.add_column(str(col))
                for row in df.head(100).itertuples(index=False):
                    table.add_row(*[_cell(v) for v in row])
                console.print(table)
                if len(df) > 100:
                    console.print(f"[dim]... showing 100 of {len(df)} rows[/dim]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


def _stale_views(conn, registered: list[str],
                 previous: list[str] | None) -> list[str]:
    """カタログから落とすべきビュー。

    previous (前回 _stdf_mount_state に記録した registered) が分かる時は
    我々が作った分の差分だけを対象にし、ユーザーが shell で作ったビューを
    巻き込まない。前回状態が無い旧 DB は区別できないため、従来通り
    非登録ビューを全部落とす。
    """
    current = set(registered)
    if previous is None:
        return [
            r[0]
            for r in conn.execute(
                "SELECT view_name FROM duckdb_views() WHERE NOT internal"
            ).fetchall()
            if r[0] not in current
        ]
    return [v for v in previous if v not in current]


@db.command()
@click.option("--refresh", is_flag=True,
              help="ビューを強制的に再登録する(通常は不要 — 変化が必要なときは自動検出される)")
@click.pass_context
def shell(ctx, refresh):
    """Open a DuckDB interactive shell with the canonical views persisted.

    Every other command uses a throwaway :memory: connection + setup_views();
    nothing else ever writes config.storage.database. This command creates/
    refreshes that file by opening a real connection to it and running
    setup_views() against it (so the views persist in the file itself), then
    launches the `duckdb` CLI on it.

    Registration is linear in the store's Parquet file count and is paid
    roughly twice (the *_final / lots / wafer_yield_final views re-bind their
    base view's glob), which is what made startup slow as the store grew.
    Because the views ARE globs, they keep seeing newly ingested data with no
    re-registration — so the catalog in the database file is reused as long as
    the SET of views that should exist is unchanged (store_fingerprint:
    which table directories exist, plus the gross-die map). Pass --refresh
    to rebuild the catalog by hand.
    """
    config: Config = ctx.obj["config"]
    db_path = config.storage.database
    db_path.parent.mkdir(parents=True, exist_ok=True)

    import time

    import duckdb as duckdb_mod

    from .mounts import setup_views, store_fingerprint

    fingerprint = store_fingerprint(config.storage.data_dir, config.gross_die_map)
    try:
        conn = duckdb_mod.connect(str(db_path))
    except duckdb_mod.Error as e:
        # このファイルはビューのカタログを持つだけのキャッシュで、中身は
        # data_dir から再生成できる。開けない(壊れた・別バージョンが書いた・
        # 別プロセスが掴んでいる)ときは、消して作り直すのが正しい復旧手順。
        console.print(f"[red]Error:[/red] {db_path} を開けません — {e}")
        console.print("[dim]このファイルは再生成可能なキャッシュです。"
                      "削除してもう一度実行してください:[/dim]")
        console.print(f"[dim]  rm {db_path}   /   Remove-Item {db_path}[/dim]")
        sys.exit(1)
    registered: list[str] | None = None
    previous: list[str] | None = None
    if not refresh:
        try:
            row = conn.execute(
                "SELECT fingerprint, registered FROM _stdf_mount_state"
            ).fetchone()
            if row:
                previous = row[1].split(",")
                if row[0] == fingerprint:
                    registered = previous
        except duckdb_mod.Error:
            pass  # 初回、または旧バージョンが作った DB — 下で登録する

    if registered is None:
        # --refresh / fingerprint 変化でも前回一覧は残っているので読む
        # (refresh 分岐では上の try が走らない)。
        if previous is None:
            try:
                row = conn.execute(
                    "SELECT registered FROM _stdf_mount_state"
                ).fetchone()
                if row:
                    previous = row[0].split(",")
            except duckdb_mod.Error:
                pass
        t0 = time.time()
        try:
            registered = setup_views(conn, config.storage.data_dir, config.gross_die_map)
        except Exception as e:
            conn.close()
            console.print(f"[red]Error:[/red] {e}")
            sys.exit(1)
        # 過去の登録の残骸を落とす。setup_views() は CREATE OR REPLACE しかしない
        # ので、一度でも作られたビューはこの永続ファイルに残り続ける — 消えた
        # 定義(dbt 時代のマートビューが典型)は、参照先の Parquet ごと消えると
        # IOException を投げるし、残っていれば古いデータを黙って返す。前回一覧が
        # あれば差分だけを落とし、ユーザーが shell で作ったビューは残す。
        stale = _stale_views(conn, registered, previous)
        for view in stale:
            conn.execute(f'DROP VIEW IF EXISTS "{view}"')

        conn.execute(
            "CREATE OR REPLACE TABLE _stdf_mount_state AS "
            "SELECT ? AS fingerprint, ? AS registered, now() AS registered_at",
            [fingerprint, ",".join(registered)],
        )
        console.print(f"[dim]Registered {len(registered)} views in {time.time() - t0:.1f}s[/dim]")
        if stale:
            console.print(f"[dim]Dropped {len(stale)} stale view(s): "
                          f"{', '.join(stale)}[/dim]")
    else:
        console.print(f"[dim]Reusing {len(registered)} views from {db_path.name} "
                      f"(--refresh で再登録)[/dim]")
    conn.close()

    console.print(f"[bold]Database:[/bold] {db_path}")
    console.print(f"[dim]Registered: {', '.join(registered)}[/dim]")
    console.print(f"[bold]Opening DuckDB shell...[/bold]")
    console.print()

    import subprocess
    subprocess.run(["duckdb", str(db_path)])


@db.command()
@click.pass_context
def verify(ctx):
    """test_data の retest_flag 不変条件を検証する。

    storage.py が ingest 時に確定させるフラグが壊れていると、test_data_final
    (= retest_flag = 0)が黙って誤った行集合を返す — 測定値を疑う前にここを
    見る。ストア全体を走査するので、日常的にではなく大量 ingest のあとや
    結果が疑わしいときに回す。
    """
    from .mounts import FLAG_INVARIANTS

    config: Config = ctx.obj["config"]
    failures = 0
    with AnalysisSession(config.storage.data_dir, config=config) as s:
        for name, description, sql in FLAG_INVARIANTS:
            df = s.q(sql)
            if df.empty:
                console.print(f"[green]OK[/green]   {name}")
            else:
                failures += 1
                console.print(f"[red]NG[/red]   {name} — {description}")
                console.print(f"       違反 {len(df):,} 件（先頭 5 件）")
                console.print(df.head(5).to_string(index=False))
    if failures:
        console.print(f"\n[red]{failures} 件の不変条件が破れています。"
                      "該当ロットを再 ingest してください。[/red]")
        sys.exit(1)
    console.print("\n[green]すべての不変条件を満たしています。[/green]")


def _run_ingest_batch(
    config,
    sync_manager,
    to_ingest: list[tuple],
    cleanup: bool,
    verbose: bool,
    timeout: int = 300,
    max_workers: int = 4,
):
    """Ingest a batch of STDF files using a concurrent subprocess worker pool.

    Returns:
        (successes, failures) lists of IngestResult.
    """
    from .worker import run_ingest_pool
    from .atomic import atomic_write_json

    # save=False: the callback runs once per file; rewriting the whole
    # sync_history.json per file is O(N^2). The single flush is in a finally
    # because it MUST also run on abort — run_ingest_pool re-raises through
    # future.result() and Ctrl+C raises KeyboardInterrupt. Without it an
    # aborted run leaves every already-ingested file marked ingested=False, so
    # the next fetch re-ingests it and auto-increments retest_num into a bogus
    # retest={n} partition (run_ingest_pool's on_success docstring promises
    # exactly this durability).
    try:
        successes, failures = run_ingest_pool(
            files=to_ingest,
            data_dir=config.storage.data_dir,
            compression=config.processing.compression,
            max_workers=max_workers,
            timeout=timeout,
            on_success=lambda r: sync_manager.mark_ingested(r.remote_path, save=False),
        )
    finally:
        sync_manager.save()

    # Structured failure record for automation (always written, even if empty).
    atomic_write_json(
        config.storage.data_dir / "ingest_failures.json",
        {
            "failures": [
                {"path": str(r.local_path), "remote_path": r.remote_path, "error": r.error}
                for r in failures
            ]
        },
    )

    console.print(f"\n[green]✓[/green] Ingested {len(successes)} files")
    if failures:
        console.print(f"[yellow]![/yellow] {len(failures)} files failed (will retry on next fetch)")

    if cleanup and successes:
        console.print("\n[bold]Cleaning up source files...[/bold]")
        cleaned = 0
        for result in successes:
            try:
                if result.local_path.exists():
                    result.local_path.unlink()
                    cleaned += 1
            except Exception as e:
                if verbose:
                    console.print(f"  [yellow]![/yellow] Could not delete {result.local_path.name}: {e}")
        console.print(f"[green]✓[/green] Deleted {cleaned} source files")

    return successes, failures


_CORRUPT_DIR = "_corrupt"


def _download_files(
    client,
    config,
    sync_manager,
    files: list[tuple[str, str, str, str]],
    verbose: bool = False,
) -> tuple[list[tuple[str, Path, str, str]], list[tuple[str, str]]]:
    """Download every candidate, surviving files that turn out to be corrupt.

    A .gz whose stream is broken at the source cannot be recovered by retrying,
    so it is moved to downloads/_corrupt/ and recorded in the sync history -
    otherwise it is re-downloaded on every run. One bad file must never stop
    the files behind it from being fetched.

    Returns (downloaded, corrupt, failed):
      downloaded - (remote_path, local_path, product, test_type)
      corrupt    - (remote_path, error) quarantined, never retried automatically
      failed     - (remote_path, error) transient, retried on the next run
    """
    from rich.progress import BarColumn

    from .ftp_client import CorruptDownloadError

    downloaded: list[tuple[str, Path, str, str]] = []
    corrupt: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []
    if not files:
        return downloaded, corrupt, failed

    # The per-file mark_downloaded() calls below defer their JSON write
    # (save=False) and are flushed once at the end. That flush is in a
    # finally because a Ctrl+C mid-batch must still leave the already-
    # downloaded files recorded — otherwise the next run re-downloads them
    # and get_pending_ingest() never sees them.
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=console,
        ) as progress:
            dl_task = progress.add_task("Downloading...", total=len(files))

            for remote_path, prod, ttype, filename in files:
                # Create subdirectory structure: downloads/product/test_type/
                local_dir = config.storage.download_dir / prod / ttype
                try:
                    local_file = client.download_file(remote_path, local_dir, decompress=True)
                except CorruptDownloadError as e:
                    quarantined = _quarantine(e.compressed_path, config)
                    sync_manager.mark_corrupt(
                        remote_path=remote_path,
                        product=prod,
                        test_type=ttype,
                        error=e.reason,
                        quarantine_path=quarantined,
                    )
                    corrupt.append((remote_path, e.reason))
                    progress.update(dl_task, advance=1, description=f"Corrupt {filename}")
                    if verbose:
                        console.print(f"  [red]![/red] {filename}: {e.reason}")
                    continue
                except Exception as e:
                    # Anything else (network drop, disk full) is not the file's
                    # fault: leave it unrecorded so the next run retries it.
                    failed.append((remote_path, f"{type(e).__name__}: {e}"))
                    progress.update(dl_task, advance=1, description=f"Failed {filename}")
                    if verbose:
                        console.print(f"  [red]![/red] {filename}: {type(e).__name__}: {e}")
                    continue

                # Track in sync history (persisted once after the batch below).
                sync_manager.mark_downloaded(
                    remote_path=remote_path,
                    local_path=local_file,
                    product=prod,
                    test_type=ttype,
                    save=False,
                )
                # A --retry-corrupt run that succeeds clears the quarantine record.
                if sync_manager.is_corrupt(remote_path):
                    sync_manager.clear_corrupt(remote_path)

                downloaded.append((remote_path, local_file, prod, ttype))
                progress.update(dl_task, advance=1, description=f"Downloaded {filename}")

    finally:
        sync_manager.save()  # batched save=False marks above
    return downloaded, corrupt, failed


def _quarantine(compressed_path: Path | None, config) -> Path | None:
    """Move a corrupt download out of the ingest path, keeping it for analysis."""
    if compressed_path is None or not compressed_path.exists():
        return None
    dest_dir = config.storage.download_dir / _CORRUPT_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / compressed_path.name
    try:
        shutil.move(str(compressed_path), str(dest))
    except OSError:
        compressed_path.unlink(missing_ok=True)
        return None
    return dest


@main.command()
@click.option("--product", "-p", multiple=True, help="Product filter (can specify multiple)")
@click.option("--test-type", "-t", multiple=True, help="Test type filter (CP, FT)")
@click.option("--limit", "-n", type=int, help="Maximum files to fetch")
@click.option("--ingest/--no-ingest", default=True, help="Auto-ingest after download")
@click.option("--cleanup/--no-cleanup", default=True, help="Delete source files after successful ingest")
@click.option("--force", "-f", is_flag=True, help="Force re-download even if file exists")
@click.option("--reingest", is_flag=True, help="Re-ingest downloaded files (skip FTP download)")
@click.option("--retry-corrupt", is_flag=True, help="Retry files previously quarantined as corrupt")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def fetch(ctx, product: tuple, test_type: tuple, limit: int | None, ingest: bool, cleanup: bool, force: bool, reingest: bool, retry_corrupt: bool, verbose: bool):
    """
    Fetch STDF files from FTP server with incremental sync.

    Downloads only new files from FTP (skips already downloaded).
    Uses filters from config.yaml, or override with --product and --test-type.
    Use --force to re-download all files.
    Use --reingest to re-ingest previously downloaded files without FTP.
    """
    from .ftp_client import FTPClient

    config: Config = ctx.obj["config"]
    config.ensure_directories()

    # Initialize sync manager
    sync_history_file = config.storage.data_dir / "sync_history.json"
    sync_manager = SyncManager(sync_history_file)

    # --reingest mode: skip FTP, just re-ingest pending files
    if reingest:
        console.print(f"\n[bold]stdf - Re-ingest pending files[/bold]")
        console.print(f"  Sync History: {sync_manager.get_downloaded_count()} files tracked")
        console.print()

        pending = sync_manager.get_pending_ingest()
        if not pending:
            console.print("[dim]No pending files to ingest[/dim]")
            return

        console.print(f"  Pending files: {len(pending)}")
        _run_ingest_batch(config, sync_manager, pending, cleanup, verbose)
        return

    console.print(f"\n[bold]STDF Platform - Fetch from FTP[/bold]")
    console.print(f"  Host: {config.ftp.host}")
    console.print(f"  Sync History: {sync_manager.get_downloaded_count()} files tracked")

    # Show filters
    cli_products = list(product) if product else None
    cli_test_types = list(test_type) if test_type else None
    
    if cli_products or cli_test_types:
        if cli_products:
            console.print(f"  Products (CLI): {', '.join(cli_products)}")
        else:
            console.print("  Products: [dim]all[/dim]")
        if cli_test_types:
            console.print(f"  Test Types (CLI): {', '.join(cli_test_types)}")
        else:
            console.print("  Test Types: [dim]all[/dim]")
    elif config.filters:
        console.print("  Filters (from config):")
        for f in config.filters:
            console.print(f"    - {f.product}: {', '.join(f.test_types)}")
    else:
        console.print("  Filters: [dim]all products/test types[/dim]")
    
    if force:
        console.print("  [yellow]Force mode: re-downloading all files[/yellow]")
    console.print()

    try:
        with FTPClient(config.ftp) as client:
            # Determine products and test types to fetch
            if cli_products:
                ftp_products = cli_products
            elif config.filters:
                ftp_products = [f.product for f in config.filters]
            else:
                ftp_products = None  # All products
            
            if cli_test_types:
                ftp_test_types = cli_test_types
            elif config.filters:
                # Get unique test types from filters
                ftp_test_types = list(set(
                    tt for f in config.filters for tt in f.test_types
                ))
            else:
                ftp_test_types = ["CP", "FT"]  # Default to both
            
            # List files
            files = list(client.list_stdf_files(
                products=ftp_products,
                test_types=ftp_test_types,
            ))
            
            # Apply config filters for fine-grained product/test_type matching
            if config.filters and not cli_products and not cli_test_types:
                files = [(f, p, t, n) for f, p, t, n in files if config.should_fetch(p, t)]

            # Filter out excluded filenames
            if config.exclude:
                before = len(files)
                files = [(f, p, t, n) for f, p, t, n in files if not config.should_exclude(n)]
                excluded = before - len(files)
                if excluded > 0:
                    console.print(f"  [dim]Excluded {excluded} files matching exclude patterns[/dim]")

            # Filter out already downloaded (unless force)
            if not force:
                new_files = [(f, p, t, n) for f, p, t, n in files if not sync_manager.is_downloaded(f)]
                skipped = len(files) - len(new_files)
                if skipped > 0:
                    console.print(f"  [dim]Skipping {skipped} already downloaded files[/dim]")
                files = new_files

            # Filter out files quarantined as unrecoverable. Retrying them only
            # re-downloads the same broken bytes; they need a re-export first.
            if not retry_corrupt:
                kept = [(f, p, t, n) for f, p, t, n in files if not sync_manager.is_corrupt(f)]
                quarantined = len(files) - len(kept)
                if quarantined > 0:
                    console.print(
                        f"  [yellow]Skipping {quarantined} file(s) quarantined as corrupt[/yellow] "
                        f"[dim](re-export at the source, then --retry-corrupt)[/dim]"
                    )
                    for entry in sync_manager.get_corrupt():
                        console.print(
                            f"    [dim]• {Path(entry['remote_path']).name} - {entry['error']}[/dim]"
                        )
                files = kept

            if limit:
                files = files[:limit]

            if not files:
                console.print("[yellow]No new files to download[/yellow]")
                return

            console.print(f"  Files to download: {len(files)}")
            console.print()

            downloaded, corrupt, failed = _download_files(
                client, config, sync_manager, files, verbose=verbose
            )

        console.print(f"\n[green]✓[/green] Downloaded {len(downloaded)} files")
        if corrupt:
            console.print(
                f"[red]![/red] {len(corrupt)} file(s) could not be decompressed and were "
                f"quarantined in {config.storage.download_dir / _CORRUPT_DIR}"
            )
            for remote_path, error in corrupt:
                console.print(f"    [red]•[/red] {Path(remote_path).name} - {error}")
            console.print(
                "  [dim]These are skipped from now on. Ask the source to re-export them, "
                "then run `stdf fetch --retry-corrupt`.[/dim]"
            )
        if failed:
            console.print(f"[yellow]![/yellow] {len(failed)} file(s) failed to download "
                          f"(will retry on the next run)")
            for remote_path, error in failed:
                console.print(f"    [yellow]•[/yellow] {Path(remote_path).name} - {error}")

        # Auto-ingest if enabled
        if ingest:
            to_ingest = list(downloaded)
            pending = sync_manager.get_pending_ingest()
            downloaded_remotes = {r for r, _, _, _ in downloaded}
            for remote_path, local_path, prod, ttype in pending:
                if remote_path not in downloaded_remotes and local_path.exists():
                    to_ingest.append((remote_path, local_path, prod, ttype))

            if to_ingest:
                retry_count = len(to_ingest) - len(downloaded)
                if retry_count > 0:
                    console.print(f"\n[bold]Ingesting files...[/bold] ({retry_count} pending retry)")
                else:
                    console.print("\n[bold]Ingesting files...[/bold]")
                _run_ingest_batch(config, sync_manager, to_ingest, cleanup, verbose)
            else:
                console.print("\n[dim]No files to ingest[/dim]")

        # Everything else has been fetched and ingested; still exit non-zero so
        # the nightly task reports the bad files instead of hiding them.
        if corrupt or failed:
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if verbose:
            console.print_exception()
        sys.exit(1)


# ── export group ──────────────────────────────────────────────────

@main.group(name="export")
def export_grp():
    """Export lot test results to CSV (JMP-ready)."""
    pass


@export_grp.command(name="lot")
@click.argument("lot_ids", nargs=-1, required=True)
@click.argument("output", type=click.Path(path_type=Path))
@click.option("--pivot/--no-pivot", default=True, help="Pivot test results (one row per part)")
@click.pass_context
def export_lot(ctx, lot_ids: tuple, output: Path, pivot: bool):
    """
    Export test results for specified lots to CSV (JMP-ready).

    LOT_IDS: One or more lot IDs to export
    OUTPUT: Output CSV file path

    Example:
        stdf export lot E6A773.00 E6A774.00 results.csv
    """
    config: Config = ctx.obj["config"]

    console.print(f"\n[bold]stdf - Export Lot[/bold]")
    console.print(f"  Lots: {', '.join(lot_ids)}")
    console.print(f"  Output: {output}")
    console.print(f"  Pivot: {'Yes' if pivot else 'No'}")
    console.print()

    placeholders = ", ".join(f"${i+1}" for i in range(len(lot_ids)))
    params = list(lot_ids)

    try:
        with AnalysisSession(config.storage.data_dir, config=config) as s:
            if pivot:
                # DuckDB PIVOT with dynamic values cannot use bound parameters.
                # Fetch long-format first, then pivot via pandas (matches web API).
                sql = f"""
                SELECT
                    td.lot_id,
                    td.wafer_id,
                    td.part_id,
                    td.die_key,
                    p.x_coord,
                    p.y_coord,
                    p.hard_bin,
                    p.soft_bin,
                    p.passed AS part_passed,
                    td.test_name,
                    td.result
                FROM test_data_final td
                JOIN parts_final p
                    ON  td.lot_id   = p.lot_id
                    AND td.wafer_id = p.wafer_id
                    AND td.die_key  = p.die_key
                WHERE td.lot_id IN ({placeholders})
                ORDER BY td.lot_id, td.wafer_id, td.die_key, td.test_name
                """
                long_df = s.q(sql, params)
                if long_df.empty:
                    df = long_df
                else:
                    # part_id は index に入れない。test_data_final は再測定
                    # されたテストだけを新しい run から採るので、1ダイの行が
                    # run をまたいで別々の part_id を持ち、part_id を index に
                    # 入れると同じダイが NaN 補完された2行に割れる。
                    index_cols = ["lot_id", "wafer_id", "die_key",
                                  "x_coord", "y_coord", "hard_bin", "soft_bin",
                                  "part_passed"]
                    df = long_df.pivot_table(
                        index=index_cols, columns="test_name",
                        values="result", aggfunc="first",
                    )
                    df.columns.name = None
                    df = df.reset_index()
            else:
                sql = f"""
                SELECT
                    td.lot_id,
                    td.wafer_id,
                    td.part_id,
                    p.x_coord,
                    p.y_coord,
                    p.hard_bin,
                    p.soft_bin,
                    td.test_num,
                    td.test_name,
                    td.result,
                    td.passed,
                    td.lo_limit,
                    td.hi_limit,
                    td.units
                FROM test_data_final td
                JOIN parts_final p
                    ON  td.lot_id   = p.lot_id
                    AND td.wafer_id = p.wafer_id
                    AND td.die_key  = p.die_key
                WHERE td.lot_id IN ({placeholders})
                ORDER BY td.lot_id, td.wafer_id, td.die_key, td.test_num
                """
                df = s.q(sql, params)

            if df.empty:
                console.print("[yellow]No results to export[/yellow]")
                return

            df.to_csv(output, index=False)
            console.print(f"[green]✓[/green] Exported {len(df):,} rows to {output}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)



if __name__ == "__main__":
    main()
