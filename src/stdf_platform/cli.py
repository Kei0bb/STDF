"""CLI interface for STDF Platform."""

import sys
import gzip
import tempfile
import shutil
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from . import __version__
from .config import Config
from .database import Database
from .sync_manager import SyncManager


console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="stdf2pq")
@click.option("--config", "-c", type=click.Path(path_type=Path), help="Config file path")
@click.option("--env", "-e", default=None, help="Environment name (e.g. dev). Isolates data to data-{env}/")
@click.pass_context
def main(ctx, config: Path | None, env: str | None):
    """stdf2pq - STDF to Parquet converter and analysis DB."""
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

    console.print(f"\n[bold]stdf2pq - Ingest[/bold]")
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


# ── db group ──────────────────────────────────────────────────────

@main.group()
def db():
    """Database operations (lots, query, shell)."""
    pass


@db.command()
@click.option("--lot", "-l", help="Filter by lot ID")
@click.pass_context
def lots(ctx, lot: str | None):
    """List ingested lots."""
    config: Config = ctx.obj["config"]

    try:
        with Database(config.storage) as db_conn:
            results = db_conn.get_lot_summary(lot)

            if not results:
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

            for row in results:
                table.add_row(
                    row["lot_id"],
                    row["part_type"] or "",
                    f"{row['job_name']} ({row['job_rev']})" if row["job_name"] else "",
                    str(row["wafer_count"] or 0),
                    f"{row['total_parts'] or 0:,}",
                    f"{row['good_parts'] or 0:,}",
                    f"{row['yield_pct'] or 0:.2f}%",
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@main.group()
def analyze():
    """Run analysis on ingested data."""
    pass


@analyze.command()
@click.argument("lot_id")
@click.pass_context
def yield_cmd(ctx, lot_id: str):
    """Analyze yield by wafer for a lot."""
    config: Config = ctx.obj["config"]

    try:
        with Database(config.storage) as db:
            results = db.get_wafer_yield(lot_id)

            if not results:
                console.print(f"[yellow]No data found for lot {lot_id}[/yellow]")
                return

            table = Table(title=f"Wafer Yield - {lot_id}")
            table.add_column("Wafer ID", style="cyan")
            table.add_column("Total", justify="right")
            table.add_column("Good", justify="right")
            table.add_column("Yield %", justify="right", style="green")

            for row in results:
                yield_pct = row["yield_pct"] or 0
                style = "green" if yield_pct >= 90 else "yellow" if yield_pct >= 80 else "red"
                table.add_row(
                    row["wafer_id"],
                    f"{row['total'] or 0:,}",
                    f"{row['good'] or 0:,}",
                    f"[{style}]{yield_pct:.2f}%[/{style}]",
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@analyze.command()
@click.argument("lot_id")
@click.option("--top", "-n", default=10, help="Number of top failing tests")
@click.pass_context
def test_fail(ctx, lot_id: str, top: int):
    """Show top failing tests for a lot."""
    config: Config = ctx.obj["config"]

    try:
        with Database(config.storage) as db:
            results = db.get_test_fail_rate(lot_id, top)

            if not results:
                console.print(f"[yellow]No test failures found for lot {lot_id}[/yellow]")
                return

            table = Table(title=f"Top {top} Failing Tests - {lot_id}")
            table.add_column("Test #", style="cyan", justify="right")
            table.add_column("Test Name")
            table.add_column("Total", justify="right")
            table.add_column("Fails", justify="right")
            table.add_column("Fail %", justify="right", style="red")

            for row in results:
                table.add_row(
                    str(row["test_num"]),
                    row["test_name"] or "",
                    f"{row['total']:,}",
                    f"{row['fails']:,}",
                    f"{row['fail_rate']:.2f}%",
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@analyze.command()
@click.argument("lot_id")
@click.pass_context
def bins(ctx, lot_id: str):
    """Show bin distribution for a lot."""
    config: Config = ctx.obj["config"]

    try:
        with Database(config.storage) as db:
            results = db.get_bin_summary(lot_id)

            if not results:
                console.print(f"[yellow]No bin data found for lot {lot_id}[/yellow]")
                return

            table = Table(title=f"Bin Distribution - {lot_id}")
            table.add_column("Soft Bin", style="cyan", justify="right")
            table.add_column("Count", justify="right")
            table.add_column("Percent", justify="right")

            for row in results:
                table.add_row(
                    str(row["soft_bin"]),
                    f"{row['count']:,}",
                    f"{row['pct']:.2f}%",
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@db.command()
@click.argument("sql")
@click.pass_context
def query(ctx, sql: str):
    """Execute SQL query against the database."""
    config: Config = ctx.obj["config"]

    try:
        with Database(config.storage) as db_conn:
            results = db_conn.query(sql)

            if not results:
                console.print("[yellow]No results[/yellow]")
                return

            # Create table with columns from results
            table = Table(title="Query Results")
            columns = list(results[0].keys())

            for col in columns:
                table.add_column(col)

            for row in results[:100]:  # Limit to 100 rows
                table.add_row(*[str(v) if v is not None else "" for v in row.values()])

            console.print(table)

            if len(results) > 100:
                console.print(f"[dim]... showing 100 of {len(results)} rows[/dim]")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@db.command()
@click.pass_context
def shell(ctx):
    """Open DuckDB interactive shell."""
    config: Config = ctx.obj["config"]

    console.print(f"[bold]Opening DuckDB shell...[/bold]")
    console.print(f"Database: {config.storage.database}")
    console.print()

    import subprocess
    subprocess.run(["duckdb", str(config.storage.database)])


def _run_ingest_batch(
    config,
    sync_manager,
    to_ingest: list[tuple],
    cleanup: bool,
    verbose: bool,
    timeout: int = 300,
):
    """Ingest a batch of STDF files using an isolated subprocess.

    Each file is parsed and saved in an isolated subprocess via
    stdf_platform._ingest_worker to efficiently release memory
    and avoid crashing the main process.
    """
    import json
    import subprocess
    import sys
    import threading
    import time
    from pathlib import Path

    log_path = Path(config.storage.data_dir) / "ingest_worker.log"
    stderr_lines: list[str] = []

    def _collect_stderr(proc):
        """Read stderr line by line into a list (and optionally log file)."""
        try:
            log_file = open(log_path, "a", encoding="utf-8")
        except OSError:
            log_file = None
        for line in proc.stderr:
            stderr_lines.append(line.rstrip())
            if log_file:
                log_file.write(line)
                log_file.flush()
        if log_file:
            log_file.close()

    success = 0
    failed = 0
    ingested_files = []

    for remote_path, local_path, prod, ttype in to_ingest:
        try:
            stderr_lines.clear()

            cmd = [
                sys.executable, "-m", "stdf_platform._ingest_worker",
                str(local_path),
                prod,
                str(config.storage.data_dir),
                config.processing.compression,
            ]
            console.print(f"  [dim]cmd: {' '.join(cmd)}[/dim]")

            # Run parse + save in a completely separate process
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            except Exception as popen_err:
                console.print(f"  [red]✗[/red] {local_path.name}: Popen failed: {popen_err}")
                failed += 1
                continue

            # Stream stderr in real time (visible in log even on hang)
            stderr_thread = threading.Thread(target=_collect_stderr, args=(proc,), daemon=True)
            stderr_thread.start()

            try:
                stdout, _ = proc.communicate(timeout=timeout)
                stderr_thread.join(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate()
                stderr_thread.join(timeout=5)
                last_log = stderr_lines[-1] if stderr_lines else "no output"
                console.print(f"  [red]✗[/red] {local_path.name}: timed out after {timeout}s")
                console.print(f"    [dim]last worker output: {last_log}[/dim]")
                failed += 1
                continue

            console.print(f"  [dim]returncode={proc.returncode}, stderr_lines={len(stderr_lines)}, stdout_len={len(stdout)}[/dim]")

            if proc.returncode != 0:
                if stderr_lines:
                    for line in stderr_lines:
                        console.print(f"    [red]{line}[/red]")
                else:
                    console.print(f"  [red]✗[/red] {local_path.name}: worker exited with code {proc.returncode}, no stderr output")
                failed += 1
                continue

            if verbose and stderr_lines:
                for line in stderr_lines:
                    console.print(f"    [dim]{line}[/dim]")

            # Parse worker output
            try:
                result = json.loads(stdout)
                sub_process = result.get("sub_process", "UNKNOWN")
                test_category = result.get("test_category", "OTHER")
            except json.JSONDecodeError:
                sub_process = "UNKNOWN"
                test_category = "OTHER"

            sync_manager.mark_ingested(remote_path)
            console.print(f"  [green]✓[/green] {local_path.name} ({prod}/{test_category}/{sub_process})")
            success += 1
            ingested_files.append(local_path)
        except Exception as e:
            console.print(f"  [red]✗[/red] {local_path.name}: {e}")
            failed += 1

    console.print(f"\n[green]✓[/green] Ingested {success} files")
    if failed:
        console.print(f"[yellow]![/yellow] {failed} files failed (will retry on next fetch)")

    # Cleanup source files after successful ingest
    if cleanup and ingested_files:
        console.print("\n[bold]Cleaning up source files...[/bold]")
        cleaned = 0
        for local_path in ingested_files:
            try:
                if local_path.exists():
                    local_path.unlink()
                    cleaned += 1
            except Exception as e:
                if verbose:
                    console.print(f"  [yellow]![/yellow] Could not delete {local_path.name}: {e}")
        console.print(f"[green]✓[/green] Deleted {cleaned} source files")


@main.command()
@click.option("--product", "-p", multiple=True, help="Product filter (can specify multiple)")
@click.option("--test-type", "-t", multiple=True, help="Test type filter (CP, FT)")
@click.option("--limit", "-n", type=int, help="Maximum files to fetch")
@click.option("--ingest/--no-ingest", default=True, help="Auto-ingest after download")
@click.option("--cleanup/--no-cleanup", default=True, help="Delete source files after successful ingest")
@click.option("--force", "-f", is_flag=True, help="Force re-download even if file exists")
@click.option("--reingest", is_flag=True, help="Re-ingest downloaded files (skip FTP download)")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def fetch(ctx, product: tuple, test_type: tuple, limit: int | None, ingest: bool, cleanup: bool, force: bool, reingest: bool, verbose: bool):
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
        console.print(f"\n[bold]stdf2pq - Re-ingest pending files[/bold]")
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

            # Filter out already downloaded (unless force)
            if not force:
                new_files = [(f, p, t, n) for f, p, t, n in files if not sync_manager.is_downloaded(f)]
                skipped = len(files) - len(new_files)
                if skipped > 0:
                    console.print(f"  [dim]Skipping {skipped} already downloaded files[/dim]")
                files = new_files

            if limit:
                files = files[:limit]

            if not files:
                console.print("[yellow]No new files to download[/yellow]")
                return

            console.print(f"  Files to download: {len(files)}")
            console.print()

            from rich.progress import BarColumn

            downloaded = []
            if files:
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
                        local_file = client.download_file(remote_path, local_dir, decompress=True)

                        # Track in sync history
                        sync_manager.mark_downloaded(
                            remote_path=remote_path,
                            local_path=local_file,
                            product=prod,
                            test_type=ttype,
                        )

                        downloaded.append((remote_path, local_file, prod, ttype))
                        progress.update(dl_task, advance=1, description=f"Downloaded {filename}")

        console.print(f"\n[green]✓[/green] Downloaded {len(downloaded)} files")

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

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if verbose:
            console.print_exception()
        sys.exit(1)


# ── export group ──────────────────────────────────────────────────

@main.group(name="export")
def export_grp():
    """Export data to CSV or Parquet."""
    pass


@export_grp.command(name="csv")
@click.argument("sql")
@click.argument("output", type=click.Path(path_type=Path))
@click.option("--format", "-f", type=click.Choice(["csv", "parquet"]), default="csv", help="Output format")
@click.pass_context
def export_csv(ctx, sql: str, output: Path, format: str):
    """
    Export query results to CSV or Parquet file.

    SQL: SQL query to execute
    OUTPUT: Output file path

    Example:
        stdf2pq export csv "SELECT * FROM test_data" results.csv
    """
    config: Config = ctx.obj["config"]

    console.print(f"\n[bold]stdf2pq - Export[/bold]")
    console.print(f"  Query: {sql[:50]}..." if len(sql) > 50 else f"  Query: {sql}")
    console.print(f"  Output: {output}")
    console.print()

    try:
        with Database(config.storage) as db_conn:
            df = db_conn.query_df(sql)

            if df.empty:
                console.print("[yellow]No results to export[/yellow]")
                return

            if format == "csv":
                df.to_csv(output, index=False)
            else:
                df.to_parquet(output, index=False)

            console.print(f"[green]✓[/green] Exported {len(df):,} rows to {output}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


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
        stdf2pq export lot E6A773.00 E6A774.00 results.csv
    """
    config: Config = ctx.obj["config"]

    console.print(f"\n[bold]stdf2pq - Export Lot[/bold]")
    console.print(f"  Lots: {', '.join(lot_ids)}")
    console.print(f"  Output: {output}")
    console.print(f"  Pivot: {'Yes' if pivot else 'No'}")
    console.print()

    placeholders = ", ".join(f"${i+1}" for i in range(len(lot_ids)))
    params = list(lot_ids)

    try:
        with Database(config.storage) as db_conn:
            if pivot:
                sql = f"""
                PIVOT (
                    SELECT
                        tr.lot_id,
                        tr.wafer_id,
                        tr.part_id,
                        p.x_coord,
                        p.y_coord,
                        p.hard_bin,
                        p.soft_bin,
                        p.passed as part_passed,
                        t.test_name,
                        tr.result
                    FROM test_results tr
                    JOIN parts p ON tr.part_id = p.part_id
                    JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
                    WHERE tr.lot_id IN ({placeholders})
                )
                ON test_name
                USING first(result)
                GROUP BY lot_id, wafer_id, part_id, x_coord, y_coord, hard_bin, soft_bin, part_passed
                ORDER BY lot_id, wafer_id, part_id
                """
            else:
                sql = f"""
                SELECT
                    tr.lot_id,
                    tr.wafer_id,
                    tr.part_id,
                    p.x_coord,
                    p.y_coord,
                    p.hard_bin,
                    p.soft_bin,
                    t.test_num,
                    t.test_name,
                    tr.result,
                    tr.passed,
                    t.lo_limit,
                    t.hi_limit,
                    t.units
                FROM test_results tr
                JOIN parts p ON tr.part_id = p.part_id
                JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
                WHERE tr.lot_id IN ({placeholders})
                ORDER BY tr.lot_id, tr.wafer_id, tr.part_id, t.test_num
                """

            df = db_conn.query_df(sql, params)

            if df.empty:
                console.print("[yellow]No results to export[/yellow]")
                return

            df.to_csv(output, index=False)
            console.print(f"[green]✓[/green] Exported {len(df):,} rows to {output}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@main.command()
@click.option("--port", "-p", default=8501, help="Port to run on")
@click.pass_context
def web(ctx, port: int):
    """Start the Streamlit web UI."""
    import subprocess
    
    app_path = Path(__file__).parent / "app.py"
    
    console.print(f"\n[bold]STDF Platform - Web UI[/bold]")
    console.print(f"  URL: http://localhost:{port}")
    console.print()
    console.print("[dim]Press Ctrl+C to stop[/dim]")
    
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        str(app_path),
        "--server.port", str(port),
        "--server.headless", "true",
    ])


if __name__ == "__main__":
    main()
