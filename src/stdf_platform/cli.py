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
from .parser import parse_stdf
from .storage import ParquetStorage
from .database import Database
from .sync_manager import SyncManager


console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="stdf-platform")
@click.option("--config", "-c", type=click.Path(path_type=Path), help="Config file path")
@click.pass_context
def main(ctx, config: Path | None):
    """STDF Data Platform - Semiconductor test data analysis."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = Config.load(config)


@main.command()
@click.argument("stdf_file", type=click.Path(exists=True, path_type=Path))
@click.option("--product", "-p", help="Product name (auto-detect from path if not specified)")
@click.option("--test-type", "-t", type=click.Choice(["CP", "FT"]), help="Test type (auto-detect from path if not specified)")
@click.option("--from-path", is_flag=True, help="Extract product/test-type from file path")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def ingest(ctx, stdf_file: Path, product: str | None, test_type: str | None, from_path: bool, verbose: bool):
    """
    Ingest an STDF file into the data platform.

    STDF_FILE: Path to the STDF file to ingest

    Product and test type can be specified via options or auto-detected from file path.
    Expected path structure: .../product/test_type/file.stdf
    """
    config: Config = ctx.obj["config"]
    config.ensure_directories()

    # Extract product/test_type from path if requested or not specified
    if from_path or (product is None and test_type is None):
        parts = stdf_file.resolve().parts
        # Look for CP or FT in path to identify test_type
        for i, part in enumerate(parts):
            if part.upper() in ("CP", "FT"):
                test_type = part.upper()
                if i > 0:
                    product = parts[i - 1]
                break

    # Default values if still not found
    product = product or "UNKNOWN"
    test_type = test_type or "UNKNOWN"

    console.print(f"\n[bold]STDF Platform - Ingest[/bold]")
    console.print(f"  File: {stdf_file}")
    console.print(f"  Product: {product}")
    console.print(f"  Test Type: {test_type}")
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

        # Parse STDF
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Parsing STDF file...", total=None)
            data = parse_stdf(file_to_parse)
            progress.update(task, description="[green]✓[/green] Parsed STDF file")

        console.print(f"  Lot ID: {data.lot_id}")
        console.print(f"  Part Type: {data.part_type}")
        console.print(f"  Job: {data.job_name} ({data.job_rev})")
        console.print(f"  Wafers: {len(data.wafers)}")
        console.print(f"  Parts: {len(data.parts)}")
        console.print(f"  Tests: {len(data.tests)}")
        console.print(f"  Test Results: {len(data.test_results)}")
        console.print()

        # Save to Parquet
        storage = ParquetStorage(config.storage)
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            prog_task = progress.add_task("Saving to Parquet...", total=None)
            counts = storage.save_stdf_data(data, product, test_type, config.processing.compression)
            progress.update(prog_task, description="[green]✓[/green] Saved to Parquet")

        # Show results
        table = Table(title="Saved Records")
        table.add_column("Table", style="cyan")
        table.add_column("Count", justify="right", style="green")

        for table_name, count in counts.items():
            table.add_row(table_name, f"{count:,}")

        console.print(table)
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


@main.command()
@click.option("--lot", "-l", help="Filter by lot ID")
@click.pass_context
def lots(ctx, lot: str | None):
    """List ingested lots."""
    config: Config = ctx.obj["config"]

    try:
        with Database(config.storage) as db:
            results = db.get_lot_summary(lot)

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


@main.command()
@click.argument("sql")
@click.pass_context
def query(ctx, sql: str):
    """Execute SQL query against the database."""
    config: Config = ctx.obj["config"]

    try:
        with Database(config.storage) as db:
            results = db.query(sql)

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


@main.command()
@click.pass_context
def shell(ctx):
    """Open DuckDB interactive shell."""
    config: Config = ctx.obj["config"]

    console.print(f"[bold]Opening DuckDB shell...[/bold]")
    console.print(f"Database: {config.storage.database}")
    console.print()

    import subprocess
    subprocess.run(["duckdb", str(config.storage.database)])


@main.command()
@click.option("--product", "-p", multiple=True, help="Product filter (can specify multiple)")
@click.option("--test-type", "-t", multiple=True, help="Test type filter (CP, FT)")
@click.option("--limit", "-n", type=int, help="Maximum files to fetch")
@click.option("--ingest/--no-ingest", default=True, help="Auto-ingest after download")
@click.option("--force", "-f", is_flag=True, help="Force re-download even if file exists")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def fetch(ctx, product: tuple, test_type: tuple, limit: int | None, ingest: bool, force: bool, verbose: bool):
    """
    Fetch STDF files from FTP server with incremental sync.

    Downloads only new files from FTP (skips already downloaded).
    Uses filters from config.yaml, or override with --product and --test-type.
    Use --force to re-download all files.
    """
    from .ftp_client import FTPClient

    config: Config = ctx.obj["config"]
    config.ensure_directories()

    # Initialize sync manager
    sync_history_file = config.storage.download_dir / ".sync_history.json"
    sync_manager = SyncManager(sync_history_file)

    console.print(f"\n[bold]STDF Platform - Fetch from FTP[/bold]")
    console.print(f"  Host: {config.ftp.host}")
    console.print(f"  Sync History: {sync_manager.get_downloaded_count()} files tracked")

    # Show filters
    # CLI options override config settings
    cli_products = list(product) if product else None
    cli_test_types = list(test_type) if test_type else None
    
    # Show which filters are active
    if config.filters and not cli_products and not cli_test_types:
        console.print("  Filters (from config):")
        for f in config.filters:
            console.print(f"    - {f.product}: {', '.join(f.test_types)}")
    else:
        products_to_show = cli_products or config.products
        test_types_to_show = cli_test_types or config.test_types
        if products_to_show:
            console.print(f"  Products: {', '.join(products_to_show)}")
        else:
            console.print("  Products: [dim]all[/dim]")
        console.print(f"  Test Types: {', '.join(test_types_to_show)}")
    
    if force:
        console.print("  [yellow]Force mode: re-downloading all files[/yellow]")
    console.print()

    try:
        with FTPClient(config.ftp) as client:
            # List all files first (no filter at FTP level to allow config.should_fetch)
            files = list(client.list_stdf_files(
                products=cli_products,
                test_types=cli_test_types or config.test_types,
            ))
            
            # Apply config filters if no CLI override and filters are defined
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
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
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
        if ingest and downloaded:
            console.print("\n[bold]Ingesting files...[/bold]")
            storage = ParquetStorage(config.storage)
            success = 0
            failed = 0

            for remote_path, local_path, prod, ttype in downloaded:
                try:
                    data = parse_stdf(local_path)
                    storage.save_stdf_data(data, prod, ttype, config.processing.compression)
                    sync_manager.mark_ingested(remote_path)
                    console.print(f"  [green]✓[/green] {local_path.name} ({prod}/{ttype})")
                    success += 1
                except Exception as e:
                    console.print(f"  [red]✗[/red] {local_path.name}: {e}")
                    failed += 1

            console.print(f"\n[green]✓[/green] Ingested {success} files")
            if failed:
                console.print(f"[yellow]![/yellow] {failed} files failed")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if verbose:
            console.print_exception()
        sys.exit(1)


@main.command()
@click.argument("sql")
@click.argument("output", type=click.Path(path_type=Path))
@click.option("--format", "-f", type=click.Choice(["csv", "parquet"]), default="csv", help="Output format")
@click.pass_context
def export(ctx, sql: str, output: Path, format: str):
    """
    Export query results to CSV or Parquet file.

    SQL: SQL query to execute
    OUTPUT: Output file path

    Example:
        stdf-platform export "SELECT * FROM test_results WHERE lot_id='LOT001'" results.csv
    """
    config: Config = ctx.obj["config"]

    console.print(f"\n[bold]STDF Platform - Export[/bold]")
    console.print(f"  Query: {sql[:50]}..." if len(sql) > 50 else f"  Query: {sql}")
    console.print(f"  Output: {output}")
    console.print()

    try:
        with Database(config.storage) as db:
            # Get DataFrame
            df = db.query_df(sql)

            if df.empty:
                console.print("[yellow]No results to export[/yellow]")
                return

            # Export based on format
            if format == "csv":
                df.to_csv(output, index=False)
            else:
                df.to_parquet(output, index=False)

            console.print(f"[green]✓[/green] Exported {len(df):,} rows to {output}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@main.command()
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
        stdf-platform export-lot E6A773.00 E6A774.00 results.csv
    """
    config: Config = ctx.obj["config"]

    console.print(f"\n[bold]STDF Platform - Export Lot[/bold]")
    console.print(f"  Lots: {', '.join(lot_ids)}")
    console.print(f"  Output: {output}")
    console.print(f"  Pivot: {'Yes' if pivot else 'No'}")
    console.print()

    lot_list = ", ".join(f"'{lot}'" for lot in lot_ids)

    try:
        with Database(config.storage) as db:
            if pivot:
                # Pivot format: one row per part, columns are test parameters
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
                    WHERE tr.lot_id IN ({lot_list})
                )
                ON test_name
                USING first(result)
                GROUP BY lot_id, wafer_id, part_id, x_coord, y_coord, hard_bin, soft_bin, part_passed
                ORDER BY lot_id, wafer_id, part_id
                """
            else:
                # Long format: one row per test result
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
                WHERE tr.lot_id IN ({lot_list})
                ORDER BY tr.lot_id, tr.wafer_id, tr.part_id, t.test_num
                """

            df = db.query_df(sql)

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
