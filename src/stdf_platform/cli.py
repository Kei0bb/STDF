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
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def ingest(ctx, stdf_file: Path, verbose: bool):
    """
    Ingest an STDF file into the data platform.

    STDF_FILE: Path to the STDF file to ingest
    """
    config: Config = ctx.obj["config"]
    config.ensure_directories()

    console.print(f"\n[bold]STDF Platform - Ingest[/bold]")
    console.print(f"  File: {stdf_file}")
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
            task = progress.add_task("Saving to Parquet...", total=None)
            counts = storage.save_stdf_data(data, config.processing.compression)
            progress.update(task, description="[green]✓[/green] Saved to Parquet")

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
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def fetch(ctx, product: tuple, test_type: tuple, limit: int | None, ingest: bool, verbose: bool):
    """
    Fetch STDF files from FTP server.

    Downloads files from FTP and optionally ingests them into the database.
    Uses filters from config.yaml, or override with --product and --test-type.
    """
    from .ftp_client import fetch_stdf_files

    config: Config = ctx.obj["config"]
    config.ensure_directories()

    console.print(f"\n[bold]STDF Platform - Fetch from FTP[/bold]")
    console.print(f"  Host: {config.ftp.host}")

    # Show filters
    products = list(product) if product else config.products
    test_types = list(test_type) if test_type else config.test_types

    if products:
        console.print(f"  Products: {', '.join(products)}")
    else:
        console.print("  Products: [dim]all[/dim]")
    console.print(f"  Test Types: {', '.join(test_types)}")
    console.print()

    try:
        # Fetch files
        downloaded = fetch_stdf_files(
            config,
            products=products if products else None,
            test_types=test_types,
            limit=limit,
        )

        if not downloaded:
            console.print("[yellow]No files found matching filters[/yellow]")
            return

        console.print(f"\n[green]✓[/green] Downloaded {len(downloaded)} files")

        # Auto-ingest if enabled
        if ingest:
            console.print("\n[bold]Ingesting files...[/bold]")
            storage = ParquetStorage(config.storage)
            success = 0
            failed = 0

            for local_path, prod, ttype in downloaded:
                try:
                    data = parse_stdf(local_path)
                    storage.save_stdf_data(data, config.processing.compression)
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


if __name__ == "__main__":
    main()

