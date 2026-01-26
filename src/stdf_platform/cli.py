"""CLI interface for STDF Platform."""

import sys
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

    try:
        # Parse STDF
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Parsing STDF file...", total=None)
            data = parse_stdf(stdf_file)
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


if __name__ == "__main__":
    main()
