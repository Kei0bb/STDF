"""CLI interface for stdf2parquet."""

import sys
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table

from . import __version__
from .converter import convert_stdf_to_parquet
from .records import RECORD_TYPES


console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="stdf2parquet")
def main():
    """STDF to Parquet converter for semiconductor test data."""
    pass


@main.command()
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.argument("output_dir", type=click.Path(path_type=Path))
@click.option(
    "--records",
    "-r",
    help="Comma-separated list of record types to convert (e.g., PTR,PIR,PRR)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Disable progress bar",
)
def convert(
    input_file: Path,
    output_dir: Path,
    records: str | None,
    verbose: bool,
    no_progress: bool,
):
    """
    Convert STDF file to Parquet format.

    INPUT_FILE: Path to the STDF file to convert
    OUTPUT_DIR: Directory where Parquet files will be written
    """
    # Validate input file
    if not input_file.exists():
        console.print(f"[red]Error:[/red] Input file not found: {input_file}")
        sys.exit(1)

    # Parse record filter
    record_filter = None
    if records:
        record_filter = set(r.strip().upper() for r in records.split(","))
        invalid_records = record_filter - set(RECORD_TYPES)
        if invalid_records:
            console.print(
                f"[yellow]Warning:[/yellow] Unknown record types: {', '.join(invalid_records)}"
            )

    # Show conversion info
    console.print(f"\n[bold]STDF to Parquet Converter[/bold]")
    console.print(f"  Input:  {input_file}")
    console.print(f"  Output: {output_dir}/")
    if record_filter:
        console.print(f"  Records: {', '.join(sorted(record_filter))}")
    console.print()

    try:
        if no_progress:
            results = convert_stdf_to_parquet(
                input_file, output_dir, record_filter
            )
        else:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console,
            ) as progress:
                task = progress.add_task("Converting...", total=None)

                def update_progress(count: int):
                    progress.update(task, description=f"Processing record {count:,}...")

                results = convert_stdf_to_parquet(
                    input_file, output_dir, record_filter, update_progress
                )

        # Show results
        if results:
            table = Table(title="Conversion Results")
            table.add_column("Record Type", style="cyan")
            table.add_column("Count", justify="right", style="green")
            table.add_column("Output File", style="dim")

            for record_type in sorted(results.keys()):
                count = results[record_type]
                table.add_row(
                    record_type,
                    f"{count:,}",
                    f"{record_type}.parquet"
                )

            console.print(table)
            console.print(
                f"\n[green]✓[/green] Successfully converted {sum(results.values()):,} records "
                f"to {len(results)} Parquet files"
            )
        else:
            console.print("[yellow]Warning:[/yellow] No records were converted")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if verbose:
            console.print_exception()
        sys.exit(1)


@main.command()
def list_records():
    """List all supported STDF record types."""
    table = Table(title="Supported STDF Record Types")
    table.add_column("Record Type", style="cyan")
    table.add_column("Description")

    record_descriptions = {
        "FAR": "File Attributes Record",
        "ATR": "Audit Trail Record",
        "MIR": "Master Information Record",
        "MRR": "Master Results Record",
        "PCR": "Part Count Record",
        "HBR": "Hardware Bin Record",
        "SBR": "Software Bin Record",
        "PMR": "Pin Map Record",
        "PGR": "Pin Group Record",
        "PLR": "Pin List Record",
        "RDR": "Retest Data Record",
        "SDR": "Site Description Record",
        "WIR": "Wafer Information Record",
        "WRR": "Wafer Results Record",
        "WCR": "Wafer Configuration Record",
        "PIR": "Part Information Record",
        "PRR": "Part Results Record",
        "TSR": "Test Synopsis Record",
        "PTR": "Parametric Test Record",
        "MPR": "Multiple-Result Parametric Record",
        "FTR": "Functional Test Record",
        "BPS": "Begin Program Section Record",
        "EPS": "End Program Section Record",
        "GDR": "Generic Data Record",
        "DTR": "Datalog Text Record",
    }

    for record_type in RECORD_TYPES:
        description = record_descriptions.get(record_type, "")
        table.add_row(record_type, description)

    console.print(table)


if __name__ == "__main__":
    main()
