"""Concurrent STDF ingest worker pool.

Manages N simultaneous ingest subprocesses using ThreadPoolExecutor.
Each thread owns one subprocess call — subprocess isolation is preserved
for memory safety while gaining true parallelism across files.
"""

import json
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

console = Console()


@dataclass
class IngestResult:
    local_path: Path
    remote_path: Optional[str]
    success: bool
    sub_process: str = "UNKNOWN"
    test_category: str = "OTHER"
    error: str = ""


def _run_single(
    local_path: Path,
    product: str,
    data_dir: Path,
    compression: str,
    timeout: int,
    log_path: Optional[Path],
) -> IngestResult:
    """Run one ingest worker subprocess. Called from a thread pool worker.

    stderr is captured via communicate() to avoid a race condition where a
    separate reader thread and communicate() both try to close the same pipe
    (causes ValueError on Windows/CPython 3.14).
    """
    cmd = [
        sys.executable, "-m", "stdf_platform._ingest_worker",
        str(local_path),
        product,
        str(data_dir),
        compression,
    ]

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",  # explicit UTF-8 for WSL2 / Windows compatibility
        )
    except Exception as e:
        return IngestResult(
            local_path=local_path, remote_path=None,
            success=False, error=f"Popen failed: {e}",
        )

    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        _, stderr = proc.communicate()
        last_line = (stderr or "").strip().splitlines()[-1] if stderr else "no output"
        return IngestResult(
            local_path=local_path, remote_path=None,
            success=False, error=f"timed out after {timeout}s (last: {last_line})",
        )

    # Append stderr to log file after process exits (avoids pipe race condition)
    if log_path and stderr:
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(stderr)
        except OSError:
            pass

    if proc.returncode != 0:
        stderr_lines = (stderr or "").strip().splitlines()
        error_msg = stderr_lines[-1] if stderr_lines else f"exit code {proc.returncode}"
        return IngestResult(
            local_path=local_path, remote_path=None,
            success=False, error=error_msg,
        )

    try:
        result = json.loads(stdout)
        return IngestResult(
            local_path=local_path,
            remote_path=None,
            success=True,
            sub_process=result.get("sub_process", "UNKNOWN"),
            test_category=result.get("test_category", "OTHER"),
        )
    except json.JSONDecodeError:
        return IngestResult(local_path=local_path, remote_path=None, success=True)


def run_ingest_pool(
    files: list[tuple],
    data_dir: Path,
    compression: str,
    max_workers: int = 4,
    timeout: int = 300,
) -> tuple[list[IngestResult], list[IngestResult]]:
    """Ingest files concurrently using a subprocess worker pool.

    Args:
        files: List of (remote_path, local_path, product, ttype) tuples.
        data_dir: Root data directory for Parquet output.
        compression: Parquet compression (gzip, snappy, etc.).
        max_workers: Max concurrent subprocesses (default 4).
        timeout: Per-file timeout in seconds (default 300).

    Returns:
        (successes, failures) — lists of IngestResult.
    """
    log_path = data_dir / "ingest_worker.log"
    successes: list[IngestResult] = []
    failures: list[IngestResult] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task(f"Ingesting ({max_workers} workers)...", total=len(files))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    _run_single,
                    local_path, product, data_dir, compression, timeout, log_path,
                ): (remote_path, local_path, product)
                for remote_path, local_path, product, _ttype in files
            }

            for future in as_completed(future_map):
                remote_path, local_path, product = future_map[future]
                result = future.result()
                result.remote_path = remote_path

                if result.success:
                    successes.append(result)
                    progress.console.print(
                        f"  [green]✓[/green] {local_path.name}"
                        f"  [dim]{product}/{result.test_category}/{result.sub_process}[/dim]"
                    )
                else:
                    failures.append(result)
                    progress.console.print(
                        f"  [red]✗[/red] {local_path.name}: {result.error}"
                    )

                progress.advance(task)

    return successes, failures
