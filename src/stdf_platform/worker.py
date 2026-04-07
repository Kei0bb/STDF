"""Concurrent STDF ingest worker pool.

Manages N simultaneous ingest subprocesses using ThreadPoolExecutor.
Each thread owns one subprocess call — subprocess isolation is preserved
for memory safety while gaining true parallelism across files.
"""

import json
import sys
import subprocess
import threading
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
    """Run one ingest worker subprocess. Called from a thread pool worker."""
    cmd = [
        sys.executable, "-m", "stdf_platform._ingest_worker",
        str(local_path),
        product,
        str(data_dir),
        compression,
    ]

    stderr_lines: list[str] = []

    def _collect_stderr(proc):
        try:
            log_file = open(log_path, "a", encoding="utf-8") if log_path else None
        except OSError:
            log_file = None
        for line in proc.stderr:
            stderr_lines.append(line.rstrip())
            if log_file:
                log_file.write(line)
                log_file.flush()
        if log_file:
            log_file.close()

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",  # explicit UTF-8 for WSL2 / Windows compatibility
        )
    except Exception as e:
        return IngestResult(
            local_path=local_path, remote_path=None,
            success=False, error=f"Popen failed: {e}",
        )

    stderr_thread = threading.Thread(target=_collect_stderr, args=(proc,), daemon=True)
    stderr_thread.start()

    try:
        stdout, _ = proc.communicate(timeout=timeout)
        stderr_thread.join(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        stderr_thread.join(timeout=5)
        last = stderr_lines[-1] if stderr_lines else "no output"
        return IngestResult(
            local_path=local_path, remote_path=None,
            success=False, error=f"timed out after {timeout}s (last: {last})",
        )

    if proc.returncode != 0:
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
