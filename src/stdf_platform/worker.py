"""Concurrent STDF ingest worker pool.

Manages N simultaneous ingest subprocesses using ThreadPoolExecutor.
Each thread owns one subprocess call — subprocess isolation is preserved
for memory safety while gaining true parallelism across files.
"""

import json
import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

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

# Cap on data/ingest_worker.log before it is rotated to .1 (single backup).
LOG_MAX_BYTES = 5 * 1024 * 1024


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
            # Child stdio is pinned to UTF-8 via PYTHONIOENCODING (Python 3.14 on
            # Windows is pre-PEP-686 and otherwise defaults child stdio to the
            # locale encoding, e.g. cp932 on Japanese Windows, so error messages
            # would arrive mangled). errors="replace" is a parent-side safety
            # net: if some third-party output still isn't valid UTF-8, a stray
            # byte degrades to U+FFFD instead of raising UnicodeDecodeError and
            # killing the reader thread (as seen on Japanese Windows).
            encoding="utf-8",
            errors="replace",
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
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

    # Append stderr to log file after process exits (avoids pipe race condition).
    # Rotate to a single .1 backup once the log exceeds LOG_MAX_BYTES.
    if log_path and stderr:
        try:
            if log_path.exists() and log_path.stat().st_size >= LOG_MAX_BYTES:
                backup = log_path.with_suffix(log_path.suffix + ".1")
                try:
                    os.replace(log_path, backup)
                except OSError:
                    pass
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
    on_success: Optional[Callable[[IngestResult], None]] = None,
) -> tuple[list[IngestResult], list[IngestResult]]:
    """Ingest files concurrently using a subprocess worker pool.

    Args:
        files: List of (remote_path, local_path, product, ttype) tuples.
        data_dir: Root data directory for Parquet output.
        compression: Parquet compression (zstd, gzip, snappy, etc.).
        max_workers: Max concurrent subprocesses (default 4).
        timeout: Per-file timeout in seconds (default 300).
        on_success: Optional callback invoked on the pool's consumer thread
            once per successful file (with its IngestResult, remote_path
            already set), so callers can persist progress incrementally —
            an aborted run (Ctrl+C mid-batch) does not lose track of files
            that were already ingested.

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
                    if on_success is not None:
                        on_success(result)
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
