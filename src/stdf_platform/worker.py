"""Concurrent STDF ingest worker pool.

Manages N simultaneous ingest subprocesses using ThreadPoolExecutor.
Each thread owns one subprocess call — subprocess isolation is preserved
for memory safety while gaining true parallelism across files.
"""

import json
import os
import sys
import subprocess
import threading
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


def _lot_key(local_path: Path) -> str:
    """Grouping key for same-lot serialization: filename text before the
    first '_' (e.g. "2613-X03_00_SC0G29A_...@FT1_1#202604050254.std" ->
    "2613-X03"). If there is no '_', the whole filename is the key, so that
    file gets its own single-file group (under-grouping is safe — it only
    costs parallelism, never correctness).
    """
    name = local_path.name
    return name.split("_", 1)[0] if "_" in name else name


def _ts_key(local_path: Path) -> str:
    """Sort key for chronological order within a lot group: the text after
    the last '#' in the filename stem (e.g. "...#202604050254" ->
    "202604050254"). Falls back to the full filename if there is no '#'.
    Plain string comparison is fine — these are zero-padded timestamps.
    """
    stem = local_path.stem
    return stem.rsplit("#", 1)[-1] if "#" in stem else local_path.name


def run_ingest_pool(
    files: list[tuple],
    data_dir: Path,
    compression: str,
    max_workers: int = 4,
    timeout: int = 300,
    on_success: Optional[Callable[[IngestResult], None]] = None,
) -> tuple[list[IngestResult], list[IngestResult]]:
    """Ingest files concurrently using a subprocess worker pool.

    Files are grouped by lot (see `_lot_key`) and each lot's files run
    SEQUENTIALLY, in measurement-time order (`_ts_key`), within one group
    worker. Different lots still run in parallel. This is required because
    files from the same lot share mutable on-disk state that a
    read-then-write race would corrupt:

      1. the lots table always rewrites the same lot-level data.parquet —
         two concurrent writers would clobber each other or trip the
         os.replace retry (see storage.py).
      2. `_get_next_retest_num` is a read-then-write directory scan — two
         workers processing the same wafer concurrently can both read the
         same "next" retest number and both claim it.
      3. `_demote_superseded` reads and rewrites OLDER retest files when a
         die/test key is re-measured — a second worker's os.replace on the
         same file races the first (WinError 5 on Windows) and can corrupt
         retest_flag ranks.

    Grouping and ordering rely on the FTP filename convention: the lot
    prefix is the text before the first '_', and the measurement timestamp
    is the text after the last '#'. A wrong grouping only costs parallelism
    in one direction: OVER-grouping (e.g. two unrelated files sharing a
    prefix) just serializes files that could have run in parallel, which is
    safe. UNDER-grouping cannot silently happen for files that really share
    a lot, because `_lot_key` falls back to the literal filename (its own
    single-file group) only when there is no '_' at all — an unexpected
    naming scheme degrades to one group per file, not to merging unrelated
    lots.

    Args:
        files: List of (remote_path, local_path, product, ttype) tuples.
        data_dir: Root data directory for Parquet output.
        compression: Parquet compression (zstd, gzip, snappy, etc.).
        max_workers: Max concurrent lot GROUPS (default 4). Files within a
            group always run one at a time regardless of this value.
        timeout: Per-file timeout in seconds (default 300).
        on_success: Optional callback invoked once per successful file (from
            whichever group-worker thread completed it — callers doing I/O
            in this callback must be thread-safe), with its IngestResult
            (remote_path already set), so callers can persist progress
            incrementally — an aborted run (Ctrl+C mid-batch) does not lose
            track of files that were already ingested.

    Returns:
        (successes, failures) — lists of IngestResult.
    """
    log_path = data_dir / "ingest_worker.log"
    successes: list[IngestResult] = []
    failures: list[IngestResult] = []
    result_lock = threading.Lock()

    # Group files by (product, lot) and sort each group chronologically so a
    # single group-worker thread processes one lot's files in measurement
    # order, never concurrently.
    groups: dict[tuple[str, str], list[tuple]] = {}
    for remote_path, local_path, product, ttype in files:
        key = (product, _lot_key(local_path))
        groups.setdefault(key, []).append((remote_path, local_path, product, ttype))
    for key in groups:
        groups[key].sort(key=lambda f: _ts_key(f[1]))

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

        def _run_group(group_files: list[tuple]) -> None:
            for remote_path, local_path, product, _ttype in group_files:
                result = _run_single(
                    local_path, product, data_dir, compression, timeout, log_path,
                )
                result.remote_path = remote_path

                with result_lock:
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

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            group_futures = [
                executor.submit(_run_group, group_files)
                for group_files in groups.values()
            ]
            for future in as_completed(group_futures):
                future.result()  # propagate any unexpected exception

    return successes, failures
