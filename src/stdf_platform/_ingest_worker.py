"""Subprocess worker for STDF ingest with timeout support.

Called by _run_ingest_batch() via subprocess.Popen.
Runs parse_stdf + save_stdf_data in an isolated process so it can be
killed with SIGKILL if it hangs.

Usage:
    python -m stdf_platform._ingest_worker <file> <product> <data_dir> <compression> [--allow-truncated]

<file> may be a .gz: it is decompressed to a temp dir (removed afterwards, also
on failure), parsed from there, and the ORIGINAL basename is stored as
source_file.

Outputs JSON to stdout on success:
    {"ok": true, "sub_process": "CP11", "test_category": "CP"}

Exits with code 1 on failure, stderr contains the error message.
"""

import gzip
import json
import shutil
import sys
import tempfile
import zlib
from pathlib import Path

GZIP_ERRORS = (gzip.BadGzipFile, EOFError, zlib.error)


def _decompress(src: Path, dest_dir: Path) -> Path:
    """Gunzip `src` into `dest_dir`, named like the original minus `.gz`.

    A broken stream raises RuntimeError("<name>: corrupt gzip ...") so the
    parent reports it as an ordinary per-file failure.
    """
    out = dest_dir / src.name[:-3]
    try:
        with gzip.open(src, "rb") as f_in, open(out, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    except GZIP_ERRORS as e:
        raise RuntimeError(f"{src.name}: corrupt gzip ({e})") from e
    return out


def main():
    args = sys.argv[1:]
    allow_truncated = "--allow-truncated" in args
    override = next((a.split("=", 1)[1] for a in args if a.startswith("--sub-process=")), "")
    args = [a for a in args if a != "--allow-truncated" and not a.startswith("--sub-process=")]
    if len(args) < 4:
        print("Usage: _ingest_worker <file> <product> <data_dir> <compression> "
              "[--allow-truncated]", file=sys.stderr)
        sys.exit(1)

    import time

    file_path = Path(args[0])
    product = args[1]
    data_dir = Path(args[2])
    compression = args[3]

    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    print(f"[worker] start: {file_path.name} ({file_size_mb:.1f} MB)", file=sys.stderr)

    from .parser import parse_stdf
    from .storage import ParquetStorage, _get_test_category
    from .config import StorageConfig

    t0 = time.monotonic()
    if file_path.suffix.lower() == ".gz":
        with tempfile.TemporaryDirectory(prefix="stdf_gz_") as tmp:
            data = parse_stdf(_decompress(file_path, Path(tmp)),
                              allow_truncated=allow_truncated)
    else:
        data = parse_stdf(file_path, allow_truncated=allow_truncated)
    if data.truncated:
        print(f"[worker] WARNING: {file_path.name} is truncated; "
              "kept complete records only", file=sys.stderr)
    t_parse = time.monotonic() - t0
    print(
        f"[worker] parsed: {len(data.parts)} parts, {len(data.test_results)} results "
        f"({t_parse:.1f}s)",
        file=sys.stderr,
    )

    sub_process = override or data.test_code or "UNKNOWN"
    test_category = _get_test_category(sub_process)

    storage_config = StorageConfig(data_dir=data_dir)
    storage = ParquetStorage(storage_config)

    t1 = time.monotonic()
    _ = storage.save_stdf_data(
        data,
        product=product,
        test_category=test_category,
        sub_process=sub_process,
        # X.stdf.gz and its decompressed X.stdf (what fetch ingests) are the
        # same source: strip .gz so a re-ingest reuses the retest number.
        source_file=file_path.name[:-3] if file_path.name.lower().endswith(".gz")
        else file_path.name,
        compression=compression,
    )
    t_save = time.monotonic() - t1
    print(f"[worker] saved parquet ({t_save:.1f}s)", file=sys.stderr)

    # Output result as JSON for parent process
    json.dump({
        "ok": True,
        "sub_process": sub_process,
        "test_category": test_category,
        "lot_id": data.lot_id,
        "wafer_count": len(data.wafers),
        "part_count": len(data.parts),
        "test_count": len(data.tests),
    }, sys.stdout)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
