"""Subprocess worker for STDF ingest with timeout support.

Called by _run_ingest_batch() via subprocess.Popen.
Runs parse_stdf + save_stdf_data in an isolated process so it can be
killed with SIGKILL if it hangs.

Usage:
    python -m stdf_platform._ingest_worker <file> <product> <data_dir> <compression>

Outputs JSON to stdout on success:
    {"ok": true, "sub_process": "CP11", "test_category": "CP"}

Exits with code 1 on failure, stderr contains the error message.
"""

import json
import sys
from pathlib import Path


def main():
    if len(sys.argv) < 5:
        print("Usage: _ingest_worker <file> <product> <data_dir> <compression>", file=sys.stderr)
        sys.exit(1)

    import time

    file_path = Path(sys.argv[1])
    product = sys.argv[2]
    data_dir = Path(sys.argv[3])
    compression = sys.argv[4]

    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    print(f"[worker] start: {file_path.name} ({file_size_mb:.1f} MB)", file=sys.stderr)

    from .parser import parse_stdf
    from .storage import ParquetStorage, _get_test_category
    from .config import StorageConfig

    t0 = time.monotonic()
    data = parse_stdf(file_path)
    t_parse = time.monotonic() - t0
    print(
        f"[worker] parsed: {len(data.parts)} parts, {len(data.test_results)} results "
        f"({t_parse:.1f}s)",
        file=sys.stderr,
    )

    sub_process = data.test_code or "UNKNOWN"
    test_category = _get_test_category(sub_process)

    storage_config = StorageConfig(data_dir=data_dir)
    storage = ParquetStorage(storage_config)

    t1 = time.monotonic()
    _counts, pa_tables = storage.save_stdf_data(
        data,
        product=product,
        test_category=test_category,
        sub_process=sub_process,
        source_file=file_path.name,
        compression=compression,
    )
    t_save = time.monotonic() - t1
    print(f"[worker] saved parquet ({t_save:.1f}s)", file=sys.stderr)

    # Optional: write to ClickHouse (requires STDF_CH_HOST env var)
    ch_host = sys.argv[5] if len(sys.argv) > 5 else ""
    if ch_host:
        import os
        from .ch_writer import get_client, write_tables
        t2 = time.monotonic()
        try:
            ch = get_client(
                host=ch_host,
                port=int(os.environ.get("STDF_CH_PORT", "8123")),
                database=os.environ.get("STDF_CH_DB", "stdf"),
                username=os.environ.get("STDF_CH_USER", "default"),
                password=os.environ.get("STDF_CH_PASS", ""),
            )
            inserted = write_tables(ch, pa_tables)
            t_ch = time.monotonic() - t2
            print(f"[worker] clickhouse insert {inserted} ({t_ch:.1f}s)", file=sys.stderr)
        except Exception as e:
            print(f"[worker] clickhouse insert failed (parquet OK): {e}", file=sys.stderr)

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
