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

    file_path = Path(sys.argv[1])
    product = sys.argv[2]
    data_dir = Path(sys.argv[3])
    compression = sys.argv[4]

    from .parser import parse_stdf
    from .storage import ParquetStorage, _get_test_category
    from .config import StorageConfig

    data = parse_stdf(file_path)

    sub_process = data.test_code or "UNKNOWN"
    test_category = _get_test_category(sub_process)

    storage_config = StorageConfig(data_dir=data_dir)
    storage = ParquetStorage(storage_config)
    storage.save_stdf_data(
        data,
        product=product,
        test_category=test_category,
        sub_process=sub_process,
        source_file=file_path.name,
        compression=compression,
    )

    # Output result as JSON for parent process
    json.dump({
        "ok": True,
        "sub_process": sub_process,
        "test_category": test_category,
    }, sys.stdout)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
