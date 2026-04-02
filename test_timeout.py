from stdf_platform.config import Config
from stdf_platform.sync_manager import SyncManager
from stdf_platform.cli import _run_ingest_batch
from pathlib import Path
import time
import sys

# Replace _ingest_worker code with an infinite loop to simulate hanging Rust code
with open("src/stdf_platform/_ingest_worker.py", "r") as f:
    orig = f.read()

import os
with open("src/stdf_platform/_ingest_worker.py", "w") as f:
    f.write('''import time
import sys
# Simulate a hang inside native code (or Python)
print("Hanging forever...", file=sys.stderr)
while True:
    time.sleep(1)
''')

config = Config()
sync = SyncManager(Path("sync.json"))
to_ingest = [("remote_path", Path("docs/sample_queries.md"), "PROD", "CP")]

try:
    print("Running ingest batch, should timeout after 2 seconds...")
    _run_ingest_batch(config, sync, to_ingest, cleanup=False, timeout=2, verbose=True)
finally:
    # restore
    with open("src/stdf_platform/_ingest_worker.py", "w") as f:
        f.write(orig)
