from stdf_platform.config import Config
from stdf_platform.sync_manager import SyncManager
from stdf_platform.cli import _run_ingest_batch
from pathlib import Path

config = Config()
sync = SyncManager(Path("sync.json"))

to_ingest = [
    ("remote_path", Path("docs/sample_queries.md"), "PROD", "CP") 
]

_run_ingest_batch(config, sync, to_ingest, cleanup=False, timeout=2, verbose=True)
