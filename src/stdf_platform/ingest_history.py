"""Tracks which local STDF files have been successfully ingested.

Used by `ingest-all` to support resuming interrupted batch ingests.
State is persisted to {data_dir}/ingest_history.json.
"""

import json
from datetime import datetime
from pathlib import Path

from .atomic import atomic_write_json


class IngestHistory:
    """Tracks locally-ingested STDF files so interrupted batches can be resumed."""

    def __init__(self, history_file: Path):
        self.history_file = history_file
        # key: str(resolved path) → {"ingested_at": ISO datetime}
        self._done: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if self.history_file.exists():
            try:
                data = json.loads(self.history_file.read_text(encoding="utf-8"))
                self._done = data.get("ingested", {})
            except Exception:
                self._done = {}

    def _save(self) -> None:
        atomic_write_json(self.history_file, {"ingested": self._done})

    def is_done(self, path: Path) -> bool:
        return str(path.resolve()) in self._done

    def mark_done_batch(self, paths: list[Path]) -> None:
        now = datetime.now().isoformat()
        for p in paths:
            self._done[str(p.resolve())] = now
        self._save()
