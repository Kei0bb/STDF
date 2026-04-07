"""Tracks which local STDF files have been successfully ingested.

Used by `ingest-all` to support resuming interrupted batch ingests.
State is persisted to {data_dir}/ingest_history.json.
"""

import json
from datetime import datetime
from pathlib import Path


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
        self.history_file.parent.mkdir(parents=True, exist_ok=True)
        self.history_file.write_text(
            json.dumps({"ingested": self._done}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def is_done(self, path: Path) -> bool:
        return str(path.resolve()) in self._done

    def mark_done_batch(self, paths: list[Path]) -> None:
        now = datetime.now().isoformat()
        for p in paths:
            self._done[str(p.resolve())] = now
        self._save()

    def count(self) -> int:
        return len(self._done)
