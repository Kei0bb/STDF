"""Tracks which local STDF files have been successfully ingested.

Used by `ingest-all` to support resuming interrupted batch ingests.
State is persisted to {data_dir}/ingest_history.json.
"""

import json
import os
import tempfile
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
        fd, tmp = tempfile.mkstemp(
            dir=self.history_file.parent, prefix=".ingest_history.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"ingested": self._done}, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.history_file)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def is_done(self, path: Path) -> bool:
        return str(path.resolve()) in self._done

    def mark_done_batch(self, paths: list[Path]) -> None:
        now = datetime.now().isoformat()
        for p in paths:
            self._done[str(p.resolve())] = now
        self._save()

    def count(self) -> int:
        return len(self._done)
