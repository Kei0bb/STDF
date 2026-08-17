"""Sync manager for tracking downloaded and ingested STDF files."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from .atomic import atomic_write_json


class SyncManager:
    """Manages sync history for FTP downloads."""

    def __init__(self, history_file: Path):
        """
        Initialize sync manager.

        Args:
            history_file: Path to JSON history file
        """
        self.history_file = history_file
        self._history: dict = {"files": {}, "corrupt": {}}
        self._load()

    def _load(self) -> None:
        """Load history from file."""
        if self.history_file.exists():
            try:
                with open(self.history_file, "r", encoding="utf-8") as f:
                    self._history = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._history = {"files": {}, "corrupt": {}}
        # "corrupt" was added later; histories written before then lack it.
        self._history.setdefault("files", {})
        self._history.setdefault("corrupt", {})

    def _save(self) -> None:
        """Save history atomically."""
        atomic_write_json(self.history_file, self._history)

    def is_downloaded(self, remote_path: str) -> bool:
        """
        Check if a file has already been downloaded.

        Args:
            remote_path: Remote file path

        Returns:
            True if file has been downloaded
        """
        return remote_path in self._history["files"]

    def mark_downloaded(
        self,
        remote_path: str,
        local_path: Path,
        product: str,
        test_type: str,
        file_size: Optional[int] = None,
    ) -> None:
        """
        Mark a file as downloaded.

        Args:
            remote_path: Remote file path
            local_path: Local file path
            product: Product name
            test_type: Test type (CP/FT)
            file_size: File size in bytes
        """
        self._history["files"][remote_path] = {
            "product": product,
            "test_type": test_type,
            "local_path": str(local_path),
            "downloaded_at": datetime.now().isoformat(),
            "file_size": file_size,
            "ingested": False,
        }
        self._save()

    def mark_ingested(self, remote_path: str) -> None:
        """
        Mark a file as ingested.

        Args:
            remote_path: Remote file path
        """
        if remote_path in self._history["files"]:
            self._history["files"][remote_path]["ingested"] = True
            self._history["files"][remote_path]["ingested_at"] = datetime.now().isoformat()
            self._save()

    def get_pending_ingest(self) -> list[tuple[str, Path, str, str]]:
        """
        Get files that have been downloaded but not ingested.

        Returns:
            List of tuples (remote_path, local_path, product, test_type)
        """
        pending = []
        for remote_path, entry in self._history["files"].items():
            if not entry.get("ingested", False):
                pending.append((
                    remote_path,
                    Path(entry["local_path"]),
                    entry["product"],
                    entry["test_type"],
                ))
        return pending

    def get_downloaded_count(self) -> int:
        """Get count of downloaded files."""
        return len(self._history["files"])

    # ── corrupt (unrecoverable) files ─────────────────────────────────────
    #
    # A file whose gzip stream is broken at the source can never be recovered
    # by re-downloading. Without a record of that, `fetch` retries it on every
    # run and - since it used to abort the run - blocked every file behind it.

    def is_corrupt(self, remote_path: str) -> bool:
        """Check if a file was quarantined as unrecoverable."""
        return remote_path in self._history["corrupt"]

    def mark_corrupt(
        self,
        remote_path: str,
        product: str,
        test_type: str,
        error: str,
        quarantine_path: Optional[Path] = None,
    ) -> None:
        """Record a file as unrecoverable so later runs skip it.

        Reversible via `clear_corrupt()` - the source is expected to re-export
        the file to the same remote path once the problem is fixed.
        """
        self._history["corrupt"][remote_path] = {
            "product": product,
            "test_type": test_type,
            "error": error,
            "quarantine_path": str(quarantine_path) if quarantine_path else None,
            "detected_at": datetime.now().isoformat(),
        }
        self._save()

    def get_corrupt(self) -> list[dict]:
        """List quarantined files, each entry including its remote_path."""
        return [
            {"remote_path": remote_path, **entry}
            for remote_path, entry in self._history["corrupt"].items()
        ]

    def clear_corrupt(self, remote_path: Optional[str] = None) -> int:
        """Forget quarantined files so they are retried. Returns the count cleared."""
        if remote_path is None:
            cleared = len(self._history["corrupt"])
            self._history["corrupt"] = {}
        else:
            cleared = 1 if self._history["corrupt"].pop(remote_path, None) else 0
        if cleared:
            self._save()
        return cleared
