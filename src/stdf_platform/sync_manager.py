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
        self._history: dict = {"files": {}}
        self._load()

    def _load(self) -> None:
        """Load history from file."""
        if self.history_file.exists():
            try:
                with open(self.history_file, "r", encoding="utf-8") as f:
                    self._history = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._history = {"files": {}}

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
