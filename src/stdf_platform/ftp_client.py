"""FTP client for downloading STDF files."""

import ftplib
import gzip
import fnmatch
from pathlib import Path
from typing import Generator

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from .config import FTPConfig


class FTPClient:
    """FTP client for STDF file retrieval."""

    def __init__(self, config: FTPConfig):
        self.config = config
        self._ftp: ftplib.FTP | None = None

    def connect(self) -> None:
        """Connect to FTP server."""
        self._ftp = ftplib.FTP()
        self._ftp.connect(self.config.host, self.config.port)
        self._ftp.login(self.config.username, self.config.password)

    def disconnect(self) -> None:
        """Disconnect from FTP server."""
        if self._ftp:
            try:
                self._ftp.quit()
            except Exception:
                pass
            self._ftp = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False

    def list_stdf_files(self, path: str | None = None) -> Generator[str, None, None]:
        """
        List STDF files in the given path.

        Args:
            path: Directory path to list (defaults to base_path)

        Yields:
            Full paths to STDF files
        """
        if self._ftp is None:
            raise RuntimeError("Not connected to FTP server")

        search_path = path or self.config.base_path

        try:
            files = self._ftp.nlst(search_path)
        except ftplib.error_perm:
            return

        for file_path in files:
            filename = Path(file_path).name
            for pattern in self.config.patterns:
                if fnmatch.fnmatch(filename.lower(), pattern.lower()):
                    yield file_path
                    break

    def download_file(
        self,
        remote_path: str,
        local_dir: Path,
        decompress: bool = True,
        progress: Progress | None = None,
    ) -> Path:
        """
        Download a file from FTP server.

        Args:
            remote_path: Path on FTP server
            local_dir: Local directory to save to
            decompress: Whether to decompress .gz files
            progress: Optional Rich progress bar

        Returns:
            Path to downloaded file
        """
        if self._ftp is None:
            raise RuntimeError("Not connected to FTP server")

        local_dir.mkdir(parents=True, exist_ok=True)

        filename = Path(remote_path).name
        local_path = local_dir / filename

        # Download file
        with open(local_path, "wb") as f:
            self._ftp.retrbinary(f"RETR {remote_path}", f.write)

        # Decompress if needed
        if decompress and (filename.endswith(".gz")):
            decompressed_path = local_path.with_suffix("")
            if local_path.suffix == ".gz":
                # Remove .gz extension
                decompressed_path = local_dir / filename[:-3]

            with gzip.open(local_path, "rb") as f_in:
                with open(decompressed_path, "wb") as f_out:
                    f_out.write(f_in.read())

            # Remove compressed file
            local_path.unlink()
            return decompressed_path

        return local_path


def download_stdf_files(
    config: FTPConfig,
    local_dir: Path,
    remote_path: str | None = None,
    limit: int | None = None,
) -> list[Path]:
    """
    Download STDF files from FTP server.

    Args:
        config: FTP configuration
        local_dir: Local directory to save files
        remote_path: Remote path to search (optional)
        limit: Maximum number of files to download

    Returns:
        List of downloaded file paths
    """
    downloaded = []

    with FTPClient(config) as client:
        files = list(client.list_stdf_files(remote_path))

        if limit:
            files = files[:limit]

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
        ) as progress:
            task = progress.add_task("Downloading...", total=len(files))

            for remote_file in files:
                local_file = client.download_file(
                    remote_file, local_dir, decompress=True, progress=progress
                )
                downloaded.append(local_file)
                progress.update(task, advance=1)

    return downloaded
