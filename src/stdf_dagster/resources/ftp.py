"""Dagster resource wrapping stdf_platform.ftp_client.

FTP connection settings are loaded from config.yaml (via STDFConfigResource)
at runtime, not hardcoded in the resource definition.
"""

from pathlib import Path
from typing import Optional

from dagster import ConfigurableResource

from stdf_platform.config import Config
from stdf_platform.ftp_client import FTPClient


class FTPResource(ConfigurableResource):
    """FTP connection resource for STDF file retrieval.

    All connection settings (host, port, username, password, base_path, patterns)
    are loaded from config.yaml at runtime. Environment variables in config.yaml
    (e.g. ${FTP_USER}) are automatically expanded.
    """

    config_path: str = "config.yaml"

    def _load_ftp_config(self) -> Config:
        """Load the full config including FTP settings."""
        return Config.load(Path(self.config_path))

    def get_client(self) -> FTPClient:
        """Create and return an FTPClient using config.yaml settings."""
        config = self._load_ftp_config()
        return FTPClient(config.ftp)

    def list_new_files(
        self,
        downloaded: set[str],
        config: Optional[Config] = None,
    ) -> list[dict]:
        """List STDF files on FTP that haven't been downloaded yet.

        Applies product/test_type filters from config.yaml.

        Args:
            downloaded: Set of already-downloaded remote paths.
            config: Optional pre-loaded Config (avoids double-loading).

        Returns:
            List of dicts with keys: remote_path, product, test_type, filename
        """
        if config is None:
            config = self._load_ftp_config()

        new_files = []
        with self.get_client() as client:
            for remote_path, product, test_type, filename in client.list_stdf_files():
                # Skip already-downloaded files
                if remote_path in downloaded:
                    continue
                # Apply product/test_type filters from config.yaml
                if not config.should_fetch(product, test_type):
                    continue
                new_files.append({
                    "remote_path": remote_path,
                    "product": product,
                    "test_type": test_type,
                    "filename": filename,
                })
        return new_files

    def download(self, remote_path: str, local_dir: Path) -> Path:
        """Download a single file from FTP.

        Args:
            remote_path: Remote file path on FTP.
            local_dir: Local directory to save the file.

        Returns:
            Path to the downloaded (and decompressed) file.
        """
        with self.get_client() as client:
            return client.download_file(remote_path, local_dir, decompress=True)
