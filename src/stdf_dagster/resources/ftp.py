"""Dagster resource wrapping stdf_platform.ftp_client."""

from pathlib import Path
from typing import Optional

from dagster import ConfigurableResource

from stdf_platform.config import FTPConfig
from stdf_platform.ftp_client import FTPClient


class FTPResource(ConfigurableResource):
    """FTP connection resource for STDF file retrieval.

    Configuration is loaded from STDFConfigResource at runtime.
    Provides a context-managed FTP client.
    """

    host: str = "localhost"
    port: int = 21
    username: str = ""
    password: str = ""
    base_path: str = "/"
    patterns: list[str] = ["*.stdf", "*.stdf.gz", "*.std", "*.std.gz"]

    def get_client(self) -> FTPClient:
        """Create and return an FTPClient instance."""
        config = FTPConfig(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            base_path=self.base_path,
            patterns=self.patterns,
        )
        return FTPClient(config)

    def list_new_files(
        self,
        downloaded: set[str],
        products: Optional[list[str]] = None,
        test_types: Optional[list[str]] = None,
    ) -> list[dict]:
        """List STDF files on FTP that haven't been downloaded yet.

        Args:
            downloaded: Set of already-downloaded remote paths.
            products: Optional product filter.
            test_types: Optional test type filter.

        Returns:
            List of dicts with keys: remote_path, product, test_type, filename
        """
        new_files = []
        with self.get_client() as client:
            for remote_path, product, test_type, filename in client.list_stdf_files(
                products=products, test_types=test_types
            ):
                if remote_path not in downloaded:
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
