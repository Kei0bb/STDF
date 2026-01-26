"""FTP client for downloading STDF files."""

import ftplib
import gzip
import fnmatch
from pathlib import Path
from typing import Generator

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from .config import FTPConfig, Config


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

    def list_directories(self, path: str = "/") -> list[str]:
        """List directories in the given path."""
        if self._ftp is None:
            raise RuntimeError("Not connected to FTP server")

        dirs = []
        try:
            self._ftp.cwd(path)
            items = []
            self._ftp.retrlines("LIST", items.append)

            for item in items:
                # Parse FTP LIST output
                parts = item.split()
                if len(parts) >= 9 and item.startswith("d"):
                    # Directory
                    name = " ".join(parts[8:])
                    dirs.append(name)
        except ftplib.error_perm:
            pass

        return dirs

    def list_stdf_files(
        self,
        path: str | None = None,
        products: list[str] | None = None,
        test_types: list[str] | None = None,
    ) -> Generator[tuple[str, str, str, str], None, None]:
        """
        List STDF files matching filters.

        Args:
            path: Base path to search
            products: List of product names to filter (None = all)
            test_types: List of test types (CP, FT) to filter

        Yields:
            Tuple of (full_path, product, test_type, filename)
        """
        if self._ftp is None:
            raise RuntimeError("Not connected to FTP server")

        base_path = path or self.config.base_path

        # Get product directories
        product_dirs = self.list_directories(base_path)

        for product in product_dirs:
            # Filter by product if specified
            if products and product not in products:
                continue

            product_path = f"{base_path}/{product}".replace("//", "/")

            # Get test type directories (CP, FT, etc.)
            test_type_dirs = self.list_directories(product_path)

            for test_type in test_type_dirs:
                # Filter by test type if specified
                if test_types and test_type not in test_types:
                    continue

                test_type_path = f"{product_path}/{test_type}"

                # Get lot directories
                lot_dirs = self.list_directories(test_type_path)

                for lot in lot_dirs:
                    lot_path = f"{test_type_path}/{lot}"

                    # List files in lot directory
                    try:
                        files = self._ftp.nlst(lot_path)
                    except ftplib.error_perm:
                        continue

                    for file_path in files:
                        filename = Path(file_path).name
                        for pattern in self.config.patterns:
                            if fnmatch.fnmatch(filename.lower(), pattern.lower()):
                                yield file_path, product, test_type, filename
                                break

    def download_file(
        self,
        remote_path: str,
        local_dir: Path,
        decompress: bool = True,
    ) -> Path:
        """
        Download a file from FTP server.

        Args:
            remote_path: Path on FTP server
            local_dir: Local directory to save to
            decompress: Whether to decompress .gz files

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
        if decompress and filename.endswith(".gz"):
            decompressed_path = local_dir / filename[:-3]

            with gzip.open(local_path, "rb") as f_in:
                with open(decompressed_path, "wb") as f_out:
                    f_out.write(f_in.read())

            # Remove compressed file
            local_path.unlink()
            return decompressed_path

        return local_path


def fetch_stdf_files(
    config: Config,
    products: list[str] | None = None,
    test_types: list[str] | None = None,
    limit: int | None = None,
) -> list[tuple[Path, str, str]]:
    """
    Fetch STDF files from FTP server.

    Args:
        config: Full configuration
        products: Product filter (overrides config if provided)
        test_types: Test type filter (overrides config if provided)
        limit: Maximum number of files to download

    Returns:
        List of tuples (local_path, product, test_type)
    """
    # Use CLI args if provided, else config
    filter_products = products if products else (config.products if config.products else None)
    filter_test_types = test_types if test_types else config.test_types

    downloaded = []

    with FTPClient(config.ftp) as client:
        files = list(client.list_stdf_files(
            products=filter_products,
            test_types=filter_test_types,
        ))

        if limit:
            files = files[:limit]

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
        ) as progress:
            task = progress.add_task("Downloading...", total=len(files))

            for remote_path, product, test_type, filename in files:
                # Create subdirectory structure: downloads/product/test_type/
                local_dir = config.storage.download_dir / product / test_type
                local_file = client.download_file(remote_path, local_dir, decompress=True)
                downloaded.append((local_file, product, test_type))
                progress.update(task, advance=1, description=f"Downloaded {filename}")

    return downloaded
