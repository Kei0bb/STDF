"""Configuration management."""

import os
from pathlib import Path
from dataclasses import dataclass, field

import yaml


@dataclass
class FTPConfig:
    """FTP connection configuration."""
    host: str = "localhost"
    port: int = 21
    username: str = ""
    password: str = ""
    base_path: str = "/"
    patterns: list[str] = field(default_factory=lambda: ["*.stdf", "*.stdf.gz", "*.std", "*.std.gz"])

    def __post_init__(self):
        # Expand environment variables
        if self.username.startswith("${") and self.username.endswith("}"):
            env_var = self.username[2:-1]
            self.username = os.environ.get(env_var, "")
        if self.password.startswith("${") and self.password.endswith("}"):
            env_var = self.password[2:-1]
            self.password = os.environ.get(env_var, "")


@dataclass
class StorageConfig:
    """Storage configuration."""
    data_dir: Path = field(default_factory=lambda: Path("./data"))
    database: Path = field(default_factory=lambda: Path("./data/stdf.duckdb"))
    download_dir: Path = field(default_factory=lambda: Path("./downloads"))

    def __post_init__(self):
        if isinstance(self.data_dir, str):
            self.data_dir = Path(self.data_dir)
        if isinstance(self.database, str):
            self.database = Path(self.database)
        if isinstance(self.download_dir, str):
            self.download_dir = Path(self.download_dir)


@dataclass
class ProcessingConfig:
    """Processing configuration."""
    batch_size: int = 1000
    compression: str = "snappy"


@dataclass
class ProductFilter:
    """Product-specific filter."""
    product: str
    test_types: list[str] = field(default_factory=lambda: ["CP", "FT"])


@dataclass
class Config:
    """Main configuration."""
    ftp: FTPConfig = field(default_factory=FTPConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    products: list[str] = field(default_factory=list)  # Legacy: simple product list
    test_types: list[str] = field(default_factory=lambda: ["CP", "FT"])  # Legacy: global test types
    filters: list[ProductFilter] = field(default_factory=list)  # New: product-specific filters

    def get_filter_for_product(self, product: str) -> list[str] | None:
        """
        Get test types for a specific product.
        
        Returns:
            List of test types, or None if product should be skipped
        """
        if self.filters:
            for f in self.filters:
                if f.product == product:
                    return f.test_types
            return None  # Product not in filters, skip it
        
        # Legacy mode: use global products/test_types
        if self.products and product not in self.products:
            return None
        return self.test_types

    def should_fetch(self, product: str, test_type: str) -> bool:
        """Check if product/test_type combination should be fetched."""
        allowed_types = self.get_filter_for_product(product)
        if allowed_types is None:
            return False
        return test_type in allowed_types

    @classmethod
    def load(cls, config_path: Path | None = None) -> "Config":
        """Load configuration from YAML file."""
        if config_path is None:
            config_path = Path("config.yaml")

        if not config_path.exists():
            return cls()

        with open(config_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if data is None:
            return cls()

        ftp_data = data.get("ftp", {})
        storage_data = data.get("storage", {})
        processing_data = data.get("processing", {})
        products = data.get("products", []) or []
        test_types = data.get("test_types", ["CP", "FT"]) or ["CP", "FT"]
        
        # Parse new filters format
        filters_data = data.get("filters", []) or []
        filters = [
            ProductFilter(
                product=f.get("product", ""),
                test_types=f.get("test_types", ["CP", "FT"])
            )
            for f in filters_data
            if f.get("product")
        ]

        return cls(
            ftp=FTPConfig(**ftp_data) if ftp_data else FTPConfig(),
            storage=StorageConfig(**storage_data) if storage_data else StorageConfig(),
            processing=ProcessingConfig(**processing_data) if processing_data else ProcessingConfig(),
            products=products,
            test_types=test_types,
            filters=filters,
        )

    def ensure_directories(self):
        """Create necessary directories."""
        self.storage.data_dir.mkdir(parents=True, exist_ok=True)
        self.storage.database.parent.mkdir(parents=True, exist_ok=True)
        self.storage.download_dir.mkdir(parents=True, exist_ok=True)
