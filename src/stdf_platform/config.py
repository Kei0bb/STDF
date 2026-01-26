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
    patterns: list[str] = field(default_factory=lambda: ["*.stdf", "*.stdf.gz"])

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

    def __post_init__(self):
        if isinstance(self.data_dir, str):
            self.data_dir = Path(self.data_dir)
        if isinstance(self.database, str):
            self.database = Path(self.database)


@dataclass
class ProcessingConfig:
    """Processing configuration."""
    batch_size: int = 1000
    compression: str = "snappy"


@dataclass
class Config:
    """Main configuration."""
    ftp: FTPConfig = field(default_factory=FTPConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)

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

        return cls(
            ftp=FTPConfig(**ftp_data) if ftp_data else FTPConfig(),
            storage=StorageConfig(**storage_data) if storage_data else StorageConfig(),
            processing=ProcessingConfig(**processing_data) if processing_data else ProcessingConfig(),
        )

    def ensure_directories(self):
        """Create necessary directories."""
        self.storage.data_dir.mkdir(parents=True, exist_ok=True)
        self.storage.database.parent.mkdir(parents=True, exist_ok=True)
