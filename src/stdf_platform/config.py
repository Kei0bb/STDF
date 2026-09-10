"""Configuration management."""

import fnmatch
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
    data_dir: Path = field(default_factory=lambda: Path("./var/data"))
    database: Path = field(default_factory=lambda: Path("./var/data/stdf.duckdb"))
    download_dir: Path = field(default_factory=lambda: Path("./var/downloads"))

    def __post_init__(self):
        if isinstance(self.data_dir, str):
            self.data_dir = Path(self.data_dir)
        if isinstance(self.database, str):
            self.database = Path(self.database)
        if isinstance(self.download_dir, str):
            self.download_dir = Path(self.download_dir)

    def resolve_against(self, base: Path) -> "StorageConfig":
        """Return a copy with relative paths made absolute against `base`.

        `base` is the directory holding the config.yaml these values came
        from. Without this, a relative data_dir ("./var/data") resolves
        against the *process cwd*, so the same config.yaml points at a
        different store depending on where the interpreter was started:
        the CLI (always run from the repo root) saw the real store, while a
        VSCode Interactive Window whose cwd is workspace/ silently resolved
        to workspace/var/data — a directory that does not exist. That failed
        soundlessly, because setup_views() skips tables whose directory is
        missing, so the session came up with zero views instead of an error.
        Anchoring to the config file's own directory makes one config.yaml
        mean one store, from any cwd.
        """
        return StorageConfig(
            data_dir=self.data_dir if self.data_dir.is_absolute() else base / self.data_dir,
            database=self.database if self.database.is_absolute() else base / self.database,
            download_dir=(self.download_dir if self.download_dir.is_absolute()
                          else base / self.download_dir),
        )

    def with_env(self, env: str | None) -> "StorageConfig":
        """Return a new config with paths adjusted for the given environment.

        The env store is a sibling of the configured data_dir, not a fixed
        top-level directory: data_dir=./var/data + env="dev" → ./var/data-dev.
        Deriving it (rather than hardcoding "./data-{env}") is what keeps an
        env run inside whatever runtime root data_dir points at — a hardcoded
        path would recreate ./data-dev at the project root and defeat it.
        Default (env=None) returns self unchanged.
        """
        if not env:
            return self
        env_dir = self.data_dir.parent / f"{self.data_dir.name}-{env}"
        return StorageConfig(
            data_dir=env_dir,
            database=env_dir / self.database.name,
            download_dir=self.download_dir,
        )


@dataclass
class ProcessingConfig:
    """Processing configuration."""
    compression: str = "zstd"


@dataclass
class ServerConfig:
    """Read-only HTTP query server (stdf serve)."""
    host: str = "0.0.0.0"
    port: int = 8555
    max_rows: int = 10000


@dataclass
class ProductFilter:
    """Product-specific filter."""
    product: str
    test_types: list[str] = field(default_factory=lambda: ["CP", "FT"])


@dataclass
class ProductConfig:
    """Per-product settings (gross die, etc.)."""
    gross_die: int | None = None
    gd_fail_bin: int = 200


@dataclass
class Config:
    """Main configuration."""
    ftp: FTPConfig = field(default_factory=FTPConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    filters: list[ProductFilter] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    products: dict[str, ProductConfig] = field(default_factory=dict)

    @property
    def gross_die_map(self) -> dict[str, tuple[int, int]]:
        """Returns {product: (gross_die, gd_fail_bin)} for products with gross_die set."""
        return {
            prod: (pc.gross_die, pc.gd_fail_bin)
            for prod, pc in self.products.items()
            if pc.gross_die is not None
        }

    def should_exclude(self, path: str) -> bool:
        """Return True if the filename matches any exclude pattern (case-insensitive fnmatch)."""
        if not self.exclude:
            return False
        name = Path(path).name
        for pattern in self.exclude:
            if fnmatch.fnmatch(name.lower(), pattern.lower()):
                return True
        return False

    def should_fetch(self, product: str, test_type: str) -> bool:
        """
        Check if product/test_type combination should be fetched.
        
        If filters is empty, all products/test_types are allowed.
        Test type matching uses prefix (CP matches CP, CP1, CP2, etc.)
        """
        if not self.filters:
            return True  # No filters = fetch all
        
        for f in self.filters:
            if f.product == product:
                # Prefix match for test_type (CP matches CP1, CP2, etc.)
                for tt in f.test_types:
                    if test_type.upper().startswith(tt.upper()):
                        return True
                return False
        return False  # Product not in filters, skip it

    @classmethod
    def load(cls, config_path: Path | None = None) -> "Config":
        """Load configuration from YAML file.

        Resolution order: explicit arg → STDF_CONFIG env var → cwd config.yaml.
        If the resolved path does not exist, defaults are returned.
        """
        if config_path is None:
            env_path = os.environ.get("STDF_CONFIG")
            config_path = Path(env_path) if env_path else Path("config.yaml")

        if not config_path.exists():
            return cls()

        with open(config_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if data is None:
            return cls()

        ftp_data = data.get("ftp", {})
        storage_data = data.get("storage", {})
        processing_data = data.get("processing", {})
        server_data = data.get("server", {}) or {}

        # Parse filters format
        filters_data = data.get("filters", []) or []
        filters = []
        for f in filters_data:
            # Skip if not a dictionary
            if not isinstance(f, dict):
                continue
            product = f.get("product", "")
            if product:
                filters.append(ProductFilter(
                    product=product,
                    test_types=f.get("test_types", ["CP", "FT"])
                ))

        exclude = [str(p) for p in (data.get("exclude") or [])]

        products: dict[str, ProductConfig] = {}
        for prod, pc_data in (data.get("products") or {}).items():
            if not isinstance(pc_data, dict):
                continue
            products[str(prod)] = ProductConfig(
                gross_die=int(pc_data["gross_die"]) if pc_data.get("gross_die") is not None else None,
                gd_fail_bin=int(pc_data.get("gd_fail_bin", 200)),
            )

        return cls(
            ftp=FTPConfig(**ftp_data) if ftp_data else FTPConfig(),
            # Relative paths are anchored to the config file's directory, not
            # the cwd — see StorageConfig.resolve_against.
            storage=(StorageConfig(**storage_data) if storage_data
                     else StorageConfig()).resolve_against(config_path.parent.resolve()),
            processing=ProcessingConfig(
                **{k: v for k, v in processing_data.items() if k == "compression"}
            ) if processing_data else ProcessingConfig(),
            server=ServerConfig(
                **{k: v for k, v in server_data.items()
                   if k in ("host", "port", "max_rows")}
            ),
            filters=filters,
            exclude=exclude,
            products=products,
        )

    def ensure_directories(self):
        """Create necessary directories."""
        self.storage.data_dir.mkdir(parents=True, exist_ok=True)
        self.storage.database.parent.mkdir(parents=True, exist_ok=True)
        self.storage.download_dir.mkdir(parents=True, exist_ok=True)
