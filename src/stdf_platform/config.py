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

    def with_env(self, env: str | None) -> "StorageConfig":
        """Return a new config with paths adjusted for the given environment.

        e.g. env="dev" → data-dev/, data-dev/stdf.duckdb
        Default (env=None) returns self unchanged.
        """
        if not env:
            return self
        return StorageConfig(
            data_dir=Path(f"./data-{env}"),
            database=Path(f"./data-{env}/stdf.duckdb"),
            download_dir=self.download_dir,
        )

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"


@dataclass
class ProcessingConfig:
    """Processing configuration."""
    compression: str = "zstd"


@dataclass
class ReportingConfig:
    """Report engine configuration."""
    histogram_top_n: int = 20
    # product -> list of test_num always rendered as a histogram regardless of fail rate
    always_include_tests: dict[str, list[int]] = field(default_factory=dict)


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
    reporting: ReportingConfig = field(default_factory=ReportingConfig)
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
        reporting_data = data.get("reporting", {}) or {}

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

        raw_always = reporting_data.get("always_include_tests", {}) or {}
        always_include = {
            str(prod): [int(t) for t in (tests or [])]
            for prod, tests in raw_always.items()
        }
        reporting = ReportingConfig(
            histogram_top_n=int(reporting_data.get("histogram_top_n", 20)),
            always_include_tests=always_include,
        )

        return cls(
            ftp=FTPConfig(**ftp_data) if ftp_data else FTPConfig(),
            storage=StorageConfig(**storage_data) if storage_data else StorageConfig(),
            processing=ProcessingConfig(
                **{k: v for k, v in processing_data.items() if k == "compression"}
            ) if processing_data else ProcessingConfig(),
            reporting=reporting,
            filters=filters,
            exclude=exclude,
            products=products,
        )

    def ensure_directories(self):
        """Create necessary directories."""
        self.storage.data_dir.mkdir(parents=True, exist_ok=True)
        self.storage.database.parent.mkdir(parents=True, exist_ok=True)
        self.storage.download_dir.mkdir(parents=True, exist_ok=True)
