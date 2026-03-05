"""Dagster resource wrapping stdf_platform.config.Config."""

from pathlib import Path
from typing import Optional

from dagster import ConfigurableResource, InitResourceContext

from stdf_platform.config import Config


class STDFConfigResource(ConfigurableResource):
    """STDF Platform configuration resource.

    Loads config from config.yaml and provides it to other resources/assets.
    """

    config_path: str = "config.yaml"
    env: Optional[str] = None

    def load_config(self) -> Config:
        """Load and return the STDF platform Config object."""
        cfg = Config.load(Path(self.config_path))
        if self.env:
            cfg.storage = cfg.storage.with_env(self.env)
        cfg.ensure_directories()
        return cfg
