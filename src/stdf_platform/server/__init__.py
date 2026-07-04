"""Read-only HTTP query server over the Parquet store."""

from .app import create_app, router

__all__ = ["create_app", "router"]
