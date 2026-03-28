"""Dagster resource wrapping stdf_platform.parser."""

from pathlib import Path

from dagster import ConfigurableResource

from stdf_platform.parser import parse_stdf, STDFData, _USE_RUST


class STDFParserResource(ConfigurableResource):
    """STDF binary parser resource.

    Uses Rust parser if available, with automatic Python fallback.
    """

    @property
    def uses_rust(self) -> bool:
        """Whether the Rust parser is being used."""
        return _USE_RUST

    def parse(self, file_path: Path) -> STDFData:
        """Parse an STDF file.

        Args:
            file_path: Path to the STDF (or .stdf.gz) file.

        Returns:
            Parsed STDFData object.
        """
        return parse_stdf(file_path)
