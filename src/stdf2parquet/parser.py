"""STDF file parser using semi-ate-stdf."""

from pathlib import Path
from typing import Generator

from Semi_ATE.STDF import utils as stdf_utils


def parse_stdf(file_path: Path) -> Generator[tuple[str, dict], None, None]:
    """
    Parse an STDF file and yield records.

    Args:
        file_path: Path to the STDF file

    Yields:
        Tuple of (record_type, record_data)
    """
    records = stdf_utils.records_from_file(str(file_path))

    for record in records:
        record_type = record.__class__.__name__
        record_data = {}

        # Extract all fields from the record
        for field_name in record.fields:
            try:
                value = getattr(record, field_name, None)
                record_data[field_name] = value
            except Exception:
                record_data[field_name] = None

        yield record_type, record_data


def get_record_counts(file_path: Path) -> dict[str, int]:
    """
    Get counts of each record type in an STDF file.

    Args:
        file_path: Path to the STDF file

    Returns:
        Dictionary mapping record type to count
    """
    counts: dict[str, int] = {}

    for record_type, _ in parse_stdf(file_path):
        counts[record_type] = counts.get(record_type, 0) + 1

    return counts
