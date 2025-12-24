"""STDF file parser using pystdf."""

from pathlib import Path
from typing import Generator

from pystdf.IO import Parser
from pystdf.Records import V4


# Mapping from pystdf record classes to record type names
RECORD_CLASS_MAP = {
    V4.far: "FAR",
    V4.atr: "ATR",
    V4.mir: "MIR",
    V4.mrr: "MRR",
    V4.pcr: "PCR",
    V4.hbr: "HBR",
    V4.sbr: "SBR",
    V4.pmr: "PMR",
    V4.pgr: "PGR",
    V4.plr: "PLR",
    V4.rdr: "RDR",
    V4.sdr: "SDR",
    V4.wir: "WIR",
    V4.wrr: "WRR",
    V4.wcr: "WCR",
    V4.pir: "PIR",
    V4.prr: "PRR",
    V4.tsr: "TSR",
    V4.ptr: "PTR",
    V4.mpr: "MPR",
    V4.ftr: "FTR",
    V4.bps: "BPS",
    V4.eps: "EPS",
    V4.gdr: "GDR",
    V4.dtr: "DTR",
}


class RecordCollector:
    """Collects records from pystdf parser."""

    def __init__(self):
        self.records: list[tuple[str, dict]] = []
        self._current_record_class = None

    def __call__(self, record_class, field_values):
        """Callback for pystdf parser."""
        if record_class in RECORD_CLASS_MAP:
            record_type = RECORD_CLASS_MAP[record_class]
            record_data = {}

            # Map field names to values
            field_names = [f[0] for f in record_class.fieldMap]
            for name, value in zip(field_names, field_values):
                record_data[name] = value

            self.records.append((record_type, record_data))


def parse_stdf(file_path: Path) -> Generator[tuple[str, dict], None, None]:
    """
    Parse an STDF file and yield records.

    Args:
        file_path: Path to the STDF file

    Yields:
        Tuple of (record_type, record_data)
    """
    collector = RecordCollector()

    # Create parser and add collector as handler
    parser = Parser(inp=open(str(file_path), "rb"))

    # Register handler for all known record types
    for record_class in RECORD_CLASS_MAP.keys():
        parser.addSink(collector)
        break  # Only need to add once

    try:
        parser.parse()
    except Exception as e:
        raise RuntimeError(f"Failed to parse STDF file: {e}")

    # Yield collected records
    for record in collector.records:
        yield record


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
