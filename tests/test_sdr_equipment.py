"""Tests for SDR-derived equipment metadata on the `runs` table.

The four IDs the fab cares about (handler / probe card / loadboard / socket)
live in the Site Description Record (STDF V4 1/80), not in the MIR. The tester
*unit* name lives in MIR.NODE_NAM. This module covers parsing both, the
multi-SDR collapse rule, and their round trip through `runs` Parquet into the
DuckDB view. See docs/schema.md (runs).
"""

import struct
import sys
from pathlib import Path

import duckdb

# make_test_stdf lives alongside this test in tests/
sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_test_stdf import cn, record, sdr as _sdr  # noqa: E402

from stdf_platform.config import StorageConfig  # noqa: E402
from stdf_platform.mounts import setup_views  # noqa: E402
from stdf_platform.parser import parse_stdf  # noqa: E402
from stdf_platform.storage import ParquetStorage  # noqa: E402


# The nine columns this feature adds to RUNS_SCHEMA.
EQUIPMENT_COLUMNS = [
    "node_name",
    "handler_type", "handler_id",
    "probe_card_type", "probe_card_id",
    "loadboard_type", "loadboard_id",
    "socket_type", "socket_id",
]


def _mir(node_nam: str = "TESTER01") -> bytes:
    """Minimal MIR with a settable NODE_NAM."""
    body = (
        struct.pack("<IIB", 1700000000, 1700000000, 1)
        + struct.pack("<BBBHB", ord(" "), ord(" "), ord(" "), 0, ord(" "))
        + cn("LOT1")        # LOT_ID
        + cn("PARTX")       # PART_TYP
        + cn(node_nam)      # NODE_NAM
        + cn("J750")        # TSTR_TYP
        + cn("CP_TEST")     # JOB_NAM
        + cn("Rev01")       # JOB_REV
        + cn("")            # SBLOT_ID
        + cn("OPE01")       # OPER_NAM
        + cn("")            # EXEC_TYP
        + cn("")            # EXEC_VER
        + cn("CP1")         # TEST_COD
    )
    return record(1, 10, body)


def _write_stdf(path: Path, *records: bytes) -> Path:
    buf = record(0, 10, struct.pack("BB", 2, 4))  # FAR
    for rec in records:
        buf += rec
    buf += record(1, 20, struct.pack("<I", 1700003600))  # MRR
    path.write_bytes(buf)
    return path


# --------------------------------------------------------------------------
# Parser
# --------------------------------------------------------------------------

def test_multiple_sdrs_collapse_to_sorted_distinct_csv(tmp_path):
    """Site groups on one handler but different sockets; an empty value in
    one group is dropped rather than joined in as ''."""
    data = parse_stdf(_write_stdf(
        tmp_path / "a.stdf",
        _mir(),
        _sdr(site_grp=0, site_nums=(1, 2), cont_id="SKT-B"),
        _sdr(site_grp=1, site_nums=(3, 4), cont_id="SKT-A"),
        _sdr(site_grp=2, site_nums=(5, 6), cont_id=""),
    ))

    assert data.socket_id == "SKT-A,SKT-B"
    # Fields identical across both SDRs stay single-valued.
    assert data.handler_id == "HND-01"


def test_truncated_sdr_keeps_the_fields_it_carried(tmp_path):
    """Real testers omit the tail of the SDR; that must not lose HAND_ID."""
    data = parse_stdf(_write_stdf(
        tmp_path / "a.stdf", _mir(), _sdr(truncate_after_hand_id=True)
    ))

    assert data.handler_id == "HND-01"
    assert data.socket_id == ""


# --------------------------------------------------------------------------
# Storage + view
# --------------------------------------------------------------------------

def _ingest(tmp_path: Path, *records: bytes) -> ParquetStorage:
    storage = ParquetStorage(
        StorageConfig(data_dir=tmp_path / "data", database=tmp_path / "db.duckdb")
    )
    data = parse_stdf(_write_stdf(tmp_path / "src.stdf", *records))
    # No WIR/PRR in these files → runs falls back to the FT-shaped '' identity.
    storage.save_stdf_data(
        data, product="PROD", test_category="CP", sub_process="CP1",
        source_file="src.stdf",
    )
    return storage


def test_runs_view_exposes_equipment_columns(tmp_path):
    _ingest(tmp_path, _mir(node_nam="TESTER07"), _sdr())

    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path / "data")
    row = conn.execute(f"SELECT {', '.join(EQUIPMENT_COLUMNS)} FROM runs").fetchone()

    # Parser (MIR.NODE_NAM + SDR field order, skipping DIB/CABL) -> runs
    # Parquet -> view, in one pass.
    assert row == ("TESTER07",
                   "HND-MODEL", "HND-01", "CARD-MODEL", "CARD-01",
                   "LB-MODEL", "LB-01", "SKT-MODEL", "SKT-01")
