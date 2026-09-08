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
import pyarrow as pa
import pyarrow.parquet as pq

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

def test_sdr_ids_and_types_are_parsed(tmp_path):
    data = parse_stdf(_write_stdf(tmp_path / "a.stdf", _mir(), _sdr()))

    assert data.handler_type == "HND-MODEL"
    assert data.handler_id == "HND-01"
    assert data.probe_card_type == "CARD-MODEL"
    assert data.probe_card_id == "CARD-01"
    assert data.loadboard_type == "LB-MODEL"
    assert data.loadboard_id == "LB-01"
    assert data.socket_type == "SKT-MODEL"
    assert data.socket_id == "SKT-01"


def test_mir_node_name_is_kept(tmp_path):
    data = parse_stdf(_write_stdf(tmp_path / "a.stdf", _mir(node_nam="TESTER07")))

    assert data.node_name == "TESTER07"


def test_multiple_sdrs_collapse_to_sorted_distinct_csv(tmp_path):
    """Two site groups on one handler but different sockets."""
    data = parse_stdf(_write_stdf(
        tmp_path / "a.stdf",
        _mir(),
        _sdr(site_grp=0, site_nums=(1, 2), cont_id="SKT-B"),
        _sdr(site_grp=1, site_nums=(3, 4), cont_id="SKT-A"),
    ))

    assert data.socket_id == "SKT-A,SKT-B"
    # Fields identical across both SDRs stay single-valued.
    assert data.handler_id == "HND-01"


def test_empty_sdr_fields_are_dropped_from_the_join(tmp_path):
    data = parse_stdf(_write_stdf(
        tmp_path / "a.stdf",
        _mir(),
        _sdr(site_grp=0, cont_id="SKT-A"),
        _sdr(site_grp=1, cont_id=""),
    ))

    assert data.socket_id == "SKT-A"


def test_file_without_sdr_leaves_equipment_fields_empty(tmp_path):
    data = parse_stdf(_write_stdf(tmp_path / "a.stdf", _mir()))

    assert data.handler_id == ""
    assert data.probe_card_id == ""
    assert data.loadboard_id == ""
    assert data.socket_id == ""


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


def test_runs_parquet_carries_equipment_columns(tmp_path):
    _ingest(tmp_path, _mir(node_nam="TESTER07"), _sdr())

    p = (tmp_path / "data" / "runs" / "product=PROD" / "test_category=CP"
         / "sub_process=CP1" / "lot_id=LOT1" / "wafer_id=" / "retest=0"
         / "data.parquet")
    table = pq.ParquetFile(p).read()

    for col in EQUIPMENT_COLUMNS:
        assert col in table.column_names, f"{col} missing from runs Parquet"
    assert table["node_name"][0].as_py() == "TESTER07"
    assert table["handler_id"][0].as_py() == "HND-01"
    assert table["probe_card_id"][0].as_py() == "CARD-01"
    assert table["loadboard_id"][0].as_py() == "LB-01"
    assert table["socket_id"][0].as_py() == "SKT-01"


def test_runs_view_exposes_equipment_columns(tmp_path):
    _ingest(tmp_path, _mir(node_nam="TESTER07"), _sdr())

    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path / "data")
    row = conn.execute(
        "SELECT node_name, handler_id, probe_card_id, loadboard_id, socket_id FROM runs"
    ).fetchone()

    assert row == ("TESTER07", "HND-01", "CARD-01", "LB-01", "SKT-01")


def test_runs_view_tolerates_pre_equipment_parquet(tmp_path):
    """A store holding runs files written before this change must still mount."""
    _ingest(tmp_path, _mir(), _sdr())

    old_dir = (tmp_path / "data" / "runs" / "product=PROD" / "test_category=CP"
               / "sub_process=CP1" / "lot_id=OLDLOT" / "wafer_id=" / "retest=0")
    old_dir.mkdir(parents=True)
    pq.write_table(
        pa.table({"lot_id": ["OLDLOT"], "wafer_id": [""], "job_name": ["OLD"]}),
        old_dir / "data.parquet",
    )

    conn = duckdb.connect(":memory:")
    setup_views(conn, tmp_path / "data")
    rows = dict(conn.execute(
        "SELECT lot_id, socket_id FROM runs ORDER BY lot_id"
    ).fetchall())

    assert rows["LOT1"] == "SKT-01"
    assert rows["OLDLOT"] is None


def test_analysis_session_runs_returns_equipment_columns(tmp_path):
    from stdf_platform.analysis.session import AnalysisSession

    _ingest(tmp_path, _mir(node_nam="TESTER07"), _sdr())

    with AnalysisSession(data_dir=tmp_path / "data") as s:
        df = s.runs()

    for col in EQUIPMENT_COLUMNS:
        assert col in df.columns, f"{col} missing from AnalysisSession.runs()"
    assert df["socket_id"].iloc[0] == "SKT-01"


def test_generated_ft_sample_carries_two_site_group_sdrs(tmp_path):
    """The synthetic FT file exercises the multi-SDR collapse end to end."""
    from make_test_stdf import make_ft_stdf

    path = tmp_path / "ft.stdf"
    make_ft_stdf(path, lot_id="FTLOT1", parts=2)
    data = parse_stdf(path)

    assert data.handler_id == "HND-FT-01"
    assert data.socket_id == "SKT-A,SKT-B"


def test_generated_cp_sample_carries_one_sdr(tmp_path):
    from make_test_stdf import make_stdf

    path = tmp_path / "cp.stdf"
    make_stdf(path, lot_id="CPLOT1", num_wafers=1, parts_per_wafer=2)
    data = parse_stdf(path)

    assert data.node_name == "TESTER01"
    assert data.probe_card_id == "PC-01"
