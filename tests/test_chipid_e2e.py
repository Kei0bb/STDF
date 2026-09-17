"""End-to-end tests for EN-S0-CHIPID_R decoding and chipid storage/views."""

import sys
from pathlib import Path

import duckdb
import pytest

# make_ft_stdf lives alongside this test in tests/
sys.path.insert(0, str(Path(__file__).resolve().parent))
import make_test_stdf as gen  # noqa: E402
from make_test_stdf import make_ft_stdf  # noqa: E402

from stdf_platform.parser import parse_stdf  # noqa: E402
from stdf_platform.storage import ParquetStorage  # noqa: E402
from stdf_platform.config import StorageConfig  # noqa: E402
from stdf_platform.chipid import decode_chipid  # noqa: E402
from stdf_platform.mounts import setup_views  # noqa: E402


def _storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _ingest_ft(storage, ft_file, **kw):
    data = parse_stdf(ft_file)
    storage.save_stdf_data(
        data, product="CHIPLET2D", test_category="FT", sub_process="FT1",
        source_file=ft_file.name, **kw,
    )
    return data


# ── parser ────────────────────────────────────────────────────────────────────

def test_parser_emits_two_chipids_per_part(tmp_path):
    ft_file = tmp_path / "FT.stdf"
    expected = make_ft_stdf(ft_file, "FTLOT01", parts=8)
    data = parse_stdf(ft_file)

    assert len(data.chip_ids) == len(expected) == 16
    # occurrence indices alternate 0,1 per package
    occ = [c["chip_occurrence_index"] for c in data.chip_ids]
    assert occ == [0, 1] * 8
    # part_txt (barcode) bound from PRR, on both chip_ids and parts
    assert data.chip_ids[0]["part_txt"] == "2D-FTLOT01-0000"
    assert data.chip_ids[1]["part_txt"] == "2D-FTLOT01-0000"
    assert [p["part_txt"] for p in data.parts][:3] == [
        "2D-FTLOT01-0000", "2D-FTLOT01-0001", "2D-FTLOT01-0002"
    ]
    # the two dies of one package decode to different fabs
    assert decode_chipid(data.chip_ids[0]["efuse_raw"])["origin_fab"] == "TSMC1"
    assert decode_chipid(data.chip_ids[1]["efuse_raw"])["origin_fab"] == "TSMC2"


@pytest.mark.parametrize("key", ["EN-S0-CHIPID_R", "EN-SO-CHIPID_R"],
                         ids=["digit_zero_real", "letter_o_spec"])
def test_both_key_spellings_are_parsed(tmp_path, monkeypatch, key):
    """Regression for the O-vs-0 bug: real files use 'EN-S0-CHIPID_R' (digit
    zero); the letter-O spelling from the spec must still parse."""
    orig = gen.gdr_chipid
    monkeypatch.setattr(gen, "gdr_chipid", lambda efuse: orig(efuse, key))
    ft_file = tmp_path / "FT.stdf"
    make_ft_stdf(ft_file, "FTKEY", parts=3)
    assert key.encode() in ft_file.read_bytes()
    data = parse_stdf(ft_file)
    assert len(data.chip_ids) == 6  # 3 packages x 2 dies, key matched


# ── storage: CHIPID parquet ──────────────────────────────────────────────────

def test_chipid_parquet_decoded_correctly(tmp_path):
    ft_file = tmp_path / "FT.stdf"
    make_ft_stdf(ft_file, "FTLOT01", parts=4)
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft_file)

    chip_glob = (tmp_path / "chipid").as_posix() + "/**/*.parquet"
    conn = duckdb.connect()
    rows = conn.execute(f"""
        SELECT origin_fab, origin_lot, origin_wafer, origin_x, origin_y
        FROM read_parquet('{chip_glob}', hive_partitioning=true)
        WHERE part_txt = '2D-FTLOT01-0002' ORDER BY chip_occurrence_index
    """).fetchall()
    # die0: TSMC1 HKPFJK W11 (10+2, 20+2); die1: TSMC2 ABCDEF W7 (100+2, 200+2)
    assert rows[0] == ("TSMC1", "HKPFJK", 11, 12, 22)
    assert rows[1] == ("TSMC2", "ABCDEF", 7, 102, 202)


def test_chipid_final_dedups_by_decoded_efuse_not_occurrence(tmp_path):
    """chipid_final identity is the decoded eFuse, robust to GDR order swaps."""
    ft_file = tmp_path / "FT.stdf"
    make_ft_stdf(ft_file, "FTLOT01", parts=3)  # 3 packages x 2 dies = 6 distinct efuses
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft_file)
    _ingest_ft(storage, ft_file)  # retest run — same dies, retest_num increments

    conn = duckdb.connect()
    assert "chipid_final" in setup_views(conn, tmp_path)
    # raw doubled by re-ingest; final collapses to the 6 distinct decoded dies
    assert conn.execute("SELECT COUNT(*) FROM chipid").fetchone()[0] == 12
    assert conn.execute("SELECT COUNT(*) FROM chipid_final").fetchone()[0] == 6
    # every surviving row is the latest retest
    assert conn.execute(
        "SELECT MIN(retest_num) FROM chipid_final"
    ).fetchone()[0] == 1


def test_cp_does_not_write_chipid_table(tmp_path):
    """ChipID is FT-only: CP ingestion must not create a chipid table even if
    the GDRs (and thus parsed chip_ids) are present."""
    ft_file = tmp_path / "src.stdf"
    make_ft_stdf(ft_file, "CPLOT", parts=3)
    data = parse_stdf(ft_file)
    assert len(data.chip_ids) == 6  # parser still captures them

    storage = _storage(tmp_path)
    counts = storage.save_stdf_data(
        data, product="P", test_category="CP", sub_process="CP1",
        source_file=ft_file.name,
    )
    assert not (tmp_path / "chipid").exists()
    assert "chipid" not in counts
