"""End-to-end tests for EN-S0-CHIPID_R decoding and the CHIP ID columns of parts."""

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
    # occurrence indices alternate 0,1 per package; die comes from the suite GDR
    occ = [c["chip_occurrence_index"] for c in data.chip_ids]
    assert occ == [0, 1] * 8
    assert [c["die_id"] for c in data.chip_ids] == ["C0", "C1"] * 8
    assert all(c["association"] == "following suite exact" for c in data.chip_ids)
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
    monkeypatch.setattr(gen, "gdr_chipid", lambda efuse, **kw: orig(efuse, key, **kw))
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

    conn = duckdb.connect()
    setup_views(conn, tmp_path)
    rows = conn.execute("""
        SELECT c0_origin_fab, c0_origin_lot, c0_origin_wafer, c0_origin_x, c0_origin_y,
               c1_origin_fab, c1_origin_lot, c1_origin_wafer, c1_origin_x, c1_origin_y,
               chip_status, chip_warnings
        FROM parts WHERE part_txt = '2D-FTLOT01-0002'
    """).fetchall()
    # die0: TSMC1 HKPFJK W11 (10+2, 20+2); die1: TSMC2 ABCDEF W7 (100+2, 200+2)
    assert rows == [("TSMC1", "HKPFJK", 11, 12, 22,
                     "TSMC2", "ABCDEF", 7, 102, 202, "complete", "")]


def test_parts_final_keeps_latest_retest_of_each_package(tmp_path):
    """parts_final = latest run of each package (identity chip_key); the CHIP ID
    columns ride along on the same row."""
    storage = _storage(tmp_path)
    for name, pkgs in (("a.stdf", [0, 1, 2]), ("b.stdf", [2, 0])):  # partial, reordered
        f = tmp_path / name
        make_ft_stdf(f, "FTLOT01", pkgs=pkgs)
        _ingest_ft(storage, f)

    conn = duckdb.connect()
    views = setup_views(conn, tmp_path)
    assert "chipid_final" not in views and "chipid" not in views
    assert conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0] == 5
    assert conn.execute("SELECT COUNT(*) FROM parts_final").fetchone()[0] == 3
    # packages 0 and 2 come from retest 1
    got = dict(conn.execute(
        "SELECT chip_key, retest_num FROM parts_final").fetchall())
    assert len(got) == 3 and sorted(got.values()) == [0, 1, 1]
    pk = conn.execute(
        "SELECT chip_status, c0_origin_fab, c1_origin_fab FROM parts_final").fetchall()
    assert pk == [("complete", "TSMC1", "TSMC2")] * 3


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
    assert set(counts) == {"runs", "parts", "test_data"}
