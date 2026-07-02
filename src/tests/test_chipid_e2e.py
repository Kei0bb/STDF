"""End-to-end tests for EN-SO-CHIPID_R decoding and FT retest handling."""

import sys
from pathlib import Path

import duckdb

# make_ft_stdf lives alongside this test in src/tests/
sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_test_stdf import make_ft_stdf  # noqa: E402

from stdf_platform.parser import parse_stdf  # noqa: E402
from stdf_platform.storage import ParquetStorage  # noqa: E402
from stdf_platform.config import StorageConfig  # noqa: E402
from stdf_platform.chipid import decode_chipid  # noqa: E402
from stdf_platform.views import _DEDUP_UNIT  # noqa: E402


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
    # part_txt (barcode) bound from PRR
    assert data.chip_ids[0]["part_txt"] == "2D-FTLOT01-0000"
    assert data.chip_ids[1]["part_txt"] == "2D-FTLOT01-0000"
    # the two dies of one package decode to different fabs
    assert decode_chipid(data.chip_ids[0]["efuse_raw"])["origin_fab"] == "TSMC1"
    assert decode_chipid(data.chip_ids[1]["efuse_raw"])["origin_fab"] == "TSMC2"


def test_parser_part_txt_on_parts(tmp_path):
    ft_file = tmp_path / "FT.stdf"
    make_ft_stdf(ft_file, "FTLOT01", parts=3)
    data = parse_stdf(ft_file)
    assert [p["part_txt"] for p in data.parts] == [
        "2D-FTLOT01-0000", "2D-FTLOT01-0001", "2D-FTLOT01-0002"
    ]


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


# ── FT retest: re-ingest must increment, not overwrite ───────────────────────

def test_ft_reingest_increments_retest(tmp_path):
    ft_file = tmp_path / "FT.stdf"
    make_ft_stdf(ft_file, "FTLOT01", parts=4)
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft_file)
    _ingest_ft(storage, ft_file)  # retest run

    parts_base = (tmp_path / "parts" / "product=CHIPLET2D" /
                  "test_category=FT" / "sub_process=FT1" /
                  "lot_id=FTLOT01" / "wafer_id=")
    retest_dirs = sorted(d.name for d in parts_base.iterdir())
    assert retest_dirs == ["retest=0", "retest=1"], retest_dirs

    # chipid table also increments
    chip_base = (tmp_path / "chipid" / "product=CHIPLET2D" /
                 "test_category=FT" / "sub_process=FT1" /
                 "lot_id=FTLOT01" / "wafer_id=")
    chip_dirs = sorted(d.name for d in chip_base.iterdir())
    assert chip_dirs == ["retest=0", "retest=1"], chip_dirs


# ── FT-aware dedup view picks latest retest by part_txt ──────────────────────

def test_ft_dedup_view_keeps_latest_retest(tmp_path):
    ft_file = tmp_path / "FT.stdf"
    # First run: part index 0 fails. Retest run: all pass.
    make_ft_stdf(ft_file, "FTLOT01", parts=4, fail_part_ids={0})
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft_file)

    ft_retest = tmp_path / "FT_retest.stdf"
    make_ft_stdf(ft_retest, "FTLOT01", parts=4)  # all pass
    _ingest_ft(storage, ft_retest)

    parts_glob = (tmp_path / "parts").as_posix() + "/**/*.parquet"
    conn = duckdb.connect()
    conn.execute(f"""
        CREATE VIEW parts AS
        SELECT * FROM read_parquet('{parts_glob}', hive_partitioning=true)
    """)
    conn.execute(f"""
        CREATE VIEW parts_final AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY lot_id, {_DEDUP_UNIT} ORDER BY retest_num DESC
            ) AS rn FROM parts
        ) WHERE rn = 1
    """)

    # 4 packages, not collapsed to 1 (the old wafer/coord key would collapse FT)
    total = conn.execute("SELECT COUNT(*) FROM parts_final").fetchone()[0]
    assert total == 4

    # package 0 now reflects the passing retest
    passed = conn.execute(
        "SELECT passed FROM parts_final WHERE part_txt = '2D-FTLOT01-0000'"
    ).fetchone()[0]
    assert passed is True


def test_chipid_final_dedups_by_decoded_efuse_not_occurrence(tmp_path):
    """chipid_final identity is the decoded eFuse, robust to GDR order swaps."""
    ft_file = tmp_path / "FT.stdf"
    make_ft_stdf(ft_file, "FTLOT01", parts=3)  # 3 packages x 2 dies = 6 distinct efuses
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft_file)
    _ingest_ft(storage, ft_file)  # retest run — same dies, retest_num increments

    chip_glob = (tmp_path / "chipid").as_posix() + "/**/*.parquet"
    conn = duckdb.connect()
    conn.execute(f"""
        CREATE VIEW chipid AS
        SELECT * FROM read_parquet('{chip_glob}', hive_partitioning=true)
    """)
    conn.execute("""
        CREATE VIEW chipid_final AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY lot_id, efuse_raw ORDER BY retest_num DESC
            ) AS rn FROM chipid
        ) WHERE rn = 1
    """)
    # raw doubled by re-ingest; final collapses to the 6 distinct decoded dies
    assert conn.execute("SELECT COUNT(*) FROM chipid").fetchone()[0] == 12
    assert conn.execute("SELECT COUNT(*) FROM chipid_final").fetchone()[0] == 6
    # every surviving row is the latest retest
    assert conn.execute(
        "SELECT MIN(retest_num) FROM chipid_final"
    ).fetchone()[0] == 1


def test_real_digit_zero_key_is_parsed(tmp_path):
    """Regression for the O-vs-0 bug: real files use 'EN-S0-CHIPID_R' (zero)."""
    from make_test_stdf import gdr_chipid
    from stdf_platform.chipid import CHIPID_KEY
    # the generator now defaults to the canonical digit-zero key
    assert CHIPID_KEY == "EN-S0-CHIPID_R"
    assert b"EN-S0-CHIPID_R" in gdr_chipid("0b" + "0" * 64)

    ft_file = tmp_path / "FT.stdf"
    make_ft_stdf(ft_file, "FTZERO", parts=3)  # emits digit-zero key GDRs
    data = parse_stdf(ft_file)
    assert len(data.chip_ids) == 6  # 3 packages x 2 dies, key matched


def test_letter_o_spelling_also_accepted(tmp_path):
    """The letter-O spelling from the spec must still parse (accept-set)."""
    # force the generator to emit the letter-O key, then confirm it still binds
    import make_test_stdf as gen
    orig = gen.gdr_chipid
    gen.gdr_chipid = lambda efuse, key="EN-SO-CHIPID_R": orig(efuse, key)
    try:
        ft_file = tmp_path / "FTO.stdf"
        gen.make_ft_stdf(ft_file, "FTO", parts=2)
        data = parse_stdf(ft_file)
        assert len(data.chip_ids) == 4
    finally:
        gen.gdr_chipid = orig


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


def test_ft_still_writes_chipid_table(tmp_path):
    ft_file = tmp_path / "ft.stdf"
    make_ft_stdf(ft_file, "FTLOT", parts=3)
    storage = _storage(tmp_path)
    _ingest_ft(storage, ft_file)  # test_category="FT"
    assert (tmp_path / "chipid").exists()
