"""CHIP ID <-> C0/C1 suite association, per-site part tracking, chip_key and the
FT package identity (eFuse chip_key only). Parser-level unless noted."""

import struct
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_test_stdf import (  # noqa: E402
    cn, encode_chipid, ft_mir, gdr_chipid, gdr_suite, make_ft_stdf, package_efuses,
    record,
)

from stdf_platform.chipid import decode_chipid  # noqa: E402
from stdf_platform.config import StorageConfig  # noqa: E402
from stdf_platform.mounts import setup_views  # noqa: E402
from stdf_platform.parser import CHIPID_SUITE_WINDOW, parse_stdf  # noqa: E402
from stdf_platform.storage import ParquetStorage  # noqa: E402

FAR = record(0, 10, struct.pack("BB", 2, 4))
E0, E1 = package_efuses(0)           # a package's C0 / C1 eFuses
E0b, E1b = package_efuses(1)


def pir(site=1):
    return record(5, 10, bytes([1, site]))


def prr(site=1, part_txt=""):
    return record(5, 20, struct.pack("<BBBHHHhhI", 1, site, 0, 1, 1, 1, -32768, -32768, 0)
                  + cn("") + cn(part_txt))


def ptr(value, site=1, test_num=1):
    return record(15, 10, struct.pack("<IBBBBf", test_num, 1, site, 0, 0, value) + cn("T"))


def noise(site=1):
    return gdr_suite("", site, text="SOMETHING_ELSE")


def key_of(efuse):
    return f"EF|{int(efuse[2:], 2):016X}"


def _write(tmp_path, *records, lot="L1", name="f.stdf"):
    f = tmp_path / name
    f.write_bytes(FAR + ft_mir(lot) + b"".join(records))
    return f


def parse(tmp_path, *records):
    return parse_stdf(_write(tmp_path, *records))


def package(c0=E0, c1=E1, site=1, part_txt=""):
    """PIR, PTR, [CHIP ID, suite] x2 (C0 then C1), PRR."""
    return (pir(site) + ptr(1.0, site) + gdr_chipid(c0, site=site) + gdr_suite("C0", site)
            + gdr_chipid(c1, site=site) + gdr_suite("C1", site) + prr(site, part_txt))


def views(tmp_path, *files):
    storage = ParquetStorage(StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb"))
    for f in files:
        storage.save_stdf_data(parse_stdf(f), product="P", test_category="FT",
                               sub_process="FT1", source_file=f.name)
    conn = duckdb.connect()
    setup_views(conn, tmp_path)
    return conn


# 1 -------------------------------------------------------------------------

def test_normal_package_resolves_c0_c1(tmp_path):
    d = parse(tmp_path, package())
    assert [(c["die_id"], c["efuse_raw"], c["association"], c["warnings"])
            for c in d.chip_ids] == [
        ("C0", E0, "following suite exact", []), ("C1", E1, "following suite exact", [])]
    assert d.chip_ids[0]["suite_name"] == "CHIPID_NRD0_C0"
    assert d.chip_ids[0]["suite_gdr_offset"] > d.chip_ids[0]["gdr_offset"]
    assert d.parts[0]["chip_key"] == key_of(E0)  # from C0


# 2 -------------------------------------------------------------------------

def test_unrelated_gdr_between_chip_and_suite_is_window(tmp_path):
    d = parse(tmp_path, pir(), gdr_chipid(E0), noise(), gdr_suite("C0"), prr())
    (c,) = d.chip_ids
    assert (c["die_id"], c["association"]) == ("C0", "following suite window")


def test_suite_beyond_window_is_unresolved(tmp_path):
    d = parse(tmp_path, pir(), gdr_chipid(E0), *[noise()] * (CHIPID_SUITE_WINDOW + 1),
              gdr_suite("C0"), prr())
    (c,) = d.chip_ids
    assert (c["die_id"], c["association"]) == ("UNKNOWN", "unresolved")


# 3 -------------------------------------------------------------------------

def test_missing_c1_status(tmp_path):
    f = _write(tmp_path,
               pir(), gdr_chipid(E0), gdr_suite("C0"), prr(),
               package(E0b, E1b))
    conn = views(tmp_path, f)
    got = dict(conn.execute("SELECT c0_chipid_hex, chip_status FROM parts").fetchall())
    assert got == {decode_chipid(E0)["chipid_hex"]: "missing C1",
                   decode_chipid(E0b)["chipid_hex"]: "complete"}
    # C1 columns stay NULL when the C1 CHIP ID is missing
    assert conn.execute(
        "SELECT c1_chipid_hex FROM parts WHERE chip_status = 'missing C1'").fetchone() == (None,)


# 4 -------------------------------------------------------------------------

def test_duplicate_c0_kept_and_ambiguous(tmp_path):
    f = _write(tmp_path, pir(), gdr_chipid(E0), gdr_suite("C0"), gdr_chipid(E0b),
               gdr_suite("C0"), gdr_chipid(E1), gdr_suite("C1"), prr())
    d = parse_stdf(f)
    c0 = [c for c in d.chip_ids if c["die_id"] == "C0"]
    assert len(c0) == 2
    assert all("duplicate C0 CHIP ID in part" in c["warnings"] for c in c0)
    assert d.parts[0]["chip_key"] == key_of(E1)  # ambiguous C0 never forms the key
    conn = views(tmp_path, f)
    # the first C0 candidate's values are kept, the status says it is ambiguous
    assert conn.execute("SELECT chip_status, c0_chipid_hex FROM parts").fetchall() == [
        ("ambiguous", decode_chipid(E0)["chipid_hex"])]


# 5 -------------------------------------------------------------------------

def test_reversed_order_identified_by_suite_name(tmp_path):
    d = parse(tmp_path, pir(), gdr_chipid(E1), gdr_suite("C1"),
              gdr_chipid(E0), gdr_suite("C0"), prr())
    assert [(c["die_id"], c["efuse_raw"]) for c in d.chip_ids] == [("C1", E1), ("C0", E0)]
    assert d.parts[0]["chip_key"] == key_of(E0)


# 6 -------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["0b" + "1" * 63, "0b" + "1" * 65, "0b" + "2" * 64],
                         ids=["63bits", "65bits", "non_binary"])
def test_malformed_value_keeps_row_and_parse_continues(tmp_path, bad):
    d = parse(tmp_path, package(c0=bad), package(E0b, E1b))
    assert len(d.parts) == 2
    first = [c for c in d.chip_ids if c["part_id"] == d.parts[0]["part_id"]]
    assert first[0]["efuse_raw"] == bad
    dec = decode_chipid(bad)
    assert dec["valid"] is False and dec["warnings"]
    assert d.parts[0]["chip_key"] == key_of(E1)   # bad C0 gives no key; C1 does
    assert d.parts[1]["chip_key"] == key_of(E0b)  # later part unaffected


# 7 -------------------------------------------------------------------------

def test_part_boundary_suite_in_next_part_is_not_used(tmp_path):
    d = parse(tmp_path, pir(), gdr_chipid(E0), prr(), pir(), gdr_suite("C0"), prr())
    (c,) = d.chip_ids
    assert (c["die_id"], c["association"]) == ("UNKNOWN", "unresolved")
    assert c["part_id"] == d.parts[0]["part_id"]
    assert "CHIP ID found but no C0/C1 suite association found" in c["warnings"]
    assert d.parts[0]["chip_key"] == "" == d.parts[1]["chip_key"]


# 8 -------------------------------------------------------------------------

def test_multisite_interleave_binds_results_and_chip_ids_per_site(tmp_path):
    d = parse(
        tmp_path,
        pir(1), pir(2),
        ptr(1.0, 1), ptr(2.0, 2),
        gdr_chipid(E0, site=1), gdr_suite("C0", 1),
        gdr_chipid(E0b, site=2), gdr_suite("C0", 2),
        ptr(11.0, 1), ptr(22.0, 2),
        gdr_chipid(E1, site=1), gdr_suite("C1", 1),
        gdr_chipid(E1b, site=2), gdr_suite("C1", 2),
        prr(1), prr(2),
    )
    pid = {p["site_num"]: p["part_id"] for p in d.parts}
    assert len(pid) == 2 and pid[1] != pid[2]
    assert {(r["part_id"], r["result"]) for r in d.test_results} == {
        (pid[1], 1.0), (pid[2], 2.0), (pid[1], 11.0), (pid[2], 22.0)}
    by_site = {s: [(c["die_id"], c["efuse_raw"]) for c in d.chip_ids if c["part_id"] == pid[s]]
               for s in (1, 2)}
    assert by_site == {1: [("C0", E0), ("C1", E1)], 2: [("C0", E0b), ("C1", E1b)]}
    # the tester site is not the die: both sites have a C0 and a C1
    assert {c["site_num"] for c in d.chip_ids} == {1, 2}
    assert {p["site_num"]: p["chip_key"] for p in d.parts} == {1: key_of(E0), 2: key_of(E0b)}


# 9 -------------------------------------------------------------------------

def test_sample_values_decode_to_xy_wafer_fab(tmp_path):
    c0 = encode_chipid(1, "E6B156", 6, 292, 105)
    c1 = encode_chipid(1, "E6B156", 6, 284, 105)
    conn = views(tmp_path, _write(tmp_path, package(c0, c1)))
    row = conn.execute(
        "SELECT c0_origin_fab, c0_origin_wafer, c0_origin_x, c0_origin_y, c0_origin_lot, "
        "c1_origin_fab, c1_origin_wafer, c1_origin_x, c1_origin_y, chip_status, chip_warnings "
        "FROM parts").fetchone()
    assert row == ("TSMC1", 6, 292, 105, "E6B156", "TSMC1", 6, 284, 105, "complete", "")


# 10 ------------------------------------------------------------------------

def test_partial_retest_overlays_by_chip_key_in_any_order(tmp_path):
    a, b = tmp_path / "a.stdf", tmp_path / "b.stdf"
    make_ft_stdf(a, "FTR", pkgs=[0, 1, 2, 3], fail_part_ids={0, 3})
    make_ft_stdf(b, "FTR", pkgs=[3, 0])  # different order/positions, all pass
    conn = views(tmp_path, a, b)

    got = conn.execute(
        "SELECT part_txt, retest_num, passed FROM parts_final ORDER BY part_txt").fetchall()
    assert got == [("2D-FTR-0000", 1, True), ("2D-FTR-0001", 0, True),
                   ("2D-FTR-0002", 0, True), ("2D-FTR-0003", 1, True)]
    td = conn.execute(
        "SELECT part_txt, retest_num FROM test_data_final ORDER BY part_txt").fetchall()
    assert td == [("2D-FTR-0000", 1), ("2D-FTR-0001", 0), ("2D-FTR-0002", 0), ("2D-FTR-0003", 1)]
    # CHIP ID columns follow the surviving part: 4 complete packages
    assert conn.execute(
        "SELECT COUNT(*) FROM parts_final WHERE chip_status = 'complete'").fetchone()[0] == 4


# 11 ------------------------------------------------------------------------

def test_same_package_reinserted_in_one_file_keeps_its_key(tmp_path):
    # Same C0 AND C1 on two parts = one package tested twice in the lot.
    d = parse(tmp_path, package(E0, E1), package(E0b, E1b), package(E0, E1))
    assert [p["chip_key"] for p in d.parts] == [key_of(E0), key_of(E0b), key_of(E0)]
    again = [c for c in d.chip_ids
             if c["part_id"] in {d.parts[0]["part_id"], d.parts[2]["part_id"]}]
    assert len(again) == 4
    assert all("package re-inserted in file" in c["warnings"] for c in again)


def test_same_c0_with_different_c1_clears_both(tmp_path):
    d = parse(tmp_path, package(E0, E1), package(E0, E1b), package(E0b, E1b))
    assert [p["chip_key"] for p in d.parts] == ["", "", key_of(E0b)]
    shared_ids = {d.parts[0]["part_id"], d.parts[1]["part_id"]}
    shared = [c for c in d.chip_ids if c["part_id"] in shared_ids]
    assert len(shared) == 4
    assert all("CHIP ID shared by several parts in file" in c["warnings"] for c in shared)
    assert all(c["chip_key"] == "" for c in shared)


def test_package_reinserted_in_one_file_counts_once_and_overlays_later_file(tmp_path):
    a, b = tmp_path / "a.stdf", tmp_path / "b.stdf"
    make_ft_stdf(a, "FTR", pkgs=[0, 1, 0], fail_part_ids={0})  # pkg0 twice in one file
    conn = views(tmp_path, a)
    assert conn.execute(
        "SELECT part_txt, part_seq, passed FROM parts_final ORDER BY part_txt").fetchall() == [
        ("2D-FTR-0000", 3, False), ("2D-FTR-0001", 2, True)]
    make_ft_stdf(b, "FTR", pkgs=[0])  # later file: pkg0 passes
    conn = views(tmp_path, a, b)
    assert conn.execute(
        "SELECT part_txt, retest_num, passed FROM parts_final ORDER BY part_txt").fetchall() == [
        ("2D-FTR-0000", 1, True), ("2D-FTR-0001", 0, True)]


# 12 ------------------------------------------------------------------------

def test_package_without_chip_id_is_never_overlaid(tmp_path):
    a, b = tmp_path / "a.stdf", tmp_path / "b.stdf"
    make_ft_stdf(a, "FTN", parts=3, with_chipid=False)
    make_ft_stdf(b, "FTN", parts=3, with_chipid=False)  # same barcodes
    conn = views(tmp_path, a, b)
    got = conn.execute(
        "SELECT retest_num, COUNT(*) FROM parts_final GROUP BY 1 ORDER BY 1").fetchall()
    assert got == [(0, 3), (1, 3)]
    assert conn.execute("SELECT COUNT(*) FROM test_data_final").fetchone()[0] == 6
