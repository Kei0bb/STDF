"""parser.py の直接テスト（生成器が作らないレコード/破損入力を含む）。"""

import struct

from stdf_platform.parser import STDFParser
from make_test_stdf import cn, make_ft_stdf, record


def _mir(lot: str) -> bytes:
    return record(1, 10,
        struct.pack("<IIB", 0, 1700000000, 1)
        + struct.pack("<BBBHB", 32, 32, 32, 0, 32)
        + cn(lot) + cn("PT") + cn("NODE") + cn("J750")
        + cn("JOB") + cn("Rev01") + cn("") + cn("OP") + cn("") + cn("")
        + cn("FT1"))


def test_record_boundaries_survive_overlong_cn_and_unknown_record(tmp_path):
    """壊れた Cn 長で次レコードへ読み越しても rec_len 境界へ復帰し、未知
    レコードも読み飛ばして、後続の MIR を正しく読めること。"""
    bad_lot = bytes([200]) + b"LOT"          # 長さ 200 だが実データ 3 バイト
    mir_bad = record(1, 10,
        struct.pack("<IIB", 0, 1, 1)
        + struct.pack("<BBBHB", 32, 32, 32, 0, 32) + bad_lot)
    # MIR の後に続く必須フィールドがない壊れたレコード + 正しい MIR
    raw = (record(0, 10, struct.pack("BB", 2, 4)) + mir_bad
           + record(99, 99, b"\x01\x02\x03")   # 未知レコードは rec_len 分読み飛ばす
           + _mir("GOOD_LOT"))
    p = tmp_path / "x.stdf"
    p.write_bytes(raw)
    assert STDFParser().parse(p).lot_id == "GOOD_LOT"


def test_big_endian_far_switches_endianness(tmp_path):
    def be_record(rec_typ: int, rec_sub: int, data: bytes) -> bytes:
        # 実ファイルでは FAR のヘッダ(REC_LEN)自身もファイルのバイトオーダー
        # で書かれる。FAR も含めて全レコードを big-endian で書く。
        return struct.pack(">HBB", len(data), rec_typ, rec_sub) + data

    mir = (
        struct.pack(">IIB", 0, 1700000000, 1)
        + struct.pack(">BBBHB", 32, 32, 32, 0, 32)
        + cn("BE_LOT") + cn("PT") + cn("NODE") + cn("J750")
        + cn("JOB") + cn("Rev01") + cn("") + cn("OP") + cn("") + cn("")
    )
    far = be_record(0, 10, struct.pack("BB", 1, 4))  # cpu_type=1 = big endian
    p = tmp_path / "be.stdf"
    p.write_bytes(far + be_record(1, 10, mir))
    assert STDFParser().parse(p).lot_id == "BE_LOT"


def test_mpr_expands_per_pin_and_resolves_pin_name(tmp_path):
    pmr = record(1, 60,
        struct.pack("<HH", 7, 0) + cn("") + cn("") + cn("PIN7"))
    mpr = record(15, 15,
        struct.pack("<IBBBBHH", 42, 1, 1, 0x00, 0x00, 2, 2)  # rtn_icnt=2, rslt_cnt=2
        + struct.pack("B", 0x00)                            # RTN_STAT nibbles
        + struct.pack("<ff", 0.25, 0.75)                    # RTN_RSLT
        + cn("") + cn("") + struct.pack("<B", 0xFF)         # txt, alarm, opt_flag
        + struct.pack("<bbb", 0, 0, 0)
        + struct.pack("<ff", -1.0, 1.0) + struct.pack("<ff", 0.0, 0.0)
        + struct.pack("<HH", 7, 7)                          # RTN_INDX
        + cn("V"))
    raw = record(0, 10, struct.pack("BB", 2, 4)) + _mir("L") + pmr + mpr
    p = tmp_path / "mpr.stdf"
    p.write_bytes(raw)
    data = STDFParser().parse(p)
    rows = [r for r in data.test_results if r["test_num"] == 42]
    assert [r["pin_num"] for r in rows] == [7, 7]
    assert [r["pin_name"] for r in rows] == ["PIN7", "PIN7"]
    assert [r["result"] for r in rows] == [0.25, 0.75]


def test_ftr_is_recorded_without_result(tmp_path):
    ftr = record(15, 20, struct.pack("<IBBB", 77, 1, 1, 0x00))
    raw = record(0, 10, struct.pack("BB", 2, 4)) + _mir("L") + ftr
    p = tmp_path / "ftr.stdf"
    p.write_bytes(raw)
    data = STDFParser().parse(p)
    assert data.tests[77]["rec_type"] == "FTR"
    assert data.test_results[0]["result"] is None


def test_parser_keeps_prr_part_id(tmp_path):
    """PRR.PART_ID は part_serial として残す(barcode 空の FT の identity)。"""
    make_ft_stdf(tmp_path / "ft.stdf", lot_id="FL", parts=2)
    data = STDFParser().parse(tmp_path / "ft.stdf")
    assert [p["part_serial"] for p in data.parts] == ["UNIT0000", "UNIT0001"]


# ── TEST_FLG / OPT_FLAG / PART_FLG ──────────────────────────────────────

def _ptr(test_flg: int, opt_flag: int, value: float = 1.0, lo: float = 0.0,
         hi: float = 2.0, test_num: int = 1) -> bytes:
    return record(15, 10,
        struct.pack("<IBBBB", test_num, 1, 1, test_flg, 0x00)
        + struct.pack("<f", value) + cn("T") + cn("")
        + struct.pack("B", opt_flag) + struct.pack("<bbb", 0, 0, 0)
        + struct.pack("<ff", lo, hi) + cn("V"))


def _parse(tmp_path, *records):
    p = tmp_path / "f.stdf"
    p.write_bytes(record(0, 10, struct.pack("BB", 2, 4)) + _mir("L") + b"".join(records))
    return STDFParser().parse(p)


def test_ptr_result_invalid_flag_gives_no_result(tmp_path):
    d = _parse(tmp_path, _ptr(0x02, 0x00))
    (r,) = d.test_results
    assert r["result"] is None and r["test_flg"] == 0x02


def test_ptr_not_executed_or_passfail_invalid_gives_no_verdict(tmp_path):
    d = _parse(tmp_path, _ptr(0x10, 0x00, test_num=1), _ptr(0x40, 0x00, test_num=2),
               _ptr(0x80, 0x00, test_num=3), _ptr(0x00, 0x00, test_num=4))
    assert [r["passed"] for r in d.test_results] == [None, None, False, True]


def test_ptr_opt_flag_no_limit_gives_none(tmp_path):
    d = _parse(tmp_path,
               _ptr(0x00, 0x40, test_num=1),    # no low limit
               _ptr(0x00, 0x80, test_num=2),    # no high limit
               _ptr(0x00, 0x00, test_num=3))    # both limits valid
    assert (d.tests[1]["lo_limit"], d.tests[1]["hi_limit"]) == (None, 2.0)
    assert (d.tests[2]["lo_limit"], d.tests[2]["hi_limit"]) == (0.0, None)
    assert (d.tests[3]["lo_limit"], d.tests[3]["hi_limit"]) == (0.0, 2.0)


def test_ptr_limit_missing_in_first_record_is_filled_by_later_one(tmp_path):
    d = _parse(tmp_path, _ptr(0x00, 0x40, test_num=1), _ptr(0x00, 0x00, test_num=1))
    assert (d.tests[1]["lo_limit"], d.tests[1]["hi_limit"]) == (0.0, 2.0)


def test_prr_part_flg_pass_fail_invalid_gives_no_verdict(tmp_path):
    def part(flg, x):
        return (record(5, 10, struct.pack("BB", 1, 1))
                + record(5, 20, struct.pack("<BBBHHHhhI", 1, 1, flg, 1, 1, 1, x, 0, 0)
                         + cn("") + cn("")))
    d = _parse(tmp_path, part(0x00, 0), part(0x08, 1), part(0x10, 2))
    assert [(p["passed"], p["part_flg"]) for p in d.parts] == [
        (True, 0x00), (False, 0x08), (None, 0x10)]


def test_invalid_flags_are_stored_as_null(tmp_path):
    """parts.passed / test_data.passed / result / limits are NULL (not False / 0)
    when the flags say they are invalid; the raw flags are kept."""
    import duckdb
    from stdf_platform.config import StorageConfig
    from stdf_platform.mounts import setup_views
    from stdf_platform.storage import ParquetStorage

    part = (record(5, 10, struct.pack("BB", 1, 1))
            + _ptr(0x02, 0x40, test_num=1)      # result invalid, no low limit
            + _ptr(0x10, 0x00, test_num=2)      # not executed
            + _ptr(0x40, 0x00, test_num=3)      # pass/fail invalid
            + _ptr(0x00, 0x00, test_num=4)
            + record(5, 20, struct.pack("<BBBHHHhhI", 1, 1, 0x10, 4, 1, 1, 3, 4, 0)
                     + cn("") + cn("")))
    data = _parse(tmp_path, part)
    store = tmp_path / "store"
    ParquetStorage(StorageConfig(data_dir=store)).save_stdf_data(
        data, product="P", test_category="CP", sub_process="CP1", source_file="f.stdf")

    conn = duckdb.connect()
    setup_views(conn, store)
    assert conn.execute("SELECT passed, part_flg FROM parts_final").fetchall() == [(None, 0x10)]
    rows = conn.execute(
        "SELECT test_num, result, passed, test_flg, lo_limit, hi_limit "
        "FROM test_data_final ORDER BY test_num").fetchall()
    assert rows == [(1, None, "P", 0x02, None, 2.0),
                    (2, 1.0, None, 0x10, 0.0, 2.0),
                    (3, 1.0, None, 0x40, 0.0, 2.0),
                    (4, 1.0, "P", 0x00, 0.0, 2.0)]
