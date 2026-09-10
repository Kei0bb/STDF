"""parser.py の直接テスト（生成器が作らないレコード/破損入力を含む）。"""

import struct

from stdf_platform.parser import STDFParser
from make_test_stdf import cn, record


def _mir(lot: str) -> bytes:
    return record(1, 10,
        struct.pack("<IIB", 0, 1700000000, 1)
        + struct.pack("<BBBHB", 32, 32, 32, 0, 32)
        + cn(lot) + cn("PT") + cn("NODE") + cn("J750")
        + cn("JOB") + cn("Rev01") + cn("") + cn("OP") + cn("") + cn("")
        + cn("FT1"))


def test_overlong_cn_does_not_desync_next_record(tmp_path):
    """壊れた Cn 長で次レコードへ読み越しても、rec_len 境界へ復帰して
    後続の MIR を正しく読めること。"""
    bad_lot = bytes([200]) + b"LOT"          # 長さ 200 だが実データ 3 バイト
    mir_bad = record(1, 10,
        struct.pack("<IIB", 0, 1, 1)
        + struct.pack("<BBBHB", 32, 32, 32, 0, 32) + bad_lot)
    # MIR の後に続く必須フィールドがない壊れたレコード + 正しい MIR
    raw = record(0, 10, struct.pack("BB", 2, 4)) + mir_bad + _mir("GOOD_LOT")
    p = tmp_path / "x.stdf"
    p.write_bytes(raw)
    assert STDFParser().parse(p).lot_id == "GOOD_LOT"


def test_big_endian_far_switches_endianness(tmp_path):
    def be_record(rec_typ: int, rec_sub: int, data: bytes) -> bytes:
        # FAR で byte order が決まった後は、レコードヘッダの REC_LEN も
        # その byte order で読まれる (parser._read_header は _s_u2 を使う)。
        return struct.pack(">HBB", len(data), rec_typ, rec_sub) + data

    mir = (
        struct.pack(">IIB", 0, 1700000000, 1)
        + struct.pack(">BBBHB", 32, 32, 32, 0, 32)
        + cn("BE_LOT") + cn("PT") + cn("NODE") + cn("J750")
        + cn("JOB") + cn("Rev01") + cn("") + cn("OP") + cn("") + cn("")
    )
    far = record(0, 10, struct.pack("BB", 1, 4))   # cpu_type=1 = big endian
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
    raw = record(0, 10, struct.pack("BB", 2, 4)) + pmr + mpr
    p = tmp_path / "mpr.stdf"
    p.write_bytes(raw)
    data = STDFParser().parse(p)
    rows = [r for r in data.test_results if r["test_num"] == 42]
    assert [r["pin_num"] for r in rows] == [7, 7]
    assert [r["pin_name"] for r in rows] == ["PIN7", "PIN7"]
    assert [r["result"] for r in rows] == [0.25, 0.75]


def test_ftr_is_recorded_without_result(tmp_path):
    ftr = record(15, 20, struct.pack("<IBBB", 77, 1, 1, 0x00))
    raw = record(0, 10, struct.pack("BB", 2, 4)) + ftr
    p = tmp_path / "ftr.stdf"
    p.write_bytes(raw)
    data = STDFParser().parse(p)
    assert data.tests[77]["rec_type"] == "FTR"
    assert data.test_results[0]["result"] is None


def test_unknown_record_is_skipped(tmp_path):
    raw = (record(0, 10, struct.pack("BB", 2, 4))
           + record(99, 99, b"\x01\x02\x03")     # 未知レコード
           + _mir("AFTER_UNKNOWN"))
    p = tmp_path / "unk.stdf"
    p.write_bytes(raw)
    assert STDFParser().parse(p).lot_id == "AFTER_UNKNOWN"
