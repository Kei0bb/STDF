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
