import pytest

from stdf_platform.chipid import decode_chipid, words_to_binary64


# Authoritative TSMC vectors (offset lookup tables required).
@pytest.mark.parametrize("hexval, lot, wafer, x, y", [
    ("0x13998DC75193D350", "E6B156", 11, 99, 115),
    ("0x139189C75193D350", "E6B156", 11, 98, 114),
    ("0x134191A75193D34C", "E6B155", 10, 100, 104),
])
def test_decode_official_tsmc_vectors(hexval, lot, wafer, x, y):
    d = decode_chipid("0b" + format(int(hexval, 16), "064b"))
    assert d["valid"] is True
    assert d["origin_fab_code"] == 1
    assert d["origin_fab"] == "TSMC1"
    assert d["origin_lot"] == lot
    assert d["origin_wafer"] == wafer
    assert d["origin_x"] == x
    assert d["origin_y"] == y
    assert d["reserved_bits"] == "00"
    assert d["warnings"] == []


EXAMPLE_BITS = format(0x13998DC75193D350, "064b")


@pytest.mark.parametrize("fab_bits, code, name", [
    ("0110", 6, "TSMC2"),
    ("0000", 0, "UNKNOWN"),
])
def test_decode_fab_code(fab_bits, code, name):
    d = decode_chipid("0b" + fab_bits + EXAMPLE_BITS[4:])
    assert d["valid"] is True
    assert d["origin_fab_code"] == code
    assert d["origin_fab"] == name
    assert d["origin_x"] == 99       # coordinates still decode


@pytest.mark.parametrize("value", [
    "garbage",
    "0b1010",            # too short
    "0b" + "2" * 64,     # non-binary
    None,
])
def test_decode_invalid_marks_not_valid(value):
    d = decode_chipid(value)
    assert d["valid"] is False
    assert d["origin_lot"] is None
    assert d["efuse_raw"] == value


def _bits(fab=None, wafer=None, reserved=None, lot=None):
    """EXAMPLE_BITS with selected fields replaced."""
    b = EXAMPLE_BITS
    if fab is not None:
        b = format(fab, "04b") + b[4:]
    if wafer is not None:
        b = b[:22] + format(wafer, "05b") + b[27:]
    if lot is not None:
        b = b[:27] + lot + b[62:]
    if reserved is not None:
        b = b[:62] + reserved
    return b


def test_hex_uint64_and_clean_warnings():
    d = decode_chipid("0b" + EXAMPLE_BITS)
    assert d["chipid_hex"] == "0x13998DC75193D350"
    assert d["chipid_uint64"] == 0x13998DC75193D350
    assert (d["origin_lot"], d["origin_wafer"], d["origin_x"], d["origin_y"]) == ("E6B156", 11, 99, 115)
    assert d["origin_fab"] == "TSMC1"
    assert d["warnings"] == []


@pytest.mark.parametrize("value", [
    EXAMPLE_BITS[:-1],             # 63 bits
    EXAMPLE_BITS + "0",            # 65 bits
    EXAMPLE_BITS[:-1] + "x",       # non-binary char
])
def test_malformed_values_do_not_raise(value):
    d = decode_chipid(value)
    assert d["valid"] is False
    assert d["efuse_raw"] == value
    assert d["chipid_hex"] is None and d["chipid_uint64"] is None
    assert d["origin_fab"] is None and d["origin_lot"] is None
    assert d["warnings"] == ["malformed CHIP ID"]


def test_non_tsmc_fab_decodes_common_fields_only():
    d = decode_chipid(_bits(fab=3))
    assert d["valid"] is True
    assert d["origin_fab"] == "UMC"
    assert d["origin_lot"] is None
    assert (d["origin_wafer"], d["origin_x"], d["origin_y"]) == (11, 99, 115)
    assert d["warnings"] == ["unsupported fab decoder: UMC"]


def test_undefined_fab_code():
    d = decode_chipid(_bits(fab=15))
    assert d["origin_fab"] == "UNKNOWN"
    assert d["origin_lot"] is None
    assert "undefined fab code 15" in d["warnings"]


@pytest.mark.parametrize("enc, wafer", [(3, 0), (2, -1), (29, 26)])
def test_wafer_out_of_range(enc, wafer):
    d = decode_chipid(_bits(wafer=enc))
    assert d["origin_wafer"] == wafer
    assert d["warnings"] == [f"wafer out of range: {wafer}"]


def test_tsmc_reserved_bits_kept_with_warning():
    d = decode_chipid(_bits(reserved="01"))
    assert d["reserved_bits"] == "01"
    assert d["origin_lot"] == "E6B156"
    assert d["warnings"] == ["reserved bits not 00: 01"]


def test_non_tsmc_reserved_bits_not_checked():
    d = decode_chipid(_bits(fab=3, reserved="11"))
    assert d["reserved_bits"] == "11"
    assert all("reserved" not in w for w in d["warnings"])


def test_lot_char_out_of_range():
    # char1 index 0 (placeholder), char3 index 63 (placeholder)
    lot = "00000" + "001110" + "111111" + EXAMPLE_BITS[44:62]
    d = decode_chipid(_bits(lot=lot))
    assert d["origin_lot"][0] == "?" and d["origin_lot"][2] == "?"
    assert d["origin_lot"][1] == "0"
    assert "lot char 1 out of range (code 0)" in d["warnings"]
    assert "lot char 3 out of range (code 63)" in d["warnings"]


def test_words_to_binary64_signed_roundtrip():
    n = 0xF3998DC7_9193D350               # bit31 set in both words
    hi, lo = n >> 32, n & 0xFFFFFFFF
    sh, sl = hi - (1 << 32), lo - (1 << 32)   # as signed int32 (negative)
    assert sh < 0 and sl < 0
    assert words_to_binary64(sl, sh) == format(n, "064b")
    assert words_to_binary64(lo, hi) == format(n, "064b")
    assert decode_chipid(words_to_binary64(sl, sh))["chipid_uint64"] == n
