import pytest

from stdf_platform.chipid import decode_chipid


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


EXAMPLE_BITS = format(0x13998DC75193D350, "064b")


@pytest.mark.parametrize("fab_bits, code, name", [
    ("0110", 6, "TSMC2"),
    ("0000", 0, "UNSUPPORTED"),
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
