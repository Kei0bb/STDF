from stdf_platform.chipid import decode_chipid, normalize_efuse, CHIPID_KEY


EXAMPLE = "0b0001001110011001100011011100011101010001100100111101001101010000"


def test_normalize_strips_0b_prefix():
    assert normalize_efuse(EXAMPLE) == EXAMPLE[2:]
    assert normalize_efuse(EXAMPLE[2:]) == EXAMPLE[2:]


def test_normalize_rejects_bad_input():
    assert normalize_efuse("0b1010") is None          # too short
    assert normalize_efuse("0b" + "2" * 64) is None    # non-binary
    assert normalize_efuse(None) is None


def test_decode_example():
    d = decode_chipid(EXAMPLE)
    assert d["valid"] is True
    assert d["origin_fab_code"] == 1
    assert d["origin_fab"] == "TSMC1"
    assert d["origin_lot"] == "HKPFJK"
    assert d["origin_wafer"] == 11
    assert d["origin_x"] == 99
    assert d["origin_y"] == 115
    assert d["reserved_bits"] == "00"


def test_decode_fab6_is_tsmc2():
    # Flip the fab nibble to 0110 (=6) on the example.
    bits = "0110" + EXAMPLE[2:][4:]
    d = decode_chipid("0b" + bits)
    assert d["origin_fab_code"] == 6
    assert d["origin_fab"] == "TSMC2"


def test_decode_unsupported_fab_still_preserved():
    bits = "0000" + EXAMPLE[2:][4:]  # fab_code 0
    d = decode_chipid("0b" + bits)
    assert d["valid"] is True
    assert d["origin_fab"] == "UNSUPPORTED"
    # coordinates still decode
    assert d["origin_x"] == 99


def test_decode_invalid_marks_not_valid():
    d = decode_chipid("garbage")
    assert d["valid"] is False
    assert d["origin_lot"] is None
    assert d["efuse_raw"] == "garbage"


def test_chipid_key_constant():
    assert CHIPID_KEY == "EN-SO-CHIPID_R"
