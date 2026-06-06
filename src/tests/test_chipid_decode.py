from stdf_platform.chipid import decode_chipid, normalize_efuse, CHIPID_KEY, CHIPID_KEYS


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
    assert d["origin_lot"] == "E6B156"
    assert d["origin_wafer"] == 11
    assert d["origin_x"] == 99
    assert d["origin_y"] == 115
    assert d["reserved_bits"] == "00"


def test_decode_official_tsmc_vectors():
    # Authoritative TSMC vectors (offset lookup tables required).
    vectors = [
        ("0x13998DC75193D350", "E6B156", 11, 99, 115),
        ("0x139189C75193D350", "E6B156", 11, 98, 114),
        ("0x134191A75193D34C", "E6B155", 10, 100, 104),
    ]
    for hexval, lot, wafer, x, y in vectors:
        bits = "0b" + format(int(hexval, 16), "064b")
        d = decode_chipid(bits)
        assert d["origin_lot"] == lot, hexval
        assert d["origin_wafer"] == wafer, hexval
        assert d["origin_x"] == x, hexval
        assert d["origin_y"] == y, hexval


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
    # canonical key uses a DIGIT ZERO ("EN-S0-..."), matching real STDF files
    assert CHIPID_KEY == "EN-S0-CHIPID_R"
    assert CHIPID_KEY[4] == "0"  # guard against the letter-O regression


def test_chipid_keys_accepts_both_spellings():
    # both the digit-zero (real) and letter-O (spec) spellings are accepted
    assert "EN-S0-CHIPID_R" in CHIPID_KEYS
    assert "EN-SO-CHIPID_R" in CHIPID_KEYS
    assert CHIPID_KEY in CHIPID_KEYS
