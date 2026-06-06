"""TSMC-only EN-SO-CHIPID_R eFuse/ChipID decoder.

The GDR record carries a key string ``EN-SO-CHIPID_R`` and a value string of the
form ``0b`` followed by a 64-bit binary string. That 64-bit field is a textual
eFuse/ChipID bitfield (not ciphertext) and is decoded with the TSMC-only layout
below to recover the die's origin (fab / lot / wafer / x / y).

Bit layout (Python slicing is MSB-based, index 0 = most significant bit)::

    EFUSE[0:4]   fab bits
    EFUSE[4:13]  Y coordinate, 9-bit unsigned
    EFUSE[13:22] X coordinate, 9-bit unsigned
    EFUSE[22:27] wafer encoded value, 5-bit unsigned (wafer = value - 3)
    EFUSE[27:62] lot encoded field (1x CHAR1 + 5x CHAR2, 6 chars total)
    EFUSE[62:64] reserved
"""

# Key string used inside GDR records to mark a ChipID value field.
# NOTE: the real eFuse key uses a DIGIT ZERO ("EN-S0-..."), not letter O.
# The spec was transcribed with a letter O ("EN-SO-..."); the two are visually
# identical in many fonts. We accept both spellings to be safe; the digit-zero
# form is canonical (it is what actual STDF files contain).
CHIPID_KEY = "EN-S0-CHIPID_R"
CHIPID_KEYS = frozenset({"EN-S0-CHIPID_R", "EN-SO-CHIPID_R"})

# Java-compatible TSMC lot-number character tables.
LOTNO_CHAR1 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ_"          # 27 chars (5-bit index)
LOTNO_CHAR2 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"  # 36 chars (6-bit index)

# fab_code -> fab name (TSMC only).
_FAB_NAMES = {1: "TSMC1", 6: "TSMC2"}


def normalize_efuse(value: str) -> str | None:
    """Normalize a GDR value string to a 64-character binary string.

    Accepts an optional ``0b`` prefix. Returns ``None`` if the input is not a
    clean 64-bit binary string.
    """
    if value is None:
        return None
    s = value.strip()
    if s[:2].lower() == "0b":
        s = s[2:]
    if len(s) != 64 or any(c not in "01" for c in s):
        return None
    return s


def _lot_char(efuse: str, lo: int, hi: int, table: str) -> str:
    idx = int(efuse[lo:hi], 2)
    return table[idx] if idx < len(table) else "?"


def decode_chipid(value: str) -> dict:
    """Decode an EN-SO-CHIPID_R value string into origin fields.

    Returns a dict with the decoded fields. ``valid`` is False (and decoded
    fields are None) when the input is not a 64-bit binary string. Non-TSMC fab
    codes still decode coordinates/lot but report ``origin_fab='UNSUPPORTED'``;
    every occurrence is preserved either way.
    """
    efuse = normalize_efuse(value)
    if efuse is None:
        return {
            "efuse_raw": value,
            "valid": False,
            "origin_fab_code": None,
            "origin_fab": None,
            "origin_lot": None,
            "origin_wafer": None,
            "origin_x": None,
            "origin_y": None,
            "reserved_bits": None,
        }

    fab_code = int(efuse[0:4], 2)
    lot = (
        _lot_char(efuse, 27, 32, LOTNO_CHAR1)
        + _lot_char(efuse, 32, 38, LOTNO_CHAR2)
        + _lot_char(efuse, 38, 44, LOTNO_CHAR2)
        + _lot_char(efuse, 44, 50, LOTNO_CHAR2)
        + _lot_char(efuse, 50, 56, LOTNO_CHAR2)
        + _lot_char(efuse, 56, 62, LOTNO_CHAR2)
    )

    return {
        "efuse_raw": efuse,
        "valid": True,
        "origin_fab_code": fab_code,
        "origin_fab": _FAB_NAMES.get(fab_code, "UNSUPPORTED"),
        "origin_lot": lot,
        "origin_wafer": int(efuse[22:27], 2) - 3,
        "origin_x": int(efuse[13:22], 2),
        "origin_y": int(efuse[4:13], 2),
        "reserved_bits": efuse[62:64],
    }
