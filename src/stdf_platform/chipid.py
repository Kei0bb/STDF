"""EN-S0-CHIPID_R eFuse/ChipID decoder (lot decoding is TSMC-only).

The GDR record carries a key string ``EN-S0-CHIPID_R`` and a value string of the
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

Validation: a malformed value (not a clean 64-bit binary string) gives
``valid=False``, all decoded fields None and ``warnings=["malformed CHIP ID"]``.
Anything else still decodes and ``valid`` stays True; suspicious content is
reported in ``warnings`` (list[str], empty when clean) instead:

- undefined fab code (not in 1..8) -> ``origin_fab='UNKNOWN'``
- non-TSMC fab -> ``origin_lot=None`` (the lot tables are TSMC-only)
- TSMC lot char index on a ``_`` placeholder -> ``?`` at that position
- wafer outside 1..25, TSMC reserved bits != ``00``

``chipid_hex`` / ``chipid_uint64`` expose the raw 64 bits. ``words_to_binary64``
builds the binary string from two 32-bit words (e.g. signed ints from a tester).
"""

# Key string used inside GDR records to mark a ChipID value field.
# NOTE: the real eFuse key uses a DIGIT ZERO ("EN-S0-..."), not letter O.
# The spec was transcribed with a letter O ("EN-SO-..."); the two are visually
# identical in many fonts. We accept both spellings to be safe; the digit-zero
# form is canonical (it is what actual STDF files contain).
CHIPID_KEY = "EN-S0-CHIPID_R"
CHIPID_KEYS = frozenset({"EN-S0-CHIPID_R", "EN-SO-CHIPID_R"})

# Java-compatible TSMC lot-number character tables.
#
# The encoded bit value indexes DIRECTLY into these strings, and valid
# characters do NOT start at index 0 — leading "_" placeholders set the offset.
# Do NOT use compact tables (e.g. "ABC...Z_"): index 7 must give "E" (not "H"),
# and index 20 must give "6" (not "K"). Verified against TSMC test vectors:
#   0x13998DC75193D350 -> lot E6B156 / wafer 11 / x 99 / y 115
#   0x134191A75193D34C -> lot E6B155 / wafer 10 / x 100 / y 104
# CHAR1 'A' sits at index 3 (5-bit index, padded to len 32);
# CHAR2 '0' sits at index 14 (6-bit index, padded to len 64).
LOTNO_CHAR1 = "___" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "___"                  # len 32
LOTNO_CHAR2 = "_" * 14 + "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "_" * 14  # len 64

_FAB_NAMES = {1: "TSMC1", 2: "GF", 3: "UMC", 4: "FSL", 5: "HLMC", 6: "TSMC2", 7: "Samsung", 8: "GF2"}
_TSMC_CODES = (1, 6)   # only these have a verified lot / reserved-bits layout


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


def words_to_binary64(low: int, high: int) -> str:
    """64-char binary string from two 32-bit words (high = upper 32 bits).

    Each word is masked with 0xFFFFFFFF so negative signed 32-bit ints work.
    """
    return format(high & 0xFFFFFFFF, "032b") + format(low & 0xFFFFFFFF, "032b")


def _lot_char(efuse: str, lo: int, hi: int, table: str, pos: int, warnings: list[str]) -> str:
    idx = int(efuse[lo:hi], 2)
    ch = table[idx] if idx < len(table) else "_"
    if ch == "_":
        warnings.append(f"lot char {pos} out of range (code {idx})")
        return "?"
    return ch


def decode_chipid(value: str) -> dict:
    """Decode an EN-S0-CHIPID_R value string into origin fields.

    All keys are always present. Malformed input gives ``valid=False``, decoded
    fields None and ``warnings=["malformed CHIP ID"]``. Otherwise ``valid`` is
    True and problems are listed in ``warnings`` (see module docstring).
    """
    efuse = normalize_efuse(value)
    if efuse is None:
        return {
            "efuse_raw": value,
            "valid": False,
            "chipid_hex": None,
            "chipid_uint64": None,
            "origin_fab_code": None,
            "origin_fab": None,
            "origin_lot": None,
            "origin_wafer": None,
            "origin_x": None,
            "origin_y": None,
            "reserved_bits": None,
            "warnings": ["malformed CHIP ID"],
        }

    warnings: list[str] = []
    fab_code = int(efuse[0:4], 2)
    fab = _FAB_NAMES.get(fab_code, "UNKNOWN")
    tsmc = fab_code in _TSMC_CODES
    if fab == "UNKNOWN":
        warnings.append(f"undefined fab code {fab_code}")

    lot = None
    if tsmc:
        lot = _lot_char(efuse, 27, 32, LOTNO_CHAR1, 1, warnings)
        for i, lo in enumerate(range(32, 62, 6)):
            lot += _lot_char(efuse, lo, lo + 6, LOTNO_CHAR2, i + 2, warnings)
    else:
        warnings.append(f"unsupported fab decoder: {fab}")

    wafer = int(efuse[22:27], 2) - 3
    if not 1 <= wafer <= 25:
        warnings.append(f"wafer out of range: {wafer}")
    reserved = efuse[62:64]
    if tsmc and reserved != "00":
        warnings.append(f"reserved bits not 00: {reserved}")

    n = int(efuse, 2)
    return {
        "efuse_raw": efuse,
        "valid": True,
        "chipid_hex": f"0x{n:016X}",
        "chipid_uint64": n,
        "origin_fab_code": fab_code,
        "origin_fab": fab,
        "origin_lot": lot,
        "origin_wafer": wafer,
        "origin_x": int(efuse[13:22], 2),
        "origin_y": int(efuse[4:13], 2),
        "reserved_bits": reserved,
        "warnings": warnings,
    }
