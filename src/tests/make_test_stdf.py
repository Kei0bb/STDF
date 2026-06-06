"""Generate a minimal but realistic STDF V4 test file."""

import struct
import random
from pathlib import Path

from stdf_platform.chipid import CHIPID_KEY  # canonical digit-zero key


def cn(s: str) -> bytes:
    """Encode STDF Cn string (1-byte length prefix + ASCII)."""
    b = s.encode("ascii")
    return struct.pack("B", len(b)) + b


def record(rec_typ: int, rec_sub: int, data: bytes) -> bytes:
    """Wrap data in a STDF record header (little endian)."""
    return struct.pack("<HBB", len(data), rec_typ, rec_sub) + data


LOTNO_CHAR1 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ_"
LOTNO_CHAR2 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def encode_chipid(fab_code: int, lot6: str, wafer: int, x: int, y: int) -> str:
    """Encode an EN-SO-CHIPID_R value string (inverse of chipid.decode_chipid)."""
    assert len(lot6) == 6
    bits = (
        f"{fab_code:04b}"
        + f"{y:09b}"
        + f"{x:09b}"
        + f"{wafer + 3:05b}"
        + f"{LOTNO_CHAR1.index(lot6[0]):05b}"
        + "".join(f"{LOTNO_CHAR2.index(c):06b}" for c in lot6[1:])
        + "00"  # reserved
    )
    assert len(bits) == 64, len(bits)
    return "0b" + bits


def gdr_chipid(efuse: str, key: str = CHIPID_KEY) -> bytes:
    """Build a GDR record carrying one ChipID key/value pair (Cn, Cn).

    Defaults to the canonical digit-zero key ("EN-S0-CHIPID_R") that real STDF
    files use.
    """
    body = struct.pack("<H", 2)                  # FLD_CNT
    body += struct.pack("B", 10) + cn(key)       # C*n key
    body += struct.pack("B", 10) + cn(efuse)     # C*n value
    return record(50, 10, body)


def make_ft_stdf(path: Path, lot_id: str, parts: int = 8, fail_part_ids=None):
    """Generate an FT (Final Test) STDF with 2-die chiplet ChipID GDRs.

    No WIR (FT has no wafer). Each part (package) emits two EN-SO-CHIPID_R GDRs
    (die0/die1) and a PRR carrying a unique PART_TXT 2D barcode. Returns the list
    of expected ChipID dicts so tests can assert against the parsed/stored output.
    """
    fail_part_ids = set(fail_part_ids or [])
    buf = bytearray()
    start_t = 1700000000

    buf += record(0, 10, struct.pack("BB", 2, 4))  # FAR

    mir_data = (
        struct.pack("<IIB", start_t, start_t, 1)
        + struct.pack("<BBBHB", ord(" "), ord(" "), ord(" "), 0, ord(" "))
        + cn(lot_id) + cn("CHIPLET2D") + cn("TESTER01") + cn("J750")
        + cn("FT_TEST") + cn("Rev01") + cn("") + cn("OPE01") + cn("") + cn("")
        + cn("FT1")  # TEST_COD -> sub_process FT1
    )
    buf += record(1, 10, mir_data)

    expected = []
    for i in range(parts):
        buf += record(5, 10, struct.pack("BB", 1, 1))  # PIR

        # One PTR so test_data is populated for the package
        passed = i not in fail_part_ids
        test_flg = 0x00 if passed else 0x80
        ptr = (
            struct.pack("<IBBBB", 5001, 1, 1, test_flg, 0x00)
            + struct.pack("<f", 1.0)
            + cn("FT_FUNC") + cn("") + struct.pack("B", 0x00)
            + struct.pack("<bbb", 0, 0, 0)
            + struct.pack("<f", 0.0) + struct.pack("<f", 2.0) + cn("V")
        )
        buf += record(15, 10, ptr)

        # Two dies per package, each from a different origin location
        barcode = f"2D-{lot_id}-{i:04d}"
        efuse0 = encode_chipid(1, "HKPFJK", wafer=11, x=10 + i, y=20 + i)   # TSMC1
        efuse1 = encode_chipid(6, "ABCDEF", wafer=7, x=100 + i, y=200 + i)  # TSMC2
        buf += gdr_chipid(efuse0)
        buf += gdr_chipid(efuse1)
        expected.append({"part_txt": barcode, "occ": 0, "efuse": efuse0})
        expected.append({"part_txt": barcode, "occ": 1, "efuse": efuse1})

        # PRR with PART_ID + PART_TXT (2D barcode), no wafer coords
        part_flg = 0x00 if passed else 0x08
        hard_bin = 1 if passed else 2
        soft_bin = 1 if passed else 3
        prr = struct.pack("<BBBHHHhhI", 1, 1, part_flg, 1, hard_bin, soft_bin, -32768, -32768, 0)
        prr += cn(f"UNIT{i:04d}") + cn(barcode)
        buf += record(5, 20, prr)

    buf += record(1, 20, struct.pack("<I", start_t + 3600))  # MRR

    path.write_bytes(bytes(buf))
    print(f"Created {path} ({len(buf):,} bytes, FT {parts} packages × 2 dies)")
    return expected


def make_stdf(path: Path, lot_id: str, num_wafers: int = 3, parts_per_wafer: int = 50):
    buf = bytearray()

    # FAR - File Attributes Record (cpu_type=2 = little endian, stdf_ver=4)
    buf += record(0, 10, struct.pack("BB", 2, 4))

    # MIR - Master Information Record
    start_t = 1700000000
    mir_data = (
        struct.pack("<IIB", start_t, start_t, 1)  # setup_t, start_t, stat_num
        + struct.pack("<BBBHB", ord(" "), ord(" "), ord(" "), 0, ord(" "))  # mode_cod, rtst_cod, prot_cod, burn_tim, cmod_cod
        + cn(lot_id)          # LOT_ID
        + cn("SCT101A")       # PART_TYP
        + cn("TESTER01")      # NODE_NAM
        + cn("J750")          # TSTR_TYP
        + cn("CP_TEST")       # JOB_NAM
        + cn("Rev01")         # JOB_REV
        + cn("")              # SBLOT_ID
        + cn("OPE01")         # OPER_NAM
        + cn("")              # EXEC_TYP
        + cn("")              # EXEC_VER
        + cn("CP11")          # TEST_COD  ← sub_process
    )
    buf += record(1, 10, mir_data)

    test_defs = [
        (1001, "Vth_N",  0.3,  0.8,  "V"),
        (1002, "Idsat_N", 200.0, 400.0, "uA"),
        (1003, "Vth_P",  -0.8, -0.3,  "V"),
        (1004, "Leakage", 0.0,  10.0,  "nA"),
        (1005, "Res",     90.0, 110.0, "Ohm"),
    ]

    for wafer_idx in range(num_wafers):
        wafer_id = f"W{wafer_idx + 1:02d}"

        # WIR — HEAD_NUM U1, SITE_GRP U1, START_T U4, WAFER_ID Cn (no pad)
        wir_data = struct.pack("<BBI", 1, 0, start_t + wafer_idx * 3600) + cn(wafer_id)
        buf += record(2, 10, wir_data)

        good = 0
        for part_idx in range(parts_per_wafer):
            x = (part_idx % 10) - 5
            y = (part_idx // 10) - 2

            # PIR
            buf += record(5, 10, struct.pack("BB", 1, 1))

            # PTR records
            all_pass = True
            for test_num, test_name, lo, hi, units in test_defs:
                val = random.uniform(lo * 0.95, hi * 1.05)
                passed = lo <= val <= hi
                if not passed:
                    all_pass = False

                test_flg = 0x00 if passed else 0x80
                ptr_data = (
                    struct.pack("<IBBBB", test_num, 1, 1, test_flg, 0x00)
                    + struct.pack("<f", val)
                    + cn(test_name)
                    + cn("")               # alarm_id
                    + struct.pack("B", 0x00)  # opt_flag
                    + struct.pack("<b", 0)    # res_scal
                    + struct.pack("<b", 0)    # llm_scal
                    + struct.pack("<b", 0)    # hlm_scal
                    + struct.pack("<f", lo)
                    + struct.pack("<f", hi)
                    + cn(units)
                )
                buf += record(15, 10, ptr_data)

            hard_bin = 1 if all_pass else 2
            soft_bin = 1 if all_pass else (2 if random.random() > 0.5 else 3)
            if all_pass:
                good += 1

            # PRR
            part_flg = 0x00 if all_pass else 0x08
            prr_data = struct.pack(
                "<BBBHHHhh",
                1, 1, part_flg, len(test_defs), hard_bin, soft_bin, x, y,
            )
            buf += record(5, 20, prr_data)

        # HBR
        for bin_num, name, pf, cnt in [(1, "PASS", "P", good), (2, "FAIL", "F", parts_per_wafer - good)]:
            hbr = struct.pack("<BBHIBx", 1, 0, bin_num, cnt, ord(pf)) + cn(name)
            buf += record(1, 40, hbr)

        # SBR
        for bin_num, name, pf in [(1, "BIN1_PASS", "P"), (2, "BIN2_FAIL_A", "F"), (3, "BIN3_FAIL_B", "F")]:
            cnt = good if bin_num == 1 else (parts_per_wafer - good) // 2
            sbr = struct.pack("<BBHIBx", 1, 0, bin_num, cnt, ord(pf)) + cn(name)
            buf += record(1, 50, sbr)

        # WRR
        wrr_data = struct.pack(
            "<BBIIIII",
            1, 0,
            start_t + wafer_idx * 3600 + 1800,
            parts_per_wafer, 0, 0, good,
        )
        buf += record(2, 20, wrr_data)

    # MRR
    buf += record(1, 20, struct.pack("<I", start_t + num_wafers * 3600))

    path.write_bytes(bytes(buf))
    print(f"Created {path} ({len(buf):,} bytes, {num_wafers} wafers × {parts_per_wafer} parts)")


if __name__ == "__main__":
    out = Path("test_data")
    out.mkdir(exist_ok=True)

    # Small file
    make_stdf(out / "LOT001.stdf", "LOT001", num_wafers=3, parts_per_wafer=50)

    # Larger file
    make_stdf(out / "LOT002.stdf", "LOT002", num_wafers=5, parts_per_wafer=200)
    make_stdf(out / "LOT003.stdf", "LOT003", num_wafers=5, parts_per_wafer=200)
    make_stdf(out / "LOT004.stdf", "LOT004", num_wafers=5, parts_per_wafer=200)
    make_stdf(out / "LOT005.stdf", "LOT005", num_wafers=5, parts_per_wafer=200)

    # FT chiplet file with EN-SO-CHIPID_R GDRs
    make_ft_stdf(out / "FTLOT01.stdf", "FTLOT01", parts=8)
