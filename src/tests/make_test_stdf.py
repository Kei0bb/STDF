"""Generate a minimal but realistic STDF V4 test file."""

import struct
import random
from pathlib import Path


def cn(s: str) -> bytes:
    """Encode STDF Cn string (1-byte length prefix + ASCII)."""
    b = s.encode("ascii")
    return struct.pack("B", len(b)) + b


def record(rec_typ: int, rec_sub: int, data: bytes) -> bytes:
    """Wrap data in a STDF record header (little endian)."""
    return struct.pack("<HBB", len(data), rec_typ, rec_sub) + data


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

        # WIR
        wir_data = struct.pack("<BBIx", 1, 0, start_t + wafer_idx * 3600) + cn(wafer_id)
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
