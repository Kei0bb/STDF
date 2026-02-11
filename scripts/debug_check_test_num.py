#!/usr/bin/env python3
"""Debug script to check if a specific test_num exists in STDF file."""

import sys
import struct
import gzip
from pathlib import Path

def check_test_num(file_path: Path, target_test_num: int = 210000):
    """Check if target_test_num exists in STDF file."""
    
    open_func = gzip.open if str(file_path).endswith('.gz') else open
    
    REC_PTR = (15, 10)
    REC_MPR = (15, 15)
    REC_FTR = (15, 20)
    
    found_count = 0
    total_ptr = 0
    endian = "<"  # Default little endian
    
    with open_func(file_path, "rb") as f:
        while True:
            header = f.read(4)
            if len(header) < 4:
                break
            
            rec_len = struct.unpack(endian + "H", header[0:2])[0]
            rec_typ = header[2]
            rec_sub = header[3]
            
            start_pos = f.tell()
            
            # Check for FAR to determine endianness
            if rec_typ == 0 and rec_sub == 10:
                cpu_type = struct.unpack("B", f.read(1))[0]
                if cpu_type == 2:
                    endian = ">"
                f.read(rec_len - 1)
                continue
            
            # Check PTR, MPR, FTR records
            if (rec_typ, rec_sub) in [REC_PTR, REC_MPR, REC_FTR]:
                try:
                    test_num = struct.unpack(endian + "I", f.read(4))[0]
                    
                    if (rec_typ, rec_sub) == REC_PTR:
                        total_ptr += 1
                    
                    if test_num == target_test_num:
                        found_count += 1
                        rec_name = {REC_PTR: "PTR", REC_MPR: "MPR", REC_FTR: "FTR"}[(rec_typ, rec_sub)]
                        print(f"Found test_num {target_test_num} in {rec_name} at position {start_pos}")
                except:
                    pass
            
            # Skip remaining record data
            consumed = f.tell() - start_pos
            if consumed < rec_len:
                f.read(rec_len - consumed)
    
    print(f"\nTotal PTR records: {total_ptr}")
    print(f"Found test_num {target_test_num}: {found_count} times")
    return found_count

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_check_test_num.py <stdf_file> [test_num]")
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    target = int(sys.argv[2]) if len(sys.argv) > 2 else 210000
    
    check_test_num(file_path, target)
