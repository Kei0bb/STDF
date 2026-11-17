"""Check C0/C1 CHIP ID association on real FT STDF files, without ingesting.

Read-only: parses each file with the production parser and prints a summary
(parts, candidates, C0/C1 resolved, unresolved, duplicates, malformed, complete
packages, chip_key coverage) plus the raw bytes of the first suite strings, so
the suite spelling (space vs underscore, 0 vs O) can be confirmed by byte
rather than by eye.

Usage (from the project root, on the Win11 data machine):

    uv run python scripts/diag_chipid.py var\\downloads\\...\\file.stdf.gz [more files]
    uv run python scripts/diag_chipid.py FILE --part 1     # also dump part 1's dies

Output contains counts, CHIP ID values and decoded fab/lot/wafer/x/y only — no
measurement data.
"""

import argparse
import gzip
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path

from stdf_platform.chipid import decode_chipid
from stdf_platform.parser import STDFParser


def _parse(path: Path):
    if path.suffix.lower() != ".gz":
        return STDFParser().parse(path)
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / path.stem
        with gzip.open(path, "rb") as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst)
        return STDFParser().parse(tmp)


def report(path: Path, dump_part: int | None) -> None:
    data = _parse(path)
    chips = [c for c in data.chip_ids]
    by_part: dict[str, list[dict]] = {}
    for c in chips:
        by_part.setdefault(c["part_id"], []).append(c)
    dies = Counter(c["die_id"] for c in chips)
    complete = sum(
        1 for p in data.parts
        if [c["die_id"] for c in by_part.get(p["part_id"], [])].count("C0") == 1
        and [c["die_id"] for c in by_part.get(p["part_id"], [])].count("C1") == 1
    )
    print(f"== {path.name}")
    print(f"parts total={len(data.parts)}")
    print(f"chipid candidates={len(chips)}")
    print(f"c0 resolved={dies['C0']}  c1 resolved={dies['C1']}  unresolved={dies['UNKNOWN']}")
    print(f"duplicates={sum(1 for c in chips if any('duplicate' in w for w in c['warnings']))}")
    def _parts_with(msg):
        return len({c["part_id"] for c in chips if msg in c["warnings"]})
    print(f"re-inserted packages (same C0+C1, later part wins): parts={_parts_with('package re-inserted in file')}")
    print(f"shared chip_key cleared (different C0/C1, no overlay): parts={_parts_with('CHIP ID shared by several parts in file')}")
    print(f"malformed={sum(1 for c in chips if not decode_chipid(c['efuse_raw'])['valid'])}")
    print(f"outside part={sum(1 for c in chips if not c['part_id'])}")
    print(f"packages complete={complete}")
    print(f"parts with chip_key={sum(1 for p in data.parts if p.get('chip_key'))}")
    print("associations:", dict(Counter(c["association"] for c in chips)))
    gdr_sites = Counter(c["site_num"] for c in chips)
    prr_sites = Counter(p["site_num"] for p in data.parts)
    print("GDR sites:", dict(gdr_sites), " PRR sites:", dict(prr_sites))
    if set(gdr_sites) - {None} - set(prr_sites):
        print("!! GDR site numbers differ from PRR site numbers — "
              "multi-site association is NOT trustworthy for this file")
    warn = Counter(w for c in chips for w in c["warnings"])
    if warn:
        print("warnings:", dict(warn))
    for name in sorted({c["suite_name"] for c in chips if c["suite_name"]}):
        print(f"suite {name!r} bytes={name.encode('ascii', 'replace').hex(' ')}")

    if dump_part is not None and 0 < dump_part <= len(data.parts):
        part = data.parts[dump_part - 1]
        print(f"-- part {dump_part} ({part['part_id']}, site {part['site_num']}, "
              f"chip_key {part.get('chip_key')!r})")
        for c in by_part.get(part["part_id"], []):
            d = decode_chipid(c["efuse_raw"])
            print(f"  {c['die_id']:7} {d['chipid_hex']} fab={d['origin_fab']} "
                  f"lot={d['origin_lot']} wafer={d['origin_wafer']} "
                  f"x={d['origin_x']} y={d['origin_y']} "
                  f"[{c['association']}] {c['warnings'] + d['warnings']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("files", nargs="+", type=Path)
    ap.add_argument("--part", type=int, default=None,
                    help="1-based part index to dump die by die")
    args = ap.parse_args()
    for f in args.files:
        report(f, args.part)
    return 0


if __name__ == "__main__":
    sys.exit(main())
