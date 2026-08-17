"""Diagnose `stdf fetch` gzip CRC failures (BadGzipFile: CRC check failed).

Read-only: downloads candidate files to a temp directory, verifies them, and
prints sizes / hashes / verdicts. It never writes to downloads/ and never
touches sync_history.json.

Usage (from the project root, on the machine that runs the nightly fetch):

    uv run python scripts/diag_fetch_gz.py                 # scan all pending files
    uv run python scripts/diag_fetch_gz.py --limit 10      # stop after 10 files
    uv run python scripts/diag_fetch_gz.py --remote /a/b/x.stdf.gz   # one file

For every file that fails gzip verification the script downloads it a *second*
time and compares SHA-256:

    same hash twice  -> the file on the FTP server is itself bad (bad trailer /
                        corrupt source). Re-downloading will never help.
    hash differs     -> the transfer is corrupting bytes. A retry would help.

Output contains file names, byte counts and hashes only - no measurement data.
"""

import argparse
import gzip
import hashlib
import struct
import sys
import tempfile
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from stdf_platform.config import Config  # noqa: E402
from stdf_platform.ftp_client import FTPClient  # noqa: E402
from stdf_platform.sync_manager import SyncManager  # noqa: E402


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def gz_trailer(path: Path) -> tuple[int, int] | None:
    """Return (stored_crc32, stored_isize) from the last 8 bytes of a gzip file."""
    size = path.stat().st_size
    if size < 8:
        return None
    with open(path, "rb") as f:
        f.seek(-8, 2)
        crc32, isize = struct.unpack("<II", f.read(8))
    return crc32, isize


def verify_gz(path: Path) -> tuple[str, int]:
    """Decompress fully. Returns (verdict, decompressed_bytes)."""
    total = 0
    try:
        with gzip.open(path, "rb") as f:
            while chunk := f.read(1 << 20):
                total += len(chunk)
        return "OK", total
    except gzip.BadGzipFile as e:
        return f"BadGzipFile: {e}", total
    except EOFError as e:
        return f"EOFError (truncated): {e}", total
    except OSError as e:
        return f"OSError: {e}", total


def skip_gzip_header(f) -> None:
    """Advance a binary file object past the gzip member header."""
    head = f.read(10)
    if len(head) < 10 or head[:2] != b"\x1f\x8b":
        raise ValueError("not a gzip file")
    flg = head[3]
    if flg & 0x04:  # FEXTRA
        xlen = struct.unpack("<H", f.read(2))[0]
        f.read(xlen)
    for bit in (0x08, 0x10):  # FNAME, FCOMMENT - NUL-terminated
        if flg & bit:
            while f.read(1) not in (b"\x00", b""):
                pass
    if flg & 0x02:  # FHCRC
        f.read(2)


def raw_inflate_size(path: Path) -> tuple[int, str]:
    """Decompress as raw deflate, bypassing the gzip CRC/ISIZE trailer entirely.

    Tells whether the deflate stream itself is intact: if this length matches
    the trailer's ISIZE, the payload is the right length and only its *content*
    (or the stored CRC) is wrong - i.e. a localised bit flip, not lost data.
    """
    total = 0
    dec = zlib.decompressobj(-15)  # raw deflate: no gzip wrapper, no CRC check
    try:
        with open(path, "rb") as f:
            skip_gzip_header(f)
            while chunk := f.read(1 << 20):
                total += len(dec.decompress(chunk))
                if dec.eof:
                    break
            total += len(dec.flush())
        if not dec.eof:
            return total, "deflate stream ends early (truncated)"
        return total, "deflate stream intact"
    except (zlib.error, ValueError) as e:
        return total, f"deflate stream broken: {e}"


def raw_download(client: FTPClient, remote_path: str, dest: Path) -> Path:
    with open(dest, "wb") as f:
        client._ftp.retrbinary(f"RETR {remote_path}", f.write)
    return dest


def remote_size(client: FTPClient, remote_path: str) -> int | None:
    try:
        client._ftp.voidcmd("TYPE I")
        return client._ftp.size(remote_path)
    except Exception:
        return None


SOURCE_BAD = ("SOURCE_BAD: identical bytes both times -> the file ON THE SERVER is bad. "
              "Retrying will never succeed; it must be skipped/quarantined.")
TRANSFER_BAD = ("TRANSFER_CORRUPTION: bytes differ between downloads. "
                "A download retry (with verification) would recover this file.")


def check_one(client: FTPClient, remote_path: str, tmpdir: Path, index: int, total: int) -> dict:
    """Download + verify one remote file. Returns a result record."""
    name = Path(remote_path).name
    rec: dict = {
        "index": index, "total": total, "name": name, "remote": remote_path,
        "ok": True, "verdict": None,
    }
    print(f"\n--- [{index}/{total}] {name}")
    print(f"    remote : {remote_path}")

    rsize = remote_size(client, remote_path)
    rec["remote_size"] = rsize
    print(f"    SIZE   : {rsize if rsize is not None else 'unsupported'}")

    local = raw_download(client, remote_path, tmpdir / f"a_{name}")
    lsize = local.stat().st_size
    hash_a = sha256_of(local)
    rec["local_size"] = lsize
    rec["sha_a"] = hash_a
    print(f"    got    : {lsize} bytes  sha256={hash_a[:16]}")
    rec["size_mismatch"] = rsize is not None and rsize != lsize
    if rec["size_mismatch"]:
        print(f"    [!] SIZE mismatch: server {rsize} vs local {lsize} "
              f"(diff {lsize - rsize:+d}) -> transfer is truncated/padded")

    if not name.lower().endswith(".gz"):
        print("    gunzip : not gzip, skipped")
        local.unlink()
        return rec

    trailer = gz_trailer(local)
    verdict, nbytes = verify_gz(local)
    rec["gunzip"] = verdict
    rec["output_size"] = nbytes
    rec["trailer"] = trailer
    print(f"    gunzip : {verdict}")
    print(f"    output : {nbytes} bytes")
    if trailer:
        crc, isize = trailer
        print(f"    trailer: crc32=0x{crc:08x} isize={isize} "
              f"(decompressed mod 2^32 = {nbytes & 0xFFFFFFFF})")

    if verdict == "OK":
        local.unlink()
        return rec

    rec["ok"] = False

    # How much of the payload actually survives if the CRC check is ignored?
    inflated, stream_state = raw_inflate_size(local)
    rec["inflated"] = inflated
    rec["stream_state"] = stream_state
    print(f"    ignoring CRC: {inflated} bytes recovered ({stream_state})")
    if stream_state != "deflate stream intact":
        print("    -> the stream does not reach its end marker, so the last 8 bytes "
              "are not a real trailer: bytes are MISSING from the file")
    elif trailer and inflated == trailer[1]:
        print("    -> length matches ISIZE: payload is complete, only its content "
              "(or the stored CRC) is wrong")
    elif trailer:
        print(f"    -> length does NOT match ISIZE ({trailer[1]}): data is missing")

    # Second download to separate source corruption from transport corruption.
    print("    [!] failed - re-downloading to compare...")
    local_b = raw_download(client, remote_path, tmpdir / f"b_{name}")
    hash_b = sha256_of(local_b)
    rec["sha_b"] = hash_b
    rec["local_size_b"] = local_b.stat().st_size
    print(f"    2nd    : {rec['local_size_b']} bytes  sha256={hash_b[:16]}")
    if hash_a == hash_b:
        rec["verdict"] = SOURCE_BAD
    else:
        rec["verdict"] = TRANSFER_BAD
        verdict_b, nbytes_b = verify_gz(local_b)
        rec["gunzip_b"] = verdict_b
        rec["output_size_b"] = nbytes_b
        print(f"    2nd gunzip: {verdict_b} ({nbytes_b} bytes)")
    print(f"    VERDICT: {rec['verdict']}")

    local.unlink()
    local_b.unlink()
    return rec


def print_summary(results: list[dict], checked: int, pending: int, listed: int) -> None:
    """Copy-pasteable report of every bad file and how much the run is blocking."""
    bad = [r for r in results if not r["ok"]]
    print("\n" + "=" * 72)
    print("=== SUMMARY (copy-paste this) ===")
    print("=" * 72)
    print(f"listed on server : {listed}")
    print(f"pending (not yet in sync_history) : {pending}")
    print(f"checked this run : {checked}")
    print(f"bad              : {len(bad)}")

    if not bad:
        print("\nAll checked files verified OK.")
        return

    for r in bad:
        print("\n" + "-" * 72)
        print(f"BAD  [{r['index']}/{r['total']}]  {r['name']}")
        print(f"  remote path   : {r['remote']}")
        print(f"  server SIZE   : {r.get('remote_size')}")
        print(f"  downloaded #1 : {r.get('local_size')} bytes  sha256={str(r.get('sha_a'))[:16]}")
        if "sha_b" in r:
            print(f"  downloaded #2 : {r.get('local_size_b')} bytes  sha256={str(r.get('sha_b'))[:16]}")
            print(f"  same bytes    : {r.get('sha_a') == r.get('sha_b')}")
        print(f"  SIZE mismatch : {r.get('size_mismatch')}")
        print(f"  gunzip        : {r.get('gunzip')}")
        print(f"  output        : {r.get('output_size')} bytes (yielded before the error)")
        if "inflated" in r:
            print(f"  ignoring CRC  : {r['inflated']} bytes ({r.get('stream_state')})")
        if r.get("trailer"):
            crc, isize = r["trailer"]
            intact = r.get("stream_state") == "deflate stream intact"
            print(f"  trailer       : crc32=0x{crc:08x} isize={isize}"
                  f"{'' if intact else '  (not a real trailer - stream truncated)'}")
            if intact and "inflated" in r:
                print(f"  length match  : {r['inflated'] == isize}  "
                      f"(True = payload complete, only content/CRC wrong; "
                      f"False = data missing)")
        if r.get("gunzip_b"):
            print(f"  2nd gunzip    : {r['gunzip_b']} ({r.get('output_size_b')} bytes)")
        print(f"  VERDICT       : {r['verdict']}")
        blocked = r["total"] - r["index"]
        print(f"  blocked after this file (same order as `stdf fetch`) : {blocked} file(s)")

    print("\n" + "-" * 72)
    first = min(bad, key=lambda r: r["index"])
    print(f"First failure is candidate #{first['index']} of {first['total']}. "
          f"`stdf fetch` aborts there, so the {first['total'] - first['index']} "
          f"candidate(s) after it are never downloaded.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", type=Path, default=None)
    ap.add_argument("--remote", default=None, help="Check this single remote path")
    ap.add_argument("--limit", type=int, default=None, help="Max files to check")
    ap.add_argument("--all", action="store_true",
                    help="Include files already recorded in sync_history.json")
    args = ap.parse_args()

    config = Config.load(args.config)
    sync_manager = SyncManager(config.storage.data_dir / "sync_history.json")

    with FTPClient(config.ftp) as client:
        tmpdir = Path(tempfile.mkdtemp(prefix="stdf_diag_"))
        print(f"temp dir: {tmpdir}")

        if args.remote:
            candidates = [args.remote]
            listed = 1
        else:
            if config.filters:
                products = [f.product for f in config.filters]
                test_types = sorted({tt for f in config.filters for tt in f.test_types})
            else:
                products, test_types = None, ["CP", "FT"]

            files = list(client.list_stdf_files(products=products, test_types=test_types))
            if config.filters:
                files = [x for x in files if config.should_fetch(x[1], x[2])]
            if config.exclude:
                files = [x for x in files if not config.should_exclude(x[3])]
            candidates = [x[0] for x in files]
            listed = len(candidates)
            if not args.all:
                candidates = [p for p in candidates if not sync_manager.is_downloaded(p)]
            print(f"candidates: {len(candidates)} pending / {listed} listed")

        if args.limit:
            candidates = candidates[: args.limit]

        pending = len(candidates)
        results = []
        for i, remote_path in enumerate(candidates, start=1):
            try:
                results.append(check_one(client, remote_path, tmpdir, i, pending))
            except Exception as e:  # keep going - that is the whole point
                print(f"    [!] {type(e).__name__}: {e}")
                results.append({
                    "index": i, "total": pending, "name": Path(remote_path).name,
                    "remote": remote_path, "ok": False,
                    "verdict": f"ERROR during check: {type(e).__name__}: {e}",
                })

    print_summary(results, checked=len(results), pending=pending, listed=listed)
    return 1 if any(not r["ok"] for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
