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


def check_one(client: FTPClient, remote_path: str, tmpdir: Path) -> bool:
    """Download + verify one remote file. Returns True if it is healthy."""
    name = Path(remote_path).name
    print(f"\n--- {name}")
    print(f"    remote : {remote_path}")

    rsize = remote_size(client, remote_path)
    print(f"    SIZE   : {rsize if rsize is not None else 'unsupported'}")

    local = raw_download(client, remote_path, tmpdir / f"a_{name}")
    lsize = local.stat().st_size
    hash_a = sha256_of(local)
    print(f"    got    : {lsize} bytes  sha256={hash_a[:16]}")
    if rsize is not None and rsize != lsize:
        print(f"    [!] SIZE mismatch: server {rsize} vs local {lsize} "
              f"(diff {lsize - rsize:+d}) -> transfer is truncated/padded")

    if not name.lower().endswith(".gz"):
        print("    verdict: not gzip, skipped")
        return True

    trailer = gz_trailer(local)
    verdict, nbytes = verify_gz(local)
    print(f"    gunzip : {verdict}")
    print(f"    output : {nbytes} bytes")
    if trailer:
        crc, isize = trailer
        print(f"    trailer: crc32=0x{crc:08x} isize={isize} "
              f"(decompressed mod 2^32 = {nbytes & 0xFFFFFFFF})")

    if verdict == "OK":
        local.unlink()
        return True

    # Second download to separate source corruption from transport corruption.
    print("    [!] failed - re-downloading to compare...")
    local_b = raw_download(client, remote_path, tmpdir / f"b_{name}")
    hash_b = sha256_of(local_b)
    print(f"    2nd    : {local_b.stat().st_size} bytes  sha256={hash_b[:16]}")
    if hash_a == hash_b:
        print("    VERDICT: identical bytes both times -> the file ON THE SERVER "
              "is bad. Retrying will never succeed; it must be skipped/quarantined.")
    else:
        print("    VERDICT: bytes differ between downloads -> TRANSFER corruption. "
              "A download retry (with verification) would recover this file.")
        verdict_b, nbytes_b = verify_gz(local_b)
        print(f"    2nd gunzip: {verdict_b} ({nbytes_b} bytes)")

    local.unlink()
    local_b.unlink()
    return False


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
            total = len(candidates)
            if not args.all:
                candidates = [p for p in candidates if not sync_manager.is_downloaded(p)]
            print(f"candidates: {len(candidates)} pending / {total} listed")

        if args.limit:
            candidates = candidates[: args.limit]

        bad = []
        for remote_path in candidates:
            try:
                if not check_one(client, remote_path, tmpdir):
                    bad.append(remote_path)
            except Exception as e:  # keep going - that is the whole point
                print(f"    [!] {type(e).__name__}: {e}")
                bad.append(remote_path)

    print(f"\n=== checked {len(candidates)} file(s), {len(bad)} bad ===")
    for p in bad:
        print(f"  BAD  {p}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
