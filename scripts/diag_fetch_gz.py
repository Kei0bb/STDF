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
import shutil
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


def walk_members(path: Path) -> list[dict]:
    """Walk every gzip member in the file, verifying each one independently.

    A .gz written by an appending logger is a *concatenation* of gzip members.
    `gzip.open()` stops at the first member whose trailer fails, so a whole-file
    check cannot say which member is bad or how much data follows it.
    """
    members: list[dict] = []
    size = path.stat().st_size
    offset = 0
    with open(path, "rb") as f:
        while offset < size:
            m: dict = {"index": len(members) + 1, "offset": offset}
            f.seek(offset)
            try:
                skip_gzip_header(f)
            except (ValueError, struct.error) as e:
                m.update(ok=False, state=f"no valid gzip header at offset {offset}: {e}")
                members.append(m)
                break

            dec = zlib.decompressobj(-15)
            crc = 0
            total = 0
            pos = f.tell()
            while True:
                chunk = f.read(1 << 20)
                if not chunk:
                    break
                pos += len(chunk)
                try:
                    out = dec.decompress(chunk)
                except zlib.error as e:
                    m.update(ok=False, out_size=total, state=f"deflate error: {e}")
                    break
                crc = zlib.crc32(out, crc)
                total += len(out)
                if dec.eof:
                    break
            if "state" in m:
                members.append(m)
                break

            out = dec.flush()
            crc = zlib.crc32(out, crc)
            total += len(out)
            m["out_size"] = total
            m["crc_calc"] = crc

            if not dec.eof:
                m.update(ok=False, state="member truncated (no end-of-stream marker)")
                members.append(m)
                break

            # The deflate stream ended inside the last chunk read; whatever
            # followed it is in unused_data, so the trailer starts here.
            trailer_at = pos - len(dec.unused_data)
            f.seek(trailer_at)
            raw = f.read(8)
            if len(raw) < 8:
                m.update(ok=False, state="member trailer missing (file ends early)")
                members.append(m)
                break
            crc_stored, isize_stored = struct.unpack("<II", raw)
            m["crc_stored"] = crc_stored
            m["isize_stored"] = isize_stored
            m["comp_size"] = (trailer_at + 8) - offset
            crc_ok = crc_stored == crc
            len_ok = isize_stored == (total & 0xFFFFFFFF)
            m["ok"] = crc_ok and len_ok
            if m["ok"]:
                m["state"] = "OK"
            elif len_ok:
                m["state"] = "CRC MISMATCH (length correct -> content altered, no bytes lost)"
            elif crc_ok:
                m["state"] = "ISIZE MISMATCH (crc correct)"
            else:
                m["state"] = "CRC + ISIZE MISMATCH (bytes lost or added)"
            members.append(m)
            offset = trailer_at + 8

    return members


def find_gzip_headers(path: Path, cap: int = 32) -> list[int]:
    """Offsets that look like the start of a gzip member.

    Distinguishes "one member that broke internally" (magic only at offset 0)
    from "two uploads spliced together" (a second plausible header further in).
    Requires magic + CM=8 (deflate) + no reserved FLG bits, which makes a false
    positive inside compressed data unlikely but not impossible.
    """
    hits: list[int] = []
    overlap = b""
    buf_start = 0  # absolute offset of overlap[0]
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            buf = overlap + chunk
            start = 0
            while (i := buf.find(b"\x1f\x8b\x08", start)) != -1:
                # A full match needs the FLG byte too, so a hit inside the
                # 3-byte overlap can never have been reported already.
                if i + 4 <= len(buf) and not (buf[i + 3] & 0xE0):
                    hits.append(buf_start + i)
                    if len(hits) >= cap:
                        return hits
                start = i + 1
            keep = min(3, len(buf))
            overlap = buf[len(buf) - keep:]
            buf_start += len(buf) - keep
    return hits


def print_members(members: list[dict]) -> None:
    print(f"    gzip members : {len(members)}")
    total_out = 0
    for m in members:
        total_out += m.get("out_size") or 0
        mark = "ok " if m.get("ok") else "BAD"
        line = (f"      [{mark}] #{m['index']} off={m['offset']} "
                f"comp={m.get('comp_size', '?')} out={m.get('out_size', '?')}")
        if "crc_stored" in m:
            line += (f" crc stored=0x{m['crc_stored']:08x} calc=0x{m['crc_calc']:08x}"
                     f" isize={m['isize_stored']}")
        print(line)
        if not m.get("ok"):
            print(f"            -> {m['state']}")
    bad = [m for m in members if not m.get("ok")]
    print(f"    total decompressed : {total_out} bytes, "
          f"{len(bad)} bad member(s) of {len(members)}")


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


def interpret(rec: dict, file_size: int) -> list[str]:
    """Turn the raw measurements into a plain statement of what is wrong."""
    out: list[str] = []
    members = rec.get("members") or []
    headers = rec.get("header_offsets") or []
    trailer = rec.get("trailer")
    consumed = sum(m.get("comp_size") or 0 for m in members)
    reached = sum(m.get("out_size") or 0 for m in members)

    spliced = [h for h in headers if h != 0]
    if len(members) == 1 and members[0].get("ok"):
        return ["single healthy member"]

    if spliced:
        out.append(f"a second gzip header exists at offset {spliced[0]} -> the file looks "
                   f"SPLICED (two uploads concatenated / a failed resume), not merely corrupt")
    elif headers == [0]:
        out.append("only one gzip header, at offset 0 -> this is a SINGLE-member file "
                   "that broke internally; it was never a concatenation of members")

    if consumed and consumed < file_size:
        pct = 100.0 * consumed / file_size
        out.append(f"the deflate stream stops after {consumed} of {file_size} bytes "
                   f"({pct:.1f}% of the file); corruption made the decoder see a false "
                   f"end-of-stream, and the remaining {file_size - consumed} bytes are "
                   f"unreachable")

    if trailer and headers == [0]:
        true_isize = trailer[1]
        if true_isize > reached:
            pct = 100.0 * reached / true_isize
            out.append(f"the file's real trailer says the payload should be {true_isize} "
                       f"bytes; only {reached} bytes ({pct:.1f}%) can be decompressed "
                       f"-> NOT salvageable, the file must be re-exported at the source")

    return out or ["no clear interpretation - inspect the member table above"]


SOURCE_BAD = ("SOURCE_BAD: identical bytes both times -> the file ON THE SERVER is bad. "
              "Retrying will never succeed; it must be skipped/quarantined.")
TRANSFER_BAD = ("TRANSFER_CORRUPTION: bytes differ between downloads. "
                "A download retry (with verification) would recover this file.")


def check_one(client: FTPClient | None, remote_path: str, tmpdir: Path,
              index: int, total: int, local_file: Path | None = None,
              keep_dir: Path | None = None) -> dict:
    """Verify one file. Downloads it unless `local_file` is given."""
    name = Path(remote_path).name
    rec: dict = {
        "index": index, "total": total, "name": name, "remote": remote_path,
        "ok": True, "verdict": None,
    }
    print(f"\n--- [{index}/{total}] {name}")
    print(f"    remote : {remote_path}")

    if local_file is not None:
        rsize = None
        local = local_file
        print("    SIZE   : (local file - no FTP)")
    else:
        rsize = remote_size(client, remote_path)
        print(f"    SIZE   : {rsize if rsize is not None else 'unsupported'}")
        local = raw_download(client, remote_path, tmpdir / f"a_{name}")
    rec["remote_size"] = rsize

    lsize = local.stat().st_size
    hash_a = sha256_of(local)
    rec["local_size"] = lsize
    rec["sha_a"] = hash_a
    print(f"    got    : {lsize} bytes  sha256={hash_a[:16]}")
    rec["size_mismatch"] = rsize is not None and rsize != lsize
    if rec["size_mismatch"]:
        print(f"    [!] SIZE mismatch: server {rsize} vs local {lsize} "
              f"(diff {lsize - rsize:+d}) -> transfer is truncated/padded")

    def discard(p: Path) -> None:
        if local_file is None:  # never delete a file the user pointed us at
            p.unlink(missing_ok=True)

    if not name.lower().endswith(".gz"):
        print("    gunzip : not gzip, skipped")
        discard(local)
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
        discard(local)
        return rec

    rec["ok"] = False

    # How much of the FIRST member survives if the CRC check is ignored?
    inflated, stream_state = raw_inflate_size(local)
    rec["inflated"] = inflated
    rec["stream_state"] = stream_state
    print(f"    ignoring CRC: {inflated} bytes recovered from member #1 ({stream_state})")

    # Per-member walk: a whole-file check stops at the first bad member and
    # cannot tell how many members there are or how much data follows.
    members = walk_members(local)
    rec["members"] = members
    print_members(members)

    headers = find_gzip_headers(local)
    rec["header_offsets"] = headers
    print(f"    gzip headers found at: {headers if headers else 'none'}")

    rec["interpretation"] = interpret(rec, lsize)
    for line in rec["interpretation"]:
        print(f"    => {line}")

    if keep_dir is not None and local_file is None:
        keep_dir.mkdir(parents=True, exist_ok=True)
        kept = keep_dir / name
        shutil.copy(local, kept)
        rec["kept"] = str(kept)
        print(f"    kept   : {kept}")

    if local_file is not None:
        rec["verdict"] = "LOCAL FILE - no second download, transfer not re-tested"
        print(f"    VERDICT: {rec['verdict']}")
        return rec

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

    discard(local)
    local_b.unlink(missing_ok=True)
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
            print(f"  last 8 bytes  : crc32=0x{crc:08x} isize={isize}  "
                  f"(final member's trailer - only meaningful if the file has 1 member)")
        if r.get("members"):
            members = r["members"]
            bad_m = [m for m in members if not m.get("ok")]
            out_total = sum(m.get("out_size") or 0 for m in members)
            print(f"  gzip members  : {len(members)}  ({len(bad_m)} bad)")
            print(f"  decompressed  : {out_total} bytes across all members")
            for m in members:
                mark = "ok " if m.get("ok") else "BAD"
                print(f"    [{mark}] #{m['index']} off={m['offset']} "
                      f"comp={m.get('comp_size', '?')} out={m.get('out_size', '?')}"
                      + (f" crc stored=0x{m['crc_stored']:08x} calc=0x{m['crc_calc']:08x}"
                         f" isize={m['isize_stored']}" if "crc_stored" in m else "")
                      + ("" if m.get("ok") else f"  <- {m['state']}"))
        if "header_offsets" in r:
            print(f"  gzip headers  : {r['header_offsets'] or 'none'}")
        for line in r.get("interpretation", []):
            print(f"  => {line}")
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
    ap.add_argument("--local", type=Path, default=None,
                    help="Inspect an already-downloaded .gz file; no FTP connection")
    ap.add_argument("--keep", type=Path, default=None,
                    help="Copy every failing download into this directory for re-inspection")
    args = ap.parse_args()

    if args.local:
        rec = check_one(None, str(args.local), Path("."), 1, 1, local_file=args.local)
        print_summary([rec], checked=1, pending=1, listed=1)
        return 1 if not rec["ok"] else 0

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
                results.append(check_one(client, remote_path, tmpdir, i, pending,
                                        keep_dir=args.keep))
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
