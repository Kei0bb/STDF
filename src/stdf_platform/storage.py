"""Parquet storage for STDF data."""

import logging
import os
import re
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .parser import SDR_FIELDS, STDFData
from .config import StorageConfig
from .chipid import decode_chipid

logger = logging.getLogger(__name__)


# Default timestamp for invalid values (compatible with Parquet viewers)
_DEFAULT_DATETIME = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _unix_to_datetime(unix_ts: int) -> datetime:
    """Convert Unix timestamp to datetime. Returns default for invalid values."""
    if unix_ts is None or unix_ts <= 0:
        return _DEFAULT_DATETIME
    try:
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    except (OSError, ValueError):
        return _DEFAULT_DATETIME


# PyArrow schemas for each table
#
# `lots` is NOT written to Parquet — it used to be one row per lot, rewritten
# in place on every file (`data/lots/.../lot_id={lot}/data.parquet`), which
# meant only the LAST ingested file's MIR survived for a lot. That info now
# lives in `runs` at (file x wafer identity) granularity; `mounts.py` derives
# a `lots` VIEW from it by aggregation. See docs/schema.md (runs / lots)
# for the full column reference and the changed `lots` semantics.
RUNS_SCHEMA = pa.schema([
    ("lot_id", pa.string()),
    ("wafer_id", pa.string()),        # '' for FT (no WIR)
    ("product", pa.string()),
    ("test_category", pa.string()),  # CP, FT, OTHER
    ("sub_process", pa.string()),    # CP1, FT2, etc. (from STDF MIR.TEST_COD)
    ("retest_num", pa.int64()),       # from the same wafer_retest_map as parts/test_data
    ("part_type", pa.string()),
    ("job_name", pa.string()),
    ("job_rev", pa.string()),
    ("start_time", pa.timestamp("ms", tz="UTC")),
    ("finish_time", pa.timestamp("ms", tz="UTC")),
    ("tester_type", pa.string()),
    ("operator", pa.string()),
    # Equipment identity. node_name is MIR.NODE_NAM (the tester unit, vs
    # tester_type = its model); the rest come from SDR (1/80), collapsed to a
    # sorted comma-joined set when a file carries several SDRs — see
    # parser.SDR_FIELDS and docs/schema.md (runs).
    ("node_name", pa.string()),        # MIR.NODE_NAM
    ("handler_type", pa.string()),     # SDR.HAND_TYP
    ("handler_id", pa.string()),       # SDR.HAND_ID
    ("probe_card_type", pa.string()),  # SDR.CARD_TYP
    ("probe_card_id", pa.string()),    # SDR.CARD_ID
    ("loadboard_type", pa.string()),   # SDR.LOAD_TYP
    ("loadboard_id", pa.string()),     # SDR.LOAD_ID
    ("socket_type", pa.string()),      # SDR.CONT_TYP
    ("socket_id", pa.string()),        # SDR.CONT_ID
    # WRR (CP wafers; NULL for FT) — tester-reported counts, summed over heads
    ("wafer_start_time", pa.timestamp("ms", tz="UTC")),
    ("wafer_finish_time", pa.timestamp("ms", tz="UTC")),
    ("part_count", pa.int64()),
    ("good_count", pa.int64()),
    ("rtst_count", pa.int64()),
    ("abrt_count", pa.int64()),
    ("test_rev", pa.string()),       # Rev04 等（ファイル名から）
    ("source_file", pa.string()),    # 元ファイル名
])

PARTS_SCHEMA = pa.schema([
    ("part_id", pa.string()),    # per-file synthetic id — never join on it
    ("part_seq", pa.int64()),    # PIR order within the file
    ("part_txt", pa.string()),   # PRR.PART_TXT
    ("part_serial", pa.string()),  # PRR.PART_ID
    ("chip_key", pa.string()),   # FT package identity from eFuse (C0, else C1); '' = none
    ("lot_id", pa.string()),
    ("wafer_id", pa.string()),
    ("head_num", pa.int64()),
    ("site_num", pa.int64()),
    ("x_coord", pa.int64()),
    ("y_coord", pa.int64()),
    ("hard_bin", pa.int64()),
    ("soft_bin", pa.int64()),
    ("passed", pa.bool_()),      # NULL when PART_FLG says pass/fail is invalid
    ("part_flg", pa.int64()),    # raw PRR.PART_FLG
    ("test_count", pa.int64()),
    ("test_time", pa.int64()),
    ("retest_num", pa.int64()),
    ("retest_flag", pa.int64()),  # 0 = newest occurrence of this die (see _recompute_flags)
    # 2-die package CHIP ID (EN-S0-CHIPID_R + CHIPID NRD0 C0/C1 suite GDRs)
    ("c0_chipid_hex", pa.string()),
    ("c0_origin_fab", pa.string()),
    ("c0_origin_lot", pa.string()),
    ("c0_origin_wafer", pa.int64()),
    ("c0_origin_x", pa.int64()),
    ("c0_origin_y", pa.int64()),
    ("c1_chipid_hex", pa.string()),
    ("c1_origin_fab", pa.string()),
    ("c1_origin_lot", pa.string()),
    ("c1_origin_wafer", pa.int64()),
    ("c1_origin_x", pa.int64()),
    ("c1_origin_y", pa.int64()),
    ("chip_status", pa.string()),    # NULL (no CHIP ID) / complete / ambiguous / missing C0|C1
    ("chip_warnings", pa.string()),  # '; '-joined, '' when clean
])

# Unified test data schema (merged tests + test_results)
TEST_DATA_SCHEMA = pa.schema([
    # Part identification
    ("lot_id", pa.string()),
    ("wafer_id", pa.string()),
    ("part_id", pa.string()),
    ("part_seq", pa.int64()),    # = parts.part_seq (flag ordering within a file)
    ("part_txt", pa.string()),   # PRR.PART_TXT
    ("part_serial", pa.string()),  # PRR.PART_ID
    ("chip_key", pa.string()),   # FT package identity from eFuse — see PARTS_SCHEMA
    ("x_coord", pa.int64()),
    ("y_coord", pa.int64()),
    # Test identification
    ("test_num", pa.int64()),
    ("test_name", pa.string()),
    ("rec_type", pa.string()),  # PTR, MPR, FTR
    # Test parameters
    ("lo_limit", pa.float64()),
    ("hi_limit", pa.float64()),
    ("units", pa.string()),
    # Test result
    ("result", pa.float64()),
    ("passed", pa.string()),      # "P" / "F"; NULL = not executed or pass/fail invalid
    ("test_flg", pa.int64()),     # raw TEST_FLG (0x02 result invalid, 0x10 not run, 0x20 aborted)
    ("retest_num", pa.int64()),
    # MPR per-pin fields (NULL for PTR/FTR)
    ("pin_num", pa.int64()),
    ("pin_name", pa.string()),
    # Ingest-time dedup metadata (flag = rank of the die occurrence, see _recompute_flags)
    ("exec_seq", pa.int64()),     # 0-based occurrence order of the key within this run
    ("retest_flag", pa.int64()),  # recency rank across retest runs; 0 = newest
])



def _get_test_category(test_type: str) -> str:
    """Extract test category from test type (CP11 -> CP, FT2 -> FT, PT -> CP)."""
    test_type_upper = test_type.upper()
    if test_type_upper.startswith("CP"):
        return "CP"
    elif test_type_upper.startswith("PT"):
        return "CP"  # PT = CP (Probing Test)
    elif test_type_upper.startswith("FT"):
        return "FT"
    return "OTHER"


def extract_test_rev_from_filename(filename: str) -> str:
    """
    Extract test revision (Rev04, Rev01, etc.) from filename.
    
    Examples:
        "E6A773.00-01_01_SCT101A_L000_CP11_5_Rev04_00...stdf.gz" -> "Rev04"
        "LOT001_Rev01_001.stdf" -> "Rev01"
    """
    # re is imported at module level
    pattern = r'[_\-](Rev\d+)[_\-\.]'
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        return match.group(1)
    return ""


class ParquetStorage:
    """Parquet storage for STDF data with Hive-style partitioning."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.data_dir = config.data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _sanitize(value: str) -> str:
        """Sanitize a string for use in file/directory names.
        
        Removes or replaces characters that are invalid on Windows:
        \\ / : * ? " < > | and control characters (including NULL).
        """
        # Remove control characters (0x00-0x1F)
        sanitized = re.sub(r'[\x00-\x1f]', '', value)
        # Replace Windows-invalid characters with underscore
        sanitized = re.sub(r'[\\/:*?"<>|]', '_', sanitized)
        return sanitized.strip()

    def _get_table_path(
        self, 
        table_name: str, 
        product: str = "", 
        test_category: str = "",
        sub_process: str = ""
    ) -> Path:
        """
        Get path for a table with product/test_category/sub_process partitioning.
        
        Hierarchy: product -> test_category (CP/FT) -> sub_process (CP11/FT2)
        """
        base = self.data_dir / table_name
        if product and test_category:
            path = base / f"product={self._sanitize(product)}" / f"test_category={self._sanitize(test_category)}"
            if sub_process:
                path = path / f"sub_process={self._sanitize(sub_process)}"
            return path
        return base

    def _write_parquet(self, table: pa.Table, path: Path, compression: str = "zstd"):
        """
        Write Parquet file.

        Format version 2.6 (the highest pyarrow exposes) with default dictionary
        encoding. Real-data benchmarks (scripts/bench_parquet_real.py) showed
        v2.6 default is size- and fetch-neutral vs the old v1.0, while the modern
        column encodings it *enables* are a net loss for our float64 measurement
        data: BYTE_STREAM_SPLIT inflates files ~15% and slows fetch, and DELTA on
        ints trades ~13% smaller files for ~16% slower fetch — the wrong trade
        when large-fetch latency is the bottleneck. So we move to v2.6 but do NOT
        set column_encoding. Only DuckDB and pyarrow read these files; both
        support v2.6 fully (no external/legacy reader to keep at v1.0).

        Written atomically: a temp file in the same directory is renamed onto
        `path` via os.replace, so a reader on a network share never observes a
        partial file; the temp file is removed if writing fails. The replace
        is retried with exponential backoff on PermissionError (see below) to
        absorb transient external file locks; the temp file is still cleaned
        up if all retries are exhausted.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
        os.close(fd)
        try:
            pq.write_table(
                table,
                tmp,
                compression=compression,
                version="2.6",  # default dictionary encoding (no BSS/DELTA — see docstring)
                use_dictionary=True,
                write_statistics=True,
                coerce_timestamps="ms",  # Millisecond precision
                allow_truncated_timestamps=True,
            )
            # Same-lot write races (two workers targeting this same path) are
            # prevented upstream by per-lot serialization in worker.py — this
            # retry is only a safety net for transient locks held by EXTERNAL
            # processes (e.g. an antivirus scanner, or a reader briefly
            # holding the destination open on Windows), which surface as
            # PermissionError on os.replace.
            backoff_delays = (0.1, 0.2, 0.4, 0.8, 1.6)
            for attempt in range(5):
                try:
                    os.replace(tmp, path)
                    break
                except PermissionError:
                    if attempt == 4:
                        raise
                    time.sleep(backoff_delays[attempt])
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ── retest numbering ────────────────────────────────────────────────
    def _group_dir(self, table: str, product: str, test_category: str,
                   sub_process: str, lot_id: str, wafer_id: str) -> Path:
        """Directory of one lot/wafer group (holds its retest=N/ subdirs)."""
        return (self._get_table_path(table, product, test_category, sub_process)
                / f"lot_id={self._sanitize(lot_id)}"
                / f"wafer_id={self._sanitize(wafer_id)}")

    @staticmethod
    def _retest_dirs(group: Path) -> dict[int, Path]:
        out: dict[int, Path] = {}
        if group.exists():
            for child in group.iterdir():
                if child.is_dir() and child.name.startswith("retest="):
                    try:
                        out[int(child.name.split("=", 1)[1])] = child
                    except ValueError:
                        pass
        return out

    def _retest_num_for(self, product: str, test_category: str, sub_process: str,
                        lot_id: str, wafer_id: str, source_file: str) -> int:
        """Retest number for this file's run in a lot/wafer group.

        Re-ingesting the same source file reuses its own retest number (its
        files are overwritten), so a retry after a crash or timeout replaces
        the partial run instead of adding a duplicate. Otherwise max + 1 over
        every table's retest dirs (an aborted write may have left a dir in
        only some tables).
        """
        if source_file:
            runs = self._group_dir("runs", product, test_category, sub_process,
                                   lot_id, wafer_id)
            for k, d in sorted(self._retest_dirs(runs).items()):
                f = d / "data.parquet"
                if f.exists():
                    col = pq.ParquetFile(f).read(columns=["source_file"]).column(0).to_pylist()
                    if source_file in col:
                        return k
        nums = [k for t in ("runs", "parts", "test_data")
                for k in self._retest_dirs(self._group_dir(
                    t, product, test_category, sub_process, lot_id, wafer_id))]
        return max(nums, default=-1) + 1

    # ── retest flags ────────────────────────────────────────────────────
    def _pending_path(self, group: tuple) -> Path:
        import hashlib
        h = hashlib.sha1("\x1f".join(group).encode()).hexdigest()[:16]
        return self.data_dir / "_pending" / f"{h}.json"

    def _recompute_flags(self, product: str, test_category: str, sub_process: str,
                         lot_id: str, wafer_id: str) -> None:
        """Rewrite retest_flag of parts and test_data for one lot/wafer group.

        Every occurrence of a die (one part of one run) is ordered
        newest-first by (run MIR start time, retest_num, part_seq) —
        measurement time, not ingest order, and a die measured twice in one
        file keeps only its later part. retest_flag = rank of the occurrence
        among the die's occurrences (0 = newest), written to its parts row
        AND all of its test_data rows: the final view holds only the winning
        occurrence's measurements (tests it did not run are absent, never
        filled in from an older run). The die identity is
        mounts.die_key_expr().

        The flags are a pure function of the files, so this is idempotent:
        rerunning it after an interruption converges (see recover_pending).
        Only files whose flags change are rewritten.
        """
        from .mounts import die_key_expr

        runs = self._group_dir("runs", product, test_category, sub_process, lot_id, wafer_id)
        run_files = [d / "data.parquet" for d in self._retest_dirs(runs).values()
                     if (d / "data.parquet").exists()]
        con = duckdb.connect()
        try:
            if run_files:
                con.execute(
                    "CREATE TEMP TABLE ord AS SELECT retest_num AS k, "
                    "MAX(NULLIF(start_time, TIMESTAMPTZ '1970-01-01 00:00:00+00')) AS t "
                    "FROM read_parquet(?, union_by_name=true) GROUP BY ALL",
                    [[f.as_posix() for f in run_files]])
            else:
                con.execute("CREATE TEMP TABLE ord (k BIGINT, t TIMESTAMPTZ)")

            # Every occurrence of a die (one part of one run) is ranked once,
            # over the occurrences found in parts AND test_data, and both
            # tables take their flag from that ranking: test_data_final is
            # exactly the winning occurrence's rows, never a mix of runs.
            src = ("read_parquet(?, filename=true, file_row_number=true, "
                   "hive_partitioning=false, union_by_name=true)")
            tables = []
            for table, schema in (("parts", PARTS_SCHEMA), ("test_data", TEST_DATA_SCHEMA)):
                group = self._group_dir(table, product, test_category, sub_process,
                                        lot_id, wafer_id)
                files = [d / "data.parquet" for _, d in sorted(self._retest_dirs(group).items())
                         if (d / "data.parquet").exists()]
                if not files:
                    continue
                # Older files may lack part_seq / chip_key: typed NULL arm.
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE src_{table} AS
                    WITH s AS (
                        SELECT * FROM {src}
                        UNION ALL BY NAME
                        SELECT CAST(NULL AS BIGINT) AS part_seq,
                               CAST(NULL AS VARCHAR) AS chip_key,
                               CAST(NULL AS BIGINT) AS retest_flag WHERE FALSE
                    )
                    SELECT filename, file_row_number, retest_flag AS old_flag,
                           {die_key_expr()} AS die, retest_num AS k,
                           COALESCE(part_seq, 0) AS seq
                    FROM s
                """, [[f.as_posix() for f in files]])
                tables.append((table, schema))
            if not tables:
                return
            con.execute(f"""
                CREATE TEMP TABLE rk AS
                SELECT die, k, seq,
                       DENSE_RANK() OVER (PARTITION BY die
                           ORDER BY ord.t DESC NULLS LAST, k DESC, seq DESC) - 1 AS flag
                FROM ({" UNION ".join(f"SELECT die, k, seq FROM src_{t}" for t, _ in tables)}) o
                LEFT JOIN ord USING (k)
            """)

            for table, schema in tables:
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE fl AS
                    SELECT s.filename, s.file_row_number, s.old_flag, rk.flag
                    FROM src_{table} s JOIN rk USING (die, k, seq)
                """)
                changed = [r[0] for r in con.execute(
                    "SELECT DISTINCT filename FROM fl "
                    "WHERE flag IS DISTINCT FROM old_flag ORDER BY 1").fetchall()]
                for fname in changed:
                    # con.sql(...).to_arrow_table(): DuckDB 2.0 removed the
                    # connection-level .arrow(); the relation API works on 1.4+.
                    con.execute("SET VARIABLE f = ?", [fname])
                    updated = con.sql(f"""
                        SELECT src.* EXCLUDE (file_row_number, retest_flag),
                               fl.flag AS retest_flag
                        FROM read_parquet(getvariable('f'), file_row_number=true,
                                          hive_partitioning=false) src
                        JOIN (SELECT file_row_number, flag FROM fl
                              WHERE filename = getvariable('f')) fl
                          USING (file_row_number)
                        ORDER BY file_row_number
                    """).to_arrow_table()
                    for col in schema.names:
                        if col not in updated.column_names:
                            updated = updated.append_column(
                                col, pa.nulls(updated.num_rows, schema.field(col).type))
                    self._write_parquet(updated.select(schema.names).cast(schema), Path(fname))
        finally:
            con.close()

    def pending_count(self) -> int:
        """Lot groups whose flags an interrupted ingest left stale."""
        pend = self.data_dir / "_pending"
        return sum(1 for _ in pend.glob("*.json")) if pend.exists() else 0

    @contextmanager
    def ingest_lock(self, on_wait=None):
        """Exclusive ingest lock on data_dir, across processes.

        Flag recomputation, retest numbering and pending recovery
        read-then-write a lot's files, so only one ingest batch may run on a
        store at a time. An OS lock (released when the process dies, so a
        killed run never leaves it stuck); a second batch waits.
        `on_wait()` is called once if the lock is busy.
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        fh = open(self.data_dir / ".ingest.lock", "a+b")
        try:
            waited = False
            while True:
                try:
                    if os.name == "nt":
                        import msvcrt
                        fh.seek(0)
                        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if not waited and on_wait is not None:
                        on_wait()
                    waited = True
                    time.sleep(1.0)
            try:
                yield
            finally:
                if os.name == "nt":
                    import msvcrt
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()

    def recover_pending(self) -> int:
        """Finish flag recomputation left over by an interrupted ingest.

        save_stdf_data records each lot/wafer group in data_dir/_pending/
        before writing and removes it after the flags are recomputed, so a
        group still listed there may have stale flags. Returns the number of
        groups repaired; cheap when nothing is pending.
        """
        import json
        pend = self.data_dir / "_pending"
        if not pend.exists():
            return 0
        n = 0
        for marker in sorted(pend.glob("*.json")):
            group = json.loads(marker.read_text(encoding="utf-8"))
            self._recompute_flags(*group)
            marker.unlink(missing_ok=True)
            n += 1
        return n

    # ── write ───────────────────────────────────────────────────────────
    def save_stdf_data(
        self,
        data: STDFData,
        product: str = "UNKNOWN",
        test_category: str = "UNKNOWN",
        sub_process: str = "",
        source_file: str = "",
        compression: str = "zstd",
    ) -> dict[str, int]:
        """Save one parsed STDF file to Parquet: runs, parts, test_data.

        Per lot/wafer group (CP: each wafer; FT: wafer_id=''): pick the retest
        number, write runs FIRST (it records source_file, which is what lets a
        retry reuse the same retest number), then parts and test_data, then
        recompute the group's retest flags. The group is listed in
        data_dir/_pending/ for the whole write so recover_pending() can finish
        an interrupted one.

        Returns {table_name: row_count}.
        """
        from .atomic import atomic_write_json

        sub_process = sub_process or ""
        test_rev = extract_test_rev_from_filename(source_file)

        # FT has no WIR: parts carry wafer_id '' and that is its group. A file
        # with a MIR but no WIR/PRR still gets a runs row under ''.
        wafer_ids: list[str] = []
        for w in data.wafers:
            if w.get("wafer_id", "") not in wafer_ids:
                wafer_ids.append(w.get("wafer_id", ""))
        for part in data.parts:
            if part.get("wafer_id", "") not in wafer_ids:
                wafer_ids.append(part.get("wafer_id", ""))
        if not wafer_ids:
            wafer_ids.append("")

        parts_by_w: dict[str, list[dict]] = {}
        for part in data.parts:
            parts_by_w.setdefault(part.get("wafer_id", ""), []).append(part)
        results_by_w: dict[str, list[dict]] = {}
        for r in data.test_results:
            results_by_w.setdefault(r.get("wafer_id", ""), []).append(r)
        chip_cols = _chip_columns(data.chip_ids)

        counts = {"runs": 0, "parts": 0, "test_data": 0}
        for wafer_id in wafer_ids:
            key = (product, test_category, sub_process, data.lot_id, wafer_id)
            retest_num = self._retest_num_for(*key, source_file)
            marker = self._pending_path(key)
            atomic_write_json(marker, list(key))

            def path(table: str) -> Path:
                return (self._group_dir(table, *key) / f"retest={retest_num}"
                        / "data.parquet")

            wrr = [w for w in data.wafers if w.get("wafer_id", "") == wafer_id]
            self._write_parquet(self._runs_table(
                data, product, test_category, sub_process, wafer_id, retest_num,
                test_rev, source_file, wrr), path("runs"), compression)
            counts["runs"] += 1

            parts = parts_by_w.get(wafer_id, [])
            if parts:
                self._write_parquet(
                    _parts_table(parts, retest_num, chip_cols), path("parts"), compression)
                counts["parts"] += len(parts)

            results = results_by_w.get(wafer_id, [])
            if results:
                self._write_parquet(
                    _test_data_table(data, results, parts, retest_num),
                    path("test_data"), compression)
                counts["test_data"] += len(results)

            self._recompute_flags(*key)
            marker.unlink(missing_ok=True)

        orphans = sum(1 for c in data.chip_ids if not c.get("part_id"))
        if orphans:
            logger.warning("%s: %d CHIP ID values outside any part were dropped",
                           source_file, orphans)
        return counts

    @staticmethod
    def _runs_table(data: STDFData, product: str, test_category: str, sub_process: str,
                    wafer_id: str, retest_num: int, test_rev: str, source_file: str,
                    wrr: list[dict]) -> pa.Table:
        def total(name: str):
            return sum(w.get(name, 0) or 0 for w in wrr) if wrr else None

        starts = [w.get("start_time", 0) for w in wrr if w.get("start_time")]
        finishes = [w.get("finish_time", 0) for w in wrr if w.get("finish_time")]
        row = {
            "lot_id": data.lot_id, "wafer_id": wafer_id, "product": product,
            "test_category": test_category, "sub_process": sub_process,
            "retest_num": retest_num, "part_type": data.part_type,
            "job_name": data.job_name, "job_rev": data.job_rev,
            "start_time": _unix_to_datetime(data.start_time),
            "finish_time": _unix_to_datetime(data.finish_time),
            "tester_type": data.tester_type, "operator": data.operator,
            "node_name": data.node_name,
            **{name: getattr(data, name) for name in SDR_FIELDS},
            "wafer_start_time": _unix_to_datetime(min(starts)) if starts else None,
            "wafer_finish_time": _unix_to_datetime(max(finishes)) if finishes else None,
            "part_count": total("part_count"), "good_count": total("good_count"),
            "rtst_count": total("rtst_count"), "abrt_count": total("abrt_count"),
            "test_rev": test_rev, "source_file": source_file,
        }
        return pa.Table.from_pylist([row], schema=RUNS_SCHEMA)


_CHIP_FIELDS = ("chipid_hex", "origin_fab", "origin_lot", "origin_wafer",
                "origin_x", "origin_y")


def _chip_columns(chip_ids: list[dict]) -> dict[str, dict]:
    """part_id -> c0_*/c1_* CHIP ID columns plus chip_status / chip_warnings.

    Duplicates keep the first candidate's values and read 'ambiguous'; the
    per-candidate detail is in scripts/diag_chipid.py, not in the store.
    """
    by_part: dict[str, list[dict]] = {}
    for c in chip_ids:
        if c.get("part_id"):
            by_part.setdefault(c["part_id"], []).append(c)
    out: dict[str, dict] = {}
    for part_id, chips in by_part.items():
        cols: dict = {}
        warnings: list[str] = []
        n = {"C0": 0, "C1": 0}
        for c in chips:
            dec = decode_chipid(c.get("efuse_raw", ""))
            warnings += [f"{c['die_id']}: {w}" for w in c.get("warnings", []) + dec["warnings"]]
            die = c["die_id"]
            if die in n:
                n[die] += 1
                if n[die] == 1:
                    for f in _CHIP_FIELDS:
                        cols[f"{die.lower()}_{f}"] = dec[f]
        if n["C0"] > 1 or n["C1"] > 1:
            status = "ambiguous"
        elif n["C0"] and n["C1"]:
            status = "complete"
            if cols.get("c0_chipid_hex") == cols.get("c1_chipid_hex"):
                warnings.append("C0 and C1 CHIP IDs are identical")
        else:
            status = "missing " + ", ".join(d for d in ("C0", "C1") if not n[d])
        cols["chip_status"] = status
        cols["chip_warnings"] = "; ".join(dict.fromkeys(warnings))
        out[part_id] = cols
    return out


def _parts_table(parts: list[dict], retest_num: int, chip_cols: dict[str, dict]) -> pa.Table:
    rows = []
    for p in parts:
        row = {name: p.get(name) for name in PARTS_SCHEMA.names}
        row.update(chip_cols.get(p.get("part_id", ""), {}))
        row["retest_num"] = retest_num
        row["retest_flag"] = 0  # set by _recompute_flags
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=PARTS_SCHEMA)


def _test_data_table(data: STDFData, results: list[dict], parts: list[dict],
                     retest_num: int) -> pa.Table:
    by_id = {p.get("part_id", ""): p for p in parts}
    cols: dict[str, list] = {name: [] for name in TEST_DATA_SCHEMA.names}
    # exec_seq = 0-based occurrence order of (part, test, pin) within this run,
    # so loop measurements (e.g. an OTP dump logging 512 PTRs under one
    # test_num) stay distinguishable.
    seq: dict[tuple, int] = {}
    empty: dict = {}
    for r in results:
        part_id = r.get("part_id", "")
        p = by_id.get(part_id, empty)
        test_num = r.get("test_num", 0)
        info = data.tests.get(test_num, empty)
        pin_num = r.get("pin_num")
        k = (part_id, test_num, pin_num)
        n = seq.get(k, 0)
        seq[k] = n + 1
        row = (
            r.get("lot_id", ""), r.get("wafer_id", ""), part_id, p.get("part_seq"),
            p.get("part_txt", ""), p.get("part_serial", ""), p.get("chip_key", ""),
            p.get("x_coord", -32768), p.get("y_coord", -32768), test_num,
            info.get("test_name", ""), info.get("rec_type", "PTR"),
            info.get("lo_limit"), info.get("hi_limit"), info.get("units", ""),
            r.get("result"), _PF.get(r.get("passed")), r.get("test_flg"),
            retest_num, pin_num, r.get("pin_name"), n, 0,
        )
        for name, v in zip(_TD_ORDER, row):
            cols[name].append(v)
    return pa.table(cols, schema=TEST_DATA_SCHEMA)


_PF = {True: "P", False: "F"}

_TD_ORDER = (
    "lot_id", "wafer_id", "part_id", "part_seq", "part_txt", "part_serial", "chip_key",
    "x_coord", "y_coord", "test_num", "test_name", "rec_type", "lo_limit", "hi_limit",
    "units", "result", "passed", "test_flg", "retest_num", "pin_num", "pin_name", "exec_seq",
    "retest_flag",
)
assert set(_TD_ORDER) == set(TEST_DATA_SCHEMA.names)
