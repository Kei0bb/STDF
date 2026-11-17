"""Single source of truth for DuckDB view definitions over the Parquet store.

Imported by analysis/session.py (AnalysisSession) and workspace/query.py so
the dedup key and the base/final view SQL exist in exactly one place. Paths use
.as_posix() so the generated SQL is valid on Windows as well as POSIX hosts.

Retest dedup happens at ingest time: storage._recompute_flags writes
retest_flag (0 = newest occurrence, by run start time) on parts and
test_data, so parts_final / test_data_final are plain predicate filters that
push into the Parquet scan — never a window.
"""

import hashlib
from pathlib import Path

import duckdb


# FT identity = eFuse の chip_key（C0、なければ C1。parser._part_chip_key）。
# eFuse が無いパッケージはその run 固有のキー（retest_num + 連番 part_id）に
# して、リテスト間で重ねない。連番は「ファイル内の何個目か」でしかなく、
# 重ねると別パッケージを上書きする。バーコード (part_txt / part_serial) は
# 使わない（運用判断）。CP (座標あり) では CASE が '' を返す。
_FT_IDENTITY = (
    "COALESCE(NULLIF({p}chip_key, ''), "
    "'NOEF|' || CAST({p}retest_num AS VARCHAR) || '|' || {p}part_id)"
)


def ft_identity(prefix: str = "") -> str:
    """FT パッケージ identity の SQL 式（`prefix` でテーブル修飾）。"""
    p = f"{prefix}." if prefix else ""
    return (f"CASE WHEN {p}x_coord = -32768 AND {p}y_coord = -32768 "
            f"THEN {_FT_IDENTITY.format(p=p)} ELSE '' END")


def die_key_expr(prefix: str = "") -> str:
    """物理ダイ/パッケージを 1 文字列で表す SQL 式。

    joins は `part_id` ではなくこれを使う。part_id はファイル内連番
    (parser.py:336) で、部分リテストでは別ダイに振り直されるため。
    CP と FT をプレフィックスで分離し、負座標でも衝突しない。
    """
    p = f"{prefix}." if prefix else ""
    return (f"CASE WHEN {p}x_coord = -32768 AND {p}y_coord = -32768 "
            f"THEN 'FT|' || {_FT_IDENTITY.format(p=p)} "
            f"ELSE 'CP|' || CAST({p}x_coord AS VARCHAR) || '|' "
            f"|| CAST({p}y_coord AS VARCHAR) END")


# Die identity within a lot/wafer group (storage._recompute_flags):
#   CP = (x_coord, y_coord) — the probe location. CP testers MAY populate
#   PRR.PART_TXT with a per-part serial, so part_txt is NOT part of the CP key.
#   FT has no wafer/probe coordinates (wafer_id='', x=y=-32768); its identity
#   is the eFuse chip_key, else a per-run key that never overlays (see
#   _FT_IDENTITY above).


# Columns added after a table's first release that views reference: always
# present (NULL on older files) so a not-yet-wiped store still mounts.
# Partition dirs are omitted when empty (storage._get_table_path), so the
# partition columns are listed too.
_PARTITION_COLS = [("product", "VARCHAR"), ("test_category", "VARCHAR"),
                   ("sub_process", "VARCHAR")]
_LATE_COLUMNS = {
    "runs": _PARTITION_COLS,
    "parts": [("part_serial", "VARCHAR"), ("chip_key", "VARCHAR"),
              ("retest_flag", "BIGINT"), *_PARTITION_COLS],
    "test_data": [("part_serial", "VARCHAR"), ("chip_key", "VARCHAR"),
                  *_PARTITION_COLS],
}

# The Parquet tables. A store may still hold legacy wafers/ and chipid/ dirs
# (folded into runs / parts); they are ignored until the wipe.
_CORE = ["runs", "parts", "test_data"]

# Any change to this module (view SQL, identity) changes the fingerprint, so a
# persisted `db shell` catalog re-registers instead of keeping old views.
_SOURCE_HASH = hashlib.sha1(Path(__file__).read_bytes()).hexdigest()[:12]


def store_fingerprint(data_dir: Path,
                      gross_die_map: dict[str, tuple[int, int]] | None = None) -> str:
    """A cheap signature of everything setup_views() would register.

    Registering the views is linear in the number of Parquet files: each
    CREATE VIEW re-resolves its glob, and the derived views (*_final, lots,
    wafer_yield_final) re-bind their base view's glob a second time, so a
    session pays the walk roughly twice. That is fine for a one-shot command
    but wasteful for `stdf db shell`, which persists its catalog in a real
    database file and can simply reuse the views it registered last time.

    The views are glob expressions, so data ingested after registration is
    picked up with no re-registration — only a change in WHICH views should
    exist matters. That is what this fingerprint captures: the set of core
    table directories present, and the gross-die map (which changes the
    gross_die table and wafer_yield_final), plus a hash of this module so a
    change to the view definitions re-registers too. It is a handful of stat
    calls, not a tree walk.
    """
    parts = [f"v:{_SOURCE_HASH}"]
    parts += [t for t in _CORE if (data_dir / t).exists()]
    for prod, (gd, fb) in sorted((gross_die_map or {}).items()):
        parts.append(f"gd:{prod}={gd}/{fb}")
    return "|".join(parts)


def setup_views(
    conn: duckdb.DuckDBPyConnection,
    data_dir: Path,
    gross_die_map: dict[str, tuple[int, int]] | None = None,
) -> list[str]:
    """Register Parquet glob views and final-bin merge VIEWs.

    gross_die_map ({product: (gross_die, gd_fail_bin)}) drives the gross-die
    yield denominator and the QC-fail (unprobed) bucket at QUERY time. It is the
    single source for that definition: no synthetic rows are written to Parquet,
    so the gross-die count is robust to retests and partial/aborted probes.

    Returns the list of registered view names (base tables and the *_final
    dedup views that were created).
    """
    # Legacy store detection: `lots` used to be its own Parquet table
    # (`data/lots/.../lot_id={lot}/data.parquet`, overwritten by every file
    # ingested for that lot). It is now a VIEW derived from `runs` (see
    # below) — a store still carrying data/lots/ predates that change and
    # has no `runs` table to derive from, so refuse to proceed rather than
    # silently serving a `lots` view with zero rows.
    if (data_dir / "lots").exists():
        raise RuntimeError(
            "Legacy store detected: data/lots/ is no longer written "
            "(MIR moved to data/runs/). Wipe the data directory and "
            "re-ingest — there is no migration path."
        )
    pend = data_dir / "_pending"
    if pend.exists() and any(pend.glob("*.json")):
        import warnings
        warnings.warn(
            f"{data_dir}: an ingest is running or was interrupted — some lot "
            "groups may count a retested die twice in *_final until the next "
            "ingest batch repairs them (data/_pending/).", stacklevel=2)

    # Fail loudly on a data_dir that holds no store. Every table below is
    # registered only `if path.exists()`, so a data_dir pointing at the wrong
    # place (or at nothing) used to return an empty view list and let the
    # session come up "successfully" — the first symptom was a Catalog Error
    # on some view name, many steps removed from the actual mistake. The
    # resolved path is in the message because that path is exactly what the
    # caller got wrong.
    if not any((data_dir / t).exists() for t in _CORE):
        raise RuntimeError(
            f"No STDF store found at {data_dir.resolve()} — none of "
            f"{'/'.join(_CORE)} exist there. Check config.yaml's "
            f"storage.data_dir (relative paths resolve against the config "
            f"file's directory), or pass an explicit data_dir. "
            f"An empty store is expected only before the first ingest."
        )

    registered: list[str] = []
    for table in _CORE:
        path = data_dir / table
        if path.exists():
            # Files written by older code lack later columns; union_by_name
            # fills them with NULL instead of erroring on schema mismatch.
            rel = (
                f"read_parquet('{path.as_posix()}/**/*.parquet', "
                f"hive_partitioning=true, union_by_name=true)"
            )
            late_cols = _LATE_COLUMNS.get(table)
            if late_cols:
                # part_serial は後から追加した列。旧ファイルしかないストアでは
                # union_by_name でも列が現れないため、型付きゼロ行アームで常に
                # 存在させる。DuckDB は空アームを planning 時に除去する
                # (EXPLAIN 確認済み) ので predicate pushdown も損なわない。
                # chip_key / retest_flag / パーティション列も同様。
                nulls = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in late_cols)
                rel = (
                    f"(SELECT * FROM {rel}\n"
                    f"UNION ALL BY NAME\n"
                    f"SELECT {nulls} WHERE FALSE)"
                )
            conn.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM {rel}")
            registered.append(table)

    if "runs" in registered:
        # `lots` is now a derived VIEW, one row per lot, aggregated from the
        # per-run `runs` rows (one per file x wafer identity). Column order
        # matches the old LOTS_SCHEMA so existing consumers (analysis/session.py
        # `lots()`, `lot_product` below) need no
        # changes; job_variant_count / job_mixed are new, additive columns.
        #
        # Semantics vs the old overwritten-per-file table:
        #   - start_time = MIN over all runs in the lot, finish_time = MAX.
        #     (old: whichever file was ingested last — an ingest-order
        #     artifact, not a meaningful value)
        #   - job_name / job_rev / part_type / tester_type / operator = the
        #     value from the run with the latest start_time (arg_max). This is
        #     a real, well-defined choice (vs. old ingest-order dependence),
        #     but still just one value for a lot that may have run under
        #     multiple test program revisions — job_mixed / job_variant_count
        #     surface that instead of silently picking one.
        conn.execute("""
            CREATE OR REPLACE VIEW lots AS
            SELECT lot_id, product, test_category, sub_process,
                   arg_max(part_type,   start_time) AS part_type,
                   arg_max(job_name,    start_time) AS job_name,
                   arg_max(job_rev,     start_time) AS job_rev,
                   MIN(start_time)  AS start_time,
                   MAX(finish_time) AS finish_time,
                   arg_max(tester_type, start_time) AS tester_type,
                   arg_max(operator,    start_time) AS operator,
                   COUNT(DISTINCT (job_name, job_rev))     AS job_variant_count,
                   COUNT(DISTINCT (job_name, job_rev)) > 1 AS job_mixed
            FROM runs
            GROUP BY lot_id, product, test_category, sub_process
        """)
        registered.append("lots")

    if "parts" in registered:
        # Same flag mechanism as test_data_final (storage._recompute_flags).
        conn.execute(f"""
            CREATE OR REPLACE VIEW parts_final AS
            SELECT *, {die_key_expr()} AS die_key
            FROM parts WHERE retest_flag = 0
        """)
        registered.append("parts_final")

    if "test_data" in registered:
        # retest_flag = 0 is a plain predicate, so it pushes into the Parquet
        # scan (a ROW_NUMBER window here once made lot-scoped queries full
        # scans). All rows of the winning occurrence are kept, including loop
        # measurements — use exec_seq to tell them apart. NULL flags (files
        # from pre-flag code) are excluded: wipe and re-ingest such a store.
        conn.execute(f"""
            CREATE OR REPLACE VIEW test_data_final AS
            SELECT *, {die_key_expr()} AS die_key
            FROM test_data WHERE retest_flag = 0
        """)
        registered.append("test_data_final")

    # Gross-die table (config-derived; created empty when unset so downstream
    # LEFT JOINs work uniformly). Applied at query time only — never written to
    # Parquet.
    conn.execute(
        "CREATE OR REPLACE TABLE gross_die "
        "(product VARCHAR, gross_die BIGINT, gd_fail_bin BIGINT)"
    )
    if gross_die_map:
        conn.executemany(
            "INSERT INTO gross_die VALUES (?, ?, ?)",
            [(p, gd, fb) for p, (gd, fb) in gross_die_map.items()],
        )

    # Single-source per-(lot, wafer) yield with the gross-die denominator.
    #
    #   total = max(probed, GD) for CP wafers (wafer_id != '') of a product with
    #   a configured gross die; otherwise total = probed. GD is a CP wafer-plane
    #   concept, so FT groups (wafer_id='') always use the probed count.
    #   unprobed = total - probed counts dies lost to fab inline failure / an
    #   aborted probe — they sit in the denominator (QC fail) without any row in
    #   Parquet. GREATEST guards the rare probed>GD case from going negative.
    #
    # Every yield consumer (CLI, analysis) reads this view so the gross-die
    # definition lives in exactly one place.
    if "parts_final" in registered:
        # One row per (product, test_category, sub_process, lot, wafer): CP1
        # and CP2 of one lot are separate yields, never mixed.
        conn.execute("""
            CREATE OR REPLACE VIEW wafer_yield_final AS
            WITH probed AS (
                SELECT product, test_category, sub_process, lot_id, wafer_id,
                       COUNT(*)                                 AS probed,
                       SUM(CASE WHEN passed THEN 1 ELSE 0 END)  AS good
                FROM parts_final
                GROUP BY ALL
            ),
            joined AS (
                SELECT pr.*,
                       CASE WHEN pr.wafer_id <> '' AND gd.gross_die IS NOT NULL
                            THEN GREATEST(pr.probed, gd.gross_die)
                            ELSE pr.probed END                  AS total,
                       gd.gd_fail_bin                           AS gd_fail_bin
                FROM probed pr
                LEFT JOIN gross_die gd ON pr.product = gd.product
            )
            SELECT product, test_category, sub_process, lot_id, wafer_id,
                   probed, good, total,
                   total - probed                               AS unprobed,
                   gd_fail_bin,
                   ROUND(100.0 * good / NULLIF(total, 0), 2)     AS yield_pct
            FROM joined
        """)
        registered.append("wafer_yield_final")


    return registered
