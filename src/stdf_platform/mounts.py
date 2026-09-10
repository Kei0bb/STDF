"""Single source of truth for DuckDB view definitions over the Parquet store.

Imported by analysis/session.py (AnalysisSession) and workspace/query.py so
the dedup key and the base/final view SQL exist in exactly one place. Paths use
.as_posix() so the generated SQL is valid on Windows as well as POSIX hosts.

test_data dedup happens at ingest time (storage.py writes retest_flag/
exec_seq per row), so test_data_final below is a plain predicate filter, not
a window — see that view's comments for why. parts_final and chipid_final
are small enough that the ROW_NUMBER() window cost is negligible, and are
left as-is: collapsing to one row per die there is exactly the desired
semantics (unlike test_data, where a die/test pair can legitimately have
many rows — loop measurements).
"""

from pathlib import Path

import duckdb


# Dedup identity within a (lot, retest) group, expressed as native partition
# columns.
#
#   CP die identity = (wafer_id, x_coord, y_coord) — the probe location. CP
#   testers MAY populate PRR.PART_TXT with a per-part serial / 2D barcode, so
#   part_txt is NOT safe to include in the CP key: the same physical die would
#   carry a different part_txt across retests and fail to dedup, inflating
#   counts by summing every retest.
#
#   FT has no wafer/probe coordinates (wafer_id='', x=y=-32768); its die
#   identity is the package barcode in part_txt.
#
# The CASE selects part_txt ONLY for coordinate-less rows (FT), and a constant
# otherwise so CP probed dies group purely by wafer_id + x/y.
_DEDUP_UNIT = (
    "wafer_id, x_coord, y_coord, "
    "CASE WHEN x_coord = -32768 AND y_coord = -32768 THEN part_txt ELSE '' END"
)


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
    table directories present, the set of mart files, and the gross-die map
    (which changes the gross_die table and wafer_yield_final). It is a
    handful of stat calls, not a tree walk.
    """
    parts = [t for t in ["runs", "wafers", "parts", "test_data", "chipid"]
             if (data_dir / t).exists()]
    marts_dir = data_dir / "marts"
    if marts_dir.exists():
        parts += ["mart:" + f.name for f in sorted(marts_dir.glob("*.parquet"))]
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

    # Fail loudly on a data_dir that holds no store. Every table below is
    # registered only `if path.exists()`, so a data_dir pointing at the wrong
    # place (or at nothing) used to return an empty view list and let the
    # session come up "successfully" — the first symptom was a Catalog Error
    # on some view name, many steps removed from the actual mistake. The
    # resolved path is in the message because that path is exactly what the
    # caller got wrong.
    _CORE = ["runs", "wafers", "parts", "test_data", "chipid"]
    if not any((data_dir / t).exists() for t in [*_CORE, "marts"]):
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
            # test_data and runs can mix pre-migration files with new ones
            # (test_data: no exec_seq/retest_flag; runs: no SDR equipment
            # columns); union_by_name fills the missing columns with NULL
            # instead of erroring on schema mismatch.
            extra_opt = (
                ", union_by_name=true" if table in ("test_data", "runs") else ""
            )
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table} AS
                SELECT * FROM read_parquet(
                    '{path.as_posix()}/**/*.parquet', hive_partitioning=true{extra_opt}
                )
            """)
            registered.append(table)

    if "runs" in registered:
        # `lots` is now a derived VIEW, one row per lot, aggregated from the
        # per-run `runs` rows (one per file x wafer identity). Column order
        # matches the old LOTS_SCHEMA so existing consumers (get_lot_summary,
        # trend.py, analysis/session.py `lots()`, `lot_product` below) need no
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
        conn.execute(f"""
            CREATE OR REPLACE VIEW parts_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, {_DEDUP_UNIT}
                    ORDER BY retest_num DESC
                ) AS rn FROM parts
            ) WHERE rn = 1
        """)
        registered.append("parts_final")

    if "test_data" in registered:
        # test_data is large enough that the old ROW_NUMBER()-per-key window
        # (still used below for parts_final/chipid_final) was a real cost:
        # a window's PARTITION BY is not a predicate, so a WHERE on a
        # non-PARTITION-BY column (e.g. `test_name LIKE ...` scoped to one
        # lot) can never be pushed below the window — DuckDB had to
        # materialize the *entire* windowed relation before filtering,
        # turning a lot-scoped query into a full-table scan (12+ minutes on
        # the real store). Dedup is now precomputed at ingest time instead
        # (storage.py writes `retest_flag`; 0 = the newest run containing a
        # key — see _DEDUP_UNIT above for the key and _demote_superseded for
        # how older runs get bumped). `retest_flag = 0` is a plain predicate,
        # so DuckDB pushes it into the Parquet scan same as any other filter
        # — a lot-scoped test_name query is now a cheap scan again.
        #
        # Row-semantics change vs the old window view: the window kept
        # exactly ONE arbitrary row per (die, test, pin) per lot, silently
        # discarding loop measurements (e.g. an OTP dump logging 512 PTRs
        # under one test_num — 511 of 512 rows were dropped) and potentially
        # hiding a failing iteration. The flag-based view keeps ALL rows of
        # the latest run, including every loop iteration; use `exec_seq`
        # (0-based occurrence order within the run) to distinguish them when
        # one-value-per-test is wanted.
        #
        # Rows with retest_flag IS NULL (test_data files written by
        # pre-flag/pre-migration code — see union_by_name above) are
        # EXCLUDED here, not treated as "current": there is no reliable
        # per-key recency signal for them, so silently including them risks
        # mixing stale and current measurements. A store in this state must
        # be re-ingested (the user's own WIPE-and-re-ingest plan covers
        # this); the dbt/tests/assert_*.sql singular tests (run by
        # `stdf build`'s `dbt test`) detect and report it.
        conn.execute("""
            CREATE OR REPLACE VIEW test_data_final AS
            SELECT * FROM test_data WHERE retest_flag = 0
        """)
        registered.append("test_data_final")

    if "chipid" in registered:
        # die identity = decoded ChipID (efuse_raw), NOT positional
        # chip_occurrence_index (which can swap die0/die1 across retests).
        conn.execute("""
            CREATE OR REPLACE VIEW chipid_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, efuse_raw
                    ORDER BY retest_num DESC
                ) AS rn FROM chipid
            ) WHERE rn = 1
        """)
        registered.append("chipid_final")

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
        if "lots" in registered:
            lot_product = (
                "SELECT lot_id, ANY_VALUE(product) AS product FROM lots GROUP BY lot_id"
            )
        else:
            # No lots table (rare; some unit tests write only parts) → product
            # unknown → GD cannot be resolved → fall back to probed counts.
            lot_product = "SELECT NULL AS lot_id, NULL AS product WHERE FALSE"
        conn.execute(f"""
            CREATE OR REPLACE VIEW wafer_yield_final AS
            WITH probed AS (
                SELECT lot_id, wafer_id,
                       COUNT(*)                                 AS probed,
                       SUM(CASE WHEN passed THEN 1 ELSE 0 END)  AS good
                FROM parts_final
                GROUP BY lot_id, wafer_id
            ),
            lp AS ({lot_product}),
            joined AS (
                SELECT pr.lot_id, pr.wafer_id, pr.probed, pr.good,
                       CASE WHEN pr.wafer_id <> '' AND gd.gross_die IS NOT NULL
                            THEN GREATEST(pr.probed, gd.gross_die)
                            ELSE pr.probed END                  AS total,
                       gd.gd_fail_bin                           AS gd_fail_bin
                FROM probed pr
                LEFT JOIN lp ON pr.lot_id = lp.lot_id
                LEFT JOIN gross_die gd ON lp.product = gd.product
            )
            SELECT lot_id, wafer_id, probed, good, total,
                   total - probed                               AS unprobed,
                   gd_fail_bin,
                   ROUND(100.0 * good / NULLIF(total, 0), 2)     AS yield_pct
            FROM joined
        """)
        registered.append("wafer_yield_final")

    # 分析マート(dbt が data/marts/ に external materialization した Parquet)
    # をファイル名 = ビュー名でマウントする。定義の中身は dbt/models/marts/ が
    # 唯一の持ち主 — ここは名前を貼るだけ。
    marts_dir = data_dir / "marts"
    if marts_dir.exists():
        # "gross_die" is a TABLE (not a VIEW), created above regardless of
        # whether any mart exists — CREATE OR REPLACE VIEW over it would
        # raise duckdb.CatalogException (table vs view) and abort
        # setup_views() entirely. A mart named after any already-registered
        # canonical view (e.g. "parts", "lots") would otherwise silently
        # CREATE OR REPLACE it, last-registration-wins, and mask the real
        # table. Reserve both: the names already in `registered` plus
        # "gross_die".
        reserved = set(registered) | {"gross_die"}
        for f in sorted(marts_dir.glob("*.parquet")):
            name = f.stem
            if not name.isidentifier() or name in reserved:
                continue  # 想定外のファイル名・予約名は黙って飛ばさず登録もしない
            conn.execute(
                f"CREATE OR REPLACE VIEW {name} AS "
                f"SELECT * FROM read_parquet('{f.as_posix()}')"
            )
            registered.append(name)

    return registered
