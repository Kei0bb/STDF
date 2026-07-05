"""Single source of truth for DuckDB view definitions over the Parquet store.

Imported by database.py, web/api/deps.py, cli.py (db verify-flags) and
query.py so the dedup key and the base/final view SQL exist in exactly one
place. Paths use .as_posix() so the generated SQL is valid on Windows as well
as POSIX hosts.

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
    registered: list[str] = []
    for table in ["lots", "wafers", "parts", "test_data", "chipid"]:
        path = data_dir / table
        if path.exists():
            # test_data alone can mix pre-migration files (no exec_seq/
            # retest_flag columns) with new ones; union_by_name fills the
            # missing columns with NULL instead of erroring on schema mismatch.
            extra_opt = ", union_by_name=true" if table == "test_data" else ""
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table} AS
                SELECT * FROM read_parquet(
                    '{path.as_posix()}/**/*.parquet', hive_partitioning=true{extra_opt}
                )
            """)
            registered.append(table)

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
        # this); `stdf db verify-flags` detects and reports it.
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

    return registered
