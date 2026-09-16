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


# FT identity = 2D barcode (part_txt) → PRR.PART_ID (part_serial) → 合成 part_id。
# 空 part_txt で全パッケージが 1 キーに潰れると、parts_final がロット全体を
# 1 ダイに畳み、_demote_superseded が他パッケージの測定値まで demote する。
# CP (座標あり) では CASE が '' を返す。
_FT_IDENTITY = (
    "COALESCE(NULLIF({p}part_txt, ''), NULLIF({p}part_serial, ''), {p}part_id)"
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


# Dedup identity within a (lot, retest) group, expressed as native partition
# columns.
#
#   CP die identity = (wafer_id, x_coord, y_coord) — the probe location. CP
#   testers MAY populate PRR.PART_TXT with a per-part serial / 2D barcode, so
#   part_txt is NOT safe to include in the CP key: the same physical die would
#   carry a different part_txt across retests and fail to dedup, inflating
#   counts by summing every retest.
#
#   FT has no wafer/probe coordinates (wafer_id='', x=y=-32768); its identity
#   is the package barcode, falling back to PRR.PART_ID then the synthetic
#   per-file part_id (see ft_identity / die_key_expr above).
_DEDUP_UNIT = f"wafer_id, x_coord, y_coord, {ft_identity()}"


# test_data の retest_flag 整合性キー。_DEDUP_UNIT(ダイ識別)に test_num/pin_num を
# 足したもの — flag はこの粒度で付く(storage.py が ingest 時に確定させる)。
_FLAG_KEY = (
    f"lot_id, wafer_id, x_coord, y_coord, {ft_identity()}, test_num, pin_num"
)

# storage.py が ingest 時に確定させる retest_flag の不変条件。壊れていれば
# test_data_final(= retest_flag = 0)が黙って誤った行集合を返すので、
# 測定値そのものより先にここが疑わしい。`stdf db verify` が実行する。
#
# 元は `stdf db verify-flags`(7813cb3 で廃止)→ dbt/tests/assert_*.sql(dbt 撤去で
# 再び CLI へ)。SQL は素の DuckDB なので、どの経路からでも同じものが走る。
FLAG_INVARIANTS: list[tuple[str, str, str]] = [
    (
        "null_flags",
        "retest_flag が NULL の行(フラグ導入前に ingest されたファイル)",
        "SELECT lot_id, COUNT(*) AS n FROM test_data "
        "WHERE retest_flag IS NULL GROUP BY lot_id",
    ),
    (
        "dup_current",
        "同じキーの flag=0 が複数の run に跨っている",
        f"SELECT {_FLAG_KEY} FROM test_data WHERE retest_flag = 0 "
        f"GROUP BY {_FLAG_KEY} HAVING COUNT(DISTINCT retest_num) > 1",
    ),
    (
        "inconsistent_runs",
        "同一 run 内で同じキーのフラグが割れている",
        f"SELECT {_FLAG_KEY}, retest_num FROM test_data "
        f"GROUP BY {_FLAG_KEY}, retest_num HAVING MIN(retest_flag) != MAX(retest_flag)",
    ),
    (
        "orphaned_keys",
        "最新 run が flag=0 になっていないキー",
        f"SELECT {_FLAG_KEY} FROM test_data "
        f"GROUP BY {_FLAG_KEY} HAVING MIN(retest_flag) != 0",
    ),
]


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
    gross_die table and wafer_yield_final). It is a handful of stat calls,
    not a tree walk.
    """
    parts = [t for t in ["runs", "wafers", "parts", "test_data", "chipid"]
             if (data_dir / t).exists()]
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
            # parts/test_data/runs can mix pre-migration files with new ones
            # (parts/test_data: no part_serial; test_data: no exec_seq/retest_flag;
            # runs: no SDR equipment columns); union_by_name fills the missing
            # columns with NULL instead of erroring on schema mismatch.
            union = (
                ", union_by_name=true" if table in ("parts", "test_data", "runs") else ""
            )
            rel = (
                f"read_parquet('{path.as_posix()}/**/*.parquet', "
                f"hive_partitioning=true{union})"
            )
            if table in ("parts", "test_data"):
                # part_serial は後から追加した列。旧ファイルしかないストアでは
                # union_by_name でも列が現れないため、型付きゼロ行アームで常に
                # 存在させる。DuckDB は空アームを planning 時に除去する
                # (EXPLAIN 確認済み) ので predicate pushdown も損なわない。
                rel = (
                    f"(SELECT * FROM {rel}\n"
                    f"UNION ALL BY NAME\n"
                    f"SELECT CAST(NULL AS VARCHAR) AS part_serial WHERE FALSE)"
                )
            conn.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM {rel}")
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
                SELECT *, {die_key_expr()} AS die_key, ROW_NUMBER() OVER (
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
        # this); `stdf db verify` (FLAG_INVARIANTS above) detects and
        # reports it.
        conn.execute(f"""
            CREATE OR REPLACE VIEW test_data_final AS
            SELECT *, {die_key_expr()} AS die_key
            FROM test_data WHERE retest_flag = 0
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
            # CAST 必須: NULL 単体は INT32 になり、VARCHAR の lot_id 比較が
            # ConversionException になる。
            lot_product = (
                "SELECT CAST(NULL AS VARCHAR) AS lot_id, "
                "CAST(NULL AS VARCHAR) AS product WHERE FALSE"
            )
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
