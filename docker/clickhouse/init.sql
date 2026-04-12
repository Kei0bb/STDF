-- ClickHouse schema for stdf2pq
-- Engine choices:
--   lots/wafers: ReplacingMergeTree — re-ingest of same lot silently deduplicates
--   parts/test_data: MergeTree — append-only, no duplicates expected

CREATE DATABASE IF NOT EXISTS stdf;

-- ── lots ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stdf.lots (
    lot_id          String,
    product         String,
    test_category   LowCardinality(String),   -- CP / FT / OTHER
    sub_process     String,
    part_type       String,
    job_name        String,
    job_rev         String,
    start_time      Nullable(DateTime64(3, 'UTC')),
    finish_time     Nullable(DateTime64(3, 'UTC')),
    tester_type     String,
    operator        String
) ENGINE = ReplacingMergeTree()
ORDER BY lot_id
SETTINGS index_granularity = 8192;

-- ── wafers ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stdf.wafers (
    lot_id      String,
    wafer_id    String,
    head_num    Int32,
    start_time  Nullable(DateTime64(3, 'UTC')),
    finish_time Nullable(DateTime64(3, 'UTC')),
    part_count  Int64,
    good_count  Int64,
    rtst_count  Int64,
    abrt_count  Int64,
    test_rev    String,
    retest_num  Int32,
    source_file String
) ENGINE = ReplacingMergeTree()
ORDER BY (lot_id, wafer_id, retest_num)
SETTINGS index_granularity = 8192;

-- ── parts ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stdf.parts (
    part_id     String,
    lot_id      String,
    wafer_id    String,
    head_num    Int32,
    site_num    Int32,
    x_coord     Int32,
    y_coord     Int32,
    hard_bin    Int32,
    soft_bin    Int32,
    passed      Bool,
    test_count  Int64,
    test_time   Int64
) ENGINE = MergeTree()
ORDER BY (lot_id, wafer_id, part_id)
SETTINGS index_granularity = 8192;

-- ── test_data ─────────────────────────────────────────────────────────────
-- Largest table. ORDER BY (lot_id, test_num, part_id) optimises:
--   - Per-lot queries          : WHERE lot_id = ?
--   - Per-test aggregations    : WHERE lot_id = ? AND test_num = ?
--   - Fail-rate ranking        : GROUP BY lot_id, test_num
CREATE TABLE IF NOT EXISTS stdf.test_data (
    lot_id      String,
    wafer_id    String,
    part_id     String,
    x_coord     Int32,
    y_coord     Int32,
    test_num    Int32,
    test_name   String,
    rec_type    LowCardinality(String),   -- PTR / MPR / FTR
    lo_limit    Nullable(Float64),
    hi_limit    Nullable(Float64),
    units       LowCardinality(String),
    result      Nullable(Float64),
    passed      LowCardinality(String)    -- 'P' / 'F'
) ENGINE = MergeTree()
ORDER BY (lot_id, test_num, part_id)
SETTINGS index_granularity = 8192;
