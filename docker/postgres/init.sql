-- STDF PostgreSQL Schema
-- This file is automatically executed on first container startup.

-- ============================================================
-- Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS lots (
    lot_id        TEXT NOT NULL,
    product       TEXT NOT NULL,
    test_category TEXT NOT NULL,
    sub_process   TEXT NOT NULL DEFAULT '',
    part_type     TEXT DEFAULT '',
    job_name      TEXT DEFAULT '',
    job_rev       TEXT DEFAULT '',
    start_time    TIMESTAMPTZ,
    finish_time   TIMESTAMPTZ,
    tester_type   TEXT DEFAULT '',
    operator      TEXT DEFAULT '',
    PRIMARY KEY (product, test_category, sub_process, lot_id)
);

CREATE TABLE IF NOT EXISTS wafers (
    wafer_id    TEXT NOT NULL,
    lot_id      TEXT NOT NULL,
    product     TEXT NOT NULL,
    test_category TEXT NOT NULL,
    sub_process TEXT NOT NULL DEFAULT '',
    head_num    INTEGER DEFAULT 0,
    start_time  TIMESTAMPTZ,
    finish_time TIMESTAMPTZ,
    part_count  INTEGER DEFAULT 0,
    good_count  INTEGER DEFAULT 0,
    rtst_count  INTEGER DEFAULT 0,
    abrt_count  INTEGER DEFAULT 0,
    test_rev    TEXT DEFAULT '',
    retest_num  INTEGER DEFAULT 0,
    source_file TEXT DEFAULT '',
    PRIMARY KEY (product, test_category, sub_process, lot_id, wafer_id, retest_num)
);

CREATE TABLE IF NOT EXISTS parts (
    part_id     TEXT NOT NULL,
    lot_id      TEXT NOT NULL,
    wafer_id    TEXT NOT NULL,
    product     TEXT NOT NULL,
    test_category TEXT NOT NULL,
    sub_process TEXT NOT NULL DEFAULT '',
    head_num    INTEGER DEFAULT 0,
    site_num    INTEGER DEFAULT 0,
    x_coord     INTEGER DEFAULT -32768,
    y_coord     INTEGER DEFAULT -32768,
    hard_bin    INTEGER DEFAULT 0,
    soft_bin    INTEGER DEFAULT 0,
    passed      BOOLEAN DEFAULT FALSE,
    test_count  INTEGER DEFAULT 0,
    test_time   INTEGER DEFAULT 0,
    PRIMARY KEY (product, test_category, sub_process, lot_id, wafer_id, part_id)
);

CREATE TABLE IF NOT EXISTS test_data (
    lot_id      TEXT NOT NULL,
    wafer_id    TEXT NOT NULL,
    part_id     TEXT NOT NULL,
    product     TEXT NOT NULL,
    test_category TEXT NOT NULL,
    sub_process TEXT NOT NULL DEFAULT '',
    x_coord     INTEGER DEFAULT -32768,
    y_coord     INTEGER DEFAULT -32768,
    test_num    INTEGER NOT NULL,
    test_name   TEXT DEFAULT '',
    rec_type    TEXT DEFAULT 'PTR',
    lo_limit    DOUBLE PRECISION,
    hi_limit    DOUBLE PRECISION,
    units       TEXT DEFAULT '',
    result      DOUBLE PRECISION,
    passed      TEXT DEFAULT 'P'
);

-- ============================================================
-- Indexes (optimized for JMP query patterns)
-- ============================================================

-- lots
CREATE INDEX IF NOT EXISTS idx_lots_product ON lots (product);

-- wafers
CREATE INDEX IF NOT EXISTS idx_wafers_lot ON wafers (lot_id);
CREATE INDEX IF NOT EXISTS idx_wafers_product ON wafers (product, lot_id);

-- parts
CREATE INDEX IF NOT EXISTS idx_parts_lot_wafer ON parts (lot_id, wafer_id);
CREATE INDEX IF NOT EXISTS idx_parts_product ON parts (product, lot_id);
CREATE INDEX IF NOT EXISTS idx_parts_soft_bin ON parts (product, soft_bin);

-- test_data (most critical for JMP)
CREATE INDEX IF NOT EXISTS idx_test_data_lot_wafer ON test_data (lot_id, wafer_id);
CREATE INDEX IF NOT EXISTS idx_test_data_product_test ON test_data (product, test_num);
CREATE INDEX IF NOT EXISTS idx_test_data_product_name ON test_data (product, test_name);
CREATE INDEX IF NOT EXISTS idx_test_data_part ON test_data (lot_id, wafer_id, part_id);

-- ============================================================
-- Read-only user for JMP / BI tools
-- ============================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'stdf_reader') THEN
        CREATE ROLE stdf_reader WITH LOGIN PASSWORD 'stdf_read_only';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE stdf TO stdf_reader;
GRANT USAGE ON SCHEMA public TO stdf_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO stdf_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO stdf_reader;
