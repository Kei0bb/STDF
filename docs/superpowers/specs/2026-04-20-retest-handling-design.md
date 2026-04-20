# Retest Handling Design

**Date:** 2026-04-20  
**Scope:** `parts` / `test_data` tables + DuckDB VIEW layer + API queries

## Problem

`parts` and `test_data` currently have no `retest` partition. A second ingest of the same wafer overwrites `data.parquet`, silently discarding all initial-pass dies that were not retested. `get_fails` / `get_test_fail_rate` query raw `test_data` with only a `lot_id` filter, making fail rate denominator wrong when retest data is present.

## Retest Semantics

- Retest granularity: **wafer-unit** (one STDF file = one wafer)
- Partial retest: only failing dies are re-measured; initial-pass dies are absent from the retest file
- Final bin rule: **最終bin採用** — the most recent result per die is authoritative
- Die identity across files: `(lot_id, wafer_id, x_coord, y_coord)` — `part_id` is counter-based and resets per file, so it cannot be used for cross-file matching
- Migration: existing data will be deleted and re-ingested from scratch

## Design

### 1. Storage Layer

Add `retest={n}` partition to `parts` and `test_data`, mirroring the existing `wafers` pattern.

```
# Before
data/parts/.../lot_id=X/wafer_id=Y/data.parquet

# After
data/parts/.../lot_id=X/wafer_id=Y/retest=0/data.parquet
data/parts/.../lot_id=X/wafer_id=Y/retest=1/data.parquet
data/test_data/.../lot_id=X/wafer_id=Y/retest=0/data.parquet
data/test_data/.../lot_id=X/wafer_id=Y/retest=1/data.parquet
```

**Changes to `storage.py`:**
- Reuse `_get_next_retest_num()` for `parts` and `test_data` path construction
- Add `retest_num: int64` column to both `PARTS_SCHEMA` and `TEST_DATA_SCHEMA`
- Write `retest_num` value into each row

### 2. DuckDB VIEW Layer

Register two VIEWs at connection initialization time. These reconstruct the "complete final wafer" by selecting the highest-retest-num result per die.

```sql
CREATE VIEW parts_final AS
SELECT * EXCLUDE (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY lot_id, wafer_id, x_coord, y_coord
        ORDER BY retest_num DESC
    ) AS rn
    FROM parts
) WHERE rn = 1;

CREATE VIEW test_data_final AS
SELECT * EXCLUDE (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY lot_id, wafer_id, x_coord, y_coord, test_num
        ORDER BY retest_num DESC
    ) AS rn
    FROM test_data
) WHERE rn = 1;
```

**Behavior:**
- Dies present only in `retest=0` (initial pass, not retested) → selected from retest=0
- Dies present in `retest=1` (retested) → retest=1 result selected, overriding retest=0
- Result: `parts_final` = complete wafer with final-bin result for every die

VIEW registration location: `database.py` connection initialization and `server.py` lifespan (DuckDB `:memory:` requires re-registration per session).

### 3. API Layer

Replace all raw `parts` / `test_data` references with the VIEW equivalents. No logic changes — only the table name changes.

| File | Endpoint | Change |
|---|---|---|
| `web/api/data.py` | `GET /wafermap` | `FROM parts` → `FROM parts_final` |
| `web/api/data.py` | `GET /fails` | `FROM test_data` → `FROM test_data_final` |
| `web/api/data.py` | `GET /distribution` | `FROM test_data` → `FROM test_data_final` |
| `web/api/data.py` | `GET /tests` | `FROM test_data` → `FROM test_data_final` |
| `database.py` | `get_test_fail_rate()` | `FROM test_data` → `FROM test_data_final` |
| `web/api/export.py` | CSV export | `FROM test_data` → `FROM test_data_final` |

## Fail Rate Semantics After Fix

```
denominator = COUNT(DISTINCT (x_coord, y_coord)) in parts_final for the wafer
numerator   = dies where passed='F' in test_data_final
fail_rate   = 100.0 * numerator / denominator
```

Both numerator and denominator now reflect the final-bin result per die, eliminating double-counting from retest.

## Implementation Notes

- **`x_coord=-32768` (unknown coordinate):** STDF uses -32768 as sentinel for "coordinate unknown". Dies with this value cannot be uniquely identified by position. In practice these appear in FT (final test) flows without wafer maps. The VIEW PARTITION BY `x_coord, y_coord` will group all unknown-coord dies together, which may cause incorrect rn selection. If FT retests are in scope, a `part_id`-based fallback or separate handling is needed. For CP (wafer sort) flows this is not a concern.
- **`retest_num` consistency across tables:** `_get_next_retest_num()` scans the `wafers` partition directory. `parts` and `test_data` must call the same function in the same ingest transaction so all three tables get the same `retest_num` for a given wafer.

## Out of Scope

- `wafers` table: already has correct `retest` partition and latest-retest queries — no change needed
- `lots` table: lot-level metadata, no die-level concern
- CLI `analyze test-fail` and `analyze bin-summary`: follow the same VIEW swap as web API
