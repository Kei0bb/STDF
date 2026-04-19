# Retest Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `retest={n}` partitioning to `parts` and `test_data` tables and register `parts_final`/`test_data_final` DuckDB VIEWs that return the latest-retest result per die, so fail-rate queries and wafermap reflect the true final-bin state.

**Architecture:** Pre-calculate `retest_num` per wafer before any table is written; use the same value for `wafers`, `parts`, and `test_data` paths. Two DuckDB VIEWs (`parts_final`, `test_data_final`) apply `ROW_NUMBER() OVER (PARTITION BY lot_id, wafer_id, x_coord, y_coord ORDER BY retest_num DESC)` to reconstruct the complete final-bin wafer. All API endpoints switch from raw tables to these VIEWs.

**Tech Stack:** Python 3.13, PyArrow, DuckDB, FastAPI, pytest

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/stdf_platform/storage.py` | Modify | Add `retest_num` to PARTS_SCHEMA / TEST_DATA_SCHEMA; pre-calculate retest_num map; add retest partition to parts/test_data paths |
| `src/stdf_platform/web/api/deps.py` | Modify | Register `parts_final` and `test_data_final` VIEWs in `setup_views()` |
| `src/stdf_platform/database.py` | Modify | Register same VIEWs in `_create_views()`; update `get_test_fail_rate()` |
| `src/stdf_platform/web/api/data.py` | Modify | Switch 4 endpoints from raw tables to `*_final` VIEWs |
| `src/stdf_platform/web/api/export.py` | Modify | Switch `export_csv` and `export_preview` to `*_final` VIEWs |
| `tests/conftest.py` | Create | Shared fixtures: sample STDFData, tmp storage |
| `tests/test_retest_storage.py` | Create | Storage: retest partition paths, retest_num column |
| `tests/test_retest_view.py` | Create | DuckDB VIEW: die-level merge, final-bin selection |

---

## Task 1: Add pytest dev dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add pytest to dev dependencies**

Edit `pyproject.toml`:
```toml
[dependency-groups]
dev = [
    "httpx>=0.28.1",
    "pytest>=8.0.0",
]
```

- [ ] **Step 2: Install**

```bash
uv sync
```

Expected: resolves without error, `pytest` available.

- [ ] **Step 3: Verify pytest runs**

```bash
uv run pytest --version
```

Expected: `pytest 8.x.x`

---

## Task 2: Add `retest_num` column to PARTS_SCHEMA and TEST_DATA_SCHEMA

**Files:**
- Modify: `src/stdf_platform/storage.py:59-93`

- [ ] **Step 1: Write failing test**

Create `tests/test_retest_storage.py`:

```python
import pyarrow as pa
from stdf_platform.storage import PARTS_SCHEMA, TEST_DATA_SCHEMA


def test_parts_schema_has_retest_num():
    assert PARTS_SCHEMA.get_field_index("retest_num") >= 0
    field = PARTS_SCHEMA.field("retest_num")
    assert pa.types.is_integer(field.type)


def test_test_data_schema_has_retest_num():
    assert TEST_DATA_SCHEMA.get_field_index("retest_num") >= 0
    field = TEST_DATA_SCHEMA.field("retest_num")
    assert pa.types.is_integer(field.type)
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/test_retest_storage.py::test_parts_schema_has_retest_num tests/test_retest_storage.py::test_test_data_schema_has_retest_num -v
```

Expected: FAILED — `get_field_index` returns -1

- [ ] **Step 3: Add `retest_num` to PARTS_SCHEMA**

In `storage.py`, find `PARTS_SCHEMA`:
```python
PARTS_SCHEMA = pa.schema([
    ("part_id", pa.string()),
    ("lot_id", pa.string()),
    ("wafer_id", pa.string()),
    ("head_num", pa.int64()),
    ("site_num", pa.int64()),
    ("x_coord", pa.int64()),
    ("y_coord", pa.int64()),
    ("hard_bin", pa.int64()),
    ("soft_bin", pa.int64()),
    ("passed", pa.bool_()),
    ("test_count", pa.int64()),
    ("test_time", pa.int64()),
    ("retest_num", pa.int64()),
])
```

- [ ] **Step 4: Add `retest_num` to TEST_DATA_SCHEMA**

In `storage.py`, find `TEST_DATA_SCHEMA`:
```python
TEST_DATA_SCHEMA = pa.schema([
    ("lot_id", pa.string()),
    ("wafer_id", pa.string()),
    ("part_id", pa.string()),
    ("x_coord", pa.int64()),
    ("y_coord", pa.int64()),
    ("test_num", pa.int64()),
    ("test_name", pa.string()),
    ("rec_type", pa.string()),
    ("lo_limit", pa.float64()),
    ("hi_limit", pa.float64()),
    ("units", pa.string()),
    ("result", pa.float64()),
    ("passed", pa.string()),
    ("retest_num", pa.int64()),
])
```

- [ ] **Step 5: Run tests to verify pass**

```bash
uv run pytest tests/test_retest_storage.py::test_parts_schema_has_retest_num tests/test_retest_storage.py::test_test_data_schema_has_retest_num -v
```

Expected: PASSED

- [ ] **Step 6: Commit**

```bash
git add src/stdf_platform/storage.py tests/test_retest_storage.py
git commit -m "feat: add retest_num to PARTS_SCHEMA and TEST_DATA_SCHEMA"
```

---

## Task 3: Pre-calculate retest_num map and apply to parts/test_data storage paths

**Files:**
- Modify: `src/stdf_platform/storage.py:251-385`

**Critical invariant:** `retest_num` must be calculated BEFORE writing `wafers` (since `_get_next_retest_num` scans the `wafers` directory — calling it after wafers are written would return `n+1` instead of `n`).

- [ ] **Step 1: Write failing tests**

Append to `tests/test_retest_storage.py`:

```python
import tempfile
from pathlib import Path
from stdf_platform.storage import ParquetStorage
from stdf_platform.config import StorageConfig
from stdf_platform.parser import STDFData
import pyarrow.parquet as pq


def _make_storage(tmp_path: Path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def _make_stdf_data(lot_id="LOT1", wafer_id="W1") -> STDFData:
    data = STDFData()
    data.lot_id = lot_id
    data.part_type = "TEST"
    data.job_name = "TEST_JOB"
    data.job_rev = "A"
    data.start_time = 0
    data.finish_time = 0
    data.tester_type = "TESTER"
    data.operator = "OP"
    data._current_wafer = wafer_id
    data.wafers = [{
        "wafer_id": wafer_id, "head_num": 1,
        "start_time": 0, "finish_time": 0,
        "part_count": 2, "good_count": 1,
        "rtst_count": 0, "abrt_count": 0,
    }]
    data.parts = [
        {"part_id": f"{lot_id}_{wafer_id}_0", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 1, "y_coord": 1,
         "hard_bin": 1, "soft_bin": 1, "passed": True, "test_count": 1, "test_time": 100},
        {"part_id": f"{lot_id}_{wafer_id}_1", "lot_id": lot_id, "wafer_id": wafer_id,
         "head_num": 1, "site_num": 1, "x_coord": 2, "y_coord": 2,
         "hard_bin": 0, "soft_bin": 0, "passed": False, "test_count": 1, "test_time": 100},
    ]
    data.tests = {1: {"test_name": "VCC", "rec_type": "PTR", "lo_limit": 0.9, "hi_limit": 1.1, "units": "V"}}
    data.test_results = [
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": f"{lot_id}_{wafer_id}_0",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 1.0, "passed": True},
        {"lot_id": lot_id, "wafer_id": wafer_id, "part_id": f"{lot_id}_{wafer_id}_1",
         "test_num": 1, "head_num": 1, "site_num": 1, "result": 0.5, "passed": False},
    ]
    return data


def test_parts_first_ingest_writes_retest_0(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_stdf_data()
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="test.stdf")

    parts_path = tmp_path / "parts"
    retest_dirs = list((parts_path / "product=PROD" / "test_category=CP" /
                        "sub_process=" / "lot_id=LOT1" / "wafer_id=W1").iterdir())
    assert len(retest_dirs) == 1
    assert retest_dirs[0].name == "retest=0"


def test_parts_retest_writes_retest_1(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_stdf_data()
    # First ingest
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="test.stdf")
    # Second ingest (retest — only failing die)
    data2 = _make_stdf_data()
    data2.parts = [data2.parts[1]]  # Only the failing die
    data2.test_results = [data2.test_results[1]]
    storage.save_stdf_data(data2, product="PROD", test_category="CP", source_file="test_retest.stdf")

    parts_base = (tmp_path / "parts" / "product=PROD" / "test_category=CP" /
                  "sub_process=" / "lot_id=LOT1" / "wafer_id=W1")
    retest_dirs = sorted([d.name for d in parts_base.iterdir()])
    assert retest_dirs == ["retest=0", "retest=1"]


def test_parts_parquet_has_retest_num_column(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_stdf_data()
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="test.stdf")

    parquet_path = (tmp_path / "parts" / "product=PROD" / "test_category=CP" /
                    "sub_process=" / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet")
    table = pq.read_table(parquet_path)
    assert "retest_num" in table.schema.names
    assert table["retest_num"][0].as_py() == 0


def test_test_data_parquet_has_retest_num_column(tmp_path):
    storage = _make_storage(tmp_path)
    data = _make_stdf_data()
    storage.save_stdf_data(data, product="PROD", test_category="CP", source_file="test.stdf")

    parquet_path = (tmp_path / "test_data" / "product=PROD" / "test_category=CP" /
                    "sub_process=" / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet")
    table = pq.read_table(parquet_path)
    assert "retest_num" in table.schema.names
    assert table["retest_num"][0].as_py() == 0
```

- [ ] **Step 2: Check StorageConfig and ParquetStorage class names**

```bash
uv run python -c "from stdf_platform.storage import ParquetStorage; from stdf_platform.config import StorageConfig; print('ok')"
```

If import fails, find the correct class names:
```bash
grep -n "^class " src/stdf_platform/storage.py src/stdf_platform/config.py
```

Update test imports to match actual class names.

- [ ] **Step 3: Run to verify failure**

```bash
uv run pytest tests/test_retest_storage.py -v -k "retest_0 or retest_1 or retest_num_column"
```

Expected: FAILED — parts paths don't have retest= directory

- [ ] **Step 4: Pre-calculate wafer_retest_map in `save_stdf_data`**

In `storage.py`, replace the section starting at `# Save wafers with retest tracking` (around line 251). The full updated `save_stdf_data` body from just before wafer saving through the end of parts/test_data saving:

```python
        # Pre-calculate retest_num for every wafer BEFORE writing any table.
        # _get_next_retest_num scans the wafers directory; calling it after
        # wafers are written would return n+1 instead of n.
        wafer_retest_map: dict[str, int] = {}
        if data.wafers:
            for wafer in data.wafers:
                wafer_id = wafer.get("wafer_id", "")
                if wafer_id not in wafer_retest_map:
                    wafer_retest_map[wafer_id] = self._get_next_retest_num(
                        product, test_category, sub_process, data.lot_id, wafer_id
                    )

        # Save wafers with retest tracking
        if data.wafers:
            wafer_groups: dict[str, list] = {}
            for wafer in data.wafers:
                wafer_id = wafer.get("wafer_id", "")
                if wafer_id not in wafer_groups:
                    wafer_groups[wafer_id] = []
                wafer_groups[wafer_id].append(wafer)

            for wafer_id, wafers in wafer_groups.items():
                retest_num = wafer_retest_map[wafer_id]
                wafer_path = (
                    self._get_table_path("wafers", product, test_category, sub_process)
                    / f"lot_id={self._sanitize(data.lot_id)}"
                    / f"wafer_id={self._sanitize(wafer_id)}"
                    / f"retest={retest_num}"
                )
                wafer_path.mkdir(parents=True, exist_ok=True)

                wafer_table = pa.table({
                    "wafer_id": [w.get("wafer_id", "") for w in wafers],
                    "lot_id": [data.lot_id for _ in wafers],
                    "head_num": [w.get("head_num", 0) for w in wafers],
                    "start_time": [_unix_to_datetime(w.get("start_time", 0)) for w in wafers],
                    "finish_time": [_unix_to_datetime(w.get("finish_time", 0)) for w in wafers],
                    "part_count": [w.get("part_count", 0) for w in wafers],
                    "good_count": [w.get("good_count", 0) for w in wafers],
                    "rtst_count": [w.get("rtst_count", 0) for w in wafers],
                    "abrt_count": [w.get("abrt_count", 0) for w in wafers],
                    "test_rev": [test_rev for _ in wafers],
                    "retest_num": [retest_num for _ in wafers],
                    "source_file": [source_file for _ in wafers],
                }, schema=WAFERS_SCHEMA)
                self._write_parquet(wafer_table, wafer_path / "data.parquet", compression)

            counts["wafers"] = len(data.wafers)

        # Save parts (partitioned by lot_id, wafer_id, retest_num)
        if data.parts:
            part_groups: dict[tuple, list] = {}
            for part in data.parts:
                key = (part.get("lot_id", ""), part.get("wafer_id", ""))
                if key not in part_groups:
                    part_groups[key] = []
                part_groups[key].append(part)

            for (lot_id, wafer_id), parts in part_groups.items():
                retest_num = wafer_retest_map.get(wafer_id, 0)
                part_path = (
                    self._get_table_path("parts", product, test_category, sub_process)
                    / f"lot_id={self._sanitize(lot_id)}"
                    / f"wafer_id={self._sanitize(wafer_id)}"
                    / f"retest={retest_num}"
                )
                part_path.mkdir(parents=True, exist_ok=True)

                part_table = pa.table({
                    "part_id": [p.get("part_id", "") for p in parts],
                    "lot_id": [p.get("lot_id", "") for p in parts],
                    "wafer_id": [p.get("wafer_id", "") for p in parts],
                    "head_num": [p.get("head_num", 0) for p in parts],
                    "site_num": [p.get("site_num", 0) for p in parts],
                    "x_coord": [p.get("x_coord", -32768) for p in parts],
                    "y_coord": [p.get("y_coord", -32768) for p in parts],
                    "hard_bin": [p.get("hard_bin", 0) for p in parts],
                    "soft_bin": [p.get("soft_bin", 0) for p in parts],
                    "passed": [p.get("passed", False) for p in parts],
                    "test_count": [p.get("test_count", 0) for p in parts],
                    "test_time": [p.get("test_time", 0) for p in parts],
                    "retest_num": [retest_num for _ in parts],
                }, schema=PARTS_SCHEMA)
                self._write_parquet(part_table, part_path / "data.parquet", compression)

            counts["parts"] = len(data.parts)
```

- [ ] **Step 5: Add retest partition to test_data write path**

In `storage.py`, replace the test_data write section (around line 330):

```python
        # Save unified test data (merged tests + test_results)
        if data.test_results:
            part_coords = {}
            for part in data.parts:
                part_id = part.get("part_id", "")
                part_coords[part_id] = (
                    part.get("x_coord", -32768),
                    part.get("y_coord", -32768),
                )

            result_groups: dict[tuple, list] = {}
            for result in data.test_results:
                key = (result.get("lot_id", ""), result.get("wafer_id", ""))
                if key not in result_groups:
                    result_groups[key] = []
                result_groups[key].append(result)

            for (lot_id, wafer_id), results in result_groups.items():
                retest_num = wafer_retest_map.get(wafer_id, 0)
                result_path = (
                    self._get_table_path("test_data", product, test_category, sub_process)
                    / f"lot_id={self._sanitize(lot_id)}"
                    / f"wafer_id={self._sanitize(wafer_id)}"
                    / f"retest={retest_num}"
                )
                result_path.mkdir(parents=True, exist_ok=True)

                enriched = []
                for r in results:
                    test_num = r.get("test_num", 0)
                    test_info = data.tests.get(test_num, {})
                    part_id = r.get("part_id", "")
                    x_coord, y_coord = part_coords.get(part_id, (-32768, -32768))
                    enriched.append({
                        "lot_id": r.get("lot_id", ""),
                        "wafer_id": r.get("wafer_id", ""),
                        "part_id": part_id,
                        "x_coord": x_coord,
                        "y_coord": y_coord,
                        "test_num": test_num,
                        "test_name": test_info.get("test_name", ""),
                        "rec_type": test_info.get("rec_type", "PTR"),
                        "lo_limit": test_info.get("lo_limit"),
                        "hi_limit": test_info.get("hi_limit"),
                        "units": test_info.get("units", ""),
                        "result": r.get("result"),
                        "passed": "P" if r.get("passed", False) else "F",
                        "retest_num": retest_num,
                    })

                result_table = pa.table({
                    "lot_id": [r["lot_id"] for r in enriched],
                    "wafer_id": [r["wafer_id"] for r in enriched],
                    "part_id": [r["part_id"] for r in enriched],
                    "x_coord": [r["x_coord"] for r in enriched],
                    "y_coord": [r["y_coord"] for r in enriched],
                    "test_num": [r["test_num"] for r in enriched],
                    "test_name": [r["test_name"] for r in enriched],
                    "rec_type": [r["rec_type"] for r in enriched],
                    "lo_limit": [r["lo_limit"] for r in enriched],
                    "hi_limit": [r["hi_limit"] for r in enriched],
                    "units": [r["units"] for r in enriched],
                    "result": [r["result"] for r in enriched],
                    "passed": [r["passed"] for r in enriched],
                    "retest_num": [r["retest_num"] for r in enriched],
                }, schema=TEST_DATA_SCHEMA)
                self._write_parquet(result_table, result_path / "data.parquet", compression)

            counts["test_data"] = len(data.test_results)
```

- [ ] **Step 6: Run all storage tests**

```bash
uv run pytest tests/test_retest_storage.py -v
```

Expected: all PASSED

- [ ] **Step 7: Commit**

```bash
git add src/stdf_platform/storage.py tests/test_retest_storage.py
git commit -m "feat: add retest partition to parts and test_data storage"
```

---

## Task 4: Register parts_final and test_data_final VIEWs

**Files:**
- Modify: `src/stdf_platform/web/api/deps.py`
- Modify: `src/stdf_platform/database.py`

- [ ] **Step 1: Write failing VIEW test**

Create `tests/test_retest_view.py`:

```python
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path


def _write_parts_parquet(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([
        ("part_id", pa.string()), ("lot_id", pa.string()), ("wafer_id", pa.string()),
        ("head_num", pa.int64()), ("site_num", pa.int64()),
        ("x_coord", pa.int64()), ("y_coord", pa.int64()),
        ("hard_bin", pa.int64()), ("soft_bin", pa.int64()),
        ("passed", pa.bool_()), ("test_count", pa.int64()), ("test_time", pa.int64()),
        ("retest_num", pa.int64()),
    ])
    table = pa.table({k: [r[k] for r in rows] for k in schema.names}, schema=schema)
    pq.write_table(table, path)


def _setup_conn(data_dir: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    parts_path = data_dir / "parts"
    conn.execute(f"""
        CREATE VIEW parts AS
        SELECT * FROM read_parquet('{parts_path.as_posix()}/**/*.parquet', hive_partitioning=true)
    """)
    conn.execute("""
        CREATE VIEW parts_final AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY lot_id, wafer_id, x_coord, y_coord
                ORDER BY retest_num DESC
            ) AS rn FROM parts
        ) WHERE rn = 1
    """)
    return conn


def test_parts_final_takes_latest_retest(tmp_path):
    # retest=0: die (1,1) passed=False, die (2,2) passed=True
    _write_parts_parquet(
        tmp_path / "parts" / "product=PROD" / "test_category=CP" / "sub_process=" /
        "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet",
        [
            {"part_id": "L_W_0", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": 1, "y_coord": 1, "hard_bin": 0, "soft_bin": 0, "passed": False,
             "test_count": 1, "test_time": 100, "retest_num": 0},
            {"part_id": "L_W_1", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": 2, "y_coord": 2, "hard_bin": 1, "soft_bin": 1, "passed": True,
             "test_count": 1, "test_time": 100, "retest_num": 0},
        ]
    )
    # retest=1: only die (1,1) retested — now passed=True
    _write_parts_parquet(
        tmp_path / "parts" / "product=PROD" / "test_category=CP" / "sub_process=" /
        "lot_id=LOT1" / "wafer_id=W1" / "retest=1" / "data.parquet",
        [
            {"part_id": "L_W_0", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": 1, "y_coord": 1, "hard_bin": 1, "soft_bin": 1, "passed": True,
             "test_count": 1, "test_time": 100, "retest_num": 1},
        ]
    )
    conn = _setup_conn(tmp_path)
    rows = conn.execute(
        "SELECT x_coord, y_coord, passed, retest_num FROM parts_final WHERE lot_id='LOT1' ORDER BY x_coord"
    ).fetchall()
    # die (1,1): should pick retest=1 (passed=True)
    assert rows[0] == (1, 1, True, 1)
    # die (2,2): should pick retest=0 (passed=True, only copy)
    assert rows[1] == (2, 2, True, 0)
    assert len(rows) == 2  # full wafer, not just retest dies


def test_parts_final_fail_rate_uses_full_wafer_denominator(tmp_path):
    _write_parts_parquet(
        tmp_path / "parts" / "product=PROD" / "test_category=CP" / "sub_process=" /
        "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet",
        [
            {"part_id": f"L_W_{i}", "lot_id": "LOT1", "wafer_id": "W1", "head_num": 1, "site_num": 1,
             "x_coord": i, "y_coord": 0, "hard_bin": 1 if i < 8 else 0,
             "soft_bin": 1 if i < 8 else 0, "passed": i < 8,
             "test_count": 1, "test_time": 100, "retest_num": 0}
            for i in range(10)
        ]
    )
    conn = _setup_conn(tmp_path)
    total = conn.execute("SELECT COUNT(*) FROM parts_final WHERE lot_id='LOT1'").fetchone()[0]
    assert total == 10  # all 10 dies present
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/test_retest_view.py -v
```

Expected: FAILED — `parts_final` VIEW does not exist

- [ ] **Step 3: Add VIEW definitions to `deps.py`**

In `src/stdf_platform/web/api/deps.py`, update `setup_views`:

```python
def setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> list[str]:
    """Register Parquet glob views and final-bin merge VIEWs."""
    registered = []
    for table in ["lots", "wafers", "parts", "test_data"]:
        path = data_dir / table
        if path.exists():
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table} AS
                SELECT * FROM read_parquet(
                    '{path.as_posix()}/**/*.parquet', hive_partitioning=true
                )
            """)
            registered.append(table)

    if "parts" in registered:
        conn.execute("""
            CREATE OR REPLACE VIEW parts_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, wafer_id, x_coord, y_coord
                    ORDER BY retest_num DESC
                ) AS rn FROM parts
            ) WHERE rn = 1
        """)
        registered.append("parts_final")

    if "test_data" in registered:
        conn.execute("""
            CREATE OR REPLACE VIEW test_data_final AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY lot_id, wafer_id, x_coord, y_coord, test_num
                    ORDER BY retest_num DESC
                ) AS rn FROM test_data
            ) WHERE rn = 1
        """)
        registered.append("test_data_final")

    return registered
```

- [ ] **Step 4: Add same VIEWs to `database.py` `_create_views`**

In `src/stdf_platform/database.py`, update `_create_views`:

```python
    def _create_views(self) -> None:
        """Create views for Parquet datasets."""
        tables = ["lots", "wafers", "parts", "test_data"]
        registered = []

        for table in tables:
            table_path = self.data_dir / table
            if table_path.exists():
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table} AS
                    SELECT * FROM read_parquet('{table_path}/**/*.parquet', hive_partitioning=true)
                """)
                registered.append(table)

        if "parts" in registered:
            self.conn.execute("""
                CREATE OR REPLACE VIEW parts_final AS
                SELECT * EXCLUDE (rn) FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY lot_id, wafer_id, x_coord, y_coord
                        ORDER BY retest_num DESC
                    ) AS rn FROM parts
                ) WHERE rn = 1
            """)

        if "test_data" in registered:
            self.conn.execute("""
                CREATE OR REPLACE VIEW test_data_final AS
                SELECT * EXCLUDE (rn) FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY lot_id, wafer_id, x_coord, y_coord, test_num
                        ORDER BY retest_num DESC
                    ) AS rn FROM test_data
                ) WHERE rn = 1
            """)
```

- [ ] **Step 5: Run VIEW tests**

```bash
uv run pytest tests/test_retest_view.py -v
```

Expected: all PASSED

- [ ] **Step 6: Commit**

```bash
git add src/stdf_platform/web/api/deps.py src/stdf_platform/database.py tests/test_retest_view.py
git commit -m "feat: register parts_final and test_data_final VIEWs"
```

---

## Task 5: Switch API endpoints to *_final VIEWs

**Files:**
- Modify: `src/stdf_platform/web/api/data.py`
- Modify: `src/stdf_platform/database.py` (get_test_fail_rate)

- [ ] **Step 1: Update `get_wafermap` in `data.py`**

Find line:
```python
                FROM parts
                WHERE lot_id = ? AND wafer_id = ?
```
Replace with:
```python
                FROM parts_final
                WHERE lot_id = ? AND wafer_id = ?
```

- [ ] **Step 2: Update `get_fails` in `data.py`**

Find:
```python
                FROM test_data
                WHERE lot_id IN ({placeholders})
                GROUP BY test_num, test_name
                HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
```
Replace with:
```python
                FROM test_data_final
                WHERE lot_id IN ({placeholders})
                GROUP BY test_num, test_name
                HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
```

- [ ] **Step 3: Update `get_distribution` in `data.py`**

Find both occurrences of `FROM test_data` inside `get_distribution` (meta query and vals query), replace with `FROM test_data_final`.

- [ ] **Step 4: Update `get_tests` in `data.py`**

Find:
```python
                FROM test_data
                WHERE lot_id IN ({placeholders})
                ORDER BY test_num
```
Replace with:
```python
                FROM test_data_final
                WHERE lot_id IN ({placeholders})
                ORDER BY test_num
```

- [ ] **Step 5: Update `get_test_fail_rate` in `database.py`**

Find:
```python
        FROM test_data
        WHERE lot_id = $1
```
Replace with:
```python
        FROM test_data_final
        WHERE lot_id = $1
```

- [ ] **Step 6: Verify the server starts cleanly**

```bash
uv run stdf web &
sleep 2
curl -s http://localhost:8000/api/lots | head -c 200
kill %1
```

Expected: JSON response (empty array `[]` is fine if no data)

- [ ] **Step 7: Commit**

```bash
git add src/stdf_platform/web/api/data.py src/stdf_platform/database.py
git commit -m "feat: switch data API endpoints to parts_final and test_data_final VIEWs"
```

---

## Task 6: Switch export endpoints to *_final VIEWs

**Files:**
- Modify: `src/stdf_platform/web/api/export.py`

The JOIN in `export_csv` is `FROM test_data td JOIN parts p ON td.part_id = p.part_id AND td.lot_id = p.lot_id`. Both VIEWs select the same `retest_num` per die (via x_coord/y_coord partition), so `part_id` matches correctly between `test_data_final` and `parts_final`.

- [ ] **Step 1: Update pivot export SQL**

Find in `export_csv`:
```python
                FROM test_data td
                JOIN parts p ON td.part_id = p.part_id AND td.lot_id = p.lot_id
                WHERE {where}
                ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_name
```
Replace with:
```python
                FROM test_data_final td
                JOIN parts_final p ON td.part_id = p.part_id AND td.lot_id = p.lot_id
                WHERE {where}
                ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_name
```

- [ ] **Step 2: Update long format export SQL**

Find:
```python
                FROM test_data td
                JOIN parts p ON td.part_id = p.part_id AND td.lot_id = p.lot_id
                WHERE {where}
                ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_num
```
Replace with:
```python
                FROM test_data_final td
                JOIN parts_final p ON td.part_id = p.part_id AND td.lot_id = p.lot_id
                WHERE {where}
                ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_num
```

- [ ] **Step 3: Update export_preview SQL**

Find:
```python
                FROM test_data td
                WHERE {where}
```
Replace with:
```python
                FROM test_data_final td
                WHERE {where}
```

- [ ] **Step 4: Run all tests**

```bash
uv run pytest tests/ -v
```

Expected: all PASSED

- [ ] **Step 5: Commit**

```bash
git add src/stdf_platform/web/api/export.py
git commit -m "feat: switch export endpoints to test_data_final and parts_final VIEWs"
```

---

## Task 7: End-to-end smoke test with real data

This task verifies the complete flow with actual STDF files.

- [ ] **Step 1: Delete existing data**

```bash
rm -rf data/
```

- [ ] **Step 2: Generate test STDF files**

```bash
uv run python make_test_stdf.py
```

Expected: synthetic STDF files created in `test_data/`

- [ ] **Step 3: Ingest**

```bash
stdf ingest test_data/<first_file>.stdf --product TEST
```

Expected: no error, confirms parts/wafers/test_data row counts

- [ ] **Step 4: Verify retest=0 partition exists for all three tables**

```bash
find data/parts data/test_data -name "data.parquet" | head -20
```

Expected: paths contain `retest=0/data.parquet`

- [ ] **Step 5: Start web server and verify /api/fails responds**

```bash
uv run stdf web &
sleep 2
# Replace LOT_ID with an actual lot from the ingested data
curl -s "http://localhost:8000/api/fails?lot=<LOT_ID>"
kill %1
```

Expected: JSON array with `test_num`, `test_name`, `fail_count`, `fail_rate`

- [ ] **Step 6: Final commit**

```bash
git add -A
git commit -m "chore: verify retest handling end-to-end with test data"
```
