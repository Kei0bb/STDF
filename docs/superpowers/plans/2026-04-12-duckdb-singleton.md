# DuckDB Singleton Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ClickHouse 依存を除去し、DuckDB シングルトン接続をバックエンドにした Windows ネイティブ動作の POC 環境を構築する。

**Architecture:** `_ingest_worker` は Parquet のみ書き込む（CH 書き込みを削除）。Web API は起動時に DuckDB `:memory:` 接続を1回作成し、glob ビューを登録。`threading.Lock` で並列リクエストをシリアライズする（FastAPI の `def` エンドポイントはスレッドプールで動作するため `asyncio.Lock` は不適）。

**Tech Stack:** Python 3.10+, DuckDB 0.9+, PyArrow 14+, FastAPI, Parquet (Hive-partitioned)

---

## ファイルマップ

| ファイル | 変更種別 | 内容 |
|---|---|---|
| `src/stdf_platform/storage.py` | Modify | `save_stdf_data()` 戻り値を `dict[str,int]` に（`pa_tables` 削除） |
| `src/stdf_platform/_ingest_worker.py` | Modify | CH 書き込みブロック削除、storage の戻り値を unpack しない |
| `src/stdf_platform/worker.py` | Modify | `ch_host` 引数削除 |
| `src/stdf_platform/cli.py` | Modify | `_run_ingest_batch` から `ch_host` 削除、`web` コマンドから CH env vars 削除 |
| `src/stdf_platform/web/api/deps.py` | Rewrite | DuckDB singleton + `threading.Lock`、`get_db` 依存関数 |
| `src/stdf_platform/web/server.py` | Rewrite | DuckDB lifespan（CH lifespan を置き換え） |
| `src/stdf_platform/web/api/filters.py` | Rewrite | DuckDB SQL（CH 構文 → 標準 SQL） |
| `src/stdf_platform/web/api/data.py` | Rewrite | DuckDB SQL |
| `src/stdf_platform/web/api/export.py` | Rewrite | DuckDB SQL |
| `query.py` | Modify | DuckDB デフォルト化 |
| `config.yaml.example` | Modify | `clickhouse.host: ""` |
| `pyproject.toml` | Modify | `clickhouse-connect` を optional に |

---

## Task 1: storage.py — pa_tables 収集を削除

**Files:**
- Modify: `src/stdf_platform/storage.py:220-415`

- [ ] **Step 1: `save_stdf_data` の型シグネチャを変更**

`storage.py:220` の関数シグネチャとdocstringを以下に変更:

```python
    def save_stdf_data(
        self,
        data: STDFData,
        product: str = "UNKNOWN",
        test_category: str = "UNKNOWN",
        sub_process: str = "",
        source_file: str = "",
        compression: str = "snappy",
    ) -> dict[str, int]:
        """
        Save STDF data to Parquet files.

        Returns:
            {table_name: row_count}
        """
        counts = {}
```

- [ ] **Step 2: `pa_tables` の初期化と収集をすべて削除**

`storage.py` の `save_stdf_data` 内から以下の行をすべて削除:
- `pa_tables: dict[str, pa.Table] = {}` （237行目付近）
- `pa_tables["lots"] = lot_table` （263行目付近）
- `pa_tables["wafers"] = wafer_table` および `pa_tables["wafers"] = _pa.concat_tables(...)` （302-306行目付近）
- `pa_tables["parts"] = part_table` および `pa_tables["parts"] = _pa.concat_tables(...)` （338-342行目付近）
- `pa_tables["test_data"] = result_table` および `pa_tables["test_data"] = _pa.concat_tables(...)` （409-413行目付近）
- 関数末尾の `return counts, pa_tables` を `return counts` に変更

- [ ] **Step 3: 動作確認**

```bash
cd /Users/keisuke/Projects/STDF
uv run python -c "
from src.stdf_platform.storage import ParquetStorage
import inspect
sig = inspect.signature(ParquetStorage.save_stdf_data)
print('Return annotation:', sig.return_annotation)
"
```

期待出力: `Return annotation: dict[str, int]`（または `<class 'dict'>` 相当）

- [ ] **Step 4: Commit**

```bash
git add src/stdf_platform/storage.py
git commit -m "refactor: remove pa_tables from save_stdf_data — Parquet-only output"
```

---

## Task 2: _ingest_worker.py — CH 書き込みブロックを削除

**Files:**
- Modify: `src/stdf_platform/_ingest_worker.py`

- [ ] **Step 1: `_counts, pa_tables =` を `counts =` に変更し、CH ブロックを削除**

`_ingest_worker.py:56-85` を以下に置き換え:

```python
    counts = storage.save_stdf_data(
        data,
        product=product,
        test_category=test_category,
        sub_process=sub_process,
        source_file=file_path.name,
        compression=compression,
    )
    t_save = time.monotonic() - t1
    print(f"[worker] saved parquet ({t_save:.1f}s)", file=sys.stderr)
```

（`ch_host = sys.argv[5] if ...` から始まる CH 書き込みブロック全体を削除）

- [ ] **Step 2: 動作確認（make_test_stdf.py でテストファイル生成して ingest）**

```bash
uv run python make_test_stdf.py
uv run stdf2pq ingest test_data/LOT001_CP11.stdf --product TEST
```

期待出力: `✓ Successfully ingested LOT001_CP11.stdf`（CH エラーメッセージなし）

- [ ] **Step 3: Commit**

```bash
git add src/stdf_platform/_ingest_worker.py
git commit -m "refactor: remove ClickHouse write from ingest worker — Parquet only"
```

---

## Task 3: worker.py — ch_host 引数を削除

**Files:**
- Modify: `src/stdf_platform/worker.py:39-61` および `116-123`

- [ ] **Step 1: `_run_single` から `ch_host` を削除**

`worker.py:39-47` の `_run_single` シグネチャを変更:

```python
def _run_single(
    local_path: Path,
    product: str,
    data_dir: Path,
    compression: str,
    timeout: int,
    log_path: Optional[Path],
) -> IngestResult:
```

`worker.py:54-61` の `cmd` リストから `ch_host` を削除:

```python
    cmd = [
        sys.executable, "-m", "stdf_platform._ingest_worker",
        str(local_path),
        product,
        str(data_dir),
        compression,
    ]
```

- [ ] **Step 2: `run_ingest_pool` から `ch_host` を削除**

`worker.py:116-123` のシグネチャを変更:

```python
def run_ingest_pool(
    files: list[tuple],
    data_dir: Path,
    compression: str,
    max_workers: int = 4,
    timeout: int = 300,
) -> tuple[list[IngestResult], list[IngestResult]]:
```

`worker.py:155` の `executor.submit(...)` から `ch_host` を削除:

```python
            future_map = {
                executor.submit(
                    _run_single,
                    local_path, product, data_dir, compression, timeout, log_path,
                ): (remote_path, local_path, product)
                for remote_path, local_path, product, _ttype in files
            }
```

- [ ] **Step 3: 動作確認**

```bash
uv run python -c "from stdf_platform.worker import run_ingest_pool; import inspect; print(inspect.signature(run_ingest_pool))"
```

期待出力: `(files, data_dir, compression, max_workers=4, timeout=300)` （ch_host なし）

- [ ] **Step 4: Commit**

```bash
git add src/stdf_platform/worker.py
git commit -m "refactor: remove ch_host from worker pool — no CH dependency"
```

---

## Task 4: cli.py — ch_host と CH env vars を削除

**Files:**
- Modify: `src/stdf_platform/cli.py:395-420` (`_run_ingest_batch`)
- Modify: `src/stdf_platform/cli.py` (`web` コマンド)

- [ ] **Step 1: `_run_ingest_batch` から `ch_host` を削除**

`cli.py:395-420` の `_run_ingest_batch` 関数を変更:

```python
def _run_ingest_batch(
    config,
    sync_manager,
    to_ingest: list[tuple],
    cleanup: bool,
    verbose: bool,
    timeout: int = 300,
    max_workers: int = 4,
):
    from .worker import run_ingest_pool

    successes, failures = run_ingest_pool(
        files=to_ingest,
        data_dir=config.storage.data_dir,
        compression=config.processing.compression,
        max_workers=max_workers,
        timeout=timeout,
    )
```

（`ch_host = config.clickhouse.host if config.clickhouse.enabled else ""` 行と `ch_host=ch_host` 引数を削除）

- [ ] **Step 2: `web` コマンドを確認し、CH 環境変数セットを削除**

`cli.py` の `web` コマンド（`@main.command()` で `web` を定義している箇所）を確認:

```bash
grep -n "STDF_CH" src/stdf_platform/cli.py
```

出力された行の `os.environ["STDF_CH_HOST"] = ...` などを削除する。

- [ ] **Step 3: `web` コマンドの `STDF_DATA_DIR` 環境変数セットは残す**

`STDF_DATA_DIR` と `STDF_DB_PATH` の設定行は削除しない（DuckDB で必要）。

- [ ] **Step 4: 動作確認**

```bash
uv run stdf2pq ingest-all test_data -p TEST --workers 2
```

期待: CH エラーなしで全ファイル ingest 完了

- [ ] **Step 5: Commit**

```bash
git add src/stdf_platform/cli.py
git commit -m "refactor: remove ch_host from cli — ingest no longer touches ClickHouse"
```

---

## Task 5: deps.py — DuckDB シングルトン依存関数

**Files:**
- Rewrite: `src/stdf_platform/web/api/deps.py`

- [ ] **Step 1: `deps.py` を以下に完全置き換え**

```python
"""Shared dependencies for FastAPI routes."""

import os
from pathlib import Path
from threading import Lock
from typing import Generator

import duckdb
from fastapi import Request


# ── path helper ──────────────────────────────────────────────────────────────

def get_data_dir() -> Path:
    return Path(os.environ.get("STDF_DATA_DIR", "./data"))


# ── DuckDB view setup (called once at lifespan) ───────────────────────────────

def setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> list[str]:
    """Register Parquet glob views. Returns list of registered table names."""
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
    return registered


# ── FastAPI dependency ────────────────────────────────────────────────────────

def get_db(request: Request) -> tuple[duckdb.DuckDBPyConnection, Lock]:
    """Return (db_connection, lock) from app state. Use: db, lock = Depends(get_db)."""
    return request.app.state.db, request.app.state.db_lock
```

- [ ] **Step 2: 動作確認（import エラーがないことを確認）**

```bash
uv run python -c "from stdf_platform.web.api.deps import get_db, setup_views; print('OK')"
```

期待出力: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/stdf_platform/web/api/deps.py
git commit -m "refactor: replace CH deps with DuckDB singleton dependency"
```

---

## Task 6: server.py — DuckDB lifespan

**Files:**
- Rewrite: `src/stdf_platform/web/server.py`

- [ ] **Step 1: `server.py` を以下に完全置き換え**

```python
"""FastAPI web server for stdf2pq."""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock

import duckdb
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .api.deps import get_data_dir, setup_views
from .api.filters import router as filters_router
from .api.data import router as data_router
from .api.export import router as export_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create DuckDB singleton connection at startup; close at shutdown."""
    data_dir = get_data_dir()
    con = duckdb.connect(":memory:")
    registered = setup_views(con, data_dir)
    app.state.db = con
    app.state.db_lock = Lock()
    if registered:
        print(f"[server] DuckDB views registered: {', '.join(registered)}")
    else:
        print(f"[server] Warning: no Parquet data found in {data_dir}")
    yield
    con.close()


app = FastAPI(title="stdf2pq", version="2.0", docs_url="/api/docs", lifespan=lifespan)

app.include_router(filters_router, prefix="/api")
app.include_router(data_router, prefix="/api")
app.include_router(export_router, prefix="/api")

_static = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_static)), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(_static / "index.html"))
```

- [ ] **Step 2: 動作確認（import エラーがないことを確認）**

```bash
uv run python -c "from stdf_platform.web.server import app; print('OK')"
```

期待出力: `OK`（ClickHouse import エラーなし）

- [ ] **Step 3: Commit**

```bash
git add src/stdf_platform/web/server.py
git commit -m "refactor: replace CH lifespan with DuckDB singleton in web server"
```

---

## Task 7: filters.py — DuckDB SQL に移行

**Files:**
- Rewrite: `src/stdf_platform/web/api/filters.py`

- [ ] **Step 1: `filters.py` を以下に完全置き換え**

```python
"""Filter option endpoints — products, lots, wafers."""

from threading import Lock
from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, Query

from .deps import get_db

router = APIRouter(tags=["filters"])
DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]


@router.get("/products")
def get_products(db_tuple: DB) -> list[str]:
    db, lock = db_tuple
    try:
        with lock:
            rows = db.execute(
                "SELECT DISTINCT product FROM lots WHERE product != '' ORDER BY product"
            ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/test-categories")
def get_test_categories(db_tuple: DB) -> list[str]:
    db, lock = db_tuple
    try:
        with lock:
            rows = db.execute(
                "SELECT DISTINCT test_category FROM lots WHERE test_category != '' ORDER BY test_category"
            ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/lots")
def get_lots(
    db_tuple: DB,
    product: Annotated[list[str], Query()] = [],
    category: Annotated[list[str], Query()] = [],
) -> list[str]:
    db, lock = db_tuple
    try:
        conds, params = [], []
        if product:
            placeholders = ",".join(["?" for _ in product])
            conds.append(f"product IN ({placeholders})")
            params.extend(product)
        if category:
            placeholders = ",".join(["?" for _ in category])
            conds.append(f"test_category IN ({placeholders})")
            params.extend(category)
        where = f"WHERE {' AND '.join(conds)}" if conds else ""
        with lock:
            rows = db.execute(
                f"SELECT DISTINCT lot_id FROM lots {where} ORDER BY lot_id",
                params if params else None,
            ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@router.get("/wafers")
def get_wafers(
    db_tuple: DB,
    lot: Annotated[list[str], Query()] = [],
) -> dict:
    if not lot:
        return {"wafer_ids": [], "has_wafers": False}
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            rows = db.execute(
                f"""SELECT DISTINCT wafer_id FROM wafers
                    WHERE lot_id IN ({placeholders})
                      AND wafer_id != ''
                    ORDER BY wafer_id""",
                list(lot),
            ).fetchall()
        ids = [r[0] for r in rows]
        return {"wafer_ids": ids, "has_wafers": len(ids) > 0}
    except Exception:
        return {"wafer_ids": [], "has_wafers": False}
```

- [ ] **Step 2: 動作確認**

```bash
uv run python -c "from stdf_platform.web.api.filters import router; print('OK')"
```

期待出力: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/stdf_platform/web/api/filters.py
git commit -m "refactor: migrate filters API from ClickHouse to DuckDB"
```

---

## Task 8: data.py — DuckDB SQL に移行

**Files:**
- Rewrite: `src/stdf_platform/web/api/data.py`

- [ ] **Step 1: `data.py` を以下に完全置き換え**

```python
"""Chart data endpoints."""

from threading import Lock
from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, Query

from .deps import get_db

router = APIRouter(tags=["data"])
DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]


@router.get("/summary")
def get_summary(db_tuple: DB, lot: Annotated[list[str], Query()] = []) -> list[dict]:
    if not lot:
        return []
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            rows = db.execute(f"""
                SELECT
                    l.lot_id, l.product, l.test_category, l.sub_process,
                    l.part_type, l.job_name, l.job_rev,
                    COUNT(DISTINCT w.wafer_id)                              AS wafer_count,
                    SUM(w.part_count)                                       AS total_parts,
                    SUM(w.good_count)                                       AS good_parts,
                    ROUND(100.0 * SUM(w.good_count)
                        / NULLIF(SUM(w.part_count), 0), 2)                 AS yield_pct
                FROM lots l
                LEFT JOIN (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
                    ) AS rn FROM wafers
                ) w ON l.lot_id = w.lot_id AND w.rn = 1
                WHERE l.lot_id IN ({placeholders})
                GROUP BY l.lot_id, l.product, l.test_category, l.sub_process,
                         l.part_type, l.job_name, l.job_rev
                ORDER BY l.product, l.test_category, l.lot_id
            """, list(lot)).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/wafer-yield")
def get_wafer_yield(db_tuple: DB, lot: str = "") -> list[dict]:
    if not lot:
        return []
    db, lock = db_tuple
    try:
        with lock:
            rows = db.execute("""
                SELECT wafer_id,
                       part_count AS total,
                       good_count AS good,
                       ROUND(100.0 * good_count / NULLIF(part_count, 0), 2) AS yield_pct,
                       retest_num
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
                    ) AS rn FROM wafers WHERE lot_id = ?
                ) WHERE rn = 1
                ORDER BY wafer_id
            """, [lot]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/wafermap")
def get_wafermap(db_tuple: DB, lot: str = "", wafer: str = "") -> list[dict]:
    if not lot or not wafer:
        return []
    db, lock = db_tuple
    try:
        with lock:
            rows = db.execute("""
                SELECT x_coord, y_coord, soft_bin, hard_bin, passed, part_id
                FROM parts
                WHERE lot_id = ? AND wafer_id = ?
                ORDER BY part_id
            """, [lot, wafer]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/tests")
def get_tests(db_tuple: DB, lot: Annotated[list[str], Query()] = []) -> list[dict]:
    if not lot:
        return []
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            rows = db.execute(f"""
                SELECT DISTINCT test_num, test_name, rec_type, units, lo_limit, hi_limit
                FROM test_data
                WHERE lot_id IN ({placeholders})
                ORDER BY test_num
            """, list(lot)).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/fails")
def get_fails(
    db_tuple: DB,
    lot: Annotated[list[str], Query()] = [],
    top_n: int = 20,
) -> list[dict]:
    if not lot:
        return []
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            rows = db.execute(f"""
                SELECT
                    test_num,
                    test_name,
                    COUNT(*)                                                    AS total,
                    SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)              AS fail_count,
                    ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
                        / COUNT(*), 2)                                          AS fail_rate
                FROM test_data
                WHERE lot_id IN ({placeholders})
                GROUP BY test_num, test_name
                HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
                ORDER BY fail_rate DESC
                LIMIT ?
            """, list(lot) + [top_n]).fetchdf()
        return rows.to_dict(orient="records")
    except Exception:
        return []


@router.get("/distribution")
def get_distribution(
    db_tuple: DB,
    lot: Annotated[list[str], Query()] = [],
    test_num: int = 0,
    limit: int = 5000,
) -> dict:
    if not lot or not test_num:
        return {}
    db, lock = db_tuple
    try:
        placeholders = ",".join(["?" for _ in lot])
        with lock:
            meta = db.execute(f"""
                SELECT FIRST(test_name), FIRST(units), FIRST(lo_limit), FIRST(hi_limit)
                FROM test_data
                WHERE lot_id IN ({placeholders}) AND test_num = ?
            """, list(lot) + [test_num]).fetchone()

            if not meta:
                return {}

            vals = db.execute(f"""
                SELECT result FROM test_data
                WHERE lot_id IN ({placeholders})
                  AND test_num = ?
                  AND result IS NOT NULL
                LIMIT ?
            """, list(lot) + [test_num, limit]).fetchall()

        return {
            "test_num": test_num,
            "test_name": meta[0] or "",
            "units": meta[1] or "",
            "lo_limit": meta[2],
            "hi_limit": meta[3],
            "values": [r[0] for r in vals],
        }
    except Exception:
        return {}
```

- [ ] **Step 2: 動作確認**

```bash
uv run python -c "from stdf_platform.web.api.data import router; print('OK')"
```

期待出力: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/stdf_platform/web/api/data.py
git commit -m "refactor: migrate data API from ClickHouse to DuckDB"
```

---

## Task 9: export.py — DuckDB SQL に移行

**Files:**
- Rewrite: `src/stdf_platform/web/api/export.py`

- [ ] **Step 1: `export.py` を以下に完全置き換え**

```python
"""CSV export endpoint."""

import io
from threading import Lock
from typing import Annotated, Literal

import duckdb
import pandas as pd
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .deps import get_db

router = APIRouter(tags=["export"])
DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]


class ExportRequest(BaseModel):
    lots: list[str]
    wafers: list[str] | None = None
    test_nums: list[int] | None = None
    format: Literal["pivot", "long"] = "pivot"
    filename: str = "stdf_export.csv"


def _build_where(
    lots: list[str],
    wafers: list[str] | None,
    test_nums: list[int] | None,
) -> tuple[str, list]:
    """Return (WHERE clause, params list) for DuckDB positional params."""
    lot_ph = ",".join(["?" for _ in lots])
    conds = [f"td.lot_id IN ({lot_ph})"]
    params: list = list(lots)

    if wafers:
        w_ph = ",".join(["?" for _ in wafers])
        conds.append(f"td.wafer_id IN ({w_ph})")
        params.extend(wafers)

    if test_nums:
        t_ph = ",".join(["?" for _ in test_nums])
        conds.append(f"td.test_num IN ({t_ph})")
        params.extend(test_nums)

    return " AND ".join(conds), params


@router.post("/export/csv")
def export_csv(req: ExportRequest, db_tuple: DB) -> StreamingResponse:
    db, lock = db_tuple
    where, params = _build_where(req.lots, req.wafers, req.test_nums)

    if req.format == "pivot":
        sql = f"""
            SELECT
                td.lot_id, td.wafer_id, td.part_id,
                td.x_coord, td.y_coord,
                p.hard_bin, p.soft_bin,
                p.passed AS part_passed,
                td.test_name,
                td.result
            FROM test_data td
            JOIN parts p ON td.part_id = p.part_id AND td.lot_id = p.lot_id
            WHERE {where}
            ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_name
        """
        with lock:
            df = db.execute(sql, params).fetchdf()
        index_cols = ["lot_id", "wafer_id", "part_id", "x_coord", "y_coord",
                      "hard_bin", "soft_bin", "part_passed"]
        df = df.pivot_table(index=index_cols, columns="test_name",
                            values="result", aggfunc="first")
        df.columns.name = None
        df = df.reset_index()
    else:
        sql = f"""
            SELECT
                td.lot_id, td.wafer_id, td.part_id,
                td.x_coord, td.y_coord,
                p.hard_bin, p.soft_bin,
                td.test_num, td.test_name,
                td.result, td.passed,
                td.lo_limit, td.hi_limit, td.units
            FROM test_data td
            JOIN parts p ON td.part_id = p.part_id AND td.lot_id = p.lot_id
            WHERE {where}
            ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_num
        """
        with lock:
            df = db.execute(sql, params).fetchdf()

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    csv_bytes = buf.getvalue().encode("utf-8")

    return StreamingResponse(
        iter([csv_bytes]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{req.filename}"',
            "Content-Length": str(len(csv_bytes)),
            "X-Row-Count": str(len(df)),
        },
    )


@router.post("/export/preview")
def export_preview(req: ExportRequest, db_tuple: DB) -> dict:
    db, lock = db_tuple
    where, params = _build_where(req.lots, req.wafers, req.test_nums)
    try:
        with lock:
            row = db.execute(f"""
                SELECT
                    COUNT(DISTINCT td.part_id) AS parts,
                    COUNT(DISTINCT td.test_num) AS tests,
                    COUNT(*) AS total_rows
                FROM test_data td
                WHERE {where}
            """, params).fetchone()
        parts, tests, total = row or (0, 0, 0)
        return {
            "parts": int(parts or 0),
            "tests": int(tests or 0),
            "long_rows": int(total or 0),
            "pivot_rows": int(parts or 0),
        }
    except Exception:
        return {"parts": 0, "tests": 0, "long_rows": 0, "pivot_rows": 0}
```

- [ ] **Step 2: 動作確認**

```bash
uv run python -c "from stdf_platform.web.api.export import router; print('OK')"
```

期待出力: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/stdf_platform/web/api/export.py
git commit -m "refactor: migrate export API from ClickHouse to DuckDB"
```

---

## Task 10: query.py — DuckDB デフォルト化

**Files:**
- Modify: `query.py:26-44`

- [ ] **Step 1: CH 接続試行をオプション化（DuckDB を先に確立）**

`query.py` の Setup セルを以下に置き換え:

```python
# %% Setup — DuckDB（デフォルト）または ClickHouse（config.yaml に host 設定時）
import duckdb
import pandas as pd
from pathlib import Path

def _load_config() -> dict:
    config_path = Path(__file__).parent / "config.yaml"
    if config_path.exists():
        try:
            import yaml
            return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except Exception:
            pass
    return {}

_cfg = _load_config()

# ── DuckDB (primary) ──────────────────────────────────────────────────────────
_storage = _cfg.get("storage", {})
DATA_DIR = Path(_storage.get("data_dir", "./data"))
print(f"Data dir: {DATA_DIR}")

con = duckdb.connect(":memory:")
for table in ["lots", "wafers", "parts", "test_data"]:
    path = DATA_DIR / table
    if path.exists():
        con.execute(f"""
            CREATE OR REPLACE VIEW {table} AS
            SELECT * FROM read_parquet('{path.as_posix()}/**/*.parquet', hive_partitioning=true)
        """)
        print(f"  ✓ {table}")
    else:
        print(f"  - {table}  (not found)")
USE_CH = False
ch = None

# ── ClickHouse (optional — set clickhouse.host in config.yaml to enable) ───────
_ch_cfg = _cfg.get("clickhouse", {})
_ch_host = _ch_cfg.get("host", "")
if _ch_host:
    try:
        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=_ch_host,
            port=_ch_cfg.get("http_port", 8123),
            database=_ch_cfg.get("database", "stdf"),
            username=_ch_cfg.get("username", "default"),
            password=_ch_cfg.get("password", ""),
            connect_timeout=5,
        )
        ch.ping()
        print(f"✓ ClickHouse connected: {_ch_host}")
        USE_CH = True
    except Exception as e:
        print(f"ClickHouse unavailable ({e}), using DuckDB")

def q(sql: str, limit: int = 100, parameters: dict | None = None) -> pd.DataFrame:
    """SQL を実行して DataFrame を返す。limit=0 で全件取得。"""
    if limit > 0 and "LIMIT" not in sql.upper():
        sql = sql.rstrip("; \n") + f"\nLIMIT {limit}"
    if USE_CH:
        return ch.query_df(sql, parameters=parameters or {})
    return con.execute(sql).fetchdf()

backend = "ClickHouse" if USE_CH else "DuckDB"
print(f"\nReady ({backend}).  q(sql) で実行。")
```

- [ ] **Step 2: 動作確認**

```bash
uv run python -c "
exec(open('query.py').read().split('# %% ロット')[0])
print('Setup cell OK, USE_CH =', USE_CH)
"
```

期待出力: `Setup cell OK, USE_CH = False`（ClickHouse なしの場合）

- [ ] **Step 3: Commit**

```bash
git add query.py
git commit -m "refactor: make DuckDB the default backend in query.py"
```

---

## Task 11: config と pyproject.toml を更新

**Files:**
- Modify: `config.yaml.example`
- Modify: `pyproject.toml`

- [ ] **Step 1: `config.yaml.example` の clickhouse.host を空に**

`config.yaml.example:33-39` を以下に変更:

```yaml
# ClickHouse (オプション。Linuxサーバー運用時: host: "localhost" に変更し docker compose up -d clickhouse)
clickhouse:
  host: ""        # 空 = 無効（Parquet + DuckDB のみで動作）
  http_port: 8123
  database: "stdf"
  username: "default"
  password: ""
```

- [ ] **Step 2: `pyproject.toml` の clickhouse-connect を optional に**

`pyproject.toml:16` の `"clickhouse-connect>=0.7.0",` を `dependencies` から削除し、`optional-dependencies` に追加:

```toml
[project.optional-dependencies]
postgres = [
    "psycopg2-binary>=2.9.0",
]
clickhouse = [
    "clickhouse-connect>=0.7.0",
]
```

- [ ] **Step 3: uv sync で依存関係を再解決**

```bash
uv sync
```

（CH が optional になったため、デフォルトでは `clickhouse-connect` がインストールされなくなる）

> **Note:** `query.py` と `server.py` は CH が未インストールの場合に `import clickhouse_connect` を try/except で囲んでいるため、CH 未インストールでも動作する。

- [ ] **Step 4: エンドツーエンドテスト**

```bash
# 1. ingest が動くことを確認
uv run stdf2pq ingest test_data/LOT001_CP11.stdf --product TEST

# 2. Web UI が起動することを確認
uv run stdf2pq web &
sleep 3
curl http://localhost:8000/api/products
# 期待: ["TEST"] または []
kill %1
```

- [ ] **Step 5: Commit**

```bash
git add config.yaml.example pyproject.toml
git commit -m "chore: make clickhouse-connect optional, disable CH by default"
```

---

## Task 12: 統合確認

- [ ] **Step 1: `clickhouse-connect` が import されていないことを確認**

```bash
grep -r "import clickhouse" src/stdf_platform/web/
```

期待: `deps.py` および `server.py` に `import clickhouse` が**残っていないこと**を確認。

- [ ] **Step 2: Web UI フルフロー確認**

```bash
# test データを ingest
uv run python make_test_stdf.py
uv run stdf2pq ingest-all test_data -p TEST --workers 2

# Web サーバー起動
uv run stdf2pq web
```

ブラウザで `http://localhost:8000` を開き:
- Products ドロップダウンに `TEST` が表示される
- Lots が選択できる
- Dashboard タブが表示される
- Export タブで CSV ダウンロードが動作する

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "feat: complete DuckDB singleton migration — Docker-free Windows POC ready"
```

---

## 注意事項

**DuckDB スレッド安全性:** `threading.Lock` で全クエリをシリアライズしているため、大量並列リクエストには向かない。POC（低並列）では問題なし。本番移行時は接続プールまたは ClickHouse に戻すことを検討。

**CH コードの保持:** `ch_writer.py` / `ClickHouseConfig` は削除しない。将来の Linux サーバー移行時に `config.yaml` の `clickhouse.host` を設定するだけで CH 書き込みを再有効化できる。

**Parquet glob ビュー:** Web サーバー起動後に新しいデータを ingest しても、サーバー再起動なしでクエリに反映される（glob はクエリごとにファイルスキャン）。
