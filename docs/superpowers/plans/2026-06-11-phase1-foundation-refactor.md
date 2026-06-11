# Phase 1: Foundation Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ground-level cleanup before the reporting engine — remove dead/broken code, fix the broken `export lot` SQL, unify DuckDB view definitions into one module, make history-file writes atomic, switch the web server to per-request connections (true multi-user reads) with a `/health` endpoint, harden config resolution, write structured ingest-failure records, and tidy unused config/env knobs.

**Architecture:** Pure-Python STDF parser → Hive-partitioned Parquet → DuckDB `:memory:` views (the single source of view truth becomes `src/stdf_platform/views.py`) → Click CLI + FastAPI/Alpine web UI. Parquet is the source of truth; DuckDB connections are cheap and disposable.

**Tech Stack:** Python ≥3.10, duckdb, click, rich, pyarrow, pandas, fastapi, uvicorn, pyyaml. Tests: pytest 8 + httpx (FastAPI `TestClient`). Runner verified: **`uv run pytest src/tests -q`** (32 tests currently pass).

---

## Conventions for every task

- **Test command (verified):** run `uv run pytest src/tests -q` at the **end of every task**. Targeted runs during a task use `uv run pytest src/tests/<file>.py -q`.
- Tests live in `src/tests/`. There is **no** `conftest.py` and **no** `[tool.pytest]` config — pytest discovers `test_*.py` with `test_*` functions. Existing tests import the package directly (e.g. `from stdf_platform.config import Config`) and use the `tmp_path` fixture.
- Commit **straight to `main`** after each task (project preference). Use the exact conventional-commit messages shown. End every commit body with the Co-Authored-By trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- Baseline before starting: `git status` clean except untracked `.claude/`.

---

## Task ordering

1. **Task 1 (spec 1.1)** — delete dead code / fix test scripts (no dependencies; do first).
2. **Task 2 (spec 1.3)** — create `views.py` single source. **Must precede the web change (Task 5)** and is used by `query.py`/`database.py`.
3. **Task 3 (spec 1.2)** — fix `export lot` SQL + regression test.
4. **Task 4 (spec 1.4)** — atomic history writes.
5. **Task 5 (spec 1.5)** — per-request web connections + `/health` (depends on Task 2's `setup_views`).
6. **Task 6 (spec 1.6)** — `Config.load` resolution order.
7. **Task 7 (spec 1.7)** — structured ingest-failure JSON.
8. **Task 8 (spec 1.8)** — remove unused `batch_size` / `STDF_DB_PATH`, fix CLAUDE.md wording, log rotation.

`views.py`'s public signature is fixed once in Task 2 and reused identically everywhere:

```python
_DEDUP_UNIT: str = "wafer_id, x_coord, y_coord, part_txt"

def setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> list[str]: ...
```

---

## Task 1 — Remove dead/broken code, fix test scripts (spec 1.1)

`src/stdf_platform/app.py` is a 485-line Streamlit UI importing `streamlit`/`st_aggrid` (neither in `pyproject.toml`) and references the old schema — unimportable dead code. `src/tests/test_timeout.py` is a **dangerous module-level script** that overwrites `_ingest_worker.py` on disk. `src/tests/test_ingest.py` is a module-level script too. Neither contributes a collected pytest test (verified: both files yield 0 `test_*` functions; the suite is 32 tests with or without them). We delete `app.py` and `test_timeout.py`, and rewrite `test_ingest.py` as a real pytest using a **mocked slow worker** (covering the timeout path the old script tried to exercise, without touching the worker source on disk).

**Files:**
- Delete: `src/stdf_platform/app.py`
- Delete: `src/tests/test_timeout.py`
- Rewrite: `src/tests/test_ingest.py`

**Steps:**

- [ ] Delete the two dead files:
  ```bash
  git rm src/stdf_platform/app.py src/tests/test_timeout.py
  ```

- [ ] Confirm nothing imports `app`:
  ```bash
  grep -rn "stdf_platform.app\|import app\b\|from .app" src/ query.py || echo "no references — safe"
  ```
  Expected: `no references — safe`.

- [ ] Rewrite `src/tests/test_ingest.py` as a proper pytest. This exercises `run_ingest_pool` through `worker._run_single` with a **fake subprocess** so no real STDF file or `_ingest_worker.py` mutation is needed. Replace the entire file contents with:

  ```python
  """Tests for the concurrent ingest worker pool (worker.run_ingest_pool)."""

  import json
  import subprocess
  from pathlib import Path

  from stdf_platform import worker


  class _FakeProc:
      """Minimal stand-in for subprocess.Popen used by worker._run_single."""

      def __init__(self, *, returncode=0, stdout="", stderr="", timeout=False):
          self.returncode = returncode
          self._stdout = stdout
          self._stderr = stderr
          self._timeout = timeout
          self.killed = False

      def communicate(self, timeout=None):
          if self._timeout and not self.killed:
              raise subprocess.TimeoutExpired(cmd="fake", timeout=timeout)
          return self._stdout, self._stderr

      def kill(self):
          self.killed = True
          self._timeout = False  # second communicate() returns drained output


  def test_run_ingest_pool_success(tmp_path, monkeypatch):
      payload = json.dumps({"sub_process": "CP1", "test_category": "CP"})

      def fake_popen(cmd, **kwargs):
          return _FakeProc(returncode=0, stdout=payload, stderr="")

      monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)

      files = [(None, tmp_path / "a.stdf", "PROD", "CP")]
      successes, failures = worker.run_ingest_pool(
          files=files, data_dir=tmp_path, compression="zstd", max_workers=1, timeout=5
      )

      assert len(successes) == 1
      assert not failures
      assert successes[0].sub_process == "CP1"
      assert successes[0].test_category == "CP"


  def test_run_ingest_pool_timeout(tmp_path, monkeypatch):
      def fake_popen(cmd, **kwargs):
          return _FakeProc(timeout=True, stderr="Hanging forever...")

      monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)

      files = [(None, tmp_path / "slow.stdf", "PROD", "CP")]
      successes, failures = worker.run_ingest_pool(
          files=files, data_dir=tmp_path, compression="zstd", max_workers=1, timeout=2
      )

      assert not successes
      assert len(failures) == 1
      assert "timed out" in failures[0].error
  ```

- [ ] Run the new test and watch it pass:
  ```bash
  uv run pytest src/tests/test_ingest.py -q
  ```
  Expected: `2 passed`.

- [ ] Run the full suite:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `34 passed` (was 32 collected; the two old scripts contributed 0, the rewrite adds 2).

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  refactor: remove dead app.py, rewrite ingest test as pytest

  Delete unimportable Streamlit app.py (deps not in pyproject, old schema)
  and the dangerous test_timeout.py that overwrote _ingest_worker.py on disk.
  Rewrite test_ingest.py as proper pytest covering worker pool success and
  timeout paths via a mocked subprocess.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 2 — Single-source view definitions in `views.py` (spec 1.3)

`_DEDUP_UNIT` and the `setup_views` view-creation logic are currently triplicated: `database.py:17` + `_create_views` (lines 56–103), `web/api/deps.py:21` + `setup_views` (lines 26–77), and `query.py:39` (inline). `database.py` uses raw `{table_path}` interpolation (Windows-incompatible — backslashes), while `deps.py`/`query.py` already use `.as_posix()`. We create `src/stdf_platform/views.py` as the single source (using `.as_posix()` uniformly) and make `database.py`, `web/api/deps.py`, and `query.py` import from it.

The canonical `setup_views` is the one already in `deps.py` — it registers `*_final` names into the returned list (database.py's variant did not). We adopt that behavior.

**Files:**
- Create: `src/stdf_platform/views.py`
- Modify: `src/stdf_platform/database.py` (drop local `_DEDUP_UNIT` line 17, replace body of `_create_views` lines 56–103)
- Modify: `src/stdf_platform/web/api/deps.py` (drop local `_DEDUP_UNIT` line 21 and `setup_views` lines 26–77; import from `views`)
- Modify: `query.py` (drop inline `_DEDUP_UNIT` line 39 and inline view loop; call `setup_views`)
- Test: `src/tests/test_views.py`

**Steps:**

- [ ] Write a failing test first. Create `src/tests/test_views.py`:

  ```python
  """Tests for the single-source DuckDB view module (stdf_platform.views)."""

  import duckdb
  import pyarrow as pa
  import pyarrow.parquet as pq
  from pathlib import Path

  from stdf_platform.views import _DEDUP_UNIT, setup_views


  def _write_parts(data_dir: Path):
      path = (
          data_dir / "parts" / "product=PROD" / "test_category=CP"
          / "sub_process=" / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet"
      )
      path.parent.mkdir(parents=True, exist_ok=True)
      schema = pa.schema([
          ("lot_id", pa.string()), ("wafer_id", pa.string()), ("part_txt", pa.string()),
          ("x_coord", pa.int64()), ("y_coord", pa.int64()),
          ("soft_bin", pa.int64()), ("passed", pa.bool_()), ("retest_num", pa.int64()),
      ])
      table = pa.table({
          "lot_id": ["LOT1", "LOT1"], "wafer_id": ["W1", "W1"], "part_txt": ["", ""],
          "x_coord": [1, 2], "y_coord": [1, 2], "soft_bin": [1, 0],
          "passed": [True, False], "retest_num": [0, 0],
      }, schema=schema)
      pq.write_table(table, path)


  def test_dedup_unit_constant():
      assert _DEDUP_UNIT == "wafer_id, x_coord, y_coord, part_txt"


  def test_setup_views_registers_base_and_final(tmp_path):
      _write_parts(tmp_path)
      conn = duckdb.connect(":memory:")
      registered = setup_views(conn, tmp_path)
      assert "parts" in registered
      assert "parts_final" in registered
      n = conn.execute("SELECT COUNT(*) FROM parts_final WHERE lot_id='LOT1'").fetchone()[0]
      assert n == 2


  def test_setup_views_empty_dir_returns_empty(tmp_path):
      conn = duckdb.connect(":memory:")
      assert setup_views(conn, tmp_path) == []
  ```

- [ ] Run it and watch it fail (module does not exist yet):
  ```bash
  uv run pytest src/tests/test_views.py -q
  ```
  Expected: collection/import error — `ModuleNotFoundError: No module named 'stdf_platform.views'`.

- [ ] Create `src/stdf_platform/views.py` with the canonical implementation (copied from `deps.py:17-77`, paths via `.as_posix()`):

  ```python
  """Single source of truth for DuckDB view definitions over the Parquet store.

  Imported by database.py, web/api/deps.py and query.py so the dedup key and the
  base/final view SQL exist in exactly one place. Paths use .as_posix() so the
  generated SQL is valid on Windows as well as POSIX hosts.
  """

  from pathlib import Path

  import duckdb


  # Dedup identity within a (lot, retest) group, expressed as native partition
  # columns. CP rows have part_txt='' (so they group by wafer_id + x/y); FT rows
  # have wafer_id='' and x=y=-32768 (so they group by part_txt). Listing all four
  # columns is equivalent to the old CASE/CONCAT key for both categories — the
  # "unused" columns are constant within a category — but avoids per-row string
  # concatenation, making the ROW_NUMBER() window dedup ~25% faster.
  _DEDUP_UNIT = "wafer_id, x_coord, y_coord, part_txt"


  def setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> list[str]:
      """Register Parquet glob views and final-bin merge VIEWs.

      Returns the list of registered view names (base tables and the *_final
      dedup views that were created).
      """
      registered: list[str] = []
      for table in ["lots", "wafers", "parts", "test_data", "chipid"]:
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
          conn.execute(f"""
              CREATE OR REPLACE VIEW test_data_final AS
              SELECT * EXCLUDE (rn) FROM (
                  SELECT *, ROW_NUMBER() OVER (
                      PARTITION BY lot_id, {_DEDUP_UNIT}, test_num, pin_num
                      ORDER BY retest_num DESC
                  ) AS rn FROM test_data
              ) WHERE rn = 1
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

      return registered
  ```

- [ ] Run the new test and watch it pass:
  ```bash
  uv run pytest src/tests/test_views.py -q
  ```
  Expected: `3 passed`.

- [ ] Update `src/stdf_platform/database.py`. Remove the local `_DEDUP_UNIT` block (lines 11–17) and add the import. Replace the top of the file:

  Old (lines 1–17):
  ```python
  """DuckDB database for STDF analytics."""

  from pathlib import Path
  from typing import Any

  import duckdb

  from .config import StorageConfig


  # Dedup identity within a (lot, retest) group, expressed as native partition
  # columns. CP rows have part_txt='' (so they group by wafer_id + x/y); FT rows
  # have wafer_id='' and x=y=-32768 (so they group by part_txt). Listing all four
  # columns is equivalent to the old CASE/CONCAT key for both categories — the
  # "unused" columns are constant within a category — but avoids per-row string
  # concatenation, making the ROW_NUMBER() window dedup ~25% faster.
  _DEDUP_UNIT = "wafer_id, x_coord, y_coord, part_txt"
  ```

  New:
  ```python
  """DuckDB database for STDF analytics."""

  from typing import Any

  import duckdb

  from .config import StorageConfig
  from .views import setup_views
  ```

- [ ] Replace `Database._create_views` (lines 56–103) so it delegates to `setup_views`:

  Old (the whole method body from `def _create_views` through the `chipid_final` block):
  ```python
      def _create_views(self) -> None:
          """Create views for Parquet datasets."""
          tables = ["lots", "wafers", "parts", "test_data", "chipid"]
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
              self.conn.execute(f"""
                  CREATE OR REPLACE VIEW parts_final AS
                  SELECT * EXCLUDE (rn) FROM (
                      SELECT *, ROW_NUMBER() OVER (
                          PARTITION BY lot_id, {_DEDUP_UNIT}
                          ORDER BY retest_num DESC
                      ) AS rn FROM parts
                  ) WHERE rn = 1
              """)

          if "test_data" in registered:
              self.conn.execute(f"""
                  CREATE OR REPLACE VIEW test_data_final AS
                  SELECT * EXCLUDE (rn) FROM (
                      SELECT *, ROW_NUMBER() OVER (
                          PARTITION BY lot_id, {_DEDUP_UNIT}, test_num, pin_num
                          ORDER BY retest_num DESC
                      ) AS rn FROM test_data
                  ) WHERE rn = 1
              """)

          if "chipid" in registered:
              # die identity = decoded ChipID (efuse_raw), NOT positional
              # chip_occurrence_index (which can swap die0/die1 across retests).
              self.conn.execute("""
                  CREATE OR REPLACE VIEW chipid_final AS
                  SELECT * EXCLUDE (rn) FROM (
                      SELECT *, ROW_NUMBER() OVER (
                          PARTITION BY lot_id, efuse_raw
                          ORDER BY retest_num DESC
                      ) AS rn FROM chipid
                  ) WHERE rn = 1
              """)
  ```

  New:
  ```python
      def _create_views(self) -> None:
          """Create views for Parquet datasets (delegates to stdf_platform.views)."""
          setup_views(self.conn, self.data_dir)
  ```

- [ ] Update `src/stdf_platform/web/api/deps.py`. Remove the local `_DEDUP_UNIT` (lines 17–21) and the entire `setup_views` function (lines 24–77), and import from `views` instead. Replace lines 1–77:

  Old (lines 1–77, from the docstring through `return registered`):
  ```python
  """Shared dependencies for FastAPI routes."""

  import os
  from pathlib import Path
  from threading import Lock

  import duckdb
  from fastapi import Request


  # ── path helper ──────────────────────────────────────────────────────────────

  def get_data_dir() -> Path:
      return Path(os.environ.get("STDF_DATA_DIR", "./data"))


  # Dedup identity within a (lot, retest) group, as native partition columns.
  # Equivalent to the old CASE/CONCAT key (CP groups by wafer_id+x/y since
  # part_txt='', FT groups by part_txt since wafer_id/x/y are constant) but skips
  # per-row string concat. Mirrors stdf_platform.database._DEDUP_UNIT.
  _DEDUP_UNIT = "wafer_id, x_coord, y_coord, part_txt"


  # ── DuckDB view setup (called once at lifespan) ───────────────────────────────

  def setup_views(conn: duckdb.DuckDBPyConnection, data_dir: Path) -> list[str]:
      """Register Parquet glob views and final-bin merge VIEWs."""
      registered = []
      for table in ["lots", "wafers", "parts", "test_data", "chipid"]:
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
          conn.execute(f"""
              CREATE OR REPLACE VIEW test_data_final AS
              SELECT * EXCLUDE (rn) FROM (
                  SELECT *, ROW_NUMBER() OVER (
                      PARTITION BY lot_id, {_DEDUP_UNIT}, test_num, pin_num
                      ORDER BY retest_num DESC
                  ) AS rn FROM test_data
              ) WHERE rn = 1
          """)
          registered.append("test_data_final")

      if "chipid" in registered:
          # die identity = decoded ChipID (efuse_raw), not positional occurrence.
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

      return registered
  ```

  New:
  ```python
  """Shared dependencies for FastAPI routes."""

  import os
  from pathlib import Path
  from threading import Lock

  import duckdb
  from fastapi import Request

  from stdf_platform.views import setup_views  # re-exported for server.py imports


  # ── path helper ──────────────────────────────────────────────────────────────

  def get_data_dir() -> Path:
      return Path(os.environ.get("STDF_DATA_DIR", "./data"))
  ```

  > Note: `server.py` imports `setup_views` from `.api.deps` (`from .api.deps import get_data_dir, setup_views`). The re-export above keeps that import valid. Task 5 will rewrite `server.py`/`deps.py` further; for now we preserve the existing import surface so the suite stays green.

- [ ] Update `query.py`. Remove the inline `_DEDUP_UNIT` (lines 36–39) and the inline base-view loop + `_FINAL_SPECS`/`_make_final_views` duplication is **kept** (it powers `use_lot`/`use_all` materialization, which is out of scope), but the base-table view registration loop (lines 42–51) is replaced by `setup_views`. Make these two edits:

  Edit A — replace the `_DEDUP_UNIT` comment+assignment (current lines 36–39):

  Old:
  ```python
  # Dedup identity within a (lot, retest) group, as native partition columns.
  # CP groups by wafer_id+x/y (part_txt=''), FT groups by part_txt (wafer/x/y
  # constant). Equivalent to a CASE/CONCAT key but faster (no per-row string concat).
  _DEDUP_UNIT = "wafer_id, x_coord, y_coord, part_txt"
  ```

  New:
  ```python
  from stdf_platform.views import _DEDUP_UNIT, setup_views
  ```

  Edit B — replace the base-table registration loop (current lines 41–51):

  Old:
  ```python
  con = duckdb.connect(":memory:")
  for table in ["lots", "wafers", "parts", "test_data", "chipid"]:
      path = DATA_DIR / table
      if path.exists():
          con.execute(f"""
              CREATE OR REPLACE VIEW {table} AS
              SELECT * FROM read_parquet('{path.as_posix()}/**/*.parquet', hive_partitioning=true)
          """)
          print(f"  ✓ {table}")
      else:
          print(f"  - {table}  (not found)")
  ```

  New:
  ```python
  con = duckdb.connect(":memory:")
  for _name in setup_views(con, DATA_DIR):
      print(f"  ✓ {_name}")
  ```

  > `_FINAL_SPECS`, `_make_final_views`, `use_lot`, and `use_all` (lines 55–116) are unchanged — they still reference the imported `_DEDUP_UNIT` and provide the per-lot materialization helpers. `setup_views` already creates the `*_final` views; `_make_final_views()` re-creates them as the all-lots views, which is harmless (idempotent `CREATE OR REPLACE`).

- [ ] Verify `query.py` still imports cleanly (it runs `setup_views` and the cell SQL at import time; with no data dir it just prints "not found"-free output and the `q(...)` cells query empty/absent views — only the import + setup matters here):
  ```bash
  STDF_QUERY_SMOKE=1 uv run python -c "import importlib.util, sys; spec = importlib.util.spec_from_file_location('q', 'query.py'); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); print('query.py imported OK')" 2>&1 | tail -3
  ```
  Expected: ends with `query.py imported OK` (the `q()`/`to_csv()` demo cells may print warnings if no `data/` exists, but import must not raise on the view-setup path). If a demo cell at module scope raises because `data/` is absent, that is pre-existing behavior unrelated to this change; confirm by `git stash` + re-running. Do not add scope to "fix" the demo cells.

- [ ] Run the full suite:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `37 passed` (34 from Task 1 + 3 new view tests).

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  refactor: single-source DuckDB views in views.py

  Extract _DEDUP_UNIT and setup_views() into stdf_platform/views.py; have
  database.py, web/api/deps.py and query.py import it. database.py now uses
  .as_posix() paths (was Windows-incompatible). deps.py re-exports setup_views
  to keep server.py's import surface stable.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 3 — Fix `export lot` CLI SQL (spec 1.2)

`cli.py` `export_lot` (lines 707–760) builds SQL against tables `test_results`, `tests`, and joins on `parts.part_id` — **none of `test_results`/`tests` exist** (the real tables are `parts`/`test_data`, deduped via `parts_final`/`test_data_final`). The command is broken at runtime. Rewrite both the pivot and long-format branches against `test_data_final` + `parts_final`, joined on `(lot_id, wafer_id, part_id)` — the same join `query.py` uses (see its `to_csv` cell, lines 329–333). Columns available on `test_data_final`: `test_name`, `result`, `test_num`, `passed`, `lo_limit`, `hi_limit`, `units`; on `parts_final`: `x_coord`, `y_coord`, `hard_bin`, `soft_bin`, `passed`, `part_id`, `wafer_id`, `lot_id`.

**Files:**
- Modify: `src/stdf_platform/cli.py` (`export_lot`, lines 707–760)
- Test: `src/tests/test_export_lot.py`

**Steps:**

- [ ] Write a failing regression test. Create `src/tests/test_export_lot.py`. It builds tiny `parts` + `test_data` Parquet datasets, monkeypatches `Database` to point at the temp data dir, and invokes the Click command via `CliRunner`, asserting the long-format and pivot CSVs are produced with expected columns:

  ```python
  """Regression test: `stdf export lot` must query parts_final/test_data_final."""

  import pyarrow as pa
  import pyarrow.parquet as pq
  from pathlib import Path

  from click.testing import CliRunner

  from stdf_platform import cli
  from stdf_platform.config import Config, StorageConfig


  def _write(data_dir: Path):
      parts_path = (
          data_dir / "parts" / "product=PROD" / "test_category=CP"
          / "sub_process=" / "lot_id=LOT1" / "wafer_id=W1" / "retest=0" / "data.parquet"
      )
      parts_path.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["LOT1"], "wafer_id": ["W1"], "part_id": ["P0"],
          "x_coord": [1], "y_coord": [2], "hard_bin": [1], "soft_bin": [1],
          "passed": [True], "retest_num": [0],
      }), parts_path)

      td_path = (
          data_dir / "test_data" / "product=PROD" / "test_category=CP"
          / "sub_process=" / "lot_id=LOT1" / "data.parquet"
      )
      td_path.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["LOT1"], "wafer_id": ["W1"], "part_id": ["P0"],
          "x_coord": [1], "y_coord": [2], "part_txt": [""],
          "test_num": [100], "pin_num": [0], "test_name": ["VDD"],
          "result": [1.23], "passed": ["P"], "lo_limit": [0.0], "hi_limit": [2.0],
          "units": ["V"], "retest_num": [0],
      }), td_path)


  def _patched_config(data_dir: Path) -> Config:
      return Config(storage=StorageConfig(
          data_dir=data_dir, database=data_dir / "stdf.duckdb"
      ))


  def test_export_lot_long_format(tmp_path, monkeypatch):
      _write(tmp_path)
      monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))
      out = tmp_path / "out.csv"
      result = CliRunner().invoke(cli.main, ["export", "lot", "LOT1", str(out), "--no-pivot"])
      assert result.exit_code == 0, result.output
      text = out.read_text()
      assert "test_name" in text
      assert "VDD" in text
      assert "1.23" in text


  def test_export_lot_pivot(tmp_path, monkeypatch):
      _write(tmp_path)
      monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, p=None: _patched_config(tmp_path)))
      out = tmp_path / "pivot.csv"
      result = CliRunner().invoke(cli.main, ["export", "lot", "LOT1", str(out)])
      assert result.exit_code == 0, result.output
      header = out.read_text().splitlines()[0]
      assert "VDD" in header  # pivoted test_name becomes a column
      assert "part_id" in header
  ```

- [ ] Run it and watch it fail (current SQL references missing `test_results`/`tests`):
  ```bash
  uv run pytest src/tests/test_export_lot.py -q
  ```
  Expected: `2 failed` — error text contains `Table with name test_results does not exist` (surfaced in `result.output`).

- [ ] Rewrite the SQL in `cli.py` `export_lot`. Replace the `if pivot:` / `else:` block (current lines 712–758):

  Old (the two SQL string assignments inside `try:`):
  ```python
              if pivot:
                  sql = f"""
                  PIVOT (
                      SELECT
                          tr.lot_id,
                          tr.wafer_id,
                          tr.part_id,
                          p.x_coord,
                          p.y_coord,
                          p.hard_bin,
                          p.soft_bin,
                          p.passed as part_passed,
                          t.test_name,
                          tr.result
                      FROM test_results tr
                      JOIN parts p ON tr.part_id = p.part_id
                      JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
                      WHERE tr.lot_id IN ({placeholders})
                  )
                  ON test_name
                  USING first(result)
                  GROUP BY lot_id, wafer_id, part_id, x_coord, y_coord, hard_bin, soft_bin, part_passed
                  ORDER BY lot_id, wafer_id, part_id
                  """
              else:
                  sql = f"""
                  SELECT
                      tr.lot_id,
                      tr.wafer_id,
                      tr.part_id,
                      p.x_coord,
                      p.y_coord,
                      p.hard_bin,
                      p.soft_bin,
                      t.test_num,
                      t.test_name,
                      tr.result,
                      tr.passed,
                      t.lo_limit,
                      t.hi_limit,
                      t.units
                  FROM test_results tr
                  JOIN parts p ON tr.part_id = p.part_id
                  JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
                  WHERE tr.lot_id IN ({placeholders})
                  ORDER BY tr.lot_id, tr.wafer_id, tr.part_id, t.test_num
                  """
  ```

  New:
  ```python
              if pivot:
                  sql = f"""
                  PIVOT (
                      SELECT
                          td.lot_id,
                          td.wafer_id,
                          td.part_id,
                          p.x_coord,
                          p.y_coord,
                          p.hard_bin,
                          p.soft_bin,
                          p.passed AS part_passed,
                          td.test_name,
                          td.result
                      FROM test_data_final td
                      JOIN parts_final p
                          ON  td.lot_id   = p.lot_id
                          AND td.wafer_id = p.wafer_id
                          AND td.part_id  = p.part_id
                      WHERE td.lot_id IN ({placeholders})
                  )
                  ON test_name
                  USING first(result)
                  GROUP BY lot_id, wafer_id, part_id, x_coord, y_coord, hard_bin, soft_bin, part_passed
                  ORDER BY lot_id, wafer_id, part_id
                  """
              else:
                  sql = f"""
                  SELECT
                      td.lot_id,
                      td.wafer_id,
                      td.part_id,
                      p.x_coord,
                      p.y_coord,
                      p.hard_bin,
                      p.soft_bin,
                      td.test_num,
                      td.test_name,
                      td.result,
                      td.passed,
                      td.lo_limit,
                      td.hi_limit,
                      td.units
                  FROM test_data_final td
                  JOIN parts_final p
                      ON  td.lot_id   = p.lot_id
                      AND td.wafer_id = p.wafer_id
                      AND td.part_id  = p.part_id
                  WHERE td.lot_id IN ({placeholders})
                  ORDER BY td.lot_id, td.wafer_id, td.part_id, td.test_num
                  """
  ```

- [ ] Run the regression test and watch it pass:
  ```bash
  uv run pytest src/tests/test_export_lot.py -q
  ```
  Expected: `2 passed`.

- [ ] Run the full suite:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `39 passed` (37 + 2).

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  fix: export lot queries test_data_final/parts_final

  The pivot and long-format SQL referenced non-existent test_results/tests
  tables and joined parts on part_id alone. Rewrite against the real
  test_data_final + parts_final views joined on (lot_id, wafer_id, part_id).
  Add a CliRunner regression test for both branches.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 4 — Atomic history-file writes (spec 1.4)

`SyncManager._save` (`sync_manager.py:32-36`) and `IngestHistory._save` (`ingest_history.py:29-34`) write directly to the destination path. A crash or concurrent writer mid-write can truncate/corrupt the JSON. Switch both to "write a temp file in the same directory, then `os.replace`" (atomic on POSIX and Windows when source and dest are on the same filesystem).

**Files:**
- Modify: `src/stdf_platform/sync_manager.py` (`_save`, lines 32–36; add `import os`, `import tempfile`)
- Modify: `src/stdf_platform/ingest_history.py` (`_save`, lines 29–34; add `import os`, `import tempfile`)
- Test: `src/tests/test_atomic_save.py`

**Steps:**

- [ ] Write a failing test. Create `src/tests/test_atomic_save.py`. It asserts (a) the saved JSON round-trips, and (b) **no leftover temp files** remain in the directory, and (c) a pre-existing valid file survives if a new save targets the same path (round-trip after reopen):

  ```python
  """Atomic-write tests for SyncManager._save and IngestHistory._save."""

  import json
  from pathlib import Path

  from stdf_platform.sync_manager import SyncManager
  from stdf_platform.ingest_history import IngestHistory


  def test_sync_manager_save_is_atomic_and_clean(tmp_path):
      hist = tmp_path / "sync_history.json"
      mgr = SyncManager(hist)
      mgr.mark_downloaded("remote/a.stdf", tmp_path / "a.stdf", "PROD", "CP")

      # File is valid JSON and round-trips through a fresh reader.
      data = json.loads(hist.read_text(encoding="utf-8"))
      assert "remote/a.stdf" in data["files"]
      assert SyncManager(hist).is_downloaded("remote/a.stdf")

      # No temp residue left behind.
      leftovers = [p.name for p in tmp_path.iterdir() if p.name != "sync_history.json"]
      assert leftovers == []


  def test_ingest_history_save_is_atomic_and_clean(tmp_path):
      hist = tmp_path / "ingest_history.json"
      h = IngestHistory(hist)
      f = tmp_path / "x.stdf"
      f.write_text("x")
      h.mark_done_batch([f])

      assert IngestHistory(hist).is_done(f)
      leftovers = [p.name for p in tmp_path.iterdir()
                   if p.name not in {"ingest_history.json", "x.stdf"}]
      assert leftovers == []
  ```

- [ ] Run it and watch it pass on the round-trip assertions but **this test passes today** because the direct write also round-trips and leaves no residue. To make it a true regression guard, the value is in pinning the *atomic* behavior. Run it now to establish a green baseline against the new implementation:
  ```bash
  uv run pytest src/tests/test_atomic_save.py -q
  ```
  Expected before edits: `2 passed` (direct write already round-trips). The test's real job is to **stay green after** we switch to temp-file+replace and to fail if a future change leaves temp residue.

- [ ] Update `src/stdf_platform/sync_manager.py`. Change the imports and `_save`.

  Old (lines 1–6):
  ```python
  """Sync manager for tracking downloaded and ingested STDF files."""

  import json
  from datetime import datetime
  from pathlib import Path
  from typing import Optional
  ```

  New:
  ```python
  """Sync manager for tracking downloaded and ingested STDF files."""

  import json
  import os
  import tempfile
  from datetime import datetime
  from pathlib import Path
  from typing import Optional
  ```

  Old `_save` (lines 32–36):
  ```python
      def _save(self) -> None:
          """Save history to file."""
          self.history_file.parent.mkdir(parents=True, exist_ok=True)
          with open(self.history_file, "w", encoding="utf-8") as f:
              json.dump(self._history, f, indent=2, ensure_ascii=False)
  ```

  New:
  ```python
      def _save(self) -> None:
          """Save history atomically (temp file in same dir + os.replace)."""
          self.history_file.parent.mkdir(parents=True, exist_ok=True)
          fd, tmp = tempfile.mkstemp(
              dir=self.history_file.parent, prefix=".sync_history.", suffix=".tmp"
          )
          try:
              with os.fdopen(fd, "w", encoding="utf-8") as f:
                  json.dump(self._history, f, indent=2, ensure_ascii=False)
              os.replace(tmp, self.history_file)
          except BaseException:
              try:
                  os.unlink(tmp)
              except OSError:
                  pass
              raise
  ```

- [ ] Update `src/stdf_platform/ingest_history.py`. Change imports and `_save`.

  Old (lines 7–9):
  ```python
  import json
  from datetime import datetime
  from pathlib import Path
  ```

  New:
  ```python
  import json
  import os
  import tempfile
  from datetime import datetime
  from pathlib import Path
  ```

  Old `_save` (lines 29–34):
  ```python
      def _save(self) -> None:
          self.history_file.parent.mkdir(parents=True, exist_ok=True)
          self.history_file.write_text(
              json.dumps({"ingested": self._done}, indent=2, ensure_ascii=False),
              encoding="utf-8",
          )
  ```

  New:
  ```python
      def _save(self) -> None:
          self.history_file.parent.mkdir(parents=True, exist_ok=True)
          fd, tmp = tempfile.mkstemp(
              dir=self.history_file.parent, prefix=".ingest_history.", suffix=".tmp"
          )
          try:
              with os.fdopen(fd, "w", encoding="utf-8") as f:
                  json.dump({"ingested": self._done}, f, indent=2, ensure_ascii=False)
              os.replace(tmp, self.history_file)
          except BaseException:
              try:
                  os.unlink(tmp)
              except OSError:
                  pass
              raise
  ```

- [ ] Run the test again (now against atomic implementation) and watch it pass:
  ```bash
  uv run pytest src/tests/test_atomic_save.py -q
  ```
  Expected: `2 passed`.

- [ ] Run the full suite:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `41 passed` (39 + 2).

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  fix: atomic writes for sync/ingest history files

  Write history JSON to a temp file in the same directory then os.replace,
  preventing truncation/corruption from crashes or concurrent writers.
  Clean up the temp file on any failure.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 5 — Per-request web connections + `/health` (spec 1.5)

Today `server.py` `lifespan` opens **one** shared DuckDB connection and a global `threading.Lock`; every route does `db, lock = db_tuple` and runs queries under `with lock:`, serializing all reads. Since Parquet is the source of truth and `:memory:` connection + `setup_views` is cheap, we switch to a **per-request** connection created by a FastAPI dependency, drop the lock entirely, and add `GET /health`.

This touches `deps.py` (`get_db` now yields a fresh connection), all four routers (`data.py`, `filters.py`, `export.py` use `db, lock = db_tuple` + `with lock:`), and `server.py` (drop the lifespan singleton, add `/health`). Task 2 already made `setup_views` importable from `stdf_platform.views` and re-exported it from `deps`.

**Files:**
- Modify: `src/stdf_platform/web/api/deps.py` (replace `get_db`)
- Modify: `src/stdf_platform/web/server.py` (drop lifespan singleton + lock; add `/health`)
- Modify: `src/stdf_platform/web/api/data.py` (signatures + `db, lock = db_tuple` + `with lock:` → `db`)
- Modify: `src/stdf_platform/web/api/filters.py` (same)
- Modify: `src/stdf_platform/web/api/export.py` (same)
- Test: `src/tests/test_web_health.py`

**Steps:**

- [ ] Write a failing test. Create `src/tests/test_web_health.py`. It points `STDF_DATA_DIR` at a temp Parquet dataset, then uses `TestClient` to hit `/health` and a data endpoint, asserting both succeed without the shared-connection lock:

  ```python
  """Per-request connection + /health tests for the web server."""

  import importlib

  import pyarrow as pa
  import pyarrow.parquet as pq
  from fastapi.testclient import TestClient


  def _write_lots(data_dir):
      path = (
          data_dir / "lots" / "product=PROD" / "test_category=CP"
          / "sub_process=CP1" / "lot_id=LOT1" / "data.parquet"
      )
      path.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["LOT1"], "product": ["PROD"], "test_category": ["CP"],
          "sub_process": ["CP1"], "part_type": ["X"], "job_name": ["J"], "job_rev": ["1"],
      }), path)


  def _client(tmp_path, monkeypatch):
      _write_lots(tmp_path)
      monkeypatch.setenv("STDF_DATA_DIR", str(tmp_path))
      import stdf_platform.web.server as server
      importlib.reload(server)
      return TestClient(server.app)


  def test_health_ok(tmp_path, monkeypatch):
      with _client(tmp_path, monkeypatch) as client:
          resp = client.get("/health")
          assert resp.status_code == 200
          assert resp.json()["status"] == "ok"


  def test_products_endpoint_per_request(tmp_path, monkeypatch):
      with _client(tmp_path, monkeypatch) as client:
          resp = client.get("/api/products")
          assert resp.status_code == 200
          assert "PROD" in resp.json()
  ```

- [ ] Run it and watch it fail (no `/health` route yet):
  ```bash
  uv run pytest src/tests/test_web_health.py -q
  ```
  Expected: `test_health_ok` fails with `404`.

- [ ] Replace `get_db` in `src/stdf_platform/web/api/deps.py`. The full file after this edit:

  ```python
  """Shared dependencies for FastAPI routes."""

  import os
  from pathlib import Path

  import duckdb

  from stdf_platform.views import setup_views  # re-exported for server.py imports


  # ── path helper ──────────────────────────────────────────────────────────────

  def get_data_dir() -> Path:
      return Path(os.environ.get("STDF_DATA_DIR", "./data"))


  # ── FastAPI dependency: one fresh in-memory connection per request ────────────

  def get_db():
      """Yield a per-request DuckDB :memory: connection with views registered.

      Parquet is the source of truth and connection setup is cheap, so each
      request gets its own connection — no shared singleton, no global lock,
      no read serialization across users.
      """
      conn = duckdb.connect(":memory:")
      try:
          setup_views(conn, get_data_dir())
          yield conn
      finally:
          conn.close()
  ```

  > Removed: `from threading import Lock`, `from fastapi import Request`, and the old tuple-returning `get_db`.

- [ ] Rewrite `src/stdf_platform/web/server.py` to drop the singleton/lock and add `/health`:

  ```python
  """FastAPI web server for stdf."""

  from pathlib import Path

  from fastapi import FastAPI
  from fastapi.responses import FileResponse
  from fastapi.staticfiles import StaticFiles

  from .api.filters import router as filters_router
  from .api.data import router as data_router
  from .api.export import router as export_router


  app = FastAPI(title="stdf", version="2.0", docs_url="/api/docs")

  app.include_router(filters_router, prefix="/api")
  app.include_router(data_router, prefix="/api")
  app.include_router(export_router, prefix="/api")

  _static = Path(__file__).parent / "static"
  app.mount("/static", StaticFiles(directory=str(_static)), name="static")


  @app.get("/health")
  async def health() -> dict:
      return {"status": "ok"}


  @app.get("/")
  async def index() -> FileResponse:
      return FileResponse(str(_static / "index.html"))
  ```

  > Removed: `lifespan`, `duckdb`/`Lock`/`asynccontextmanager` imports, `setup_views`/`get_data_dir` import, and the `app.state.db`/`app.state.db_lock` setup. Views are now created per request by the `get_db` dependency.

- [ ] Update `src/stdf_platform/web/api/data.py` to use the connection directly. Change the `DB` alias and every handler. Edits:

  Old import block (lines 3–15):
  ```python
  import logging
  from threading import Lock
  from typing import Annotated

  import duckdb
  from fastapi import APIRouter, Depends, Query

  from .deps import get_db

  logger = logging.getLogger(__name__)

  router = APIRouter(tags=["data"])
  DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]
  ```

  New:
  ```python
  import logging
  from typing import Annotated

  import duckdb
  from fastapi import APIRouter, Depends, Query

  from .deps import get_db

  logger = logging.getLogger(__name__)

  router = APIRouter(tags=["data"])
  DB = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]
  ```

  Then in **every** handler in `data.py`, the parameter `db_tuple: DB` becomes `db: DB`, the line `db, lock = db_tuple` is deleted, and each `with lock:` block is unwrapped (its body dedents one level to run directly on `db`). Concrete example — `get_summary` (current lines 19–53):

  Old:
  ```python
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
                      MAX(p.wafer_count)                                      AS wafer_count,
                      MAX(p.total_parts)                                      AS total_parts,
                      MAX(p.good_parts)                                       AS good_parts,
                      MAX(p.yield_pct)                                        AS yield_pct
                  FROM lots l
                  LEFT JOIN (
                      SELECT
                          lot_id,
                          COUNT(DISTINCT NULLIF(wafer_id, ''))               AS wafer_count,
                          COUNT(*)                                          AS total_parts,
                          SUM(CASE WHEN passed THEN 1 ELSE 0 END)           AS good_parts,
                          ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                              / NULLIF(COUNT(*), 0), 2)                     AS yield_pct
                      FROM parts_final
                      GROUP BY lot_id
                  ) p ON l.lot_id = p.lot_id
                  WHERE l.lot_id IN ({placeholders})
                  GROUP BY l.lot_id, l.product, l.test_category, l.sub_process,
                           l.part_type, l.job_name, l.job_rev
                  ORDER BY l.product, l.test_category, l.lot_id
              """, list(lot)).fetchdf()
          return rows.to_dict(orient="records")
      except Exception as e:
          logger.warning("%s failed: %s", __name__, e)
          return []
  ```

  New:
  ```python
  def get_summary(db: DB, lot: Annotated[list[str], Query()] = []) -> list[dict]:
      if not lot:
          return []
      try:
          placeholders = ",".join(["?" for _ in lot])
          rows = db.execute(f"""
              SELECT
                  l.lot_id, l.product, l.test_category, l.sub_process,
                  l.part_type, l.job_name, l.job_rev,
                  MAX(p.wafer_count)                                      AS wafer_count,
                  MAX(p.total_parts)                                      AS total_parts,
                  MAX(p.good_parts)                                       AS good_parts,
                  MAX(p.yield_pct)                                        AS yield_pct
              FROM lots l
              LEFT JOIN (
                  SELECT
                      lot_id,
                      COUNT(DISTINCT NULLIF(wafer_id, ''))               AS wafer_count,
                      COUNT(*)                                          AS total_parts,
                      SUM(CASE WHEN passed THEN 1 ELSE 0 END)           AS good_parts,
                      ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(*), 0), 2)                     AS yield_pct
                  FROM parts_final
                  GROUP BY lot_id
              ) p ON l.lot_id = p.lot_id
              WHERE l.lot_id IN ({placeholders})
              GROUP BY l.lot_id, l.product, l.test_category, l.sub_process,
                       l.part_type, l.job_name, l.job_rev
              ORDER BY l.product, l.test_category, l.lot_id
          """, list(lot)).fetchdf()
          return rows.to_dict(orient="records")
      except Exception as e:
          logger.warning("%s failed: %s", __name__, e)
          return []
  ```

  Apply this same mechanical transform to the remaining `data.py` handlers (`get_wafer_yield`, `get_wafermap`, `get_tests`, and the distribution handler): drop `db, lock = db_tuple`, rename the param to `db: DB`, and dedent each `with lock:` body one level. Verify afterward:
  ```bash
  grep -n "db_tuple\|with lock\|db, lock" src/stdf_platform/web/api/data.py || echo "clean"
  ```
  Expected: `clean`.

- [ ] Apply the identical transform to `src/stdf_platform/web/api/filters.py`:
  - Drop `from threading import Lock`.
  - `DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]` → `DB = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]`.
  - Each handler: `db_tuple: DB` → `db: DB`, delete `db, lock = db_tuple`, unwrap `with lock:`.

  Verify:
  ```bash
  grep -n "db_tuple\|with lock\|db, lock\|from threading" src/stdf_platform/web/api/filters.py || echo "clean"
  ```
  Expected: `clean`.

- [ ] Apply the identical transform to `src/stdf_platform/web/api/export.py`:
  - Drop `from threading import Lock`.
  - `DB = Annotated[tuple[duckdb.DuckDBPyConnection, Lock], Depends(get_db)]` → `DB = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]`.
  - Handlers `export_csv` and `export_preview`: `db_tuple: DB` → `db: DB`, delete `db, lock = db_tuple`, unwrap the two `with lock:` blocks in `export_csv` and the one in `export_preview`.

  Verify:
  ```bash
  grep -n "db_tuple\|with lock\|db, lock\|from threading" src/stdf_platform/web/api/export.py || echo "clean"
  ```
  Expected: `clean`.

- [ ] Run the web test and watch it pass:
  ```bash
  uv run pytest src/tests/test_web_health.py -q
  ```
  Expected: `2 passed`.

- [ ] Run the full suite:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `43 passed` (41 + 2).

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat: per-request DuckDB connections + /health endpoint

  Drop the shared lifespan connection and global threading.Lock; each request
  now gets its own duckdb.connect(":memory:") + setup_views via the get_db
  dependency, removing read serialization across users. Add GET /health.
  Update data/filters/export routers to use the connection directly.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 6 — Config resolution order (spec 1.6)

`Config.load` (`config.py:113-154`) silently defaults to `Path("config.yaml")` and, if absent, returns an empty `Config()`. Under cron/Task Scheduler the cwd differs and it silently falls back to defaults. New resolution order: **explicit arg → `STDF_CONFIG` env var → cwd `config.yaml`**. Behavior preserved: if the *resolved* path doesn't exist, still return `cls()` (callers depend on this), but an explicitly-passed path that doesn't exist should be honored as the chosen path (and then fall through to defaults if missing — matching current leniency; we do not add hard failures, staying YAGNI).

**Files:**
- Modify: `src/stdf_platform/config.py` (`Config.load`, lines 113–120; add `import os` — already imported at line 4)
- Test: `src/tests/test_config_load.py`

**Steps:**

- [ ] Write a failing test. Create `src/tests/test_config_load.py`:

  ```python
  """Config.load resolution order: explicit arg → STDF_CONFIG → cwd config.yaml."""

  from pathlib import Path

  from stdf_platform.config import Config


  def _write_cfg(path: Path, data_dir: str):
      path.write_text(f"storage:\n  data_dir: {data_dir}\n", encoding="utf-8")


  def test_explicit_arg_wins(tmp_path, monkeypatch):
      explicit = tmp_path / "explicit.yaml"
      _write_cfg(explicit, "/from/explicit")
      env_cfg = tmp_path / "env.yaml"
      _write_cfg(env_cfg, "/from/env")
      monkeypatch.setenv("STDF_CONFIG", str(env_cfg))
      cfg = Config.load(explicit)
      assert cfg.storage.data_dir == Path("/from/explicit")


  def test_env_var_used_when_no_arg(tmp_path, monkeypatch):
      env_cfg = tmp_path / "env.yaml"
      _write_cfg(env_cfg, "/from/env")
      monkeypatch.setenv("STDF_CONFIG", str(env_cfg))
      monkeypatch.chdir(tmp_path)  # cwd config.yaml absent
      cfg = Config.load()
      assert cfg.storage.data_dir == Path("/from/env")


  def test_cwd_config_used_when_no_arg_no_env(tmp_path, monkeypatch):
      monkeypatch.delenv("STDF_CONFIG", raising=False)
      _write_cfg(tmp_path / "config.yaml", "/from/cwd")
      monkeypatch.chdir(tmp_path)
      cfg = Config.load()
      assert cfg.storage.data_dir == Path("/from/cwd")


  def test_missing_everything_returns_defaults(tmp_path, monkeypatch):
      monkeypatch.delenv("STDF_CONFIG", raising=False)
      monkeypatch.chdir(tmp_path)  # no config.yaml here
      cfg = Config.load()
      assert cfg.storage.data_dir == Path("./data")
  ```

- [ ] Run it and watch it fail (env-var branch not implemented):
  ```bash
  uv run pytest src/tests/test_config_load.py -q
  ```
  Expected: `test_env_var_used_when_no_arg` fails (returns default `./data`, env var ignored).

- [ ] Update `Config.load` in `src/stdf_platform/config.py`. Replace the resolution preamble (lines 113–120):

  Old:
  ```python
      @classmethod
      def load(cls, config_path: Path | None = None) -> "Config":
          """Load configuration from YAML file."""
          if config_path is None:
              config_path = Path("config.yaml")

          if not config_path.exists():
              return cls()
  ```

  New:
  ```python
      @classmethod
      def load(cls, config_path: Path | None = None) -> "Config":
          """Load configuration from YAML file.

          Resolution order: explicit arg → STDF_CONFIG env var → cwd config.yaml.
          If the resolved path does not exist, defaults are returned.
          """
          if config_path is None:
              env_path = os.environ.get("STDF_CONFIG")
              config_path = Path(env_path) if env_path else Path("config.yaml")

          if not config_path.exists():
              return cls()
  ```

  > `import os` is already present (config.py line 4). `FTPConfig`/`StorageConfig` env expansion is unaffected.

- [ ] Run the config test and watch it pass:
  ```bash
  uv run pytest src/tests/test_config_load.py -q
  ```
  Expected: `4 passed`.

- [ ] Run the full suite:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `47 passed` (43 + 4).

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat: config resolution order — arg, STDF_CONFIG, cwd

  Config.load now honors the STDF_CONFIG env var when no explicit path is
  given, before falling back to cwd config.yaml. Fixes silent default
  fallback under cron / Task Scheduler launches.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 7 — Structured ingest-failure records (spec 1.7)

After a batch ingest, automation has no machine-readable record of which files failed. `_run_ingest_batch` (`cli.py:403-447`) returns `(successes, failures)` but only prints them. Add: after the batch, write `data/ingest_failures.json` **atomically** (reusing the same temp-file+replace pattern) containing each failure's path and error. Write it from `_run_ingest_batch` using `config.storage.data_dir`. Empty failure list still writes a file with `"failures": []` so consumers can distinguish "ran, none failed" from "never ran".

To keep atomic writing DRY without over-engineering, add a tiny helper `atomic_write_json(path, obj)` in a new `src/stdf_platform/atomic.py` and (optionally) leave Task 4's inline implementations as-is (YAGNI — do not refactor Task 4; the helper is only introduced here because two *new* call sites would otherwise duplicate). Actually, to avoid divergence, this task introduces `atomic_write_json` and uses it **only** for the failures file; Task 4's history savers remain self-contained (already tested, not worth churning).

**Files:**
- Create: `src/stdf_platform/atomic.py`
- Modify: `src/stdf_platform/cli.py` (`_run_ingest_batch`, after computing `successes, failures` — lines 419–447)
- Test: `src/tests/test_ingest_failures.py`

**Steps:**

- [ ] Write a failing test. Create `src/tests/test_ingest_failures.py`. It calls `_run_ingest_batch` with a worker pool monkeypatched to return a known failure, then asserts `data/ingest_failures.json` exists with the expected structure and no temp residue:

  ```python
  """`_run_ingest_batch` writes data/ingest_failures.json atomically."""

  import json
  from pathlib import Path

  from stdf_platform import cli
  from stdf_platform.config import Config, StorageConfig
  from stdf_platform.sync_manager import SyncManager
  from stdf_platform.worker import IngestResult


  def test_writes_failures_json(tmp_path, monkeypatch):
      data_dir = tmp_path / "data"
      data_dir.mkdir()
      config = Config(storage=StorageConfig(data_dir=data_dir, database=data_dir / "db"))
      sync = SyncManager(data_dir / "sync_history.json")

      bad = data_dir / "broken.stdf"

      def fake_pool(files, data_dir, compression, max_workers, timeout):
          fail = IngestResult(local_path=bad, remote_path="r/broken.stdf",
                              success=False, error="boom")
          return [], [fail]

      monkeypatch.setattr(cli, "run_ingest_pool", fake_pool, raising=False)
      # _run_ingest_batch imports run_ingest_pool locally; patch the source too.
      monkeypatch.setattr("stdf_platform.worker.run_ingest_pool", fake_pool)

      cli._run_ingest_batch(
          config, sync, [("r/broken.stdf", bad, "PROD", "CP")],
          cleanup=False, verbose=False,
      )

      failures_file = data_dir / "ingest_failures.json"
      assert failures_file.exists()
      payload = json.loads(failures_file.read_text(encoding="utf-8"))
      assert payload["failures"][0]["error"] == "boom"
      assert payload["failures"][0]["path"].endswith("broken.stdf")

      leftovers = [p.name for p in data_dir.iterdir() if p.name.startswith(".ingest_failures")]
      assert leftovers == []
  ```

- [ ] Run it and watch it fail (no failures file written, and `atomic` module missing):
  ```bash
  uv run pytest src/tests/test_ingest_failures.py -q
  ```
  Expected: failure — `data/ingest_failures.json` does not exist.

- [ ] Create `src/stdf_platform/atomic.py`:

  ```python
  """Atomic JSON file writes (temp file in same dir + os.replace)."""

  import json
  import os
  import tempfile
  from pathlib import Path
  from typing import Any


  def atomic_write_json(path: Path, obj: Any) -> None:
      """Write `obj` as JSON to `path` atomically.

      Writes to a temp file in the destination's directory, then os.replace —
      readers never observe a partial file. Cleans up the temp file on failure.
      """
      path.parent.mkdir(parents=True, exist_ok=True)
      fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
      try:
          with os.fdopen(fd, "w", encoding="utf-8") as f:
              json.dump(obj, f, indent=2, ensure_ascii=False)
          os.replace(tmp, path)
      except BaseException:
          try:
              os.unlink(tmp)
          except OSError:
              pass
          raise
  ```

- [ ] Wire it into `_run_ingest_batch` in `cli.py`. After the `successes, failures = run_ingest_pool(...)` call and the existing print/cleanup logic, write the failures file. Edit the function body — add the import near the top of the function and the write before `return`.

  Old (lines 417–432, the start of `_run_ingest_batch` body through the failure print):
  ```python
      from .worker import run_ingest_pool

      successes, failures = run_ingest_pool(
          files=to_ingest,
          data_dir=config.storage.data_dir,
          compression=config.processing.compression,
          max_workers=max_workers,
          timeout=timeout,
      )

      for result in successes:
          sync_manager.mark_ingested(result.remote_path)

      console.print(f"\n[green]✓[/green] Ingested {len(successes)} files")
      if failures:
          console.print(f"[yellow]![/yellow] {len(failures)} files failed (will retry on next fetch)")
  ```

  New:
  ```python
      from .worker import run_ingest_pool
      from .atomic import atomic_write_json

      successes, failures = run_ingest_pool(
          files=to_ingest,
          data_dir=config.storage.data_dir,
          compression=config.processing.compression,
          max_workers=max_workers,
          timeout=timeout,
      )

      for result in successes:
          sync_manager.mark_ingested(result.remote_path)

      # Structured failure record for automation (always written, even if empty).
      atomic_write_json(
          config.storage.data_dir / "ingest_failures.json",
          {
              "failures": [
                  {"path": str(r.local_path), "remote_path": r.remote_path, "error": r.error}
                  for r in failures
              ]
          },
      )

      console.print(f"\n[green]✓[/green] Ingested {len(successes)} files")
      if failures:
          console.print(f"[yellow]![/yellow] {len(failures)} files failed (will retry on next fetch)")
  ```

- [ ] Run the failures test and watch it pass:
  ```bash
  uv run pytest src/tests/test_ingest_failures.py -q
  ```
  Expected: `1 passed`.

- [ ] Run the full suite:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `48 passed` (47 + 1).

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat: write data/ingest_failures.json after batch ingest

  Add atomic_write_json helper and have _run_ingest_batch always emit a
  structured failures record (path, remote_path, error) so automation can
  detect failures mechanically. Written atomically; empty list when none.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 8 — Minor cleanup: unused config/env, CLAUDE.md, log rotation (spec 1.8)

Four small items: (a) remove unused `ProcessingConfig.batch_size` (`config.py:64`); (b) remove the unused `STDF_DB_PATH` env var set in `cli.py:785`; (c) fix CLAUDE.md's `ProcessPoolExecutor` wording to `ThreadPoolExecutor` + subprocess; (d) add size-capped rotation for `data/ingest_worker.log` in `worker.py`.

Note: a `processing:` block in a user's `config.yaml` may still contain `batch_size:`. `Config.load` does `ProcessingConfig(**processing_data)`, which would raise `TypeError` on an unknown key. To stay backward-compatible without reintroducing the field, filter `processing_data` to known fields in `load`.

**Files:**
- Modify: `src/stdf_platform/config.py` (remove `batch_size` line 64; filter `processing_data` in `load`)
- Modify: `src/stdf_platform/cli.py` (remove `os.environ["STDF_DB_PATH"] = ...` line 785)
- Modify: `CLAUDE.md` (ProcessPoolExecutor → ThreadPoolExecutor wording)
- Modify: `src/stdf_platform/worker.py` (log rotation in `_run_single`, lines 85–91)
- Test: `src/tests/test_log_rotation.py`, plus a `batch_size`-tolerance assertion in `test_config_load.py`

**Steps:**

- [ ] Add a config regression test for the removed field's tolerance. Append to `src/tests/test_config_load.py`:

  ```python
  def test_load_tolerates_legacy_batch_size(tmp_path, monkeypatch):
      monkeypatch.delenv("STDF_CONFIG", raising=False)
      cfg_file = tmp_path / "config.yaml"
      cfg_file.write_text(
          "processing:\n  batch_size: 500\n  compression: gzip\n", encoding="utf-8"
      )
      cfg = Config.load(cfg_file)
      assert cfg.processing.compression == "gzip"
      assert not hasattr(cfg.processing, "batch_size")
  ```

- [ ] Write a failing log-rotation test. Create `src/tests/test_log_rotation.py`. It pre-fills `ingest_worker.log` past the cap, runs `_run_single` with a fake subprocess emitting stderr, and asserts the log was rotated (size bounded; a `.1` backup exists):

  ```python
  """Size-capped rotation of data/ingest_worker.log in worker._run_single."""

  import subprocess

  from stdf_platform import worker


  class _FakeProc:
      def __init__(self, stderr):
          self.returncode = 0
          self._stderr = stderr

      def communicate(self, timeout=None):
          return '{"sub_process": "CP1", "test_category": "CP"}', self._stderr

      def kill(self):
          pass


  def test_log_rotates_when_over_cap(tmp_path, monkeypatch):
      log_path = tmp_path / "ingest_worker.log"
      log_path.write_text("X" * (worker.LOG_MAX_BYTES + 10), encoding="utf-8")

      monkeypatch.setattr(
          worker.subprocess, "Popen",
          lambda cmd, **kw: _FakeProc(stderr="new error line\n"),
      )

      result = worker._run_single(
          local_path=tmp_path / "a.stdf", product="PROD", data_dir=tmp_path,
          compression="zstd", timeout=5, log_path=log_path,
      )
      assert result.success

      # Rotated: a .1 backup exists and the active log is back under the cap.
      assert (tmp_path / "ingest_worker.log.1").exists()
      assert log_path.stat().st_size <= worker.LOG_MAX_BYTES
  ```

- [ ] Run the new tests and watch them fail (`LOG_MAX_BYTES` not defined; rotation absent; legacy batch_size raises):
  ```bash
  uv run pytest src/tests/test_log_rotation.py src/tests/test_config_load.py -q
  ```
  Expected: log test errors with `AttributeError: ... LOG_MAX_BYTES`; `test_load_tolerates_legacy_batch_size` fails with `TypeError` about `batch_size`.

- [ ] Remove `batch_size` from `ProcessingConfig` in `config.py`.

  Old (lines 61–65):
  ```python
  @dataclass
  class ProcessingConfig:
      """Processing configuration."""
      batch_size: int = 1000
      compression: str = "zstd"
  ```

  New:
  ```python
  @dataclass
  class ProcessingConfig:
      """Processing configuration."""
      compression: str = "zstd"
  ```

- [ ] Make `Config.load` tolerant of unknown `processing` keys. Replace the processing construction line (currently line 151 inside the `return cls(...)`):

  Old:
  ```python
              processing=ProcessingConfig(**processing_data) if processing_data else ProcessingConfig(),
  ```

  New:
  ```python
              processing=ProcessingConfig(
                  **{k: v for k, v in processing_data.items() if k == "compression"}
              ) if processing_data else ProcessingConfig(),
  ```

- [ ] Remove the unused `STDF_DB_PATH` env var in `cli.py` `web` command.

  Old (lines 783–785):
  ```python
      config: Config = ctx.obj["config"]
      os.environ["STDF_DATA_DIR"] = str(config.storage.data_dir)
      os.environ["STDF_DB_PATH"] = str(config.storage.database)
  ```

  New:
  ```python
      config: Config = ctx.obj["config"]
      os.environ["STDF_DATA_DIR"] = str(config.storage.data_dir)
  ```

- [ ] Add log rotation to `worker.py`. Define the cap constant near the top (after `console = Console()`, line 26):

  Old:
  ```python
  console = Console()
  ```

  New:
  ```python
  console = Console()

  # Cap on data/ingest_worker.log before it is rotated to .1 (single backup).
  LOG_MAX_BYTES = 5 * 1024 * 1024
  ```

  Then replace the stderr-append block in `_run_single` (lines 85–91):

  Old:
  ```python
      # Append stderr to log file after process exits (avoids pipe race condition)
      if log_path and stderr:
          try:
              with open(log_path, "a", encoding="utf-8") as f:
                  f.write(stderr)
          except OSError:
              pass
  ```

  New:
  ```python
      # Append stderr to log file after process exits (avoids pipe race condition).
      # Rotate to a single .1 backup once the log exceeds LOG_MAX_BYTES.
      if log_path and stderr:
          try:
              if log_path.exists() and log_path.stat().st_size >= LOG_MAX_BYTES:
                  backup = log_path.with_suffix(log_path.suffix + ".1")
                  try:
                      os.replace(log_path, backup)
                  except OSError:
                      pass
              with open(log_path, "a", encoding="utf-8") as f:
                  f.write(stderr)
          except OSError:
              pass
  ```

  Add `import os` to `worker.py` imports. Old (lines 8–11):
  ```python
  import json
  import sys
  import subprocess
  from concurrent.futures import ThreadPoolExecutor, as_completed
  ```

  New:
  ```python
  import json
  import os
  import sys
  import subprocess
  from concurrent.futures import ThreadPoolExecutor, as_completed
  ```

- [ ] Fix CLAUDE.md wording. The Project Overview line currently reads "...Python-only parser with ProcessPoolExecutor for parallel batch ingestion." Replace `ProcessPoolExecutor` with the accurate mechanism:

  Old:
  ```
  Python-only parser with ProcessPoolExecutor for parallel batch ingestion.
  ```

  New:
  ```
  Python-only parser; parallel batch ingestion uses a ThreadPoolExecutor of isolated subprocesses (one subprocess per file).
  ```

- [ ] Run the targeted tests and watch them pass:
  ```bash
  uv run pytest src/tests/test_log_rotation.py src/tests/test_config_load.py -q
  ```
  Expected: `6 passed` (5 config + 1 rotation).

- [ ] Run the full suite:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `50 passed` (48 + 1 rotation + 1 legacy-batch_size).

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  chore: drop unused batch_size/STDF_DB_PATH; rotate worker log

  Remove unused ProcessingConfig.batch_size (tolerate legacy key in config.yaml)
  and the unused STDF_DB_PATH env var. Add single-backup size-capped rotation
  for data/ingest_worker.log. Fix CLAUDE.md: ThreadPoolExecutor + subprocess,
  not ProcessPoolExecutor.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Final verification (after all 8 tasks)

- [ ] Full suite green:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `50 passed`.

- [ ] Sanity grep — no stale references remain:
  ```bash
  grep -rn "test_results\|STDF_DB_PATH\|batch_size\|ProcessPoolExecutor\|db_tuple\|with lock" \
    src/stdf_platform CLAUDE.md || echo "all clear"
  ```
  Expected: `all clear` (the only `batch_size` hit acceptable is the comprehension key filter in `config.py`, which is intentional — if it appears, confirm it is that line and nothing else).

- [ ] `git log --oneline -8` shows the eight task commits on `main`.

---

## Spec coverage map

| Spec item | Task | Key deliverable |
|-----------|------|-----------------|
| 1.1 | 1 | delete app.py + test_timeout.py; rewrite test_ingest.py as pytest |
| 1.2 | 3 | export lot → test_data_final/parts_final + regression test |
| 1.3 | 2 | views.py single source; database.py/deps.py/query.py import it |
| 1.4 | 4 | atomic SyncManager._save / IngestHistory._save |
| 1.5 | 5 | per-request connections, drop lock, GET /health |
| 1.6 | 6 | Config.load: arg → STDF_CONFIG → cwd |
| 1.7 | 7 | data/ingest_failures.json (atomic_write_json) |
| 1.8 | 8 | drop batch_size/STDF_DB_PATH, CLAUDE.md wording, log rotation |
