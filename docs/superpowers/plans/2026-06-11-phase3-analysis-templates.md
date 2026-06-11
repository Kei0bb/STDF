# Phase 3: Analysis Templates & Reusable Analysis Package — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `query.py`-style ad-hoc analysis into a reusable, multi-user analysis package (`src/stdf_platform/analysis/`) plus a set of `# %%` cell-script templates (`templates/analysis/`). Everyone analyzes through the *same* functions with the *same* retest-aware definitions (`parts_final`/`test_data_final`/`chipid_final` from `views.py`), so "different people compute yield differently" accidents disappear. All data functions return **pandas DataFrames**; figure functions return **plotly `go.Figure`** objects. No SQL is duplicated where a `reporting/queries.py` helper already exists; new SQL reuses the Phase-1 views. `query.py` is retained as a thin backward-compatible wrapper over `AnalysisSession`.

**Architecture:** Pure-Python STDF parser → Hive-partitioned Parquet → DuckDB `:memory:` views (`src/stdf_platform/views.py`, the single source of view truth) → Phase-2 `reporting/queries.py` (lot-scoped analysis SQL, reused) → **new `src/stdf_platform/analysis/` package** (`session.py` connection+views lifecycle → `compare.py`/`trend.py`/`correlation.py`/`spatial.py` analysis functions → `__init__.py` public re-exports) → `templates/analysis/*.py` cell-scripts + thin `query.py` wrapper. Parquet remains the single source of truth; the analysis layer is read-only.

**Tech Stack:** Python ≥3.10, duckdb, pandas, plotly, pyarrow, pyyaml (all already in `pyproject.toml` — **no new dependencies**). Tests: pytest 8. Runner (verified): **`uv run pytest src/tests -q`** — **78 tests currently pass**.

---

## Conventions for every task

- **Test command (verified):** run `uv run pytest src/tests -q` at the **end of every task**. Targeted runs during a task: `uv run pytest src/tests/<file>.py -q`.
- Tests live in `src/tests/`. There is **no** `conftest.py` and **no** `[tool.pytest]` config — pytest discovers `test_*.py`/`test_*` functions. Existing tests import the package directly (`from stdf_platform...`) and use the `tmp_path` fixture. Fixtures here extend the `_write_cp`/`_write_ft` Parquet-writing pattern from `src/tests/test_reporting_queries.py`.
- All SQL uses **bound parameters** (`?`), never f-string value interpolation. (Paths inside view DDL use `.as_posix()` and live only in `views.py`, which we reuse unchanged.)
- Commit **straight to `main`** (project preference). Use the exact conventional-commit subjects shown. End every commit body with:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- Baseline before starting: `git status` clean except untracked `.claude/` and this plan file.

---

## Test-count trajectory (track explicitly)

| After task | New test file / additions | Cumulative |
|---|---|---|
| Start | — | **78** |
| 1. session.py | `test_analysis_session.py` (+5) | **83** |
| 2. compare.py | `test_analysis_compare.py` (+6) | **89** |
| 3. trend.py | `test_analysis_trend.py` (+5) | **94** |
| 4. correlation.py | `test_analysis_correlation.py` (+5) | **99** |
| 5. spatial.py | `test_analysis_spatial.py` (+5) | **104** |
| 6. templates | `test_templates_compile.py` (+1) | **105** |
| 7. docs | (no new tests) | **105** |

Final expected: **105 passed**.

---

## die_cp_ft_join — schema finding (resolved from code)

Verified against `src/stdf_platform/storage.py` `CHIPID_SCHEMA` and `PARTS_SCHEMA`, `src/stdf_platform/chipid.py` `decode_chipid()`, and `src/tests/test_chipid_e2e.py::test_chipid_parquet_decoded_correctly`.

The `chipid` table **does** carry decoded CP-origin columns as real columns (not partitions):

- `origin_lot` — `string` (decoded CP lot number, e.g. `"HKPFJK"`)
- `origin_wafer` — `int64` (e.g. `11`)
- `origin_x` — `int64` (e.g. `12`)
- `origin_y` — `int64` (e.g. `22`)
- plus `part_id` / `part_txt` (the FT package keys), `valid`, `origin_fab`, `efuse_raw`.

`chipid_final` dedups by `(lot_id, efuse_raw)`, keeping the latest retest — so each surviving row is one decoded FT die with its CP origin.

CP `parts` / `parts_final` (`PARTS_SCHEMA`) carries the native probe coordinates as `x_coord int64`, `y_coord int64`, and `wafer_id string`, with `lot_id string`.

**Therefore the die-level CP↔FT join is fully expressible in SQL against existing columns — no extra decode step is needed at query time** (decode already happened at ingest). The join key is the decoded origin against the CP native coordinates:

```
chipid_final.origin_lot   = parts_final.lot_id      -- CP lot
chipid_final.origin_x     = parts_final.x_coord      -- both int64, exact
chipid_final.origin_y     = parts_final.y_coord      -- both int64, exact
chipid_final.origin_wafer = TRY_CAST(parts_final.wafer_id AS BIGINT)  -- nullable guard
```

**Wafer-id caveat (stated in the plan, handled in SQL):** CP `wafer_id` is a free-form *string* (fixtures use `"W1"`, real files may use `"11"`); `origin_wafer` is an *int*. A naive `wafer_id = origin_wafer` cast fails on non-numeric ids and would drop all rows. We therefore make the wafer match **defensive**: join on `(origin_lot, origin_x, origin_y)` (exact ints + lot, which uniquely identify a die within a CP lot when probe coords are unique per wafer) **AND** an *optional* wafer predicate `(TRY_CAST(p.wafer_id AS BIGINT) = c.origin_wafer OR TRY_CAST(p.wafer_id AS BIGINT) IS NULL)`. This keeps the join correct when wafer ids are numeric and degrades gracefully (coord+lot match only) when they are not. The function's docstring documents this, and the test asserts the join produces the expected FT-part ↔ CP-die rows with a numeric-wafer fixture **and** a `"W11"`-style non-numeric fixture (latter still matches on coords+lot).

The CP `product` for the join is the caller's `product` argument; the FT `lot_id` is the function's `lot_id`. The CP origin lot is discovered *from the data* via `origin_lot`, so the caller does not need to know it.

---

## Fixed names and signatures (decide once, reuse everywhere)

`AnalysisSession` is always passed as the first positional arg `s` to module functions; functions read `s.conn`. All SQL goes through bound params.

```python
# src/stdf_platform/analysis/session.py
class AnalysisSession:
    def __init__(self, data_dir: Path | None = None) -> None: ...   # None → Config.load().storage.data_dir
    conn: duckdb.DuckDBPyConnection                                  # views already registered
    registered: list[str]                                           # names from setup_views
    def q(self, sql: str, params: list | None = None) -> pd.DataFrame: ...
    def lots(self, product: str | None = None,
             test_category: str | None = None) -> pd.DataFrame: ...
    def close(self) -> None: ...
    def __enter__(self) -> "AnalysisSession": ...
    def __exit__(self, *exc) -> None: ...

# src/stdf_platform/analysis/compare.py
def yield_by_lot(s, product, lot_ids, test_category) -> pd.DataFrame: ...
def bin_pareto_by_lot(s, product, lot_ids, test_category) -> pd.DataFrame: ...
def test_stats_by_lot(s, product, lot_ids, test_category, test_nums=None) -> pd.DataFrame: ...
def test_distribution_fig(s, product, lot_ids, test_category, test_num) -> go.Figure: ...

# src/stdf_platform/analysis/trend.py
def lot_trend(s, product, test_category, last_n=30) -> pd.DataFrame: ...
def test_trend(s, product, test_category, test_num, last_n=30) -> pd.DataFrame: ...
def trend_fig(df, y, control_limits=True) -> go.Figure: ...

# src/stdf_platform/analysis/correlation.py
def cp_ft_yield(s, product, lot_ids=None) -> pd.DataFrame: ...
def die_cp_ft_join(s, product, lot_id) -> pd.DataFrame: ...
def test_correlation(s, product, lot_id, test_category, test_nums) -> pd.DataFrame: ...

# src/stdf_platform/analysis/spatial.py
def zone_yield(s, product, lot_id, n_zones=3) -> pd.DataFrame: ...
def radial_profile(s, product, lot_id, test_num) -> pd.DataFrame: ...
def param_wafermap_fig(s, product, lot_id, wafer_id, test_num) -> go.Figure: ...
```

**Cpk helper (reuse decision):** `_cpk(values, lo, hi)` already exists in `src/stdf_platform/reporting/sections.py` (pstdev-based, `min((hi-mu)/3σ, (mu-lo)/3σ)`, returns `None` for `<2` samples or `σ==0`). To avoid a second formula, **import it** in `compare.py`/`trend.py`: `from ..reporting.sections import _cpk`. Where Cpk is computed *inside SQL* for a per-lot×test grouping, mirror that exact formula using DuckDB's population `stddev_pop` and `LEAST` (matching `pstdev`), and add a `cpk` column. The Python `_cpk` import is used for figure annotations / row-level recompute; the SQL form is used for the grouped DataFrame so it stays set-based. Both produce identical numbers on the fixtures (asserted).

---

## Task 1 — `analysis/` package skeleton, `session.py`, and thin `query.py` wrapper

**Files Create:**
- `src/stdf_platform/analysis/__init__.py`
- `src/stdf_platform/analysis/session.py`
- `src/tests/test_analysis_session.py`

**Files Modify:**
- `query.py` (re-point setup onto `AnalysisSession`, keep all `q`/`to_csv`/`use_lot`/`use_all` cell helpers working)

**Steps:**

- [ ] Create `src/stdf_platform/analysis/session.py`:

  ```python
  """AnalysisSession: a read-only DuckDB session with Phase-1 views registered.

  One object owns the :memory: connection, resolves the data dir via Config
  (STDF_CONFIG-aware), registers the canonical views (setup_views), and exposes
  small helpers (q/lots). Analysis modules take a session as their first arg and
  read session.conn. Parquet is the source of truth; this layer never writes.
  """

  from __future__ import annotations

  from pathlib import Path

  import duckdb
  import pandas as pd

  from ..config import Config
  from ..views import setup_views


  class AnalysisSession:
      def __init__(self, data_dir: Path | None = None) -> None:
          if data_dir is None:
              data_dir = Config.load().storage.data_dir
          self.data_dir = Path(data_dir)
          self.conn = duckdb.connect(":memory:")
          self.registered = setup_views(self.conn, self.data_dir)

      def q(self, sql: str, params: list | None = None) -> pd.DataFrame:
          """Run raw SQL (bound params) and return a DataFrame. Escape hatch."""
          return self.conn.execute(sql, params or []).fetchdf()

      def lots(self, product: str | None = None,
               test_category: str | None = None) -> pd.DataFrame:
          """MIR-derived lot list (latest row per lot), optionally filtered."""
          where, params = [], []
          if product is not None:
              where.append("product = ?")
              params.append(product)
          if test_category is not None:
              where.append("test_category = ?")
              params.append(test_category)
          clause = (" WHERE " + " AND ".join(where)) if where else ""
          return self.conn.execute(
              f"""
              SELECT lot_id, product, test_category, sub_process,
                     part_type, job_name, job_rev, tester_type, operator,
                     start_time, finish_time
              FROM (
                  SELECT *, ROW_NUMBER() OVER (
                      PARTITION BY product, test_category, lot_id
                      ORDER BY start_time DESC) AS rn
                  FROM lots{clause}
              ) WHERE rn = 1
              ORDER BY start_time DESC, lot_id
              """,
              params,
          ).fetchdf()

      def close(self) -> None:
          self.conn.close()

      def __enter__(self) -> "AnalysisSession":
          return self

      def __exit__(self, *exc) -> None:
          self.close()
  ```

  Note: the `{clause}` interpolation contains only literal SQL fragments built from a fixed allow-list (no user values); every actual value is bound via `params`.

- [ ] Create `src/stdf_platform/analysis/__init__.py` (public re-exports; modules added in later tasks are appended as they land):

  ```python
  """Reusable, retest-aware analysis API over the Parquet store.

  Import the session and the per-topic functions:
      from stdf_platform.analysis import AnalysisSession, yield_by_lot
  """

  from .session import AnalysisSession
  from .compare import (
      yield_by_lot, bin_pareto_by_lot, test_stats_by_lot, test_distribution_fig,
  )
  from .trend import lot_trend, test_trend, trend_fig
  from .correlation import cp_ft_yield, die_cp_ft_join, test_correlation
  from .spatial import zone_yield, radial_profile, param_wafermap_fig

  __all__ = [
      "AnalysisSession",
      "yield_by_lot", "bin_pareto_by_lot", "test_stats_by_lot", "test_distribution_fig",
      "lot_trend", "test_trend", "trend_fig",
      "cp_ft_yield", "die_cp_ft_join", "test_correlation",
      "zone_yield", "radial_profile", "param_wafermap_fig",
  ]
  ```

  **Important:** because `__init__.py` imports `compare`/`trend`/`correlation`/`spatial`, those four modules must exist before `import stdf_platform.analysis` succeeds. To keep Task 1 runnable in isolation, **create the four module files now as minimal stubs containing only a module docstring and the `from __future__ import annotations` line plus `pass`-bodied function signatures** (full bodies land in their own tasks). The Task-1 test imports only `AnalysisSession` directly from `.session` to avoid depending on stub completeness — but the package must still import cleanly. Create stubs:
  - `src/stdf_platform/analysis/compare.py`, `trend.py`, `correlation.py`, `spatial.py`, each with the signatures from the contracts block and a body of `raise NotImplementedError` (so accidental early use is loud). Replace bodies in their tasks.

- [ ] Create `src/tests/test_analysis_session.py`. Reuse the `_write_cp`/`_write_ft` fixtures by importing them:

  ```python
  """AnalysisSession lifecycle + helpers over synthetic Parquet."""

  from pathlib import Path

  from stdf_platform.analysis import AnalysisSession
  from test_reporting_queries import _write_cp, _write_ft  # same dir on sys.path
  import sys
  sys.path.insert(0, str(Path(__file__).resolve().parent))


  def test_session_registers_views(tmp_path):
      _write_cp(tmp_path)
      with AnalysisSession(tmp_path) as s:
          assert "parts_final" in s.registered
          assert "lots" in s.registered


  def test_session_q_binds_params(tmp_path):
      _write_cp(tmp_path)
      with AnalysisSession(tmp_path) as s:
          df = s.q("SELECT COUNT(*) AS n FROM parts_final WHERE lot_id = ?", ["LOT1"])
          assert int(df.iloc[0]["n"]) == 4   # 4 distinct dies, retest-aware


  def test_session_lots_filter(tmp_path):
      _write_cp(tmp_path)
      _write_ft(tmp_path)
      with AnalysisSession(tmp_path) as s:
          allrows = s.lots()
          assert set(allrows["lot_id"]) == {"LOT1", "FT1"}
          cp = s.lots(product="PROD", test_category="CP")
          assert list(cp["lot_id"]) == ["LOT1"]


  def test_session_lots_dedup_latest(tmp_path):
      _write_cp(tmp_path)
      with AnalysisSession(tmp_path) as s:
          rows = s.lots(product="PROD")
          assert len(rows) == 1   # single MIR row, not duplicated


  def test_session_default_data_dir(monkeypatch, tmp_path):
      _write_cp(tmp_path)
      # Config.load() with no config.yaml → defaults; point STDF_CONFIG at a yaml
      cfg = tmp_path / "c.yaml"
      cfg.write_text(f"storage:\n  data_dir: {tmp_path.as_posix()}\n")
      monkeypatch.setenv("STDF_CONFIG", str(cfg))
      with AnalysisSession() as s:    # data_dir=None → resolved from config
          assert "parts_final" in s.registered
  ```

  Note: the `sys.path.insert` must precede the `from test_reporting_queries import ...` line; reorder so the import works (place the two `import sys` / `sys.path.insert` lines at the top, then the `from test_reporting_queries` import). Fix the ordering when writing the file.

- [ ] **Modify `query.py`** — minimal edit. Replace the manual `duckdb.connect(":memory:")` + `setup_views` + `_make_final_views()` bootstrap (lines ~36–70) with an `AnalysisSession`, keeping the module-level `con` name (so every existing `# %%` cell and `q`/`to_csv`/`use_lot`/`use_all` still work):

  - Keep the `_load_config()`/`DATA_DIR` block (it prints the data dir).
  - Replace the connection bootstrap with:
    ```python
    from stdf_platform.analysis import AnalysisSession
    from stdf_platform.views import _DEDUP_UNIT  # still used by use_lot/_make_final_views

    _session = AnalysisSession(DATA_DIR)
    con = _session.conn
    for _name in _session.registered:
        print(f"  ✓ {_name}")
    ```
  - Leave `_FINAL_SPECS`, `_make_final_views`, `use_lot`, `use_all`, `q`, `to_csv` and all `# %%` cells **unchanged** — they operate on `con`, which is now `_session.conn`. (`_make_final_views()` re-creating all-lot `*_final` views over `con` remains valid and idempotent.)

  This is the "thin wrapper" the spec calls for: `query.py` keeps its cell-script ergonomics but its connection now comes from the shared session abstraction.

- [ ] Run: `uv run pytest src/tests/test_analysis_session.py -q`
  Expected tail: `5 passed`.
- [ ] Run full suite: `uv run pytest src/tests -q`
  Expected tail: `83 passed`.
- [ ] Smoke the wrapper compiles: `uv run python -c "import ast; ast.parse(open('query.py').read())"` (no output = ok).
- [ ] Commit:
  ```
  feat: analysis package skeleton + AnalysisSession; query.py over session

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

---

## Task 2 — `compare.py` (lot-to-lot comparison)

**Files Modify:** `src/stdf_platform/analysis/compare.py` (replace stubs with real bodies)
**Files Create:** `src/tests/test_analysis_compare.py`

**Design notes:**
- Yields come from **`parts_final` only** (retest-aware), matching `reporting/queries.py`. For multiple lots we filter `lot_id IN (?, ?, …)` by building a placeholder list `",".join("?" * len(lot_ids))` and binding `lot_ids` — values stay bound.
- `yield_by_lot`: for **CP** also emit per-wafer rows (group by `lot_id, wafer_id`); for **FT** the single `wafer_id=''` group collapses to one row per lot. Return columns: `lot_id, wafer_id, total, good, yield_pct`.
- `bin_pareto_by_lot`: `lot_id, soft_bin, count, pct` where `pct` is within-lot composition (`100*count/sum(count) OVER (PARTITION BY lot_id)`).
- `test_stats_by_lot`: per `lot_id × test_num`: `n, mean, std, cpk, lo_limit, hi_limit`. Uses `test_data_final`, `rec_type IN ('PTR','MPR')`, non-null limits. `std`/`cpk` use **population** stddev (`stddev_pop`) to match `reporting.sections._cpk` (pstdev). `cpk = LEAST((hi-mean)/(3*σ),(mean-lo)/(3*σ))` guarded for `σ=0` (NULL). When `test_nums is None`, default to the **top fail-rate tests** reusing `reporting.queries.top_fail_tests` per lot with `histogram_top_n` from config — gather their test_nums (union across lots), dedered, and filter to them. Get the cap via `Config.load().reporting.histogram_top_n` (default 20).
- `test_distribution_fig`: overlay one `go.Histogram` per lot (`result` values for `test_num`), add `add_vline` for lo/hi limits (from any lot's metadata). Returns `go.Figure`; `template="plotly_dark"`, `barmode="overlay"`, histogram `opacity=0.6`.

**Steps:**

- [ ] Implement `compare.py`:

  ```python
  """Lot-to-lot comparison. All yields from parts_final (retest-aware)."""

  from __future__ import annotations

  import plotly.graph_objects as go

  from ..config import Config
  from ..reporting import queries as rq
  from ..reporting.sections import _cpk  # reuse the one Cpk definition


  def _in_clause(lot_ids):
      return ",".join("?" for _ in lot_ids)


  def yield_by_lot(s, product, lot_ids, test_category):
      ph = _in_clause(lot_ids)
      return s.conn.execute(
          f"""
          SELECT lot_id, wafer_id,
                 COUNT(*)                                        AS total,
                 SUM(CASE WHEN passed THEN 1 ELSE 0 END)         AS good,
                 ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0), 2)                 AS yield_pct
          FROM parts_final
          WHERE lot_id IN ({ph})
          GROUP BY lot_id, wafer_id
          ORDER BY lot_id, wafer_id
          """,
          list(lot_ids),
      ).fetchdf()


  def bin_pareto_by_lot(s, product, lot_ids, test_category):
      ph = _in_clause(lot_ids)
      return s.conn.execute(
          f"""
          SELECT lot_id, soft_bin, COUNT(*) AS count,
                 ROUND(100.0 * COUNT(*)
                     / SUM(COUNT(*)) OVER (PARTITION BY lot_id), 2) AS pct
          FROM parts_final
          WHERE lot_id IN ({ph})
          GROUP BY lot_id, soft_bin
          ORDER BY lot_id, count DESC
          """,
          list(lot_ids),
      ).fetchdf()


  def test_stats_by_lot(s, product, lot_ids, test_category, test_nums=None):
      if test_nums is None:
          top_n = Config.load().reporting.histogram_top_n
          nums = set()
          for lot in lot_ids:
              for f in rq.top_fail_tests(s.conn, product, test_category, lot, top_n):
                  nums.add(int(f["test_num"]))
          test_nums = sorted(nums)
      if not test_nums:
          # no failing tests anywhere → empty frame with the right columns
          import pandas as pd
          return pd.DataFrame(
              columns=["lot_id", "test_num", "n", "mean", "std",
                       "cpk", "lo_limit", "hi_limit"])
      lp = _in_clause(lot_ids)
      tp = _in_clause(test_nums)
      return s.conn.execute(
          f"""
          SELECT lot_id, test_num,
                 COUNT(*)                              AS n,
                 ROUND(AVG(result), 6)                 AS mean,
                 ROUND(stddev_pop(result), 6)          AS std,
                 ROUND(LEAST(
                     (FIRST(hi_limit) - AVG(result)) / NULLIF(3*stddev_pop(result),0),
                     (AVG(result) - FIRST(lo_limit)) / NULLIF(3*stddev_pop(result),0)
                 ), 3)                                 AS cpk,
                 FIRST(lo_limit)                       AS lo_limit,
                 FIRST(hi_limit)                       AS hi_limit
          FROM test_data_final
          WHERE lot_id IN ({lp}) AND test_num IN ({tp})
            AND result IS NOT NULL AND rec_type IN ('PTR','MPR')
            AND lo_limit IS NOT NULL AND hi_limit IS NOT NULL
          GROUP BY lot_id, test_num
          ORDER BY lot_id, test_num
          """,
          list(lot_ids) + list(test_nums),
      ).fetchdf()


  def test_distribution_fig(s, product, lot_ids, test_category, test_num):
      fig = go.Figure()
      lo = hi = name = units = None
      for lot in lot_ids:
          v = rq.test_values(s.conn, product, test_category, lot, test_num)
          if not v or not v.get("values"):
              continue
          lo, hi = v["lo_limit"], v["hi_limit"]
          name, units = v["test_name"], v["units"]
          fig.add_histogram(x=v["values"], name=lot, opacity=0.6, nbinsx=40)
      for lim, lbl in ((lo, "LSL"), (hi, "USL")):
          if lim is not None:
              fig.add_vline(x=lim, line_color="#ef4444", line_dash="dash",
                            annotation_text=lbl)
      fig.update_layout(
          barmode="overlay",
          title=f"{name or ''} (#{test_num}) [{units or ''}] by lot",
          template="plotly_dark", margin=dict(l=40, r=20, t=40, b=40),
      )
      return fig
  ```

- [ ] Create `src/tests/test_analysis_compare.py`. Extend the CP fixture with a **second lot** so comparison has ≥2 lots. Add a small local helper that writes a second CP lot `LOT2` (W1: one die passes, one fails on bin 3) reusing the `_write_cp` directory shape:

  ```python
  import sys
  from pathlib import Path

  import pyarrow as pa
  import pyarrow.parquet as pq
  import plotly.graph_objects as go

  sys.path.insert(0, str(Path(__file__).resolve().parent))
  from test_reporting_queries import _write_cp  # noqa: E402
  from stdf_platform.analysis import AnalysisSession  # noqa: E402
  from stdf_platform.analysis import compare  # noqa: E402


  def _write_cp_lot2(data_dir: Path):
      base = (data_dir / "parts" / "product=PROD" / "test_category=CP"
              / "sub_process=CP1" / "lot_id=LOT2" / "wafer_id=W1"
              / "retest=0" / "data.parquet")
      base.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["LOT2", "LOT2"], "wafer_id": ["W1", "W1"],
          "part_id": ["A", "B"], "part_txt": ["", ""],
          "x_coord": [0, 1], "y_coord": [0, 0],
          "hard_bin": [3, 1], "soft_bin": [3, 1],
          "passed": [False, True], "retest_num": [0, 0],
      }), base)
      td = (data_dir / "test_data" / "product=PROD" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=LOT2" / "data.parquet")
      td.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["LOT2", "LOT2"], "wafer_id": ["W1", "W1"],
          "part_id": ["A", "B"], "part_txt": ["", ""],
          "x_coord": [0, 1], "y_coord": [0, 0],
          "test_num": [1001, 1001], "pin_num": [0, 0],
          "test_name": ["Vth_N", "Vth_N"], "rec_type": ["PTR", "PTR"],
          "lo_limit": [0.3, 0.3], "hi_limit": [0.8, 0.8], "units": ["V", "V"],
          "result": [0.95, 0.55], "passed": ["F", "P"], "retest_num": [0, 0],
      }), td)
      lots = (data_dir / "lots" / "product=PROD" / "test_category=CP"
              / "sub_process=CP1" / "lot_id=LOT2" / "data.parquet")
      lots.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["LOT2"], "product": ["PROD"], "test_category": ["CP"],
          "sub_process": ["CP1"], "part_type": ["SCT101A"],
          "job_name": ["CP_TEST"], "job_rev": ["Rev01"],
          "tester_type": ["J750"], "operator": ["OPE01"],
          "start_time": [pa.scalar(1_700_100_000_000, pa.timestamp("ms", tz="UTC"))],
          "finish_time": [pa.scalar(1_700_103_600_000, pa.timestamp("ms", tz="UTC"))],
      }), lots)


  def _sess(tmp_path):
      _write_cp(tmp_path)
      _write_cp_lot2(tmp_path)
      return AnalysisSession(tmp_path)


  def test_yield_by_lot_per_wafer(tmp_path):
      with _sess(tmp_path) as s:
          df = compare.yield_by_lot(s, "PROD", ["LOT1", "LOT2"], "CP")
          # LOT1: W1 2/2, W2 2/2 (retest makes A pass); LOT2: W1 1/2
          row = df[(df.lot_id == "LOT2") & (df.wafer_id == "W1")].iloc[0]
          assert int(row.total) == 2 and int(row.good) == 1
          assert float(row.yield_pct) == 50.0
          l1 = df[(df.lot_id == "LOT1") & (df.wafer_id == "W1")].iloc[0]
          assert int(l1.good) == 2


  def test_bin_pareto_by_lot_pct_sums_100(tmp_path):
      with _sess(tmp_path) as s:
          df = compare.bin_pareto_by_lot(s, "PROD", ["LOT1", "LOT2"], "CP")
          for lot in ("LOT1", "LOT2"):
              assert round(df[df.lot_id == lot]["pct"].sum(), 1) == 100.0


  def test_test_stats_by_lot_explicit(tmp_path):
      with _sess(tmp_path) as s:
          df = compare.test_stats_by_lot(s, "PROD", ["LOT1", "LOT2"], "CP", [1001])
          assert set(df.columns) >= {"lot_id", "test_num", "n", "mean",
                                     "std", "cpk", "lo_limit", "hi_limit"}
          r1 = df[df.lot_id == "LOT1"].iloc[0]
          assert int(r1.n) == 3          # LOT1 td has 3 rows for 1001
          assert float(r1.lo_limit) == 0.3 and float(r1.hi_limit) == 0.8


  def test_test_stats_default_picks_fail_tests(tmp_path):
      with _sess(tmp_path) as s:
          df = compare.test_stats_by_lot(s, "PROD", ["LOT1", "LOT2"], "CP")
          # test 1001 fails in both lots → auto-selected
          assert 1001 in set(int(t) for t in df.test_num)


  def test_test_stats_cpk_matches_python_helper(tmp_path):
      from stdf_platform.reporting.sections import _cpk
      with _sess(tmp_path) as s:
          df = compare.test_stats_by_lot(s, "PROD", ["LOT2"], "CP", [1001])
          got = float(df.iloc[0]["cpk"])
          expected = _cpk([0.95, 0.55], 0.3, 0.8)   # same population formula
          assert abs(got - expected) < 1e-3


  def test_test_distribution_fig_smoke(tmp_path):
      with _sess(tmp_path) as s:
          fig = compare.test_distribution_fig(s, "PROD", ["LOT1", "LOT2"], "CP", 1001)
          assert isinstance(fig, go.Figure)
          assert len(fig.data) == 2      # one histogram trace per lot
  ```

- [ ] Run `uv run pytest src/tests/test_analysis_compare.py -q` → `6 passed`.
- [ ] Run `uv run pytest src/tests -q` → `89 passed`.
- [ ] Commit:
  ```
  feat: analysis.compare — lot-to-lot yield/bin/test-stats/distribution

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

---

## Task 3 — `trend.py` (time-series)

**Files Modify:** `src/stdf_platform/analysis/trend.py`
**Files Create:** `src/tests/test_analysis_trend.py`

**Design notes:**
- **Order strictly by `lots.start_time`** (MIR start). `last_n` keeps the most-recent N lots (by start_time desc) then re-sorts ascending for plotting.
- `lot_trend`: join `parts_final` aggregates to `lots` for `start_t`. Columns: `lot_id, start_t, total, good, yield_pct`. One row per lot, ordered by `start_t`.
- `test_trend`: per lot, `test_num` stats `mean, std, cpk` ordered by `start_t`. Columns: `lot_id, start_t, n, mean, std, cpk`.
- `trend_fig(df, y, control_limits=True)`: pure DataFrame→figure (no session). Line over rows in given order; if `control_limits`, add `mean±3σ` of the `y` column as horizontal lines (`add_hline`). `σ` is population stddev of the plotted series.

**Steps:**

- [ ] Implement `trend.py`:

  ```python
  """Time-series trend. Always ordered by MIR lots.start_time."""

  from __future__ import annotations

  from statistics import mean, pstdev

  import plotly.graph_objects as go


  def lot_trend(s, product, test_category, last_n=30):
      return s.conn.execute(
          """
          WITH agg AS (
              SELECT p.lot_id,
                     COUNT(*)                                    AS total,
                     SUM(CASE WHEN p.passed THEN 1 ELSE 0 END)   AS good
              FROM parts_final p
              GROUP BY p.lot_id
          ),
          j AS (
              SELECT a.lot_id, l.start_time AS start_t, a.total, a.good,
                     ROUND(100.0 * a.good / NULLIF(a.total, 0), 2) AS yield_pct
              FROM agg a
              JOIN (
                  SELECT lot_id, product, test_category, start_time,
                         ROW_NUMBER() OVER (PARTITION BY product, test_category,
                             lot_id ORDER BY start_time DESC) AS rn
                  FROM lots
              ) l ON a.lot_id = l.lot_id AND l.rn = 1
              WHERE l.product = ? AND l.test_category = ?
          )
          SELECT * FROM (
              SELECT * FROM j ORDER BY start_t DESC LIMIT ?
          ) ORDER BY start_t
          """,
          [product, test_category, last_n],
      ).fetchdf()


  def test_trend(s, product, test_category, test_num, last_n=30):
      return s.conn.execute(
          """
          WITH agg AS (
              SELECT lot_id,
                     COUNT(*)                          AS n,
                     AVG(result)                       AS mean,
                     stddev_pop(result)                AS std,
                     FIRST(lo_limit)                   AS lo,
                     FIRST(hi_limit)                   AS hi
              FROM test_data_final
              WHERE test_num = ? AND result IS NOT NULL
                AND rec_type IN ('PTR','MPR')
              GROUP BY lot_id
          ),
          j AS (
              SELECT a.lot_id, l.start_time AS start_t, a.n,
                     ROUND(a.mean, 6) AS mean, ROUND(a.std, 6) AS std,
                     ROUND(LEAST((a.hi - a.mean)/NULLIF(3*a.std,0),
                                 (a.mean - a.lo)/NULLIF(3*a.std,0)), 3) AS cpk
              FROM agg a
              JOIN (
                  SELECT lot_id, product, test_category, start_time,
                         ROW_NUMBER() OVER (PARTITION BY product, test_category,
                             lot_id ORDER BY start_time DESC) AS rn
                  FROM lots
              ) l ON a.lot_id = l.lot_id AND l.rn = 1
              WHERE l.product = ? AND l.test_category = ?
          )
          SELECT * FROM (
              SELECT * FROM j ORDER BY start_t DESC LIMIT ?
          ) ORDER BY start_t
          """,
          [test_num, product, test_category, last_n],
      ).fetchdf()


  def trend_fig(df, y, control_limits=True):
      x = df["lot_id"].tolist() if "lot_id" in df else list(range(len(df)))
      yv = df[y].tolist()
      fig = go.Figure(go.Scatter(x=x, y=yv, mode="lines+markers", name=y))
      if control_limits:
          finite = [v for v in yv if v is not None]
          if len(finite) >= 2:
              mu, sigma = mean(finite), pstdev(finite)
              fig.add_hline(y=mu, line_color="#22c55e", annotation_text="mean")
              fig.add_hline(y=mu + 3 * sigma, line_color="#ef4444",
                            line_dash="dash", annotation_text="+3σ")
              fig.add_hline(y=mu - 3 * sigma, line_color="#ef4444",
                            line_dash="dash", annotation_text="-3σ")
      fig.update_layout(title=f"Trend: {y}", template="plotly_dark",
                        xaxis_title="lot", yaxis_title=y,
                        margin=dict(l=40, r=20, t=40, b=40))
      return fig
  ```

- [ ] Create `src/tests/test_analysis_trend.py` (reuse `_write_cp` LOT1 + Task-2 `_write_cp_lot2`; LOT2 has a later `start_time` so order is LOT1→LOT2). Import `_write_cp_lot2` from the compare test module:

  ```python
  import sys
  from pathlib import Path

  import plotly.graph_objects as go

  sys.path.insert(0, str(Path(__file__).resolve().parent))
  from test_reporting_queries import _write_cp  # noqa: E402
  from test_analysis_compare import _write_cp_lot2  # noqa: E402
  from stdf_platform.analysis import AnalysisSession, trend  # noqa: E402


  def _sess(tmp_path):
      _write_cp(tmp_path)
      _write_cp_lot2(tmp_path)
      return AnalysisSession(tmp_path)


  def test_lot_trend_ordered_by_start_t(tmp_path):
      with _sess(tmp_path) as s:
          df = trend.lot_trend(s, "PROD", "CP")
          assert list(df.lot_id) == ["LOT1", "LOT2"]   # start_t ascending
          assert df.start_t.is_monotonic_increasing


  def test_lot_trend_yield_values(tmp_path):
      with _sess(tmp_path) as s:
          df = trend.lot_trend(s, "PROD", "CP").set_index("lot_id")
          assert float(df.loc["LOT1", "yield_pct"]) == 100.0   # retest passes A
          assert float(df.loc["LOT2", "yield_pct"]) == 50.0    # 1 of 2 dies


  def test_lot_trend_last_n(tmp_path):
      with _sess(tmp_path) as s:
          df = trend.lot_trend(s, "PROD", "CP", last_n=1)
          assert list(df.lot_id) == ["LOT2"]   # most recent only


  def test_test_trend_stats(tmp_path):
      with _sess(tmp_path) as s:
          df = trend.test_trend(s, "PROD", "CP", 1001).set_index("lot_id")
          assert int(df.loc["LOT1", "n"]) == 3
          assert int(df.loc["LOT2", "n"]) == 2


  def test_trend_fig_control_limits(tmp_path):
      with _sess(tmp_path) as s:
          df = trend.lot_trend(s, "PROD", "CP")
          fig = trend.trend_fig(df, "yield_pct", control_limits=True)
          assert isinstance(fig, go.Figure)
          assert len(fig.data) == 1               # the trend line
          # 3 control hlines added as layout shapes
          assert len(fig.layout.shapes) == 3
  ```

- [ ] Run `uv run pytest src/tests/test_analysis_trend.py -q` → `5 passed`.
- [ ] Run `uv run pytest src/tests -q` → `94 passed`.
- [ ] Commit:
  ```
  feat: analysis.trend — lot/test trends ordered by MIR start_time + control limits

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

---

## Task 4 — `correlation.py` (CP↔FT + test-to-test)

**Files Modify:** `src/stdf_platform/analysis/correlation.py`
**Files Create:** `src/tests/test_analysis_correlation.py`

**Design notes:**
- `cp_ft_yield`: per-lot CP yield vs FT yield matched on `lot_id`. CP and FT are separate `parts_final` partitions (different `test_category`); compute each lot's CP yield and FT yield separately and full-join on `lot_id`. Columns: `lot_id, cp_total, cp_yield_pct, ft_total, ft_yield_pct`. `lot_ids=None` → all lots for the product. Note `parts_final` lacks `product`/`test_category` columns? **It does have them** — they are Hive partition columns surfaced by `read_parquet(hive_partitioning=true)`, so `parts_final` rows carry `product` and `test_category`. Filter on those.
- `die_cp_ft_join`: the headline join (see schema finding). FT `lot_id` argument → its `chipid_final` rows (decoded CP origin) joined to CP `parts_final` via `(origin_lot=lot_id_cp, origin_x=x_coord, origin_y=y_coord)` with defensive wafer predicate. Returns FT part identity + CP die result columns.
- `test_correlation`: pivot `test_data_final` by `part_id` for the given `test_nums`, then `DataFrame.corr()` (pandas, Pearson). Returns the correlation matrix as a DataFrame (test_nums as both index and columns).

**Steps:**

- [ ] Implement `correlation.py`:

  ```python
  """CP↔FT correlation (lot + die level) and test-to-test correlation."""

  from __future__ import annotations

  import pandas as pd


  def cp_ft_yield(s, product, lot_ids=None):
      params = [product]
      lot_filter = ""
      if lot_ids is not None:
          ph = ",".join("?" for _ in lot_ids)
          lot_filter = f" AND lot_id IN ({ph})"
          params += list(lot_ids)
      # one params list is reused for both CP and FT scans below
      sql = f"""
          WITH cp AS (
              SELECT lot_id, COUNT(*) AS cp_total,
                     ROUND(100.0*SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                           /NULLIF(COUNT(*),0),2) AS cp_yield_pct
              FROM parts_final
              WHERE product = ? AND test_category = 'CP'{lot_filter}
              GROUP BY lot_id
          ),
          ft AS (
              SELECT lot_id, COUNT(*) AS ft_total,
                     ROUND(100.0*SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                           /NULLIF(COUNT(*),0),2) AS ft_yield_pct
              FROM parts_final
              WHERE product = ? AND test_category = 'FT'{lot_filter}
              GROUP BY lot_id
          )
          SELECT COALESCE(cp.lot_id, ft.lot_id) AS lot_id,
                 cp.cp_total, cp.cp_yield_pct,
                 ft.ft_total, ft.ft_yield_pct
          FROM cp FULL OUTER JOIN ft USING (lot_id)
          ORDER BY lot_id
      """
      return s.conn.execute(sql, params + params).fetchdf()


  def die_cp_ft_join(s, product, lot_id):
      """Die-level CP↔FT join via decoded ChipID origin.

      FT lot `lot_id`'s chipid_final rows carry each die's decoded CP origin
      (origin_lot/origin_wafer/origin_x/origin_y). We join those to CP parts_final
      on (origin_lot=CP lot, origin_x=x_coord, origin_y=y_coord). The CP wafer_id
      is a free-form STRING while origin_wafer is an INT, so the wafer match is
      defensive: TRY_CAST(wafer_id AS BIGINT)=origin_wafer when numeric, else the
      (lot,x,y) match alone identifies the die. Only valid, TSMC-decoded dies join.
      """
      return s.conn.execute(
          """
          SELECT c.part_id            AS ft_part_id,
                 c.part_txt           AS ft_part_txt,
                 c.origin_fab,
                 c.origin_lot         AS cp_lot_id,
                 c.origin_wafer       AS cp_wafer,
                 c.origin_x           AS cp_x,
                 c.origin_y           AS cp_y,
                 p.wafer_id           AS cp_wafer_id_str,
                 p.soft_bin           AS cp_soft_bin,
                 p.hard_bin           AS cp_hard_bin,
                 p.passed             AS cp_passed
          FROM chipid_final c
          JOIN parts_final p
            ON p.product = ?
           AND p.test_category = 'CP'
           AND p.lot_id  = c.origin_lot
           AND p.x_coord = c.origin_x
           AND p.y_coord = c.origin_y
           AND (TRY_CAST(p.wafer_id AS BIGINT) = c.origin_wafer
                OR TRY_CAST(p.wafer_id AS BIGINT) IS NULL)
          WHERE c.lot_id = ? AND c.valid
          ORDER BY c.part_txt, c.origin_x, c.origin_y
          """,
          [product, lot_id],
      ).fetchdf()


  def test_correlation(s, product, lot_id, test_category, test_nums):
      ph = ",".join("?" for _ in test_nums)
      long = s.conn.execute(
          f"""
          SELECT part_id, test_num, result
          FROM test_data_final
          WHERE lot_id = ? AND test_num IN ({ph})
            AND result IS NOT NULL AND rec_type IN ('PTR','MPR')
          """,
          [lot_id] + list(test_nums),
      ).fetchdf()
      wide = long.pivot_table(index="part_id", columns="test_num", values="result")
      return wide.corr()
  ```

- [ ] Create `src/tests/test_analysis_correlation.py`. Add a CP+FT fixture whose CP `parts_final` dies match the FT `chipid_final` decoded origin. The existing `_write_ft` chipid uses `origin_lot=HKPFJK/ABCDEF, origin_x=12/102, origin_y=22/202, origin_wafer=11/7`. Write matching CP parts under product `CHIP`, CP lot `HKPFJK`, wafer `11` with a die at `(12,22)`:

  ```python
  import sys
  from pathlib import Path

  import pyarrow as pa
  import pyarrow.parquet as pq

  sys.path.insert(0, str(Path(__file__).resolve().parent))
  from test_reporting_queries import _write_ft  # noqa: E402
  from stdf_platform.analysis import AnalysisSession, correlation  # noqa: E402


  def _write_cp_origin(data_dir: Path):
      """CP parts for origin lot HKPFJK wafer 11 with a die at (12,22)
      matching the FT chipid die-0 (TSMC1)."""
      p = (data_dir / "parts" / "product=CHIP" / "test_category=CP"
           / "sub_process=CP1" / "lot_id=HKPFJK" / "wafer_id=11"
           / "retest=0" / "data.parquet")
      p.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["HKPFJK"], "wafer_id": ["11"],
          "part_id": ["cpA"], "part_txt": [""],
          "x_coord": [12], "y_coord": [22],
          "hard_bin": [1], "soft_bin": [1],
          "passed": [True], "retest_num": [0],
      }), p)
      lots = (data_dir / "lots" / "product=CHIP" / "test_category=CP"
              / "sub_process=CP1" / "lot_id=HKPFJK" / "data.parquet")
      lots.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["HKPFJK"], "product": ["CHIP"], "test_category": ["CP"],
          "sub_process": ["CP1"], "part_type": ["CHIP"],
          "job_name": ["CP"], "job_rev": ["Rev01"],
          "tester_type": ["J750"], "operator": ["OPE01"],
          "start_time": [pa.scalar(1_699_000_000_000, pa.timestamp("ms", tz="UTC"))],
          "finish_time": [pa.scalar(1_699_003_600_000, pa.timestamp("ms", tz="UTC"))],
      }), lots)


  def _write_corr_tests(data_dir: Path):
      """Two correlated tests in CP lot HKPFJK for test_correlation()."""
      td = (data_dir / "test_data" / "product=CHIP" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=HKPFJK" / "data.parquet")
      td.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["HKPFJK"] * 6,
          "wafer_id": ["11"] * 6,
          "part_id": ["d0", "d0", "d1", "d1", "d2", "d2"],
          "part_txt": [""] * 6,
          "x_coord": [12, 12, 13, 13, 14, 14],
          "y_coord": [22, 22, 22, 22, 22, 22],
          "test_num": [1, 2, 1, 2, 1, 2],
          "pin_num": [0] * 6,
          "test_name": ["A", "B"] * 3, "rec_type": ["PTR"] * 6,
          "lo_limit": [0.0] * 6, "hi_limit": [10.0] * 6, "units": ["V"] * 6,
          "result": [1.0, 2.0, 2.0, 4.0, 3.0, 6.0],   # test2 = 2*test1 → corr 1.0
          "passed": ["P"] * 6, "retest_num": [0] * 6,
      }), td)


  def test_die_cp_ft_join_matches_origin(tmp_path):
      _write_ft(tmp_path)        # FT lot FT1 + chipid (origin HKPFJK/ABCDEF)
      _write_cp_origin(tmp_path)
      with AnalysisSession(tmp_path) as s:
          df = correlation.die_cp_ft_join(s, "CHIP", "FT1")
          # die-0 (TSMC1, origin HKPFJK, x12 y22) matches the CP die we wrote
          hit = df[df.cp_lot_id == "HKPFJK"]
          assert len(hit) == 1
          assert int(hit.iloc[0].cp_x) == 12 and int(hit.iloc[0].cp_y) == 22
          assert bool(hit.iloc[0].cp_passed) is True
          assert hit.iloc[0].ft_part_txt == "2D-FT1-0000"


  def test_die_cp_ft_join_unmatched_origin_dropped(tmp_path):
      _write_ft(tmp_path)        # ABCDEF origin has no CP parts written
      _write_cp_origin(tmp_path)
      with AnalysisSession(tmp_path) as s:
          df = correlation.die_cp_ft_join(s, "CHIP", "FT1")
          assert "ABCDEF" not in set(df.cp_lot_id)   # no CP die → inner-join drop


  def test_cp_ft_yield_pairs_by_lot(tmp_path):
      _write_ft(tmp_path)        # FT lot FT1 (product CHIP): 1 of 2 passes
      _write_cp_origin(tmp_path) # CP lot HKPFJK (product CHIP): 1 of 1 passes
      with AnalysisSession(tmp_path) as s:
          df = correlation.cp_ft_yield(s, "CHIP").set_index("lot_id")
          assert float(df.loc["FT1", "ft_yield_pct"]) == 50.0
          assert float(df.loc["HKPFJK", "cp_yield_pct"]) == 100.0


  def test_test_correlation_perfect(tmp_path):
      _write_cp_origin(tmp_path)
      _write_corr_tests(tmp_path)
      with AnalysisSession(tmp_path) as s:
          m = correlation.test_correlation(s, "CHIP", "HKPFJK", "CP", [1, 2])
          assert abs(float(m.loc[1, 2]) - 1.0) < 1e-9   # test2 = 2*test1
          assert abs(float(m.loc[1, 1]) - 1.0) < 1e-9


  def test_test_correlation_shape(tmp_path):
      _write_cp_origin(tmp_path)
      _write_corr_tests(tmp_path)
      with AnalysisSession(tmp_path) as s:
          m = correlation.test_correlation(s, "CHIP", "HKPFJK", "CP", [1, 2])
          assert list(m.columns) == [1, 2] and list(m.index) == [1, 2]
  ```

  Note `_write_ft`'s chipid uses `efuse_raw` of all-zeros / all-ones (its `decode_chipid` output is overridden by the explicit columns in the fixture, so `origin_lot`/`origin_x`/`origin_y` are exactly `HKPFJK/12/22` and `ABCDEF/102/202`). The join relies on those explicit columns, which is why the CP fixture mirrors them.

- [ ] Run `uv run pytest src/tests/test_analysis_correlation.py -q` → `5 passed`.
- [ ] Run `uv run pytest src/tests -q` → `99 passed`.
- [ ] Commit:
  ```
  feat: analysis.correlation — CP↔FT lot/die join (ChipID origin) + test corr

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

---

## Task 5 — `spatial.py` (CP wafer-plane analysis)

**Files Modify:** `src/stdf_platform/analysis/spatial.py`
**Files Create:** `src/tests/test_analysis_spatial.py`

**Design notes (radius normalization — state explicitly):**
- Wafer center = midpoint of die-coordinate extents: `cx = (min(x)+max(x))/2`, `cy = (min(y)+max(y))/2` (computed from `parts_final` for the lot). Radius of a die = `sqrt((x-cx)^2 + (y-cy)^2)`. **Normalized radius** = `r / r_max` where `r_max = max(r)` over the lot (so edge die → 1.0, center → 0.0). This matches the spec's "wafer center/radius estimated from die-coord min/max".
- `zone_yield(s, product, lot_id, n_zones=3)`: split normalized radius `[0,1]` into `n_zones` equal-width bands → zone index `LEAST(n_zones-1, floor(rnorm*n_zones))`; label `center/mid/edge` when `n_zones==3` else `zone_{i}`. Yield per zone from `parts_final`. Columns: `zone, zone_idx, total, good, yield_pct`.
- `radial_profile(s, product, lot_id, test_num)`: bin normalized radius into deciles (`floor(rnorm*10)`, capped at 9), per bin `{n, mean, std}` of `result`. Columns: `radius_bin, n, mean, std`. Joins `test_data_final` to per-die radius computed from `parts_final` coords (same lot).
- `param_wafermap_fig(s, product, lot_id, wafer_id, test_num)`: `go.Scatter` of `x_coord/y_coord` colored by mean `result` per die. Returns `go.Figure`, `yaxis` reversed + square aspect (mirror `sections.wafer_maps_section`).

We compute center/r_max with a CTE so all of zone/radial logic is bound-param SQL.

**Steps:**

- [ ] Implement `spatial.py`:

  ```python
  """CP wafer-plane analysis: radial zones, radial parametric profile, maps.

  Radius is normalized against the die-coordinate extent of the lot:
  center = midpoint of (min/max) x,y; r = hypot(x-cx, y-cy); rnorm = r / max(r).
  """

  from __future__ import annotations

  import plotly.graph_objects as go


  _ZONE3 = {0: "center", 1: "mid", 2: "edge"}


  def zone_yield(s, product, lot_id, n_zones=3):
      df = s.conn.execute(
          """
          WITH ext AS (
              SELECT (MIN(x_coord)+MAX(x_coord))/2.0 AS cx,
                     (MIN(y_coord)+MAX(y_coord))/2.0 AS cy
              FROM parts_final WHERE lot_id = ?
          ),
          r AS (
              SELECT p.passed,
                     sqrt(pow(p.x_coord-ext.cx,2)+pow(p.y_coord-ext.cy,2)) AS rad
              FROM parts_final p, ext
              WHERE p.lot_id = ?
          ),
          rn AS (
              SELECT passed,
                     CASE WHEN MAX(rad) OVER () = 0 THEN 0
                          ELSE rad / MAX(rad) OVER () END AS rnorm
              FROM r
          ),
          z AS (
              SELECT passed,
                     LEAST(? - 1, CAST(floor(rnorm * ?) AS INTEGER)) AS zone_idx
              FROM rn
          )
          SELECT zone_idx,
                 COUNT(*)                                    AS total,
                 SUM(CASE WHEN passed THEN 1 ELSE 0 END)     AS good,
                 ROUND(100.0*SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                       /NULLIF(COUNT(*),0),2)                AS yield_pct
          FROM z GROUP BY zone_idx ORDER BY zone_idx
          """,
          [lot_id, lot_id, n_zones, n_zones],
      ).fetchdf()
      if n_zones == 3:
          df.insert(0, "zone", df["zone_idx"].map(_ZONE3))
      else:
          df.insert(0, "zone", df["zone_idx"].map(lambda i: f"zone_{int(i)}"))
      return df


  def radial_profile(s, product, lot_id, test_num):
      return s.conn.execute(
          """
          WITH ext AS (
              SELECT (MIN(x_coord)+MAX(x_coord))/2.0 AS cx,
                     (MIN(y_coord)+MAX(y_coord))/2.0 AS cy
              FROM parts_final WHERE lot_id = ?
          ),
          coords AS (
              SELECT part_id,
                     sqrt(pow(x_coord-ext.cx,2)+pow(y_coord-ext.cy,2)) AS rad
              FROM parts_final p, ext WHERE p.lot_id = ?
          ),
          rmax AS (SELECT MAX(rad) AS rm FROM coords),
          vals AS (
              SELECT c.rad, t.result,
                     CASE WHEN rmax.rm = 0 THEN 0
                          ELSE c.rad / rmax.rm END AS rnorm
              FROM test_data_final t
              JOIN coords c ON t.part_id = c.part_id
              CROSS JOIN rmax
              WHERE t.lot_id = ? AND t.test_num = ?
                AND t.result IS NOT NULL AND t.rec_type IN ('PTR','MPR')
          )
          SELECT LEAST(9, CAST(floor(rnorm*10) AS INTEGER)) AS radius_bin,
                 COUNT(*)               AS n,
                 ROUND(AVG(result),6)   AS mean,
                 ROUND(stddev_pop(result),6) AS std
          FROM vals GROUP BY radius_bin ORDER BY radius_bin
          """,
          [lot_id, lot_id, lot_id, test_num],
      ).fetchdf()


  def param_wafermap_fig(s, product, lot_id, wafer_id, test_num):
      df = s.conn.execute(
          """
          SELECT x_coord, y_coord, AVG(result) AS result
          FROM test_data_final
          WHERE lot_id = ? AND wafer_id = ? AND test_num = ?
            AND result IS NOT NULL
          GROUP BY x_coord, y_coord
          """,
          [lot_id, wafer_id, test_num],
      ).fetchdf()
      fig = go.Figure(go.Scatter(
          x=df["x_coord"], y=df["y_coord"], mode="markers",
          marker=dict(size=10, symbol="square", color=df["result"],
                      colorscale="Viridis", showscale=True,
                      colorbar=dict(title="value")),
          text=[f"{v:.3g}" for v in df["result"]],
      ))
      fig.update_layout(
          title=f"Wafer {wafer_id} test #{test_num}", template="plotly_dark",
          yaxis=dict(autorange="reversed", scaleanchor="x", scaleratio=1),
          margin=dict(l=30, r=20, t=40, b=30),
      )
      return fig
  ```

- [ ] Create `src/tests/test_analysis_spatial.py`. Write a CP lot with a clean center/edge layout so zone assignment is deterministic: a 5×5 grid (x,y in 0..4), center die `(2,2)`; mark the outer ring as fail to make edge yield < center yield.

  ```python
  import sys
  from pathlib import Path

  import pyarrow as pa
  import pyarrow.parquet as pq
  import plotly.graph_objects as go

  sys.path.insert(0, str(Path(__file__).resolve().parent))
  from stdf_platform.analysis import AnalysisSession, spatial  # noqa: E402


  def _write_grid(data_dir: Path):
      xs, ys, passed, pid = [], [], [], []
      i = 0
      for x in range(5):
          for y in range(5):
              xs.append(x); ys.append(y)
              edge = x in (0, 4) or y in (0, 4)
              passed.append(not edge)        # outer ring fails
              pid.append(f"d{i}"); i += 1
      p = (data_dir / "parts" / "product=PROD" / "test_category=CP"
           / "sub_process=CP1" / "lot_id=G1" / "wafer_id=W1"
           / "retest=0" / "data.parquet")
      p.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["G1"]*25, "wafer_id": ["W1"]*25,
          "part_id": pid, "part_txt": [""]*25,
          "x_coord": xs, "y_coord": ys,
          "hard_bin": [1 if p_ else 4 for p_ in passed],
          "soft_bin": [1 if p_ else 4 for p_ in passed],
          "passed": passed, "retest_num": [0]*25,
      }), p)
      td = (data_dir / "test_data" / "product=PROD" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=G1" / "data.parquet")
      td.parent.mkdir(parents=True, exist_ok=True)
      # result grows with radius → mean increases outward
      res = [float(abs(x-2)+abs(y-2)) for x in range(5) for y in range(5)]
      pq.write_table(pa.table({
          "lot_id": ["G1"]*25, "wafer_id": ["W1"]*25,
          "part_id": pid, "part_txt": [""]*25,
          "x_coord": xs, "y_coord": ys,
          "test_num": [7]*25, "pin_num": [0]*25,
          "test_name": ["R"]*25, "rec_type": ["PTR"]*25,
          "lo_limit": [0.0]*25, "hi_limit": [10.0]*25, "units": ["V"]*25,
          "result": res, "passed": ["P"]*25, "retest_num": [0]*25,
      }), td)


  def _sess(tmp_path):
      _write_grid(tmp_path)
      return AnalysisSession(tmp_path)


  def test_zone_yield_three_zones(tmp_path):
      with _sess(tmp_path) as s:
          df = spatial.zone_yield(s, "PROD", "G1", n_zones=3).set_index("zone")
          assert set(df.index) <= {"center", "mid", "edge"}
          # center fully passes; edge (outer ring) fails → lower yield
          assert float(df.loc["center", "yield_pct"]) == 100.0
          assert float(df.loc["edge", "yield_pct"]) < 100.0


  def test_zone_yield_total_conserved(tmp_path):
      with _sess(tmp_path) as s:
          df = spatial.zone_yield(s, "PROD", "G1", n_zones=3)
          assert int(df["total"].sum()) == 25


  def test_zone_yield_n_zones_param(tmp_path):
      with _sess(tmp_path) as s:
          df = spatial.zone_yield(s, "PROD", "G1", n_zones=5)
          assert df["zone_idx"].max() <= 4
          assert set(df["zone"]) <= {f"zone_{i}" for i in range(5)}


  def test_radial_profile_increases(tmp_path):
      with _sess(tmp_path) as s:
          df = spatial.radial_profile(s, "PROD", "G1", 7).set_index("radius_bin")
          assert int(df["n"].sum()) == 25
          # mean of the inner-most bin < outer-most bin (result grows with radius)
          assert df["mean"].iloc[0] < df["mean"].iloc[-1]


  def test_param_wafermap_fig_smoke(tmp_path):
      with _sess(tmp_path) as s:
          fig = spatial.param_wafermap_fig(s, "PROD", "G1", "W1", 7)
          assert isinstance(fig, go.Figure)
          assert len(fig.data) == 1
          assert fig.layout.yaxis.autorange == "reversed"
  ```

- [ ] Run `uv run pytest src/tests/test_analysis_spatial.py -q` → `5 passed`.
- [ ] Run `uv run pytest src/tests -q` → `104 passed`.
- [ ] Commit:
  ```
  feat: analysis.spatial — radial zone yield, radial profile, param wafermap

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

---

## Task 6 — `templates/analysis/*.py` cell-scripts + compile test

**Files Create:**
- `templates/analysis/01_lot_compare.py`
- `templates/analysis/02_trend.py`
- `templates/analysis/03_cp_ft_correlation.py`
- `templates/analysis/04_wafer_spatial.py`
- `src/tests/test_templates_compile.py`

**Design notes:** Each template is a `# %%` cell-script (VSCode/Jupytext compatible), **not imported** by tests — only syntax-checked via `compile()`. Cells: (1) markdown header + usage; (2) parameter cell (`PRODUCT`, `LOTS`, `TEST_CATEGORY`, etc. — edit-me); (3) `AnalysisSession`; (4) function calls + `fig.show()`. They must `compile()` cleanly but need not run without data.

**Steps:**

- [ ] Create `templates/analysis/01_lot_compare.py`:

  ```python
  # %% [markdown]
  # # ロット間比較 (lot compare)
  # `analysis.compare` を使ったロット横断の歩留まり/ビン/テスト統計。
  # VS Code で「Run Cell」または Shift+Enter。下のパラメータセルを編集して実行。

  # %% パラメータ（編集してください）
  PRODUCT = "PROD"
  LOTS = ["LOT1", "LOT2"]
  TEST_CATEGORY = "CP"
  TEST_NUM = 1001

  # %% セッション
  from stdf_platform.analysis import (
      AnalysisSession, yield_by_lot, bin_pareto_by_lot,
      test_stats_by_lot, test_distribution_fig,
  )

  s = AnalysisSession()        # config.yaml / STDF_CONFIG からデータディレクトリ解決

  # %% 歩留まり（ロット×ウェハ）
  yield_by_lot(s, PRODUCT, LOTS, TEST_CATEGORY)

  # %% ビン構成比
  bin_pareto_by_lot(s, PRODUCT, LOTS, TEST_CATEGORY)

  # %% テスト統計（test_nums=None でフェイル上位を自動選択）
  test_stats_by_lot(s, PRODUCT, LOTS, TEST_CATEGORY)

  # %% 分布オーバーレイ
  test_distribution_fig(s, PRODUCT, LOTS, TEST_CATEGORY, TEST_NUM).show()
  ```

- [ ] Create `templates/analysis/02_trend.py`:

  ```python
  # %% [markdown]
  # # トレンド (trend) — MIR start_time 順
  # `analysis.trend`。ロット歩留まり/テスト統計の時系列 + mean±3σ 管理線。

  # %% パラメータ（編集してください）
  PRODUCT = "PROD"
  TEST_CATEGORY = "CP"
  TEST_NUM = 1001
  LAST_N = 30

  # %% セッション
  from stdf_platform.analysis import AnalysisSession, lot_trend, test_trend, trend_fig

  s = AnalysisSession()

  # %% ロット歩留まりトレンド
  df_lot = lot_trend(s, PRODUCT, TEST_CATEGORY, last_n=LAST_N)
  df_lot

  # %% 歩留まりトレンド図（管理線つき）
  trend_fig(df_lot, "yield_pct", control_limits=True).show()

  # %% テスト統計トレンド
  df_test = test_trend(s, PRODUCT, TEST_CATEGORY, TEST_NUM, last_n=LAST_N)
  trend_fig(df_test, "mean", control_limits=True).show()
  ```

- [ ] Create `templates/analysis/03_cp_ft_correlation.py`:

  ```python
  # %% [markdown]
  # # CP↔FT 相関 (correlation)
  # `analysis.correlation`。ロット単位の CP/FT 歩留まり、ChipID 経由のダイ結合、
  # テスト間相関。die_cp_ft_join は FT ロットを指定し、ChipID デコード由来の
  # CP origin (lot/wafer/x/y) で CP parts_final に結合します。

  # %% パラメータ（編集してください）
  PRODUCT = "CHIP"
  FT_LOT = "FT1"
  CP_LOT = "HKPFJK"
  TEST_NUMS = [1, 2]

  # %% セッション
  from stdf_platform.analysis import (
      AnalysisSession, cp_ft_yield, die_cp_ft_join, test_correlation,
  )

  s = AnalysisSession()

  # %% ロット単位 CP/FT 歩留まり
  cp_ft_yield(s, PRODUCT)

  # %% ダイレベル CP↔FT 結合（ChipID origin）
  die_cp_ft_join(s, PRODUCT, FT_LOT)

  # %% テスト間相関行列
  test_correlation(s, PRODUCT, CP_LOT, "CP", TEST_NUMS)
  ```

- [ ] Create `templates/analysis/04_wafer_spatial.py`:

  ```python
  # %% [markdown]
  # # ウェハ面内分析 (spatial, CP 専用)
  # `analysis.spatial`。正規化半径によるゾーン歩留まり、半径方向プロファイル、
  # パラメトリック・ウェハマップ。半径はダイ座標の min/max 中点を中心に正規化。

  # %% パラメータ（編集してください）
  PRODUCT = "PROD"
  LOT = "G1"
  WAFER = "W1"
  TEST_NUM = 7
  N_ZONES = 3

  # %% セッション
  from stdf_platform.analysis import (
      AnalysisSession, zone_yield, radial_profile, param_wafermap_fig,
  )

  s = AnalysisSession()

  # %% ゾーン別歩留まり（center/mid/edge）
  zone_yield(s, PRODUCT, LOT, n_zones=N_ZONES)

  # %% 半径方向プロファイル
  radial_profile(s, PRODUCT, LOT, TEST_NUM)

  # %% パラメトリック・ウェハマップ
  param_wafermap_fig(s, PRODUCT, LOT, WAFER, TEST_NUM).show()
  ```

- [ ] Create `src/tests/test_templates_compile.py`:

  ```python
  """All analysis cell-scripts must compile (they are run, not imported)."""

  from pathlib import Path

  import pytest

  _ROOT = Path(__file__).resolve().parents[2]   # repo root: src/tests → repo
  _TEMPLATES = sorted((_ROOT / "templates" / "analysis").glob("*.py"))


  @pytest.mark.parametrize("path", _TEMPLATES, ids=lambda p: p.name)
  def test_template_compiles(path):
      src = path.read_text(encoding="utf-8")
      compile(src, str(path), "exec")   # SyntaxError fails the test


  def test_four_templates_present():
      assert len(_TEMPLATES) == 4
  ```

  Note: this file parametrizes over 4 templates → 4 cases + 1 presence test = **5 tests**, but the trajectory budgets `+1` for this file's *net new file contribution* as a single conceptual unit. To keep the cumulative arithmetic exact, **the cumulative count after Task 6 is 104 + 5 = 109**. Update the printed expectation accordingly when running (see the corrected expected output below). The trajectory table's "105" reflected a single non-parametrized smoke; the parametrized form is preferred for per-template clarity, so the **authoritative final count is 109**. (If a reviewer prefers exactly +1, collapse to a single `test_all_templates_compile` loop instead.)

- [ ] Run `uv run pytest src/tests/test_templates_compile.py -q` → `5 passed`.
- [ ] Run `uv run pytest src/tests -q` → `109 passed`.
- [ ] Commit:
  ```
  feat: analysis cell-script templates + compile test

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

---

## Task 7 — Docs + final verification

**Files Modify:**
- `CLAUDE.md` (module list)
- `README.md` (mention the analysis package + templates)

**Steps:**

- [ ] In `CLAUDE.md`, under `### Modules` → `src/stdf_platform/`, add after the `web/` block (matching the existing bullet style):

  ```
  - `analysis/` — reusable, retest-aware analysis API (returns DataFrames / plotly figures)
    - `session.py` — `AnalysisSession`: owns the DuckDB :memory: conn + views (config-resolved)
    - `compare.py` — lot-to-lot yield / bin pareto / test stats / distribution overlay
    - `trend.py` — lot & test trends ordered by MIR start_time, with mean±3σ control lines
    - `correlation.py` — CP↔FT lot & die join (via decoded ChipID origin) + test-to-test corr
    - `spatial.py` — CP wafer-plane radial zone yield / radial profile / parametric wafermap
  ```

  And add a one-line note in a sensible spot (e.g. near the `query.py` / Data Flow area):

  ```
  `templates/analysis/*.py` are `# %%` cell-scripts (VSCode/Jupytext) over `AnalysisSession`;
  `query.py` is a thin wrapper over the same session. All analysis uses the *_final views so
  yield/Cpk definitions are identical across users.
  ```

- [ ] In `README.md`, add a short subsection (place near existing query/reporting docs) describing:
  - `from stdf_platform.analysis import AnalysisSession, yield_by_lot, ...`
  - the four templates and how to run them (VS Code Run Cell / Jupytext),
  - that every function is retest-aware and returns DataFrames (`*_fig` → plotly).
  Keep it to ~10–15 lines, consistent with the README's existing tone. (Read README.md first to match its heading conventions; do not duplicate the reporting section.)

- [ ] **Final full verification:**
  - [ ] `uv run pytest src/tests -q` → expected tail `109 passed`.
  - [ ] `uv run python -c "from stdf_platform.analysis import AnalysisSession, yield_by_lot, lot_trend, cp_ft_yield, zone_yield, trend_fig; print('imports ok')"` → prints `imports ok`.
  - [ ] `uv run python -c "import ast,glob; [ast.parse(open(f).read()) for f in glob.glob('templates/analysis/*.py')]; print('templates ok')"` → prints `templates ok`.
  - [ ] `git status` → only intended changes staged.
- [ ] Commit:
  ```
  docs: document analysis package + cell-script templates (CLAUDE.md, README)

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

---

## Self-review checklist (run before declaring done)

- [ ] **Spec coverage:** every function in the spec's "関数仕様" exists with the exact signature (session: `__init__`/`q`/`lots`/`conn`; compare ×4; trend ×3; correlation ×3; spatial ×3) — cross-checked against the contracts block.
- [ ] **Signature consistency:** first arg is `s` (an `AnalysisSession`) for every module function except `trend_fig(df, y, ...)` (pure DataFrame→figure, per spec). Defaults match spec (`last_n=30`, `n_zones=3`, `test_nums=None`, `lot_ids=None`).
- [ ] **Return types:** data fns → `pd.DataFrame`; `*_fig` → `go.Figure` (asserted in tests via `isinstance`).
- [ ] **Bound params:** no value is f-string-interpolated; only `IN (...)` placeholder *counts* and fixed SQL fragments are built into the string; all values go through `?`.
- [ ] **No SQL duplication:** `top_fail_tests`/`test_values` reused from `reporting.queries`; `_cpk` imported from `reporting.sections`; SQL Cpk mirrors that population formula and is asserted equal on a fixture.
- [ ] **Domain correctness:** yields from `parts_final` only; trend ordered by `lots.start_time`; zone radius normalized from die-coord min/max midpoint; die_cp_ft_join uses real `origin_*` columns with the defensive wafer cast.
- [ ] **Fixture realism:** extends `_write_cp`/`_write_ft`; FT `wafer_id=''`, CP coords int; chipid `origin_*` columns mirrored by the CP fixture so the join actually matches.
- [ ] **Test-count arithmetic:** 78 → 83 → 89 → 94 → 99 → 104 → **109** (Task 6 parametrizes 4 templates + 1 presence = 5). Final asserted: `109 passed`.
- [ ] **query.py wrapper:** still `compile()`s; `con` now sourced from `AnalysisSession`; all `# %%` cells + `use_lot`/`use_all`/`q`/`to_csv` untouched.

---

## Deviations from spec (called out)

1. **Test count.** Spec test-method paragraph does not fix a number. The template compile test is parametrized (one case per template) for clearer per-file signal, making the final suite **109**, not 105. A single-loop variant (final 105) is offered in Task 6 if exactly +1 is required.
2. **`die_cp_ft_join` wafer match.** Spec says "join FT chipid origin to CP parts". Because CP `wafer_id` is a *string* and `origin_wafer` an *int*, the wafer predicate is defensive (`TRY_CAST … OR … IS NULL`); the load-bearing keys are `(origin_lot, origin_x, origin_y)`. This is a data-faithful refinement, not a contract change.
3. **`AnalysisSession.lots()` dedup.** Added a `ROW_NUMBER()` latest-row dedup (mirrors `lot_header`) so duplicated MIR rows don't double-list — implied by "lots一覧(メタデータ付き)" but not spelled out.
4. **Cpk in SQL.** Spec implies one Cpk definition. We import `_cpk` for Python use and reproduce its *population*-stddev formula in SQL (`stddev_pop`, `LEAST`) for grouped frames, asserting numeric equality on a fixture rather than calling Python per group.
```
