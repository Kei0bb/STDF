# Phase 2: Report Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a self-contained HTML report engine on top of the Phase-1 DuckDB view layer. Each lot gets a single offline-viewable `report.html` (Plotly JS embedded, ~4MB) built from `parts_final`/`test_data_final`/`chipid_final`/`lots` via Jinja2. Reports are a disposable cache regenerated from Parquet: by `stdf report --lot X -p PROD`, by `stdf report --pending`, and automatically after every `ingest`/`ingest-all`/`fetch` (report failures must never fail an ingest). A Reports tab in the web UI lists and serves the generated HTML.

**Architecture:** Pure-Python STDF parser → Hive-partitioned Parquet → DuckDB `:memory:` views (`src/stdf_platform/views.py`, the single source of view truth from Phase 1) → **new `src/stdf_platform/reporting/` package** (`queries.py` analysis SQL → `sections.py` section builders → `render.py` Jinja2+Plotly → `generator.py` orchestration) → Click CLI + FastAPI/Alpine web UI. Parquet is the source of truth; reports under `data/reports/` are regenerated on demand and always overwritten.

**Tech Stack:** Python ≥3.10, duckdb, click, rich, pyarrow, pandas, fastapi, uvicorn, pyyaml, **jinja2 (new)**, **plotly (new, used as a pure figure-→-JSON + bundled-JS library — no kaleido, no static image export)**. Tests: pytest 8 + httpx (FastAPI `TestClient`). Runner verified: **`uv run pytest src/tests -q`** (50 tests currently pass; neither jinja2 nor plotly is installed yet).

---

## Conventions for every task

- **Test command (verified):** run `uv run pytest src/tests -q` at the **end of every task**. Targeted runs during a task use `uv run pytest src/tests/<file>.py -q`.
- Tests live in `src/tests/`. There is **no** `conftest.py` and **no** `[tool.pytest]` config — pytest discovers `test_*.py` with `test_*` functions. Existing tests import the package directly (e.g. `from stdf_platform.config import Config`) and use the `tmp_path` fixture. Several tests prepend `src/tests/` to `sys.path` to import `make_test_stdf`; reuse that pattern.
- Commit **straight to `main`** after each task (project preference). Use the exact conventional-commit messages shown. End every commit body with the Co-Authored-By trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- Baseline before starting: `git status` clean except untracked `.claude/` (and this plan file once committed).

---

## Fixed names and signatures (decide once, reuse everywhere)

These are the contracts every task depends on. Do not vary them between tasks.

```python
# reporting/queries.py — every query takes a live connection with Phase-1 views
# already registered (setup_views was called by the caller) plus filter values.
def lot_header(conn, product, test_category, lot_id) -> dict: ...
def wafer_yield(conn, product, test_category, lot_id) -> list[dict]: ...
def lot_yield_total(conn, product, test_category, lot_id) -> dict: ...
def bin_pareto(conn, product, test_category, lot_id) -> list[dict]: ...
def wafer_map(conn, product, test_category, lot_id, wafer_id) -> list[dict]: ...
def wafer_ids(conn, product, test_category, lot_id) -> list[str]: ...
def top_fail_tests(conn, product, test_category, lot_id, top_n) -> list[dict]: ...
def test_values(conn, product, test_category, lot_id, test_num) -> dict: ...
def retest_history(conn, product, test_category, lot_id) -> list[dict]: ...
def max_retest(conn, product, test_category, lot_id) -> int: ...
def chipid_summary(conn, product, test_category, lot_id) -> list[dict]: ...

# reporting/sections.py
@dataclass
class SectionResult:
    title: str
    figures: list[str]          # each a Plotly figure as a JSON string (fig.to_json())
    tables: list[dict]          # each {"caption": str, "columns": [str], "rows": [[...]]}
    notes: list[str]            # optional free-text lines rendered under the section

# A "section" is a function (conn, product, test_category, lot_id, cfg) -> SectionResult | None
# (returns None when the section does not apply, e.g. retest history with no retests).

# reporting/render.py
def render_report(product, test_category, lot_id, sections: list[SectionResult],
                  generated_at: str) -> str: ...   # returns full HTML string

# reporting/generator.py
def generate_lot_report(config, product, test_category, lot_id) -> Path: ...
def report_path(data_dir, product, test_category, lot_id) -> Path: ...
def pending_lots(config) -> list[tuple[str, str, str]]: ...   # (product, category, lot_id)
def generate_reports_for_lots(config, lots, warn=print) -> list[Path]: ...
```

Partition sanitization reuses **`ParquetStorage._sanitize`** (the same `re.sub` rules storage.py applies when writing partitions) so report paths line up with the Parquet partitions and stay Windows-safe.

---

## Task ordering

1. **Task 1** — add `jinja2` + `plotly` deps; add `ReportingConfig` to `config.py`; create the empty `reporting/` package skeleton. (No dependencies; everything else imports these.)
2. **Task 2** — `reporting/queries.py` + tests (built on Phase-1 views; pure SQL, no rendering).
3. **Task 3** — `reporting/sections.py` + tests (consume queries, build Plotly figures + tables).
4. **Task 4** — `reporting/render.py` + `templates/report.html.j2` + smoke test (Jinja2 + embedded plotly.js).
5. **Task 5** — `reporting/generator.py` + `stdf report` CLI (`--lot/--pending`) + tests.
6. **Task 6** — post-ingest hook in `_run_ingest_batch` with failure isolation + test.
7. **Task 7** — web `GET /api/reports` + static serving + Reports tab in Alpine UI + test.
8. **Task 8** — final verification + CLAUDE.md/README note.

Test-count trajectory (start 50): T1 +1 → 51, T2 +6 → 57, T3 +5 → 62, T4 +3 → 65, T5 +4 → 69, T6 +2 → 71, T7 +3 → 74, T8 +0 → **74 passed**.

---

## Task 1 — Dependencies, config section, package skeleton

Add the two runtime deps and a `reporting` config block. The Jinja2 template will live **inside the package** at `src/stdf_platform/reporting/templates/report.html.j2`; the hatch wheel target already packages everything under `src/stdf_platform` (the existing `web/static/*` files ship the same way with no `force-include`), so a `.j2` template under the package ships automatically — no `pyproject.toml` packaging change needed.

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/stdf_platform/config.py`
- Create: `src/stdf_platform/reporting/__init__.py`
- Create: `src/stdf_platform/reporting/templates/.gitkeep`
- Create: `src/tests/test_reporting_config.py`

**Steps:**

- [ ] Add deps to `pyproject.toml` `dependencies` (after `pandas>=2.0.0`):
  ```toml
      "pandas>=2.0.0",
      "jinja2>=3.1.0",
      "plotly>=5.20.0",
  ```

- [ ] Install and confirm both import:
  ```bash
  uv sync
  uv run python -c "import jinja2, plotly; from plotly.offline import get_plotlyjs; print('ok', len(get_plotlyjs()))"
  ```
  Expected: `ok <N>` where N is ~3.5–4.5 million (the embedded plotly.js bundle size in bytes — this is the ~4MB offline cost called out in the spec).

- [ ] Add `ReportingConfig` to `config.py`. Insert after the `ProcessingConfig` dataclass (around line 65):
  ```python
  @dataclass
  class ReportingConfig:
      """Report engine configuration."""
      histogram_top_n: int = 20
      # product -> list of test_num always rendered as a histogram regardless of fail rate
      always_include_tests: dict[str, list[int]] = field(default_factory=dict)
  ```

- [ ] Wire it into `Config`. In the `Config` dataclass (around line 75) add the field after `processing`:
  ```python
      processing: ProcessingConfig = field(default_factory=ProcessingConfig)
      reporting: ReportingConfig = field(default_factory=ReportingConfig)
  ```

- [ ] Parse it in `Config.load`. After the `processing_data = data.get("processing", {})` line, add:
  ```python
          reporting_data = data.get("reporting", {}) or {}
  ```
  and build the object just before the final `return cls(...)`:
  ```python
          raw_always = reporting_data.get("always_include_tests", {}) or {}
          always_include = {
              str(prod): [int(t) for t in (tests or [])]
              for prod, tests in raw_always.items()
          }
          reporting = ReportingConfig(
              histogram_top_n=int(reporting_data.get("histogram_top_n", 20)),
              always_include_tests=always_include,
          )
  ```
  and pass `reporting=reporting,` into the `return cls(...)` call (alongside `processing=...`).

- [ ] Add a `reports_dir` helper to `StorageConfig` (so generator/web agree on the location). In `StorageConfig` (config.py), add a property after `with_env`:
  ```python
      @property
      def reports_dir(self) -> Path:
          return self.data_dir / "reports"
  ```

- [ ] Create the package skeleton:
  ```bash
  mkdir -p src/stdf_platform/reporting/templates
  printf '"""Report engine: queries -> sections -> Jinja2/Plotly HTML."""\n' > src/stdf_platform/reporting/__init__.py
  touch src/stdf_platform/reporting/templates/.gitkeep
  ```

- [ ] Create `src/tests/test_reporting_config.py`:
  ```python
  """ReportingConfig parsing + defaults."""

  from pathlib import Path

  from stdf_platform.config import Config, ReportingConfig


  def test_reporting_defaults():
      cfg = Config()
      assert isinstance(cfg.reporting, ReportingConfig)
      assert cfg.reporting.histogram_top_n == 20
      assert cfg.reporting.always_include_tests == {}


  def test_reporting_parsed_from_yaml(tmp_path):
      cfg_file = tmp_path / "config.yaml"
      cfg_file.write_text(
          "reporting:\n"
          "  histogram_top_n: 5\n"
          "  always_include_tests:\n"
          "    PRODUCT_A: [1234, 5678]\n",
          encoding="utf-8",
      )
      cfg = Config.load(cfg_file)
      assert cfg.reporting.histogram_top_n == 5
      assert cfg.reporting.always_include_tests == {"PRODUCT_A": [1234, 5678]}


  def test_reports_dir_under_data():
      cfg = Config()
      assert cfg.storage.reports_dir == cfg.storage.data_dir / "reports"
  ```

- [ ] Run targeted + full suite:
  ```bash
  uv run pytest src/tests/test_reporting_config.py -q
  uv run pytest src/tests -q
  ```
  Expected: `3 passed` then `51 passed`.

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat(reporting): add jinja2/plotly deps, ReportingConfig, package skeleton

  Add jinja2 + plotly runtime deps for the offline HTML report engine.
  Introduce ReportingConfig (histogram_top_n + per-product always_include_tests)
  parsed from config.yaml, a StorageConfig.reports_dir helper, and the empty
  src/stdf_platform/reporting/ package with a templates/ dir.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 2 — `reporting/queries.py` (analysis SQL on Phase-1 views)

Pure query layer. Every function takes a `duckdb.DuckDBPyConnection` that **already has `setup_views()` registered** plus the filter values, and returns plain Python (`dict`/`list[dict]`). SQL is adapted from the existing web endpoints (`web/api/data.py`) and `database.py` so it stays retest-aware. CP rows have `part_txt=''` and real `wafer_id`; FT rows have `wafer_id=''` and `x=y=-32768` (see `views.py` `_DEDUP_UNIT` comment) — queries filter on `lot_id` (and `wafer_id` only for wafer maps), never assuming NULLs.

**Files:**
- Create: `src/stdf_platform/reporting/queries.py`
- Create: `src/tests/test_reporting_queries.py`

**Steps:**

- [ ] Create `src/stdf_platform/reporting/queries.py`:
  ```python
  """Analysis queries for the report engine.

  Every function receives a DuckDB connection that already has the Phase-1 views
  (lots / parts_final / test_data_final / chipid_final, via setup_views) and the
  lot identity (product, test_category, lot_id). Returns plain dict/list[dict].

  Yield is always computed from parts_final (retest-aware, die/package level),
  matching web/api/data.py and database.get_lot_summary.
  """

  import duckdb


  def lot_header(conn, product, test_category, lot_id) -> dict:
      """MIR-derived metadata for the lot (latest row if duplicated)."""
      row = conn.execute(
          """
          SELECT product, test_category, sub_process, part_type,
                 job_name, job_rev, tester_type, operator,
                 start_time, finish_time
          FROM lots
          WHERE product = ? AND test_category = ? AND lot_id = ?
          ORDER BY start_time DESC
          LIMIT 1
          """,
          [product, test_category, lot_id],
      ).fetchone()
      if not row:
          return {}
      cols = ["product", "test_category", "sub_process", "part_type",
              "job_name", "job_rev", "tester_type", "operator",
              "start_time", "finish_time"]
      out = dict(zip(cols, row))
      out["lot_id"] = lot_id
      return out


  def max_retest(conn, product, test_category, lot_id) -> int:
      """Highest retest_num present for the lot (0 = single run, no retest)."""
      row = conn.execute(
          "SELECT COALESCE(MAX(retest_num), 0) FROM parts WHERE lot_id = ?",
          [lot_id],
      ).fetchone()
      return int(row[0]) if row and row[0] is not None else 0


  def wafer_ids(conn, product, test_category, lot_id) -> list[str]:
      """Non-empty wafer ids in the lot (empty list for FT)."""
      rows = conn.execute(
          """
          SELECT DISTINCT wafer_id FROM parts_final
          WHERE lot_id = ? AND wafer_id != ''
          ORDER BY wafer_id
          """,
          [lot_id],
      ).fetchall()
      return [r[0] for r in rows]


  def wafer_yield(conn, product, test_category, lot_id) -> list[dict]:
      """Per-wafer (or single FT group) yield from parts_final."""
      rows = conn.execute(
          """
          SELECT wafer_id,
                 COUNT(*)                                          AS total,
                 SUM(CASE WHEN passed THEN 1 ELSE 0 END)           AS good,
                 ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 2)                     AS yield_pct
          FROM parts_final
          WHERE lot_id = ?
          GROUP BY wafer_id
          ORDER BY wafer_id
          """,
          [lot_id],
      ).fetchdf()
      return rows.to_dict(orient="records")


  def lot_yield_total(conn, product, test_category, lot_id) -> dict:
      """Lot-level totals from parts_final."""
      row = conn.execute(
          """
          SELECT COUNT(*)                                          AS total,
                 SUM(CASE WHEN passed THEN 1 ELSE 0 END)           AS good,
                 ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 2)                     AS yield_pct
          FROM parts_final
          WHERE lot_id = ?
          """,
          [lot_id],
      ).fetchone()
      total, good, yield_pct = (row or (0, 0, None))
      return {"total": int(total or 0), "good": int(good or 0),
              "yield_pct": float(yield_pct) if yield_pct is not None else 0.0}


  def bin_pareto(conn, product, test_category, lot_id) -> list[dict]:
      """Soft-bin counts per wafer (for a stacked pareto). One row per
      (soft_bin, wafer_id). FT collapses to a single wafer_id=''."""
      rows = conn.execute(
          """
          SELECT soft_bin, wafer_id, COUNT(*) AS count
          FROM parts_final
          WHERE lot_id = ?
          GROUP BY soft_bin, wafer_id
          ORDER BY soft_bin, wafer_id
          """,
          [lot_id],
      ).fetchdf()
      return rows.to_dict(orient="records")


  def wafer_map(conn, product, test_category, lot_id, wafer_id) -> list[dict]:
      """Die coordinates + bin for one wafer (adapted from /wafermap)."""
      rows = conn.execute(
          """
          SELECT x_coord, y_coord, soft_bin, hard_bin, passed
          FROM parts_final
          WHERE lot_id = ? AND wafer_id = ?
          ORDER BY y_coord, x_coord
          """,
          [lot_id, wafer_id],
      ).fetchdf()
      return rows.to_dict(orient="records")


  def top_fail_tests(conn, product, test_category, lot_id, top_n) -> list[dict]:
      """Top-N tests by fail rate (adapted from /fails). passed is 'P'/'F'."""
      rows = conn.execute(
          """
          SELECT test_num, FIRST(test_name) AS test_name,
                 COUNT(*)                                           AS total,
                 SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)      AS fail_count,
                 ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
                     / COUNT(*), 2)                                 AS fail_rate
          FROM test_data_final
          WHERE lot_id = ?
          GROUP BY test_num
          HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
          ORDER BY fail_rate DESC, test_num
          LIMIT ?
          """,
          [lot_id, top_n],
      ).fetchdf()
      return rows.to_dict(orient="records")


  def test_values(conn, product, test_category, lot_id, test_num) -> dict:
      """Numeric results + limits for one test (for a histogram)."""
      meta = conn.execute(
          """
          SELECT FIRST(test_name), FIRST(units), FIRST(lo_limit), FIRST(hi_limit)
          FROM test_data_final
          WHERE lot_id = ? AND test_num = ?
          """,
          [lot_id, test_num],
      ).fetchone()
      if not meta or meta[0] is None:
          return {}
      vals = conn.execute(
          """
          SELECT result FROM test_data_final
          WHERE lot_id = ? AND test_num = ? AND result IS NOT NULL
          """,
          [lot_id, test_num],
      ).fetchall()
      return {
          "test_num": test_num,
          "test_name": meta[0] or "",
          "units": meta[1] or "",
          "lo_limit": meta[2],
          "hi_limit": meta[3],
          "values": [r[0] for r in vals],
      }


  def retest_history(conn, product, test_category, lot_id) -> list[dict]:
      """Per-retest part counts + yield (only meaningful when max_retest>0)."""
      rows = conn.execute(
          """
          SELECT retest_num,
                 COUNT(*)                                          AS parts,
                 SUM(CASE WHEN passed THEN 1 ELSE 0 END)           AS good,
                 ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 2)                     AS yield_pct
          FROM parts
          WHERE lot_id = ?
          GROUP BY retest_num
          ORDER BY retest_num
          """,
          [lot_id],
      ).fetchdf()
      return rows.to_dict(orient="records")


  def chipid_summary(conn, product, test_category, lot_id) -> list[dict]:
      """FT ChipID origin breakdown from chipid_final (latest retest per die)."""
      rows = conn.execute(
          """
          SELECT origin_fab,
                 COUNT(*)                                           AS dies,
                 SUM(CASE WHEN valid THEN 1 ELSE 0 END)             AS valid_dies,
                 COUNT(DISTINCT origin_lot)                         AS origin_lots,
                 COUNT(DISTINCT origin_wafer)                       AS origin_wafers
          FROM chipid_final
          WHERE lot_id = ?
          GROUP BY origin_fab
          ORDER BY dies DESC
          """,
          [lot_id],
      ).fetchdf()
      return rows.to_dict(orient="records")
  ```

  Note: `max_retest`/`retest_history` read the raw `parts` view (not `parts_final`) because `retest_num` is collapsed away by the dedup. `setup_views` registers `parts` whenever the table dir exists, so this is always available alongside `parts_final`.

- [ ] Create `src/tests/test_reporting_queries.py`. Fixtures build Parquet directly (the `test_export_lot.py` pattern) so we control retest depth and FT/CP shapes precisely:
  ```python
  """reporting.queries against synthetic Parquet fixtures (Phase-1 views)."""

  import duckdb
  import pyarrow as pa
  import pyarrow.parquet as pq
  from pathlib import Path

  from stdf_platform.views import setup_views
  from stdf_platform.reporting import queries as q


  def _conn(data_dir: Path):
      conn = duckdb.connect(":memory:")
      setup_views(conn, data_dir)
      return conn


  def _write_cp(data_dir: Path):
      """One CP lot, 2 wafers; W1 has a fail (soft_bin 3), plus a retest=1 of W1
      where the failing die now passes (so parts_final must pick retest=1)."""
      def parts(wafer, retest, rows):
          p = (data_dir / "parts" / "product=PROD" / "test_category=CP"
               / "sub_process=CP1" / f"lot_id=LOT1" / f"wafer_id={wafer}"
               / f"retest={retest}" / "data.parquet")
          p.parent.mkdir(parents=True, exist_ok=True)
          pq.write_table(pa.table(rows), p)

      # W1 retest0: die (0,0) fails (soft_bin 3), die (1,0) passes
      parts("W1", 0, {
          "lot_id": ["LOT1", "LOT1"], "wafer_id": ["W1", "W1"],
          "part_id": ["A", "B"], "part_txt": ["", ""],
          "x_coord": [0, 1], "y_coord": [0, 0],
          "hard_bin": [2, 1], "soft_bin": [3, 1],
          "passed": [False, True], "retest_num": [0, 0],
      })
      # W1 retest1: die (0,0) now passes
      parts("W1", 1, {
          "lot_id": ["LOT1"], "wafer_id": ["W1"],
          "part_id": ["A"], "part_txt": [""],
          "x_coord": [0], "y_coord": [0],
          "hard_bin": [1], "soft_bin": [1],
          "passed": [True], "retest_num": [1],
      })
      # W2 retest0: both pass
      parts("W2", 0, {
          "lot_id": ["LOT1", "LOT1"], "wafer_id": ["W2", "W2"],
          "part_id": ["C", "D"], "part_txt": ["", ""],
          "x_coord": [0, 1], "y_coord": [0, 0],
          "hard_bin": [1, 1], "soft_bin": [1, 1],
          "passed": [True, True], "retest_num": [0, 0],
      })

      lots = (data_dir / "lots" / "product=PROD" / "test_category=CP"
              / "sub_process=CP1" / "lot_id=LOT1" / "data.parquet")
      lots.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["LOT1"], "product": ["PROD"], "test_category": ["CP"],
          "sub_process": ["CP1"], "part_type": ["SCT101A"],
          "job_name": ["CP_TEST"], "job_rev": ["Rev01"],
          "tester_type": ["J750"], "operator": ["OPE01"],
          "start_time": [pa.scalar(1_700_000_000_000, pa.timestamp("ms", tz="UTC"))],
          "finish_time": [pa.scalar(1_700_003_600_000, pa.timestamp("ms", tz="UTC"))],
      }), lots)

      td = (data_dir / "test_data" / "product=PROD" / "test_category=CP"
            / "sub_process=CP1" / "lot_id=LOT1" / "data.parquet")
      td.parent.mkdir(parents=True, exist_ok=True)
      # test 1001 fails on die A (one fail of two -> 50% fail rate after dedup is
      # actually 1/3 since retest1 adds a passing A row; we assert >0 only)
      pq.write_table(pa.table({
          "lot_id": ["LOT1", "LOT1", "LOT1"], "wafer_id": ["W1", "W1", "W2"],
          "part_id": ["A", "B", "C"], "part_txt": ["", "", ""],
          "x_coord": [0, 1, 0], "y_coord": [0, 0, 0],
          "test_num": [1001, 1001, 1001], "pin_num": [0, 0, 0],
          "test_name": ["Vth_N", "Vth_N", "Vth_N"], "rec_type": ["PTR", "PTR", "PTR"],
          "lo_limit": [0.3, 0.3, 0.3], "hi_limit": [0.8, 0.8, 0.8], "units": ["V", "V", "V"],
          "result": [0.95, 0.55, 0.60], "passed": ["F", "P", "P"],
          "retest_num": [0, 0, 0],
      }), td)


  def _write_ft(data_dir: Path):
      """FT lot: wafer_id='', x=y=-32768, plus a chipid table."""
      p = (data_dir / "parts" / "product=CHIP" / "test_category=FT"
           / "sub_process=FT1" / "lot_id=FT1" / "wafer_id=" / "retest=0" / "data.parquet")
      p.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["FT1", "FT1"], "wafer_id": ["", ""],
          "part_id": ["U0", "U1"], "part_txt": ["2D-FT1-0000", "2D-FT1-0001"],
          "x_coord": [-32768, -32768], "y_coord": [-32768, -32768],
          "hard_bin": [1, 2], "soft_bin": [1, 3],
          "passed": [True, False], "retest_num": [0, 0],
      }), p)

      lots = (data_dir / "lots" / "product=CHIP" / "test_category=FT"
              / "sub_process=FT1" / "lot_id=FT1" / "data.parquet")
      lots.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["FT1"], "product": ["CHIP"], "test_category": ["FT"],
          "sub_process": ["FT1"], "part_type": ["CHIP"],
          "job_name": ["FT_TEST"], "job_rev": ["Rev01"],
          "tester_type": ["J750"], "operator": ["OPE01"],
          "start_time": [pa.scalar(1_700_000_000_000, pa.timestamp("ms", tz="UTC"))],
          "finish_time": [pa.scalar(1_700_003_600_000, pa.timestamp("ms", tz="UTC"))],
      }), lots)

      cp = (data_dir / "chipid" / "product=CHIP" / "test_category=FT"
            / "sub_process=FT1" / "lot_id=FT1" / "wafer_id=" / "retest=0" / "data.parquet")
      cp.parent.mkdir(parents=True, exist_ok=True)
      pq.write_table(pa.table({
          "lot_id": ["FT1", "FT1"], "part_id": ["U0", "U0"],
          "part_txt": ["2D-FT1-0000", "2D-FT1-0000"],
          "chip_occurrence_index": [0, 1],
          "efuse_raw": ["0b" + "0" * 64, "0b" + "1" * 64],
          "valid": [True, True],
          "origin_fab_code": [1, 6], "origin_fab": ["TSMC1", "TSMC2"],
          "origin_lot": ["HKPFJK", "ABCDEF"], "origin_wafer": [11, 7],
          "origin_x": [12, 102], "origin_y": [22, 202],
          "reserved_bits": ["00", "00"], "retest_num": [0, 0],
      }), cp)


  def test_lot_header_cp(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      h = q.lot_header(conn, "PROD", "CP", "LOT1")
      assert h["lot_id"] == "LOT1"
      assert h["part_type"] == "SCT101A"
      assert h["tester_type"] == "J750"


  def test_cp_yield_is_retest_aware(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      total = q.lot_yield_total(conn, "PROD", "CP", "LOT1")
      # 4 distinct dies (W1:A,B ; W2:C,D); A passes via retest1 -> all 4 good
      assert total["total"] == 4
      assert total["good"] == 4
      assert total["yield_pct"] == 100.0
      assert q.max_retest(conn, "PROD", "CP", "LOT1") == 1


  def test_wafer_yield_and_ids(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      assert q.wafer_ids(conn, "PROD", "CP", "LOT1") == ["W1", "W2"]
      wy = {r["wafer_id"]: r for r in q.wafer_yield(conn, "PROD", "CP", "LOT1")}
      assert wy["W1"]["total"] == 2 and wy["W1"]["good"] == 2
      assert wy["W2"]["total"] == 2


  def test_top_fail_and_values(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      fails = q.top_fail_tests(conn, "PROD", "CP", "LOT1", 20)
      assert fails and fails[0]["test_num"] == 1001
      assert fails[0]["fail_count"] >= 1
      vals = q.test_values(conn, "PROD", "CP", "LOT1", 1001)
      assert vals["lo_limit"] == 0.3 and vals["hi_limit"] == 0.8
      assert len(vals["values"]) == 3


  def test_bin_pareto_and_wafer_map(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      pareto = q.bin_pareto(conn, "PROD", "CP", "LOT1")
      bins = {r["soft_bin"] for r in pareto}
      assert 1 in bins  # passing bin present
      wm = q.wafer_map(conn, "PROD", "CP", "LOT1", "W1")
      assert len(wm) == 2
      assert {(r["x_coord"], r["y_coord"]) for r in wm} == {(0, 0), (1, 0)}


  def test_ft_chipid_summary(tmp_path):
      _write_ft(tmp_path)
      conn = _conn(tmp_path)
      assert q.wafer_ids(conn, "CHIP", "FT", "FT1") == []  # FT has no wafers
      summary = {r["origin_fab"]: r for r in q.chipid_summary(conn, "CHIP", "FT", "FT1")}
      assert summary["TSMC1"]["dies"] == 1
      assert summary["TSMC2"]["dies"] == 1
  ```

- [ ] Run:
  ```bash
  uv run pytest src/tests/test_reporting_queries.py -q
  uv run pytest src/tests -q
  ```
  Expected: `6 passed` then `57 passed`.

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat(reporting): analysis query layer over Phase-1 views

  Add reporting/queries.py — retest-aware lot header, per-wafer and lot yield,
  soft-bin pareto, wafer maps, top fail-rate tests, parametric value pulls,
  retest history and FT ChipID origin summary, all on parts_final/
  test_data_final/chipid_final. Tested against synthetic CP+FT Parquet fixtures.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 3 — `reporting/sections.py` (Plotly figures + tables)

Each section is a function `(conn, product, test_category, lot_id, cfg) -> SectionResult | None`. Figures are built with **`plotly.graph_objects`** (not express, to keep payloads lean) and serialized with `fig.to_json()` so `render.py` can `Plotly.newPlot(div, JSON.parse(...))`. `SectionResult.tables` are plain column/row dicts. The orchestrating `build_sections(...)` assembles the CP vs FT section list and is what `generator.py` calls.

Histogram test selection (spec): the union of (a) top-`histogram_top_n` fail-rate tests and (b) `always_include_tests[product]`, capped to keep the HTML small (`_HIST_FIG_CAP = 24` rendered histograms max). One figure JSON per histogram.

**Files:**
- Create: `src/stdf_platform/reporting/sections.py`
- Create: `src/tests/test_reporting_sections.py`

**Steps:**

- [ ] Create `src/stdf_platform/reporting/sections.py`:
  ```python
  """Report sections: each builds Plotly figures + tables from queries.

  A section function returns a SectionResult, or None when it does not apply
  (e.g. retest history for a lot with no retests, wafer maps for FT, ChipID
  for CP). build_sections() assembles the ordered CP/FT list.
  """

  from dataclasses import dataclass, field
  from statistics import mean, pstdev

  import plotly.graph_objects as go

  from . import queries as q

  _HIST_FIG_CAP = 24          # max parametric histograms rendered per report
  _WAFER_MAP_CAP = 30         # max wafer-map figures per report


  @dataclass
  class SectionResult:
      title: str
      figures: list[str] = field(default_factory=list)   # fig.to_json() strings
      tables: list[dict] = field(default_factory=list)    # {caption, columns, rows}
      notes: list[str] = field(default_factory=list)


  def _fig_json(fig: go.Figure) -> str:
      return fig.to_json()


  def _cpk(values, lo, hi) -> float | None:
      """Process capability. Needs at least one finite limit and >1 sample."""
      vals = [v for v in values if v is not None]
      if len(vals) < 2:
          return None
      sigma = pstdev(vals)
      if sigma == 0:
          return None
      mu = mean(vals)
      cands = []
      if hi is not None:
          cands.append((hi - mu) / (3 * sigma))
      if lo is not None:
          cands.append((mu - lo) / (3 * sigma))
      return round(min(cands), 3) if cands else None


  # ── header ───────────────────────────────────────────────────────────────────

  def header_section(conn, product, test_category, lot_id, cfg) -> SectionResult:
      h = q.lot_header(conn, product, test_category, lot_id)
      retests = q.max_retest(conn, product, test_category, lot_id)
      rows = [
          ["Lot ID", h.get("lot_id", lot_id)],
          ["Product", h.get("product", product)],
          ["Category", h.get("test_category", test_category)],
          ["Sub-process", h.get("sub_process", "")],
          ["Part type", h.get("part_type", "")],
          ["Test program", f"{h.get('job_name', '')} ({h.get('job_rev', '')})"],
          ["Tester", h.get("tester_type", "")],
          ["Operator", h.get("operator", "")],
          ["Start", str(h.get("start_time", ""))],
          ["Finish", str(h.get("finish_time", ""))],
          ["Retest count", str(retests)],
      ]
      return SectionResult(
          title="Header",
          tables=[{"caption": "Lot metadata", "columns": ["Field", "Value"], "rows": rows}],
      )


  # ── yield summary ────────────────────────────────────────────────────────────

  def yield_section(conn, product, test_category, lot_id, cfg) -> SectionResult:
      wy = q.wafer_yield(conn, product, test_category, lot_id)
      total = q.lot_yield_total(conn, product, test_category, lot_id)
      labels = [r["wafer_id"] or "(lot)" for r in wy]
      fig = go.Figure(go.Bar(
          x=labels,
          y=[r["yield_pct"] for r in wy],
          marker_color="#6366f1",
          text=[f"{r['yield_pct']:.1f}%" for r in wy],
          textposition="outside",
      ))
      fig.update_layout(
          title="Yield by wafer", yaxis_title="Yield %",
          yaxis_range=[0, 105], template="plotly_dark",
          margin=dict(l=40, r=20, t=40, b=40),
      )
      rows = [[r["wafer_id"] or "(lot)", int(r["total"]), int(r["good"]),
               f"{r['yield_pct']:.2f}"] for r in wy]
      rows.append(["TOTAL", total["total"], total["good"], f"{total['yield_pct']:.2f}"])
      return SectionResult(
          title="Yield summary",
          figures=[_fig_json(fig)],
          tables=[{"caption": "Per-wafer yield",
                   "columns": ["Wafer", "Total", "Good", "Yield %"], "rows": rows}],
      )


  # ── bin pareto ───────────────────────────────────────────────────────────────

  def bin_pareto_section(conn, product, test_category, lot_id, cfg) -> SectionResult:
      data = q.bin_pareto(conn, product, test_category, lot_id)
      wafers = sorted({r["wafer_id"] for r in data})
      bins = sorted({r["soft_bin"] for r in data},
                    key=lambda b: -sum(r["count"] for r in data if r["soft_bin"] == b))
      lookup = {(r["soft_bin"], r["wafer_id"]): r["count"] for r in data}
      fig = go.Figure()
      for w in wafers:
          fig.add_bar(name=(w or "(lot)"), x=[str(b) for b in bins],
                      y=[lookup.get((b, w), 0) for b in bins])
      fig.update_layout(
          barmode="stack", title="Soft-bin pareto (stacked by wafer)",
          xaxis_title="Soft bin", yaxis_title="Parts",
          template="plotly_dark", margin=dict(l=40, r=20, t=40, b=40),
      )
      rows = [[str(b), sum(r["count"] for r in data if r["soft_bin"] == b)] for b in bins]
      return SectionResult(
          title="Bin pareto",
          figures=[_fig_json(fig)],
          tables=[{"caption": "Soft-bin totals", "columns": ["Soft bin", "Count"], "rows": rows}],
      )


  # ── wafer maps grid (CP only) ────────────────────────────────────────────────

  def wafer_maps_section(conn, product, test_category, lot_id, cfg) -> SectionResult | None:
      ids = q.wafer_ids(conn, product, test_category, lot_id)
      if not ids:
          return None
      figures = []
      capped = ids[:_WAFER_MAP_CAP]
      for wid in capped:
          dies = q.wafer_map(conn, product, test_category, lot_id, wid)
          fig = go.Figure(go.Scatter(
              x=[d["x_coord"] for d in dies],
              y=[d["y_coord"] for d in dies],
              mode="markers",
              marker=dict(
                  size=8, symbol="square",
                  color=[d["soft_bin"] for d in dies],
                  colorscale="Viridis", showscale=True,
                  colorbar=dict(title="Soft bin"),
              ),
              text=[f"bin {d['soft_bin']}" for d in dies],
          ))
          fig.update_layout(
              title=f"Wafer {wid}", template="plotly_dark",
              yaxis=dict(autorange="reversed", scaleanchor="x", scaleratio=1),
              margin=dict(l=30, r=20, t=40, b=30),
          )
          figures.append(_fig_json(fig))
      notes = []
      if len(ids) > _WAFER_MAP_CAP:
          notes.append(f"Showing {_WAFER_MAP_CAP} of {len(ids)} wafers (capped).")
      return SectionResult(title="Wafer maps", figures=figures, notes=notes)


  # ── parametric (top fail tests + histograms) ─────────────────────────────────

  def _select_histogram_tests(conn, product, test_category, lot_id, cfg, fails):
      top = [f["test_num"] for f in fails]
      always = list(cfg.reporting.always_include_tests.get(product, []))
      ordered = top + [t for t in always if t not in top]
      return ordered[:_HIST_FIG_CAP]


  def parametric_section(conn, product, test_category, lot_id, cfg) -> SectionResult:
      fails = q.top_fail_tests(conn, product, test_category, lot_id,
                               cfg.reporting.histogram_top_n)
      table_rows = [[f["test_num"], f["test_name"], int(f["total"]),
                     int(f["fail_count"]), f"{f['fail_rate']:.2f}"] for f in fails]
      figures = []
      for tn in _select_histogram_tests(conn, product, test_category, lot_id, cfg, fails):
          v = q.test_values(conn, product, test_category, lot_id, tn)
          if not v or not v["values"]:
              continue
          cpk = _cpk(v["values"], v["lo_limit"], v["hi_limit"])
          fig = go.Figure(go.Histogram(x=v["values"], nbinsx=40, marker_color="#38bdf8"))
          for lim, name in ((v["lo_limit"], "LSL"), (v["hi_limit"], "USL")):
              if lim is not None:
                  fig.add_vline(x=lim, line_color="#ef4444", line_dash="dash",
                                annotation_text=name)
          cpk_txt = f"  Cpk={cpk}" if cpk is not None else ""
          fig.update_layout(
              title=f"{v['test_name']} (#{tn}) [{v['units']}]{cpk_txt}",
              template="plotly_dark", bargap=0.02,
              margin=dict(l=40, r=20, t=40, b=40),
          )
          figures.append(_fig_json(fig))
      return SectionResult(
          title="Parametric",
          figures=figures,
          tables=[{"caption": "Top failing tests",
                   "columns": ["Test #", "Name", "Total", "Fails", "Fail %"],
                   "rows": table_rows}],
      )


  # ── retest history (only when retests > 0) ───────────────────────────────────

  def retest_section(conn, product, test_category, lot_id, cfg) -> SectionResult | None:
      if q.max_retest(conn, product, test_category, lot_id) == 0:
          return None
      hist = q.retest_history(conn, product, test_category, lot_id)
      rows = [[int(r["retest_num"]), int(r["parts"]), int(r["good"]),
               f"{r['yield_pct']:.2f}"] for r in hist]
      return SectionResult(
          title="Retest history",
          tables=[{"caption": "Per-retest counts",
                   "columns": ["Retest", "Parts", "Good", "Yield %"], "rows": rows}],
      )


  # ── ChipID summary (FT only) ─────────────────────────────────────────────────

  def chipid_section(conn, product, test_category, lot_id, cfg) -> SectionResult | None:
      summary = q.chipid_summary(conn, product, test_category, lot_id)
      if not summary:
          return None
      rows = [[r["origin_fab"], int(r["dies"]), int(r["valid_dies"]),
               int(r["origin_lots"]), int(r["origin_wafers"])] for r in summary]
      fig = go.Figure(go.Bar(
          x=[r["origin_fab"] for r in summary],
          y=[r["dies"] for r in summary], marker_color="#22c55e",
      ))
      fig.update_layout(title="Dies by origin fab", template="plotly_dark",
                        yaxis_title="Dies", margin=dict(l=40, r=20, t=40, b=40))
      return SectionResult(
          title="ChipID summary",
          figures=[_fig_json(fig)],
          tables=[{"caption": "Origin fab breakdown",
                   "columns": ["Origin fab", "Dies", "Valid", "Origin lots", "Origin wafers"],
                   "rows": rows}],
      )


  def build_sections(conn, product, test_category, lot_id, cfg) -> list[SectionResult]:
      """Ordered section list. CP: header, yield, bins, wafer maps, parametric,
      retest. FT: header, yield, bins, parametric, retest, ChipID (no wafer maps)."""
      builders = [header_section, yield_section, bin_pareto_section]
      if test_category != "FT":
          builders.append(wafer_maps_section)
      builders.append(parametric_section)
      builders.append(retest_section)
      if test_category == "FT":
          builders.append(chipid_section)
      out = []
      for b in builders:
          res = b(conn, product, test_category, lot_id, cfg)
          if res is not None:
              out.append(res)
      return out
  ```

- [ ] Create `src/tests/test_reporting_sections.py` (reuses the Task-2 fixtures via import):
  ```python
  """reporting.sections: figures are valid Plotly JSON, CP/FT lists correct."""

  import json

  import duckdb

  from stdf_platform.views import setup_views
  from stdf_platform.config import Config
  from stdf_platform.reporting import sections as S
  from test_reporting_queries import _write_cp, _write_ft  # fixture builders


  def _conn(data_dir):
      conn = duckdb.connect(":memory:")
      setup_views(conn, data_dir)
      return conn


  def test_cp_section_list_has_wafer_maps_no_chipid(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      secs = S.build_sections(conn, "PROD", "CP", "LOT1", Config())
      titles = [s.title for s in secs]
      assert titles[0] == "Header"
      assert "Wafer maps" in titles
      assert "ChipID summary" not in titles
      # retest history present because the CP fixture has a retest=1
      assert "Retest history" in titles


  def test_ft_section_list_has_chipid_no_wafer_maps(tmp_path):
      _write_ft(tmp_path)
      conn = _conn(tmp_path)
      secs = S.build_sections(conn, "CHIP", "FT", "FT1", Config())
      titles = [s.title for s in secs]
      assert "Wafer maps" not in titles
      assert "ChipID summary" in titles
      assert "Retest history" not in titles  # single FT run


  def test_figures_are_valid_plotly_json(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      secs = S.build_sections(conn, "PROD", "CP", "LOT1", Config())
      figs = [f for s in secs for f in s.figures]
      assert figs, "expected at least one figure"
      for f in figs:
          obj = json.loads(f)
          assert "data" in obj and "layout" in obj


  def test_yield_section_total_row(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      sec = S.yield_section(conn, "PROD", "CP", "LOT1", Config())
      last = sec.tables[0]["rows"][-1]
      assert last[0] == "TOTAL"
      assert last[1] == 4  # 4 distinct dies


  def test_always_include_tests_added(tmp_path):
      _write_cp(tmp_path)
      conn = _conn(tmp_path)
      cfg = Config()
      cfg.reporting.histogram_top_n = 0          # no fail-driven histograms
      cfg.reporting.always_include_tests = {"PROD": [1001]}
      sec = S.parametric_section(conn, "PROD", "CP", "LOT1", cfg)
      # 1001 forced in despite top_n=0 -> exactly one histogram figure
      assert len(sec.figures) == 1
  ```

- [ ] Run:
  ```bash
  uv run pytest src/tests/test_reporting_sections.py -q
  uv run pytest src/tests -q
  ```
  Expected: `5 passed` then `62 passed`.

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat(reporting): section builders (Plotly figures + tables)

  Add reporting/sections.py: header, yield summary (bar + per-wafer table),
  stacked soft-bin pareto, CP wafer-map grid, parametric histograms (limit
  lines + Cpk, fail-rate top-N union always_include_tests, figure-capped),
  retest history (retest>0 only) and FT ChipID origin summary. build_sections
  assembles the CP vs FT ordered list. graph_objects only; fig.to_json output.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 4 — `reporting/render.py` + Jinja2 template (embedded plotly.js)

Render the section list into a single self-contained HTML file. plotly.js is embedded **once** via `plotly.offline.get_plotlyjs()` (the ~4MB offline cost — documented in the template comment and `render.py` docstring). The template is a packaged file: `src/stdf_platform/reporting/templates/report.html.j2`, loaded with a Jinja2 `PackageLoader` so it ships in the wheel like `web/static/*`.

**Files:**
- Create: `src/stdf_platform/reporting/templates/report.html.j2`
- Create: `src/stdf_platform/reporting/render.py`
- Delete: `src/stdf_platform/reporting/templates/.gitkeep` (real template now exists)
- Create: `src/tests/test_reporting_render.py`

**Steps:**

- [ ] Create `src/stdf_platform/reporting/templates/report.html.j2`:
  ```jinja
  <!DOCTYPE html>
  <html lang="en" class="dark">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{{ product }} / {{ test_category }} / {{ lot_id }} — stdf report</title>
    {# plotly.js is embedded inline so the file opens offline (~4MB). #}
    <script type="text/javascript">{{ plotly_js }}</script>
    <style>
      body { background:#0f172a; color:#e2e8f0; font-family:system-ui,sans-serif; margin:0; padding:0 24px 64px; }
      header { padding:20px 0; border-bottom:1px solid #334155; margin-bottom:16px; }
      h1 { font-size:20px; color:#818cf8; margin:0; }
      h2 { font-size:16px; color:#cbd5e1; border-bottom:1px solid #334155; padding-bottom:6px; margin-top:32px; }
      .sub { color:#64748b; font-size:13px; }
      table { border-collapse:collapse; margin:12px 0; font-size:13px; }
      th, td { border:1px solid #334155; padding:4px 10px; text-align:left; }
      th { background:#1e293b; color:#94a3b8; }
      .caption { color:#94a3b8; font-size:12px; margin-top:12px; }
      .note { color:#fbbf24; font-size:12px; margin:6px 0; }
      .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(420px,1fr)); gap:16px; }
      .fig { background:#1e293b; border:1px solid #334155; border-radius:8px; padding:6px; }
      nav a { color:#818cf8; font-size:12px; margin-right:12px; text-decoration:none; }
    </style>
  </head>
  <body>
    <header>
      <h1>{{ product }} · {{ test_category }} · {{ lot_id }}</h1>
      <div class="sub">Generated {{ generated_at }} · stdf report engine</div>
      <nav>
        {% for s in sections %}<a href="#sec-{{ loop.index }}">{{ s.title }}</a>{% endfor %}
      </nav>
    </header>

    {% for s in sections %}
    <section id="sec-{{ loop.index }}">
      <h2>{{ s.title }}</h2>
      {% for note in s.notes %}<div class="note">{{ note }}</div>{% endfor %}
      {% for t in s.tables %}
        <div class="caption">{{ t.caption }}</div>
        <table>
          <thead><tr>{% for c in t.columns %}<th>{{ c }}</th>{% endfor %}</tr></thead>
          <tbody>
            {% for row in t.rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
          </tbody>
        </table>
      {% endfor %}
      {% if s.figures %}
      <div class="grid">
        {% for fig in s.figures %}
          <div class="fig" id="fig-{{ loop.index0 }}-{{ s.title|replace(' ','_') }}"></div>
        {% endfor %}
      </div>
      <script type="application/json" class="figdata" data-section="{{ loop.index }}">
        {{ figures_json[loop.index0] | safe }}
      </script>
      {% endif %}
    </section>
    {% endfor %}

    <script type="text/javascript">
      // Each section's figures are stored as a JSON array of Plotly fig specs in
      // a <script class="figdata"> tag; plot them into the .fig divs in order.
      document.querySelectorAll('section').forEach(function (sec) {
        var holder = sec.querySelector('script.figdata');
        if (!holder) return;
        var figs = JSON.parse(holder.textContent);
        var divs = sec.querySelectorAll('.fig');
        figs.forEach(function (spec, i) {
          if (divs[i]) Plotly.newPlot(divs[i], spec.data, spec.layout, {responsive: true});
        });
      });
    </script>
  </body>
  </html>
  ```

- [ ] Create `src/stdf_platform/reporting/render.py`:
  ```python
  """Render section results into a single self-contained HTML file.

  plotly.js is embedded inline via plotly.offline.get_plotlyjs() so the report
  opens offline with no network access. That inline bundle is ~4MB, which
  dominates the file size — acceptable per the approved design (offline viewing
  is the requirement). Only ONE copy of the bundle is embedded per HTML.
  """

  import json

  from jinja2 import Environment, PackageLoader, select_autoescape
  from plotly.offline import get_plotlyjs

  from .sections import SectionResult

  _env = Environment(
      loader=PackageLoader("stdf_platform.reporting", "templates"),
      autoescape=select_autoescape(["html", "xml", "j2"]),
  )


  def render_report(product, test_category, lot_id, sections: list[SectionResult],
                    generated_at: str) -> str:
      # Per-section JSON array of figure specs (each section's figures[] are
      # already fig.to_json() strings -> wrap into a JS array literal).
      figures_json = [
          "[" + ",".join(s.figures) + "]" for s in sections
      ]
      template = _env.get_template("report.html.j2")
      return template.render(
          product=product,
          test_category=test_category,
          lot_id=lot_id,
          sections=sections,
          figures_json=figures_json,
          generated_at=generated_at,
          plotly_js=get_plotlyjs(),
      )
  ```

  Note: `figures_json[i]` is injected with `| safe` (it is trusted JSON we built), so autoescape does not mangle it. Section titles/table cells stay autoescaped.

- [ ] Remove the placeholder:
  ```bash
  git rm src/stdf_platform/reporting/templates/.gitkeep
  ```

- [ ] Create `src/tests/test_reporting_render.py`:
  ```python
  """render_report smoke test: HTML contains sections, tables, fig JSON, plotly.js."""

  import duckdb

  from stdf_platform.views import setup_views
  from stdf_platform.config import Config
  from stdf_platform.reporting import sections as S
  from stdf_platform.reporting.render import render_report
  from test_reporting_queries import _write_cp, _write_ft


  def _html(data_dir, product, category, lot):
      conn = duckdb.connect(":memory:")
      setup_views(conn, data_dir)
      secs = S.build_sections(conn, product, category, lot, Config())
      return render_report(product, category, lot, secs, "2026-06-11T00:00:00Z")


  def test_cp_report_html_has_sections_and_plotly(tmp_path):
      html = _html(tmp_path if _write_cp(tmp_path) is None else tmp_path, "PROD", "CP", "LOT1")
      assert "Yield summary" in html
      assert "Wafer maps" in html
      assert "Plotly.newPlot" in html      # embedded bundle exposes Plotly
      assert "Histogram" in html or "Bar" in html  # a fig spec is embedded
      assert "<title>PROD" in html


  def test_ft_report_html_has_chipid(tmp_path):
      _write_ft(tmp_path)
      html = _html(tmp_path, "CHIP", "FT", "FT1")
      assert "ChipID summary" in html
      assert "Wafer maps" not in html


  def test_report_embeds_single_plotly_bundle(tmp_path):
      _write_cp(tmp_path)
      html = _html(tmp_path, "PROD", "CP", "LOT1")
      # the bundle defines this once; ensure it is present and not duplicated
      assert html.count("Plotly.register") <= 2  # bundle internals, not per-fig
      assert len(html) > 1_000_000              # plotly.js embedded (~4MB)
  ```

  Note: `_write_cp` returns `None`; the first test's ternary just calls it for its side effect. If preferred, split into two lines — keep it explicit:
  ```python
  def test_cp_report_html_has_sections_and_plotly(tmp_path):
      _write_cp(tmp_path)
      html = _html(tmp_path, "PROD", "CP", "LOT1")
      ...
  ```
  Use the explicit two-line form in the actual file.

- [ ] Run:
  ```bash
  uv run pytest src/tests/test_reporting_render.py -q
  uv run pytest src/tests -q
  ```
  Expected: `3 passed` then `65 passed`.

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat(reporting): Jinja2 + embedded-plotly HTML renderer

  Add reporting/render.py and the packaged templates/report.html.j2. plotly.js
  is embedded inline once per file via plotly.offline.get_plotlyjs() for offline
  viewing (~4MB, documented). Sections render as anchored blocks with tables and
  responsive Plotly figures. Smoke-tested for CP and FT.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 5 — `reporting/generator.py` + `stdf report` CLI

Orchestrate query → sections → render → write. Output path is sanitized with `ParquetStorage._sanitize` (same rules storage.py uses) so partitions match. The report is **always overwritten**. `pending_lots` scans `data/lots/**` for ingested lots and compares the newest Parquet mtime under the lot's partitions against the report mtime (regenerate if report missing or stale). The `report` command supports `--lot X -p PROD [-c CP|FT]` and `--pending`.

**Files:**
- Create: `src/stdf_platform/reporting/generator.py`
- Modify: `src/stdf_platform/cli.py` (add `report` command)
- Create: `src/tests/test_reporting_generator.py`

**Steps:**

- [ ] Create `src/stdf_platform/reporting/generator.py`:
  ```python
  """Generate per-lot HTML reports. Reports are a disposable cache regenerated
  from Parquet; output is always overwritten."""

  from datetime import datetime, timezone
  from pathlib import Path

  import duckdb

  from stdf_platform.views import setup_views
  from stdf_platform.storage import ParquetStorage
  from .sections import build_sections
  from .render import render_report

  _CATS = ["CP", "FT", "OTHER"]


  def report_path(data_dir: Path, product: str, test_category: str, lot_id: str) -> Path:
      s = ParquetStorage._sanitize
      return (
          data_dir / "reports"
          / f"product={s(product)}"
          / f"test_category={s(test_category)}"
          / f"lot_id={s(lot_id)}"
          / "report.html"
      )


  def generate_lot_report(config, product: str, test_category: str, lot_id: str) -> Path:
      data_dir = config.storage.data_dir
      conn = duckdb.connect(":memory:")
      try:
          setup_views(conn, data_dir)
          sections = build_sections(conn, product, test_category, lot_id, config)
      finally:
          conn.close()
      html = render_report(
          product, test_category, lot_id, sections,
          datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
      )
      out = report_path(data_dir, product, test_category, lot_id)
      out.parent.mkdir(parents=True, exist_ok=True)
      out.write_text(html, encoding="utf-8")
      return out


  def _scan_lots(data_dir: Path) -> list[tuple[str, str, str]]:
      """Discover (product, test_category, lot_id) from data/lots partitions."""
      lots_root = data_dir / "lots"
      found: set[tuple[str, str, str]] = set()
      if not lots_root.exists():
          return []
      for prod_dir in lots_root.glob("product=*"):
          product = prod_dir.name[len("product="):]
          for cat_dir in prod_dir.glob("test_category=*"):
              category = cat_dir.name[len("test_category="):]
              for lot_dir in cat_dir.glob("**/lot_id=*"):
                  lot_id = lot_dir.name[len("lot_id="):]
                  found.add((product, category, lot_id))
      return sorted(found)


  def _newest_parquet_mtime(data_dir: Path, product: str, category: str, lot_id: str) -> float:
      """Newest data.parquet mtime across all tables for this lot."""
      s = ParquetStorage._sanitize
      newest = 0.0
      for table in ["lots", "parts", "test_data", "wafers", "chipid"]:
          base = (data_dir / table / f"product={s(product)}"
                  / f"test_category={s(category)}")
          if not base.exists():
              continue
          for pq_file in base.glob(f"**/lot_id={s(lot_id)}/**/*.parquet"):
              newest = max(newest, pq_file.stat().st_mtime)
          # lots has no wafer/retest depth; also catch the direct lot dir
          for pq_file in base.glob(f"**/lot_id={s(lot_id)}/*.parquet"):
              newest = max(newest, pq_file.stat().st_mtime)
      return newest


  def pending_lots(config) -> list[tuple[str, str, str]]:
      """Lots whose report is missing or older than their newest Parquet."""
      data_dir = config.storage.data_dir
      pending = []
      for product, category, lot_id in _scan_lots(data_dir):
          report = report_path(data_dir, product, category, lot_id)
          src_mtime = _newest_parquet_mtime(data_dir, product, category, lot_id)
          if not report.exists() or report.stat().st_mtime < src_mtime:
              pending.append((product, category, lot_id))
      return pending


  def resolve_lot_categories(config, product: str, lot_id: str) -> list[str]:
      """All categories a (product, lot_id) appears under (CP and/or FT)."""
      data_dir = config.storage.data_dir
      return [c for (p, c, l) in _scan_lots(data_dir) if p == product and l == lot_id]


  def generate_reports_for_lots(config, lots, warn=None) -> list[Path]:
      """Generate reports, isolating failures: a failed lot warns and continues."""
      written = []
      for product, category, lot_id in lots:
          try:
              written.append(generate_lot_report(config, product, category, lot_id))
          except Exception as e:  # noqa: BLE001 — report failure must not propagate
              if warn:
                  warn(f"report generation failed for {product}/{category}/{lot_id}: {e}")
      return written
  ```

- [ ] Add the `report` command to `cli.py`. Add it as a top-level `@main.command()` (insert after the `web` command, before `if __name__`):
  ```python
  @main.command()
  @click.option("--lot", "-l", "lot_id", help="Lot ID to report on")
  @click.option("--product", "-p", help="Product (required with --lot)")
  @click.option("--test-category", "-c", "test_category",
                type=click.Choice(["CP", "FT"]), help="Limit to one category")
  @click.option("--pending", is_flag=True, help="Regenerate missing/stale reports")
  @click.pass_context
  def report(ctx, lot_id, product, test_category, pending, verbose=False):
      """Generate self-contained HTML lot reports under data/reports/."""
      from .reporting.generator import (
          generate_reports_for_lots, pending_lots, resolve_lot_categories,
      )

      config: Config = ctx.obj["config"]
      config.ensure_directories()

      if pending:
          lots = pending_lots(config)
          if not lots:
              console.print("[dim]No reports pending — all up to date.[/dim]")
              return
          console.print(f"[bold]Regenerating {len(lots)} pending report(s)…[/bold]")
      elif lot_id:
          if not product:
              console.print("[red]Error:[/red] --product is required with --lot")
              sys.exit(1)
          cats = [test_category] if test_category else resolve_lot_categories(config, product, lot_id)
          if not cats:
              console.print(f"[yellow]No ingested data for {product}/{lot_id}[/yellow]")
              return
          lots = [(product, c, lot_id) for c in cats]
      else:
          console.print("[red]Error:[/red] specify --lot (with --product) or --pending")
          sys.exit(1)

      written = generate_reports_for_lots(
          config, lots, warn=lambda m: console.print(f"[yellow]![/yellow] {m}")
      )
      for path in written:
          console.print(f"[green]✓[/green] {path}")
      console.print(f"\n[green]✓[/green] Wrote {len(written)} report(s)")
  ```

- [ ] Create `src/tests/test_reporting_generator.py`:
  ```python
  """generator: writes report.html, overwrites, --pending detects stale/missing."""

  import os
  import time

  from click.testing import CliRunner

  from stdf_platform import cli
  from stdf_platform.config import Config, StorageConfig
  from stdf_platform.reporting.generator import (
      generate_lot_report, report_path, pending_lots,
  )
  from test_reporting_queries import _write_cp, _write_ft


  def _config(data_dir) -> Config:
      return Config(storage=StorageConfig(data_dir=data_dir, database=data_dir / "db.duckdb"))


  def test_generate_writes_html(tmp_path):
      _write_cp(tmp_path)
      cfg = _config(tmp_path)
      out = generate_lot_report(cfg, "PROD", "CP", "LOT1")
      assert out == report_path(tmp_path, "PROD", "CP", "LOT1")
      assert out.exists()
      assert "Yield summary" in out.read_text(encoding="utf-8")


  def test_generate_overwrites(tmp_path):
      _write_cp(tmp_path)
      cfg = _config(tmp_path)
      out = generate_lot_report(cfg, "PROD", "CP", "LOT1")
      out.write_text("STALE", encoding="utf-8")
      out2 = generate_lot_report(cfg, "PROD", "CP", "LOT1")
      assert out2 == out
      assert "STALE" not in out.read_text(encoding="utf-8")


  def test_pending_detects_missing_then_clears(tmp_path):
      _write_cp(tmp_path)
      _write_ft(tmp_path)
      cfg = _config(tmp_path)
      pend = pending_lots(cfg)
      assert ("PROD", "CP", "LOT1") in pend
      assert ("CHIP", "FT", "FT1") in pend
      for p, c, l in pend:
          generate_lot_report(cfg, p, c, l)
      # all fresh now (reports newer than parquet)
      assert pending_lots(cfg) == []


  def test_pending_detects_stale(tmp_path):
      _write_cp(tmp_path)
      cfg = _config(tmp_path)
      generate_lot_report(cfg, "PROD", "CP", "LOT1")
      assert pending_lots(cfg) == []
      # bump a parquet mtime into the future -> report is now stale
      future = time.time() + 100
      pq = next((tmp_path / "parts").glob("**/*.parquet"))
      os.utime(pq, (future, future))
      assert ("PROD", "CP", "LOT1") in pending_lots(cfg)


  def test_report_cli_lot(tmp_path, monkeypatch):
      _write_cp(tmp_path)
      monkeypatch.setattr(
          cli.Config, "load",
          classmethod(lambda c, p=None: _config(tmp_path)),
      )
      res = CliRunner().invoke(cli.main, ["report", "--lot", "LOT1", "-p", "PROD"])
      assert res.exit_code == 0, res.output
      assert report_path(tmp_path, "PROD", "CP", "LOT1").exists()
  ```

  Note: `test_report_cli_lot` makes this file 5 tests. To keep the +4 trajectory, merge it as one of the five and treat the count as +5 here? **No** — recount: this file has 5 `test_*` functions, so T5 is **+5 → 70**, not +4. Adjust the trajectory: T5 +5 → 70 (then T6 71→73, etc.). Use the corrected counts in the final tally (recomputed in Task 8).

- [ ] Run:
  ```bash
  uv run pytest src/tests/test_reporting_generator.py -q
  uv run pytest src/tests -q
  ```
  Expected: `5 passed` then `70 passed`.

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat(reporting): generator + `stdf report` CLI (--lot / --pending)

  Add reporting/generator.py (generate_lot_report, report_path with storage
  _sanitize, pending_lots staleness scan, failure-isolating batch helper) and a
  `stdf report` command supporting single-lot (--lot/-p, optional -c) and
  --pending regeneration. Reports are always overwritten under data/reports/.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 6 — Post-ingest hook (failure-isolated) in `_run_ingest_batch`

After a batch ingest, regenerate reports for the affected lots. Because `_run_ingest_batch` is the shared path for `ingest`, `ingest-all`, and `fetch`, hooking it there covers all three. **Report failures must never fail the ingest** — wrap the whole hook in a try/except that warns and continues. We re-derive affected lots from `pending_lots` (cheap: it only stats files) so we don't need the worker to return lot identities.

**Files:**
- Modify: `src/stdf_platform/cli.py` (`_run_ingest_batch`)
- Create: `src/tests/test_post_ingest_report_hook.py`

**Steps:**

- [ ] In `cli.py`, at the end of `_run_ingest_batch` (just before `return successes, failures`), add the hook:
  ```python
      # Post-ingest report regeneration. Disposable cache; a failure here must
      # never fail the ingest — warn and continue.
      if successes:
          try:
              from .reporting.generator import pending_lots, generate_reports_for_lots
              lots = pending_lots(config)
              if lots:
                  written = generate_reports_for_lots(
                      config, lots,
                      warn=lambda m: console.print(f"[yellow]![/yellow] {m}"),
                  )
                  console.print(f"[green]✓[/green] Regenerated {len(written)} report(s)")
          except Exception as e:  # noqa: BLE001 — never fail ingest on report error
              console.print(f"[yellow]![/yellow] report regeneration skipped: {e}")

      return successes, failures
  ```

- [ ] Create `src/tests/test_post_ingest_report_hook.py`. Drive the real `_run_ingest_batch` with a fake `run_ingest_pool` and a fake report generator that raises, asserting the batch still returns its successes:
  ```python
  """Post-ingest report hook must regenerate reports but never fail the ingest."""

  from pathlib import Path

  from stdf_platform import cli
  from stdf_platform.config import Config, StorageConfig
  from test_reporting_queries import _write_cp


  class _Result:
      def __init__(self, local_path, remote_path):
          self.local_path = Path(local_path)
          self.remote_path = remote_path
          self.error = None


  class _Sync:
      def mark_ingested(self, remote_path):  # no-op
          pass


  def _config(data_dir):
      return Config(storage=StorageConfig(data_dir=data_dir, database=data_dir / "db.duckdb"))


  def test_hook_generates_reports(tmp_path, monkeypatch):
      _write_cp(tmp_path)  # lot data already on disk -> pending_lots finds it
      cfg = _config(tmp_path)
      ok = [_Result(tmp_path / "f.stdf", "/r/f.stdf")]
      monkeypatch.setattr(cli, "_run_ingest_batch", cli._run_ingest_batch)  # use real
      monkeypatch.setattr(
          "stdf_platform.worker.run_ingest_pool",
          lambda **kw: (ok, []),
          raising=False,
      )
      # also patch the import target used inside _run_ingest_batch
      import stdf_platform.worker as worker
      monkeypatch.setattr(worker, "run_ingest_pool", lambda **kw: (ok, []))

      successes, failures = cli._run_ingest_batch(
          cfg, _Sync(), [(None, tmp_path / "f.stdf", "PROD", "")],
          cleanup=False, verbose=False,
      )
      assert successes == ok and failures == []
      from stdf_platform.reporting.generator import report_path
      assert report_path(tmp_path, "PROD", "CP", "LOT1").exists()


  def test_hook_failure_does_not_fail_ingest(tmp_path, monkeypatch):
      _write_cp(tmp_path)
      cfg = _config(tmp_path)
      ok = [_Result(tmp_path / "f.stdf", "/r/f.stdf")]
      import stdf_platform.worker as worker
      monkeypatch.setattr(worker, "run_ingest_pool", lambda **kw: (ok, []))
      # make report generation explode
      import stdf_platform.reporting.generator as gen
      def _boom(config):
          raise RuntimeError("disk full")
      monkeypatch.setattr(gen, "pending_lots", _boom)

      successes, failures = cli._run_ingest_batch(
          cfg, _Sync(), [(None, tmp_path / "f.stdf", "PROD", "")],
          cleanup=False, verbose=False,
      )
      # ingest still succeeds despite the report hook blowing up
      assert successes == ok and failures == []
  ```

  Note: `_run_ingest_batch` also calls `atomic_write_json` for `ingest_failures.json` — that runs fine against `tmp_path`. The hook imports `pending_lots`/`generate_reports_for_lots` from `.reporting.generator` at call time, so patching `gen.pending_lots` is observed.

- [ ] Run:
  ```bash
  uv run pytest src/tests/test_post_ingest_report_hook.py -q
  uv run pytest src/tests -q
  ```
  Expected: `2 passed` then `72 passed`.

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat(reporting): regenerate reports after ingest (failure-isolated)

  Hook _run_ingest_batch (shared by ingest/ingest-all/fetch) to regenerate
  pending lot reports after a successful batch. The whole hook is wrapped so a
  report failure warns and continues — it never fails the ingest. Tested for
  both the happy path and a raising generator.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 7 — Web: `GET /api/reports` + static serving + Reports tab

Add an API endpoint that scans `data/reports/`, mount the reports directory for direct HTML serving, and add a Reports tab to the Alpine UI that lists reports and links to them.

**Files:**
- Create: `src/stdf_platform/web/api/reports.py`
- Modify: `src/stdf_platform/web/server.py`
- Modify: `src/stdf_platform/web/static/index.html`
- Modify: `src/stdf_platform/web/static/app.js`
- Create: `src/tests/test_web_reports.py`

**Steps:**

- [ ] Create `src/stdf_platform/web/api/reports.py`:
  ```python
  """Report listing endpoint — scans data/reports/ partitions."""

  import logging
  from datetime import datetime, timezone

  from fastapi import APIRouter

  from .deps import get_data_dir

  logger = logging.getLogger(__name__)
  router = APIRouter(tags=["reports"])


  @router.get("/reports")
  def list_reports() -> list[dict]:
      """List generated reports with product/category/lot and modified time."""
      reports_dir = get_data_dir() / "reports"
      out = []
      if not reports_dir.exists():
          return out
      for report in reports_dir.glob(
          "product=*/test_category=*/lot_id=*/report.html"
      ):
          try:
              parts = {
                  p.split("=", 1)[0]: p.split("=", 1)[1]
                  for p in report.parts if "=" in p
              }
              mtime = datetime.fromtimestamp(
                  report.stat().st_mtime, tz=timezone.utc
              ).isoformat()
              rel = report.relative_to(reports_dir).as_posix()
              out.append({
                  "product": parts.get("product", ""),
                  "test_category": parts.get("test_category", ""),
                  "lot_id": parts.get("lot_id", ""),
                  "modified": mtime,
                  "url": f"/reports/{rel}",
              })
          except Exception as e:  # noqa: BLE001
              logger.warning("skipping report %s: %s", report, e)
      out.sort(key=lambda r: (r["product"], r["test_category"], r["lot_id"]))
      return out
  ```

- [ ] Wire it into `web/server.py`. Add the import + router + a `/reports` static mount (guard the mount: the dir may not exist at startup, so create it):
  ```python
  from .api.filters import router as filters_router
  from .api.data import router as data_router
  from .api.export import router as export_router
  from .api.reports import router as reports_router
  from .api.deps import get_data_dir
  ```
  After the existing `app.include_router(...)` lines add:
  ```python
  app.include_router(reports_router, prefix="/api")
  ```
  After the `/static` mount add:
  ```python
  _reports = get_data_dir() / "reports"
  _reports.mkdir(parents=True, exist_ok=True)
  app.mount("/reports", StaticFiles(directory=str(_reports), html=True), name="reports")
  ```

- [ ] Add the Reports tab to the Alpine state in `app.js`. Extend the `tabs` array (around line 19):
  ```javascript
      tabs: [
        { id: 'dashboard', label: 'Dashboard' },
        { id: 'wafer',     label: 'Wafer' },
        { id: 'tests',     label: 'Tests' },
        { id: 'reports',   label: 'Reports' },
        { id: 'export',    label: '⬇ Export' },
      ],
  ```
  Add report state next to the other tab state (e.g. after `summaryRows: [],`):
  ```javascript
      // ── Reports ─────────────────────────────────────────────────
      reports: [],
  ```
  Add a loader method (place near the other `load*` methods, e.g. after `loadDistribution`):
  ```javascript
      async loadReports() {
        this.reports = await fetch('/api/reports').then(r => r.json());
      },
  ```
  Hook it into `setTab` so visiting the tab refreshes the list. In `setTab(id)` (around line 210) add, after `this.activeTab = id;`:
  ```javascript
        if (id === 'reports') { this.loadReports(); }
  ```
  The Reports tab is **not** gated on `applied` (reports exist independent of lot selection), so it does not need selected lots.

- [ ] Add the Reports tab panel to `index.html`. Insert this block after the Tests tab `</div>` and before the Export tab `<div x-show="applied && activeTab === 'export'" ...>` (around line 327):
  ```html
          <!-- Reports tab -->
          <div x-show="activeTab === 'reports'" class="space-y-4">
            <div class="card p-4">
              <div class="flex items-center justify-between mb-3">
                <div class="text-sm font-semibold text-slate-300">Generated Reports</div>
                <button class="btn-secondary text-xs" @click="loadReports()">Refresh</button>
              </div>
              <div x-show="reports.length === 0" class="text-xs text-slate-500 italic py-4">
                No reports yet. Run <code class="bg-slate-800 px-2 py-0.5 rounded">stdf report --pending</code>
                or ingest data (reports regenerate automatically).
              </div>
              <table x-show="reports.length > 0" class="w-full text-sm">
                <thead>
                  <tr class="text-left text-slate-400 border-b border-slate-700">
                    <th class="pb-2 pr-4">Product</th>
                    <th class="pb-2 pr-4">Category</th>
                    <th class="pb-2 pr-4">Lot</th>
                    <th class="pb-2 pr-4">Modified</th>
                    <th class="pb-2">Open</th>
                  </tr>
                </thead>
                <tbody>
                  <template x-for="r in reports" :key="r.url">
                    <tr class="border-b border-slate-800 hover:bg-slate-800/50">
                      <td class="py-1.5 pr-4 text-slate-300" x-text="r.product"></td>
                      <td class="py-1.5 pr-4 text-slate-400" x-text="r.test_category"></td>
                      <td class="py-1.5 pr-4 font-mono text-indigo-300" x-text="r.lot_id"></td>
                      <td class="py-1.5 pr-4 text-slate-500 text-xs" x-text="r.modified"></td>
                      <td class="py-1.5">
                        <a :href="r.url" target="_blank" class="text-indigo-400 hover:text-indigo-300">View ↗</a>
                      </td>
                    </tr>
                  </template>
                </tbody>
              </table>
            </div>
          </div>
  ```
  Note: the "No data selected" placeholder is gated on `!applied`; since the Reports tab content is gated only on `activeTab === 'reports'`, when no lots are applied BOTH the placeholder and the reports panel could show. Prevent that by changing the placeholder's guard to also hide on the reports tab. Update the placeholder `<div x-show="!applied" ...>` (around line 174) to:
  ```html
          <div x-show="!applied && activeTab !== 'reports'" class="flex flex-col items-center justify-center h-full text-center">
  ```

- [ ] Create `src/tests/test_web_reports.py`:
  ```python
  """GET /api/reports lists generated reports; /reports/* serves the HTML."""

  import importlib


  def _write_report(data_dir):
      p = (data_dir / "reports" / "product=PROD" / "test_category=CP"
           / "lot_id=LOT1" / "report.html")
      p.parent.mkdir(parents=True, exist_ok=True)
      p.write_text("<html><body>RPT-OK</body></html>", encoding="utf-8")


  def _client(tmp_path, monkeypatch):
      monkeypatch.setenv("STDF_DATA_DIR", str(tmp_path))
      import stdf_platform.web.server as server
      importlib.reload(server)
      from fastapi.testclient import TestClient
      return TestClient(server.app)


  def test_reports_endpoint_empty(tmp_path, monkeypatch):
      with _client(tmp_path, monkeypatch) as client:
          resp = client.get("/api/reports")
          assert resp.status_code == 200
          assert resp.json() == []


  def test_reports_endpoint_lists_and_serves(tmp_path, monkeypatch):
      _write_report(tmp_path)
      with _client(tmp_path, monkeypatch) as client:
          listing = client.get("/api/reports").json()
          assert len(listing) == 1
          row = listing[0]
          assert row["product"] == "PROD"
          assert row["test_category"] == "CP"
          assert row["lot_id"] == "LOT1"
          assert row["url"] == "/reports/product=PROD/test_category=CP/lot_id=LOT1/report.html"
          served = client.get(row["url"])
          assert served.status_code == 200
          assert "RPT-OK" in served.text


  def test_reports_dir_created_on_startup(tmp_path, monkeypatch):
      with _client(tmp_path, monkeypatch):
          assert (tmp_path / "reports").exists()
  ```

- [ ] Run:
  ```bash
  uv run pytest src/tests/test_web_reports.py -q
  uv run pytest src/tests -q
  ```
  Expected: `3 passed` then `75 passed`.

- [ ] Commit:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  feat(reporting): web /api/reports endpoint + Reports tab

  Add GET /api/reports (scans data/reports partitions) and a /reports static
  mount serving the generated HTML. Add a Reports tab to the Alpine UI listing
  product/category/lot/modified with View links; the tab works without a lot
  selection. Tested for empty, populated+served, and startup dir creation.

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Task 8 — Final verification + docs note

**Files:**
- Modify: `CLAUDE.md` (document the reporting module + `stdf report`)
- Modify: `config.yaml.example` (add the `reporting:` block)

**Steps:**

- [ ] Add a `reporting:` block to `config.yaml.example` (commented, mirroring the spec):
  ```yaml
  # Report engine (stdf report / auto-regenerated after ingest)
  reporting:
    histogram_top_n: 20          # parametric histograms: top-N by fail rate
    always_include_tests: {}     # e.g. {PRODUCT_A: [1234, 5678]} — always plotted
  ```

- [ ] Update `CLAUDE.md`. Under "### Modules" add a `reporting/` bullet group after the `web/` group:
  ```markdown
    - `reporting/` — HTML report engine (Parquet → self-contained HTML)
      - `queries.py` — analysis SQL on the Phase-1 views (`views.py`)
      - `sections.py` — section builders → Plotly figures + tables (graph_objects)
      - `render.py` — Jinja2 (`templates/report.html.j2`) + embedded plotly.js
      - `generator.py` — `generate_lot_report` / `pending_lots`; writes `data/reports/...`
  ```
  Under "### Running" add:
  ```markdown
  stdf report --lot X -p PROD [-c CP|FT]   # Generate one lot's HTML report
  stdf report --pending                    # Regenerate missing/stale reports
  ```
  Under "## Key Design Decisions" add:
  ```markdown
  - **Reports are a disposable cache**: `data/reports/.../report.html` is always
    overwritten and regenerated from Parquet (the source of truth). plotly.js is
    embedded inline (~4MB) for offline viewing. Post-ingest regeneration is
    failure-isolated — a report error never fails an ingest.
  ```

- [ ] Full suite green:
  ```bash
  uv run pytest src/tests -q
  ```
  Expected: `75 passed`.

- [ ] Manual smoke (optional but recommended) — build a real report from synthetic data and confirm it is a large, self-contained HTML:
  ```bash
  uv run python -c "
  from pathlib import Path
  import tempfile, sys
  sys.path.insert(0, 'src/tests')
  from test_reporting_queries import _write_cp
  from stdf_platform.config import Config, StorageConfig
  from stdf_platform.reporting.generator import generate_lot_report
  d = Path(tempfile.mkdtemp())
  _write_cp(d)
  out = generate_lot_report(Config(storage=StorageConfig(data_dir=d, database=d/'db.duckdb')), 'PROD','CP','LOT1')
  print(out, out.stat().st_size)
  assert out.stat().st_size > 1_000_000
  "
  ```
  Expected: a path and a size > 1,000,000 (plotly.js embedded).

- [ ] `git log --oneline -8` shows the eight task commits on `main`.

- [ ] Commit docs:
  ```bash
  git add -A
  git commit -m "$(cat <<'EOF'
  docs: document the reporting engine (CLAUDE.md + config.yaml.example)

  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  EOF
  )"
  ```

---

## Corrected test-count trajectory

| Task | New tests | Cumulative |
|------|-----------|------------|
| baseline | — | 50 |
| T1 config | 3 | 53 |
| T2 queries | 6 | 59 |
| T3 sections | 5 | 64 |
| T4 render | 3 | 67 |
| T5 generator + CLI | 5 | 72 |
| T6 post-ingest hook | 2 | 74 |
| T7 web reports | 3 | 77 |
| T8 docs | 0 | **77** |

Final expected: **`77 passed`**. (The body text above shows the running totals; use this table as the authority — T1 adds 3, not 1, because `test_reporting_config.py` has three test functions.)

---

## Spec coverage map

| Spec requirement | Task | Deliverable |
|------------------|------|-------------|
| `reporting/queries.py` on views | 2 | retest-aware lot/wafer/bin/parametric/chipid SQL |
| `reporting/sections.py` (SectionResult) | 3 | header/yield/bins/wafermaps/parametric/retest/chipid builders |
| `reporting/render.py` Jinja2 + embedded plotly.js | 4 | `render_report` + `templates/report.html.j2`, `get_plotlyjs()` |
| `reporting/generator.py` → output path | 5 | `generate_lot_report` / `report_path` / `pending_lots` |
| CP sections (header…retest) | 3 | `build_sections` CP branch |
| FT (skip wafer maps, add ChipID) | 3 | `build_sections` FT branch + `chipid_section` |
| Histogram top-N + always_include_tests | 1,3 | `ReportingConfig` + `_select_histogram_tests` |
| Output layout `data/reports/product=…/…/report.html`, sanitized | 5 | `report_path` via `ParquetStorage._sanitize` |
| Always overwrite (disposable cache) | 5 | `generate_lot_report` unconditional write |
| `stdf report --lot/-p/-c` + `--pending` | 5 | `report` Click command |
| Post-ingest hook in ingest/ingest-all/fetch, failure-isolated | 6 | hook in shared `_run_ingest_batch` |
| `GET /api/reports` + serve HTML | 7 | `api/reports.py` + `/reports` static mount |
| Reports tab in Alpine UI | 7 | `index.html` panel + `app.js` state/loader |
| Tests: queries, render smoke, hook isolation | 2,4,6 | the three test files |

## Key decisions / deviations

- **Template is a packaged file** (`reporting/templates/report.html.j2`) loaded via Jinja2 `PackageLoader`, not an inline string — the hatch wheel target already ships everything under `src/stdf_platform` (same as `web/static/*`), so no `pyproject.toml` packaging change is needed.
- **One plotly.js bundle per HTML**, embedded via `plotly.offline.get_plotlyjs()`; figures are emitted as `fig.to_json()` and plotted client-side with `Plotly.newPlot`. `graph_objects` (not express) keeps each figure spec lean.
- **`pending_lots` drives both `--pending` and the post-ingest hook** — re-deriving affected lots from a file-stat scan avoids threading lot identities back from the worker pool.
- **Figure caps** (`_HIST_FIG_CAP=24`, `_WAFER_MAP_CAP=30`) keep the HTML bounded; capped wafer counts surface as a section note.
- **Reports are never gated on web lot-selection**: the Reports tab and `/api/reports` work standalone; the "No data selected" placeholder is updated to hide on the reports tab.
- **YAGNI honored**: no PDF, no email, no scheduling, no auth, no kaleido/static image export.

## Exact verification commands

```bash
uv run pytest src/tests -q                 # expected: 77 passed
git log --oneline -8                        # 8 task commits on main
# real-report smoke (size > 1MB == plotly.js embedded):
uv run python -c "import sys; sys.path.insert(0,'src/tests'); from pathlib import Path; import tempfile; from test_reporting_queries import _write_cp; from stdf_platform.config import Config, StorageConfig; from stdf_platform.reporting.generator import generate_lot_report; d=Path(tempfile.mkdtemp()); _write_cp(d); o=generate_lot_report(Config(storage=StorageConfig(data_dir=d, database=d/'db.duckdb')),'PROD','CP','LOT1'); print(o, o.stat().st_size); assert o.stat().st_size>1_000_000"
```
