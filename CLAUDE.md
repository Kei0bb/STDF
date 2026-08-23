# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`stdf` is a high-speed ETL pipeline for semiconductor test data (STDF format) → Parquet + DuckDB. CLI-first tool; no web UI. Python-only parser; parallel batch ingestion uses a ThreadPoolExecutor of isolated subprocesses (one subprocess per file).

## Commands

### Setup
```bash
uv sync                                    # Install dependencies
```

### Running
```bash
stdf ingest <file> --product PROD       # Ingest single STDF file (Parquet)
stdf ingest-all ./downloads -p PROD     # Batch ingest directory (parallel workers)
stdf fetch                              # FTP differential sync
stdf db query "SELECT ..."              # Ad-hoc DuckDB query over the views
stdf analyze yield LOT_ID               # Per-lot wafer yield (gross-die aware)
stdf serve                              # Read-only HTTP query server (multi-user)
```

### Generate test data
```bash
uv run python src/tests/make_test_stdf.py  # Generate synthetic STDF files in test_data/
```

## Architecture

### Data Flow
```
STDF file → Python parser (struct.Struct optimized) → Parquet (Hive-partitioned) → DuckDB views → CLI / analysis
```

The ingest worker runs in an **isolated subprocess** (`_ingest_worker.py`) for memory safety — parser crashes don't affect the main process. Batch ingestion uses `ThreadPoolExecutor` with configurable worker count.

### Storage Layout (Hive partitioning)
```
data/{table}/product={product}/test_category={CP|FT}/sub_process={CP1|FT2}/lot_id={lot_id}/data.parquet
data/runs/...lot_id={lot}/wafer_id={id}/retest={n}/data.parquet    # runs: one row per STDF file × wafer identity (FT: wafer_id='')
data/wafers/...lot_id={lot}/wafer_id={id}/retest={n}/data.parquet  # wafers add retest depth
```

`lots` is no longer written to Parquet at all — it is a DuckDB view derived from `runs` (see below). The `retest_num` is derived from partition depth, not stored in STDF — duplicate lot+wafer ingestions auto-increment it.

### Five Core Tables
- **runs** — MIR/MRR metadata (job_name/job_rev = test program) per (lot, wafer, retest) — one row per STDF file × wafer identity; CP: per wafer, FT: per FT-lot-run (`wafer_id=''`)
- **lots** — lot metadata, a DuckDB **view** aggregated from `runs` (one row per lot; not a Parquet table)
- **wafers** — wafer-level yield with retest tracking
- **parts** — die-level results (x/y coords, bin, pass/fail)
- **test_data** — parametric measurements from PTR/MPR/FTR records (no PK, large table)

### Modules
- `src/stdf_platform/` — core library
  - `cli.py` — Click CLI entry point
  - `parser.py` — Pure Python STDF V4 parser
  - `database.py` — DuckDB view management
  - `storage.py` — Parquet Hive-partition writer
  - `views.py` — single source for `_DEDUP_UNIT`, `setup_views(conn, data_dir, gross_die_map)`, the `lots` view (aggregated from `runs`, one row per lot), and the `wafer_yield_final` view (gross-die denominator). `test_data_final` is a plain `retest_flag = 0` filter (dedup happens at ingest time — see storage.py); `parts_final` / `chipid_final` stay `ROW_NUMBER()`-window-based (small tables, negligible cost). `setup_views()` raises `RuntimeError` if a legacy `data/lots/` directory is found (MIR moved to `data/runs/` — no migration path, wipe and re-ingest)
  - `ftp_client.py` — FTP differential sync
  - `_ingest_worker.py` — Isolated subprocess worker
  - `server/` — read-only HTTP query API (`stdf serve`). Built as an APIRouter
    (future dashboard mounts it via `include_router`); one request = one
    :memory: AnalysisSession; user SQL is single-SELECT-only with filesystem
    access locked to data_dir (`allowed_directories`). Thin VSCode client:
    `client/stdf_client.py` (requests+pandas only). See docs/multi-user-server.md
  - `analysis/` — reusable, retest-aware analysis API (returns DataFrames / plotly figures)
    - `session.py` — `AnalysisSession`: owns the DuckDB :memory: conn + views (config-resolved)
    - `compare.py` — lot-to-lot yield / bin pareto / test stats / distribution overlay
    - `trend.py` — lot & test trends ordered by MIR start_time, with mean±3σ control lines
    - `correlation.py` — CP↔FT lot & die join (via decoded ChipID origin) + test-to-test corr
    - `spatial.py` — CP wafer-plane radial zone yield / radial profile / parametric wafermap

### Analysis Package

`query.py` is a thin `# %%` cell-script wrapper over `AnalysisSession` (open in VSCode/Jupytext,
run cells with Shift+Enter). All analysis uses the `*_final` views so yield/Cpk definitions are
identical across users. `query.py` itself is gitignored (personal scratch space); the tracked
template is `query.py.example` — update the template when view definitions change, and refresh
local copies with `cp query.py.example query.py`.

### Configuration
`config.yaml` (not tracked; copy from `config.yaml.example`). Supports `${ENV_VAR}` expansion. The `--env dev` flag isolates data to `data-dev/` and skips sync history tracking.

## Key Design Decisions

- **No Rust dependency**: Pure Python parser is the only parser. Removed for simplicity.
- **Subprocess isolation**: Each ingest runs in a separate process — memory leaks or parser crashes are contained.
- **DuckDB :memory:**: All analysis (CLI, `query.py`, `AnalysisSession`) uses `:memory:` connections with `setup_views()` — Parquet is the source of truth.
- **pandas pivot for CSV export**: DuckDB dynamic PIVOT cannot be combined with `?` parameters. pandas `pivot_table()` is used instead after fetching long-format data.
- **FTP deduplication**: `sync_history.json` tracks ingested files. `--env dev` bypasses this.
- **Corrupt downloads are quarantined, never fatal**: a `.gz` whose stream is broken *at the source* cannot be recovered by re-downloading. `download_file()` raises `CorruptDownloadError` (deleting the half-written `.stdf` first — otherwise `ingest-all` would silently ingest a truncated file), and `_download_files()` moves the `.gz` to `downloads/_corrupt/` and records it under `sync_history.json`'s `corrupt` key so later runs skip it. `fetch` continues with the remaining files and exits 1 so the nightly task surfaces the problem. `stdf fetch --retry-corrupt` clears the skip once the source has re-exported the file. This replaced a bare download loop where one bad file aborted the entire run *and* was never recorded — so it was retried first on every run, blocking every file behind it (2026-08-07: 135 files stalled for 10 days). Transient errors (network/disk) are reported separately and stay retryable. `scripts/diag_fetch_gz.py` diagnoses such a file: it separates source corruption from transfer corruption, walks every gzip member, and reports how much of the payload is reachable.
- **Gzip auto-detection**: Files matching `*.stdf.gz`, `*.std.gz` are decompressed to a temp path before parsing.
- **Windows path safety**: Partition values are sanitized to remove characters invalid on Windows filesystems.
- **Product detection**: Use `--from-path` to infer product/test_type from FTP path structure `{...}/{PRODUCT}/{CP|FT}/...`.
- **Gross die at query time (CP only)**: `config.yaml` `products.<P>.gross_die` sets the per-wafer mask total. It is applied at query time via the `wafer_yield_final` view (`total = max(probed, GD)`), never written to Parquet. This is robust to retests and partial/aborted probes — dies probed across multiple runs dedup by `(wafer, x, y)`, and only genuinely-unprobed dies (`GD − probed`) inflate the denominator. Those unprobed dies show up in bin distributions under `gd_fail_bin`. GD never applies to FT (`wafer_id=''`) or to spatial/radial-zone analysis (unprobed dies have no coordinate).
- **Retest dedup at ingest time (test_data)**: `storage.py` writes `retest_flag` (per-key recency rank; 0 = newest run containing that key) and `exec_seq` (0-based occurrence order of a key within one run, e.g. for the 512 PTRs of an OTP dump under one test_num) on every test_data row, and demotes older retest files' flags when a die/test key is re-measured. `views.py`'s `test_data_final` is then just `WHERE retest_flag = 0` — a predicate DuckDB can push into the Parquet scan, unlike the old `ROW_NUMBER() OVER (PARTITION BY ...)` window, which blocked pushdown of any filter on a non-PARTITION-BY column (e.g. lot-scoped `test_name LIKE` queries took 12+ minutes on the real store). This also fixed a correctness issue: the window kept exactly one arbitrary row per (die, test, pin), silently discarding loop measurements and potentially hiding a failing iteration; the flag-based view keeps every row of the winning run. `stdf db verify-flags [--lot LOT_ID]` checks the invariants (no NULL flags, every key's newest run flagged 0, every key's rows within one run sharing a flag) and exits 1 on violation. Rows with a NULL `retest_flag` (pre-flag files) are excluded by `test_data_final` — such a store must be re-ingested, there is no migration path.
- **Per-run MIR in `runs`; `lots` is a derived view**: MIR metadata (`job_name`/`job_rev` = test program, plus `part_type`/`tester_type`/`operator`/`start_time`/`finish_time`) used to be written only to `lots`, one Parquet file per lot that every subsequent file of that lot **overwrote** (`save_stdf_data` writes a single `data/lots/.../lot_id={lot}/data.parquet`). That made a test-program change partway through a lot invisible, and left `lots.job_name`/`start_time` as an ingest-order-dependent, not time-dependent, value — the measurement tables (`wafers`/`parts`/`test_data`/`chipid`) already kept full per-`(lot, wafer, retest)` granularity, but MIR did not. `runs` fixes this by storing one row per STDF file × wafer identity (CP: per wafer; FT: per FT-lot-run, `wafer_id=''`), keyed the same way as `parts`/`test_data` (same `retest_num`, from the same `wafer_retest_map`) so it joins cleanly. `lots` is no longer written to Parquet at all — it's a `views.py` `CREATE VIEW` that aggregates `runs` per lot (`MIN(start_time)`/`MAX(finish_time)`, `arg_max(..., start_time)` for job_name/job_rev/part_type/tester_type/operator, i.e. the latest run's value), plus two new columns `job_variant_count` and `job_mixed` (`COUNT(DISTINCT (job_name, job_rev))` and `> 1`) so callers can detect TP-mixed lots without scanning `runs` themselves. This keeps `lots`'s existing column order and one-row-per-lot contract, so `analysis/trend.py`, `analysis/session.py`'s `lots()`, and `views.py`'s `lot_product` all keep working unmodified; `database.get_lot_summary` was extended only to surface the two new columns, which `stdf db lots` renders as a `⚠×N` marker on the Job cell (`stdf db programs [--lot]` and `AnalysisSession.runs()` show the per-run breakdown behind it). `wafers.test_rev`/`source_file` were dropped as part of this — a repo-wide grep found no reader of either column (write-only dead columns); the same data now lives in `runs.test_rev`/`source_file`. No migration path: `setup_views()` raises `RuntimeError` if it finds a legacy `data/lots/` directory, since a half-migrated store (old `lots` Parquet alongside new `runs`) can't be reconciled — wipe and re-ingest, same policy as the `retest_flag` introduction.
