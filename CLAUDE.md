# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`stdf` is a high-speed ETL pipeline for semiconductor test data (STDF format) → Parquet + DuckDB. It supports CLI usage and a FastAPI + Alpine.js web UI. Python-only parser; parallel batch ingestion uses a ThreadPoolExecutor of isolated subprocesses (one subprocess per file).

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
stdf web                                # Web UI at http://localhost:8000
stdf report --lot X -p PROD [-c CP|FT]   # Generate one lot's HTML report
stdf report --pending                    # Regenerate missing/stale reports
```

### Generate test data
```bash
uv run python make_test_stdf.py            # Generate synthetic STDF files in test_data/
```

## Architecture

### Data Flow
```
STDF file → Python parser (struct.Struct optimized) → Parquet (Hive-partitioned) → DuckDB views → Web UI / CLI
```

The ingest worker runs in an **isolated subprocess** (`_ingest_worker.py`) for memory safety — parser crashes don't affect the main process. Batch ingestion uses `ThreadPoolExecutor` with configurable worker count.

### Storage Layout (Hive partitioning)
```
data/{table}/product={product}/test_category={CP|FT}/sub_process={CP1|FT2}/lot_id={lot_id}/data.parquet
data/wafers/...lot_id={lot}/wafer_id={id}/retest={n}/data.parquet  # wafers add retest depth
```

The `retest_num` is derived from partition depth, not stored in STDF — duplicate lot+wafer ingestions auto-increment it.

### Four Core Tables
- **lots** — lot metadata from MIR (Master Information Record)
- **wafers** — wafer-level yield with retest tracking
- **parts** — die-level results (x/y coords, bin, pass/fail)
- **test_data** — parametric measurements from PTR/MPR/FTR records (no PK, large table)

### Modules
- `src/stdf_platform/` — core library
  - `cli.py` — Click CLI entry point
  - `parser.py` — Pure Python STDF V4 parser
  - `database.py` — DuckDB view management
  - `storage.py` — Parquet Hive-partition writer
  - `ftp_client.py` — FTP differential sync
  - `_ingest_worker.py` — Isolated subprocess worker
  - `web/` — FastAPI web app
    - `server.py` — FastAPI app + router registration
    - `api/filters.py` — products, categories, lots, wafers endpoints
    - `api/data.py` — summary, wafermap, tests, distribution endpoints
    - `api/export.py` — CSV export (pivot & long format)
    - `api/deps.py` — per-request `:memory:` DuckDB connection
    - `static/` — Alpine.js SPA (index.html, app.js, plots.js)
  - `reporting/` — HTML report engine (Parquet → self-contained HTML)
    - `queries.py` — analysis SQL on the Phase-1 views (`views.py`)
    - `sections.py` — section builders → Plotly figures + tables (graph_objects)
    - `render.py` — Jinja2 (`templates/report.html.j2`) + embedded plotly.js
    - `generator.py` — `generate_lot_report` / `pending_lots`; writes `data/reports/...`
  - `analysis/` — reusable, retest-aware analysis API (returns DataFrames / plotly figures)
    - `session.py` — `AnalysisSession`: owns the DuckDB :memory: conn + views (config-resolved)
    - `compare.py` — lot-to-lot yield / bin pareto / test stats / distribution overlay
    - `trend.py` — lot & test trends ordered by MIR start_time, with mean±3σ control lines
    - `correlation.py` — CP↔FT lot & die join (via decoded ChipID origin) + test-to-test corr
    - `spatial.py` — CP wafer-plane radial zone yield / radial profile / parametric wafermap

### Analysis Package & Cell-Script Templates

`templates/analysis/*.py` are `# %%` cell-scripts (VSCode/Jupytext) over `AnalysisSession`;
`query.py` is a thin wrapper over the same session. All analysis uses the `*_final` views so
yield/Cpk definitions are identical across users.

### Web UI DuckDB Connection Pattern
FastAPI server creates one shared DuckDB `:memory:` connection at startup (via `lifespan`). All API requests reuse this single connection — Parquet is the source of truth. CLI tools (`stdf db`) and `query.py` also use DuckDB `:memory:`.

### Configuration
`config.yaml` (not tracked; copy from `config.yaml.example`). Supports `${ENV_VAR}` expansion. The `--env dev` flag isolates data to `data-dev/` and skips sync history tracking.

## Key Design Decisions

- **No Rust dependency**: Pure Python parser is the only parser. Removed for simplicity.
- **Subprocess isolation**: Each ingest runs in a separate process — memory leaks or parser crashes are contained.
- **DuckDB :memory: for web**: Avoids file locking; Parquet is the source of truth.
- **pandas pivot for CSV export**: DuckDB dynamic PIVOT cannot be combined with `?` parameters. pandas `pivot_table()` is used instead after fetching long-format data.
- **FTP deduplication**: `sync_history.json` tracks ingested files. `--env dev` bypasses this.
- **Gzip auto-detection**: Files matching `*.stdf.gz`, `*.std.gz` are decompressed to a temp path before parsing.
- **Windows path safety**: Partition values are sanitized to remove characters invalid on Windows filesystems.
- **Product detection**: Use `--from-path` to infer product/test_type from FTP path structure `{...}/{PRODUCT}/{CP|FT}/...`.
- **Reports are a disposable cache**: `data/reports/.../report.html` is always
  overwritten and regenerated from Parquet (the source of truth). plotly.js is
  embedded inline (~4MB) for offline viewing. Post-ingest regeneration is
  failure-isolated — a report error never fails an ingest.
