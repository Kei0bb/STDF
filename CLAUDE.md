# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`stdf` is a high-speed ETL pipeline for semiconductor test data (STDF format) → Parquet + DuckDB + PostgreSQL. It supports CLI usage and a FastAPI + Alpine.js web UI. Python-only parser with ProcessPoolExecutor for parallel batch ingestion.

## Commands

### Setup
```bash
uv sync                                    # Install dependencies
uv pip install "psycopg2-binary>=2.9.0"   # Optional: PostgreSQL sync
```

### Running
```bash
docker compose up -d                       # Start PostgreSQL

stdf ingest <file> --product PROD       # Ingest single STDF file (Parquet)
stdf ingest-all ./downloads -p PROD     # Batch ingest directory (parallel workers)
stdf fetch                              # FTP differential sync
stdf web                                # Web UI at http://localhost:8000
```

### Generate test data
```bash
uv run python make_test_stdf.py            # Generate synthetic STDF files in test_data/
```

## Architecture

### Data Flow
```
STDF file → Python parser (struct.Struct optimized) → Parquet (Hive-partitioned) → DuckDB views → Web UI / PostgreSQL
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

### Web UI DuckDB Connection Pattern
FastAPI server creates one shared DuckDB `:memory:` connection at startup (via `lifespan`). All API requests reuse this single connection — Parquet is the source of truth. CLI tools (`stdf db`) and `query.py` also use DuckDB `:memory:`.

### Configuration
`config.yaml` (not tracked; copy from `config.yaml.example`). Supports `${ENV_VAR}` expansion. The `--env dev` flag isolates data to `data-dev/` and skips sync history tracking.

### PostgreSQL Multi-User Access (optional)
- `stdf` user: read/write
- `stdf_reader` user: read-only (JMP, Excel, BI tools)
- Schema initialized from `docker/postgres/init.sql`
- Indexes optimized for JMP query patterns on `test_data`

## Key Design Decisions

- **No Rust dependency**: Pure Python parser is the only parser. Removed for simplicity.
- **Subprocess isolation**: Each ingest runs in a separate process — memory leaks or parser crashes are contained.
- **DuckDB :memory: for web**: Avoids file locking; Parquet is the source of truth.
- **pandas pivot for CSV export**: DuckDB dynamic PIVOT cannot be combined with `?` parameters. pandas `pivot_table()` is used instead after fetching long-format data.
- **FTP deduplication**: `sync_history.json` tracks ingested files. `--env dev` bypasses this.
- **Gzip auto-detection**: Files matching `*.stdf.gz`, `*.std.gz` are decompressed to a temp path before parsing.
- **Windows path safety**: Partition values are sanitized to remove characters invalid on Windows filesystems.
- **Product detection**: Use `--from-path` to infer product/test_type from FTP path structure `{...}/{PRODUCT}/{CP|FT}/...`.
