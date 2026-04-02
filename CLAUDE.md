# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`stdf2pq` is a high-speed ETL pipeline for semiconductor test data (STDF format) → Parquet + DuckDB + PostgreSQL. It supports CLI usage, a Streamlit web UI, and Dagster orchestration. The Rust parser (via PyO3) provides 10–30x speedup over the Python fallback.

## Commands

### Setup
```bash
uv sync                                                        # Install dependencies
uv pip install "dagster>=1.9.0" "dagster-webserver>=1.9.0" "psycopg2-binary>=2.9.0"  # Optional Dagster deps
```

### Building the Rust Parser (optional)
```bash
cd rust
uv run maturin develop --features python --release
```

### Running
```bash
stdf2pq ingest <file>          # Ingest STDF file (auto-uses Rust parser if built)
stdf2pq fetch                   # FTP differential sync
stdf2pq web                     # Streamlit UI at http://localhost:8501
uv run streamlit run src/stdf_platform/app.py  # Alternative web UI launch

docker compose up -d            # Start PostgreSQL (required for postgres_sync)
uv run dagster dev -m stdf_dagster  # Dagster UI at http://localhost:3000
```

### Testing
```bash
uv run python test_ingest.py    # Integration test
uv run python test_timeout.py   # Timeout/subprocess test
```

## Architecture

### Data Flow
```
STDF file → Rust parser (PyO3) or Python fallback → Parquet (Hive-partitioned) → DuckDB views → PostgreSQL
```

The ingest worker runs in an **isolated subprocess** (`_ingest_worker.py`) for memory safety — parser crashes don't affect the main process.

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
- `src/stdf_platform/` — core library: CLI (`cli.py`), Python parser (`parser.py`), DuckDB (`database.py`), Parquet storage (`storage.py`), FTP (`ftp_client.py`), Streamlit UI (`app.py`)
- `src/stdf_dagster/` — Dagster orchestration: assets in `assets/`, resources in `resources/`, sensor polling FTP every 3h, daily refresh schedule at 6:00 UTC
- `rust/` — Rust binary parser with PyO3 bindings; `python.rs` exposes the interface
- `src/stdf2parquet/` — legacy module, kept for compatibility

### Dagster Asset Graph
```
raw_stdf_files → stdf_parquet_tables → duckdb_views → yield_summary / bin_distribution / test_fail_ranking
                                                    └→ postgres_sync (DuckDB → PostgreSQL bulk upsert)
```

Jobs: `full_pipeline`, `ingestion_only`, `refresh_views`. Sensor and schedule are OFF by default — enable in Dagster UI.

### Configuration
`config.yaml` (not tracked; copy from `config.yaml.example`). Supports `${ENV_VAR}` expansion. The `--env dev` flag isolates data to `data-dev/` and skips sync history tracking.

### PostgreSQL Multi-User Access
- `stdf` user: read/write (Dagster pipeline)
- `stdf_reader` user: read-only (JMP, Excel, BI tools)
- Schema initialized from `docker/postgres/init.sql`
- Indexes optimized for JMP query patterns on `test_data`

## Key Design Decisions

- **Rust parser is optional**: Python parser is always the fallback. Check availability via `try: import stdf2pq_rs`.
- **FTP deduplication**: `sync_history.json` tracks ingested files. `--env dev` bypasses this.
- **Gzip auto-detection**: Files matching `*.stdf.gz`, `*.std.gz` are decompressed to a temp path before parsing.
- **Windows path safety**: Partition values are sanitized to remove characters invalid on Windows filesystems.
- **Product detection**: Use `--from-path` to infer product/test_type from FTP path structure `{...}/{PRODUCT}/{CP|FT}/...`.
