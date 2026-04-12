# Architecture Design: DuckDB Singleton (POC向けリアーキテクチャ)

**Date:** 2026-04-12  
**Status:** Approved  
**Context:** Windows Local POC — Docker なし、ClickHouse 不使用

---

## 背景・目的

現状の ClickHouse 統合は WSL2 / Windows 環境で不安定（接続タイムアウト）であり、Docker が必要なため POC 環境での導入障壁が高い。

POC の優先目標:
- **A. Ingest 速度の実証**（ETL パイプライン検証）
- **C. CSV エクスポート**（JMP / Excel 連携）
- **D. SQL クエリ**（query.py による任意分析）

Web UI ダッシュボード（B）は優先度低。

---

## アーキテクチャ概要

```
STDF ファイル
     ↓
_ingest_worker (subprocess isolated)
     ↓ Parquet のみ書き込み（ClickHouse なし）
data/{table}/product=.../test_category=.../lot_id=.../data.parquet
     ↓ DuckDB glob ビュー（新データを自動検出）
DuckDB シングルトン接続（サーバー起動時に1回作成）
+ asyncio.Lock（並列リクエストを直列化）
     ↓
FastAPI endpoints → Web UI / CSV Export
     ↓
query.py（DuckDB デフォルト、CH はオプション）
```

**設計の核心:**
1. Ingest は Parquet のみ書き込む（CH 書き込みを除去）
2. Web API は起動時に DuckDB シングルトン接続を作成し、glob ビューを1回登録
3. ClickHouse は将来のサーバー移行用として設定・コードを保持するが、デフォルト無効

---

## Section 1: Ingest パイプライン

### 変更ファイル

**`src/stdf_platform/_ingest_worker.py`**
- CH 書き込みブロック（`sys.argv[5]` 以降）を削除
- `storage.save_stdf_data()` の戻り値を `counts` のみに戻す（`pa_tables` 不要）
- stdout JSON に `{"ok": true, ...}` を出力する構造は維持

**`src/stdf_platform/storage.py`**
- `save_stdf_data()` の戻り値を `dict[str, int]` に戻す（`tuple[dict, dict[str, pa.Table]]` から変更）
- `pa_tables` 収集ロジックを削除（メモリ効率の改善）

**`src/stdf_platform/worker.py`**
- `ch_host` パラメーターを削除
- `_run_single()` / `run_ingest_pool()` のシグネチャをクリーンアップ

**`src/stdf_platform/cli.py`**
- `_run_ingest_batch()` の `ch_host=config.clickhouse.host` 渡しを削除
- `web` コマンドの `STDF_CH_*` 環境変数セットを削除

### 変更しないファイル
- `src/stdf_platform/ch_writer.py` — 将来のサーバー移行用として保持
- `src/stdf_platform/ingest_history.py` — resume 機能はそのまま維持

---

## Section 2: Web API (DuckDB シングルトン)

### `src/stdf_platform/web/server.py`

lifespan を DuckDB シングルトンに変更:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio, duckdb
    con = duckdb.connect(":memory:")
    for table in ["lots", "wafers", "parts", "test_data"]:
        path = data_dir / table
        if path.exists():
            con.execute(f"""
                CREATE OR REPLACE VIEW {table} AS
                SELECT * FROM read_parquet('{path}/**/*.parquet',
                hive_partitioning=true)
            """)
    app.state.db = con
    app.state.db_lock = asyncio.Lock()
    yield
    con.close()
```

**DuckDB glob ビューの特性:** クエリのたびにファイルシステムをスキャンするため、ingest 後に再起動不要で新データが即座に見える。

### `src/stdf_platform/web/api/deps.py`

```python
def get_db(request: Request):
    return request.app.state.db, request.app.state.db_lock

async def run_query(db, lock, sql, params=None):
    async with lock:
        if params:
            return db.execute(sql, params).fetchdf()
        return db.execute(sql).fetchdf()
```

- `get_ch()` を削除
- asyncio.Lock により並列リクエストを直列化（POC では十分）

### `src/stdf_platform/web/api/filters.py` / `data.py` / `export.py`

ClickHouse 固有構文 → DuckDB/標準 SQL に移行:

| ClickHouse | DuckDB |
|---|---|
| `countIf(passed = 'F')` | `SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)` |
| `countDistinct(x)` | `COUNT(DISTINCT x)` |
| `nullIf(x, 0)` | `NULLIF(x, 0)` |
| `any(x)` | `FIRST(x)` |
| `{name:Array(String)}` | `= ANY(?)` (Python list) |
| `.named_results()` | `.fetchdf()` / `.fetchall()` |
| `ch.query_df(sql, parameters=p)` | `db.execute(sql, params).fetchdf()` |

Array パラメーター: `WHERE lot_id = ANY(?)` に Python list を渡す。

---

## Section 3: query.py

- DuckDB 接続を先に確立し、デフォルトで使用
- `config.yaml` の `clickhouse.host` が空でない場合のみ CH 接続を試みる
- `USE_CH = False` がデフォルト

---

## Section 4: 設定・依存関係

**`config.yaml.example`**
```yaml
clickhouse:
  host: ""   # 空 = 無効。Linuxサーバー運用時は localhost に変更
```

**`pyproject.toml`**
- `clickhouse-connect` を optional dependency に降格:
```toml
[project.optional-dependencies]
clickhouse = ["clickhouse-connect>=0.7.0"]
```

**`docker-compose.yml`**
- ClickHouse サービスは残す（将来のサーバー移行用）
- デフォルトでは起動しない設定にする（`profiles` 使用）

---

## 変更サマリー

| ファイル | 変更内容 |
|---|---|
| `_ingest_worker.py` | CH 書き込み削除、戻り値シンプル化 |
| `storage.py` | `pa_tables` 収集削除、戻り値を `dict[str, int]` に |
| `worker.py` | `ch_host` 引数削除 |
| `cli.py` | `ch_host` 渡し削除、CH 環境変数セット削除 |
| `web/server.py` | lifespan を DuckDB シングルトンに変更 |
| `web/api/deps.py` | `get_ch` 削除、`get_db` + asyncio.Lock 統一 |
| `web/api/filters.py` | CH SQL → DuckDB SQL |
| `web/api/data.py` | CH SQL → DuckDB SQL |
| `web/api/export.py` | CH SQL → DuckDB SQL |
| `query.py` | DuckDB デフォルト化 |
| `config.yaml.example` | CH host を空に |
| `pyproject.toml` | `clickhouse-connect` を optional に |

## 変更しないファイル

| ファイル | 理由 |
|---|---|
| `ch_writer.py` | 将来のサーバー移行用として保持 |
| `ingest_history.py` | resume 機能はそのまま |
| `parser.py` | 変更なし |
| `docker-compose.yml` | CH サービスは profiles で残す |

---

## 将来の拡張パス

**Linux サーバー移行時:**
1. `config.yaml` の `clickhouse.host: localhost` に変更
2. `docker compose up -d clickhouse`
3. `uv pip install "stdf2pq[clickhouse]"`
4. ingest の CH 書き込みを再有効化（`ch_writer.py` は保持済み）

---

## 非機能要件

- **Windows ネイティブ動作**: Docker 不要、`uv sync` のみでセットアップ完了
- **並列 ingest**: ThreadPoolExecutor + subprocess isolation は変更なし
- **resume 機能**: `data/ingest_history.json` による中断再開は維持
- **POC スコープ**: 同時接続数が少ない前提（asyncio.Lock で直列化）
