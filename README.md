# stdf

半導体テストデータ（STDF V4）を **Parquet + DuckDB** に変換する高速 ETL パイプライン。  
Pure Python パーサー × ThreadPoolExecutor による並列処理で、1000 ファイル以上のバッチ取り込みに対応。  
FastAPI + Alpine.js + Plotly.js の Web UI でデータを分析・CSV エクスポート。  
**Docker 不要 — `uv sync` のみでセットアップ完了。**

---

## クイックスタート

```bash
# 1. 依存インストール
uv sync

# 2. config.yaml を設定（example をコピー）
cp config.yaml.example config.yaml

# 3. データ取り込み
stdf ingest-all ./downloads -p YOUR_PRODUCT

# 4. Web UI 起動
stdf web   # → http://localhost:8000
```

---

## アーキテクチャ

### 全体データフロー

```
FTP サーバー
     ↓ ftp_client.py + sync_manager.py
downloads/
     ↓ worker.py (ThreadPoolExecutor)
     ↓   ├── Thread 1 → subprocess(_ingest_worker) ─┐
     ↓   ├── Thread 2 → subprocess(_ingest_worker) ─┤→ Parquet (Hive パーティション)
     ↓   └── Thread N → subprocess(_ingest_worker) ─┘     data/{table}/product={P}/
     ↓                    ↑                                 test_category={CP|FT}/
     ↓               parser.py                              sub_process={CP11|FT2}/
     ↓               storage.py                             lot_id={L}/
     ↓                                                      data.parquet
     ↓
DuckDB glob ビュー（起動時1回登録、クエリごとに fs スキャン）
     ↓
     ├── FastAPI Web UI  (http://localhost:8000)
     ├── CSV エクスポート
     └── query.py  (VS Code インタラクティブ)
```

---

### モジュール構成

#### Ingest パイプライン

| モジュール | 役割 |
|---|---|
| `cli.py` | Click CLI — `ingest` / `ingest-all` / `fetch` / `db` / `analyze` / `web` コマンド |
| `worker.py` | `ThreadPoolExecutor` でファイルごとに subprocess を起動・タイムアウト管理 |
| `_ingest_worker.py` | 独立 subprocess — 1ファイルを parse → Parquet 書き込みして JSON を stdout に出力 |
| `parser.py` | Pure Python STDF V4 パーサー（`struct.Struct` 最適化、FAR/MIR/WIR/PIR/PRR/PTR/MPR/FTR/PMR対応） |
| `storage.py` | PyArrow で Hive パーティション Parquet に書き込み、リテスト番号の自動採番 |
| `ftp_client.py` | `ftplib` FTP 差分ダウンロード（`.stdf.gz` 自動展開） |
| `sync_manager.py` | `sync_history.json` で FTP 取得済み・ingest 済みを追跡 |
| `ingest_history.py` | `ingest_history.json` でローカル ingest 済みファイルを追跡（`ingest-all` の再開用） |

> **Subprocess 分離の理由:** パーサーがクラッシュしても他のワーカーに影響しない。プロセスごとにメモリが解放されるため、1000+ ファイルのバッチ処理でもメモリリークが蓄積しない。

#### ストレージ層

| モジュール | 役割 |
|---|---|
| `storage.py` | Parquet Hive パーティション書き込み（5テーブル） |
| `database.py` | CLI・`query.py` 用 DuckDB コンテキストマネージャー（`query()` / `query_df()` 等ヘルパー付き） |
| `config.py` | `config.yaml` 読み込み（FTP / Storage 設定、`${ENV_VAR}` 展開対応） |

#### Web API 層

| モジュール | 役割 |
|---|---|
| `web/server.py` | FastAPI app + lifespan（DuckDB シングルトン起動） |
| `web/api/deps.py` | `get_db()` 依存関数、`setup_views()` でParquet glob ビュー登録 |
| `web/api/filters.py` | `/api/products` `/api/lots` `/api/wafers` — UI フィルター用 |
| `web/api/data.py` | `/api/summary` `/api/wafer-yield` `/api/wafermap` `/api/tests` `/api/fails` `/api/distribution` |
| `web/api/export.py` | `/api/export/csv` (Pivot / Long 形式) `/api/export/preview` |
| `web/static/` | Alpine.js + Tailwind CSS SPA（`app.js` + `plots.js` + Plotly.js） |

---

### バッチ処理の仕組み

```
N ファイル
     ↓
ThreadPoolExecutor (max_workers=N)
├── Thread 1 → subprocess(_ingest_worker) → Parquet
├── Thread 2 → subprocess(_ingest_worker) → Parquet
└── Thread N → subprocess(_ingest_worker) → Parquet
```

- 各 subprocess はメモリ分離（クラッシュしても他のワーカーに影響しない）
- 成功済みファイルを `data/ingest_history.json` に記録 → 中断後の再実行で自動スキップ
- タイムアウト超過時は SIGKILL → 次ファイルへ継続

---

### Web API の DuckDB シングルトン

FastAPI 起動時に DuckDB `:memory:` 接続を1つ作成し、`threading.Lock` で並列リクエストを直列化。  
glob ビュー (`read_parquet('data/**/*.parquet', hive_partitioning=true)`) はクエリのたびにファイルシステムをスキャンするため、**ingest 後にサーバーを再起動せずとも新データが即座に見える。**

> **`threading.Lock` を使う理由:** FastAPI の同期ルートハンドラは `asyncio` の `ThreadPoolExecutor` 上で動作するため、`asyncio.Lock` はスレッドから取得できない。将来 `async def` ハンドラに移行する場合は `asyncio.Lock` + `asyncio.to_thread()` パターンへの変更が必要。

**起動ログ例:**
```
[server] data_dir: /home/user/stdf/data
[server] DuckDB views registered: lots, wafers, parts, test_data, chipid
```
登録されたテーブル名が表示されない場合は `data_dir` のパスを確認してください。

---

## インストール

```bash
uv sync
```

---

## 使用方法

### データ取り込み

```bash
# 単ファイル
stdf ingest sample.stdf --product SCT101A

# パスから product 自動推定（.../SCT101A/CP/... 構造）
stdf ingest ./downloads/SCT101A/CP/lot001.stdf --from-path

# ディレクトリ一括（推奨）— 中断後の再実行は自動で続きから
stdf ingest-all ./downloads -p SCT101A
stdf ingest-all ./downloads -p SCT101A --workers 8 --timeout 600
stdf ingest-all ./downloads -p SCT101A --force   # 全ファイル強制再取り込み
```

### Web UI

```bash
stdf web   # http://localhost:8000
```

| タブ | 機能 |
|---|---|
| **Dashboard** | ロット別歩留まりバーチャート、KPI（総部品数 / 良品数 / 平均歩留まり） |
| **Wafer** | ウェハー別歩留まり + ウェハーマップ（Pass/Fail or Bin 色分け） |
| **Tests** | テストパラメーター一覧（AG Grid）+ 分布ヒストグラム + フェール Pareto |
| **Export** | CSV ダウンロード（Pivot形式 / Long形式）、lot・wafer・test_num フィルター |

### SQL クエリ（VS Code）

VS Code で `query.py` を開き、各セル (`# %%`) を Shift+Enter で実行（DuckDB）。
クエリ集は `docs/sample_queries.md` を参照。

```python
q("SELECT * FROM lots ORDER BY start_time DESC")   # プレビュー（既定 LIMIT 100）

# 大量データで *_final が遅いとき: 対象ロットを materialize → 以降 ~45x 高速
use_lot("E6A773.00")
q("SELECT * FROM test_data_final WHERE lot_id = 'E6A773.00'", limit=0)
use_all()                                          # 全ロットビューに復元

# 全件を高速に取り込む / ファイル出力
tbl = q("SELECT * FROM test_data_final", limit=0, as_arrow=True)  # pyarrow.Table（~3.5x）
to_csv("SELECT * FROM test_data_final WHERE lot_id = 'E6A773.00'")  # DuckDB COPY（メモリ0）
```

> `*_final` ビューはリテスト重複排除を毎クエリ計算するため巨大データでは重い。
> 1 ロット単位の作業では `use_lot()` でそのロットを materialize すると大幅に速くなる。

### 分析 API（`stdf_platform.analysis`）

`AnalysisSession` を通じてリテスト重複排除済みビューにアクセスし、DataFrame / Plotly figure を返す再利用可能な分析関数群。

```python
from stdf_platform.analysis import AnalysisSession, yield_by_lot, lot_trend, cp_ft_yield, zone_yield, trend_fig

s = AnalysisSession()                              # config.yaml を自動読み込み

# ロット間比較
df = yield_by_lot(s, product="SCT101A", lot_ids=["L001", "L002"], test_category="CP")

# トレンド（MIR start_time 順、mean±3σ コントロールライン付き）
fig = trend_fig(lot_trend(s, product="SCT101A", test_category="CP"))

# CP↔FT 歩留まり相関
df = cp_ft_yield(s, product="SCT101A")

# CP ウェハー面内ゾーン別歩留まり（n_zones=3 で Center / Middle / Edge）
df = zone_yield(s, product="SCT101A", lot_id="L001")
```

すべての関数は `*_final` ビューを使用するため、歩留まり・Cpk の定義がレポートや Web UI と完全に一致します。  
`templates/analysis/*.py` は `# %%` セル形式（VS Code / Jupytext）で、Shift+Enter で逐次実行可能。

### CLI クエリ

```bash
stdf db lots                          # ロット一覧
stdf db query "SELECT * FROM wafers"  # SQL 直接実行
stdf db shell                         # DuckDB シェル
```

### 分析コマンド

```bash
stdf analyze yield LOT001       # Wafer 歩留まり
stdf analyze test-fail LOT001   # フェールテスト上位
stdf analyze bins LOT001        # Bin 分布
```

### FTP 差分同期

```bash
stdf fetch                   # config.yaml に従って差分取得
stdf fetch -p SCT101A        # 製品指定
stdf fetch --force           # 強制再ダウンロード
stdf fetch --no-ingest       # ダウンロードのみ
stdf fetch --reingest        # DL済み未 ingest を再試行（FTP接続なし）
```

### 定期実行（Windows 11 — Task Scheduler）

毎日朝 6:00 に `stdf fetch` を自動実行するスクリプトを同梱しています。

```cmd
REM セットアップ（管理者権限で1回だけ実行）
scripts\register_task.bat

REM 動作テスト（即時実行）
schtasks /Run /TN STDF_DailyFetch

REM ログ確認
type logs\fetch_*.log

REM 登録解除
scripts\unregister_task.bat
```

| ファイル | 役割 |
|---------|------|
| `scripts/daily_fetch.ps1` | メインスクリプト — `uv run stdf fetch --verbose` 実行、`logs/fetch_YYYYMMDD_HHMMSS.log` に記録、30日超のログを自動削除 |
| `scripts/register_task.bat` | Task Scheduler にタスク登録（毎日 06:00 トリガー） |
| `scripts/unregister_task.bat` | タスク登録解除 |

---

## データ構造

### 5 テーブル

| テーブル | 説明 |
|---------|------|
| `lots` | ロット情報（product, test_category, sub_process） |
| `wafers` | ウェハー歩留まり（リテスト追跡含む）。**CP 専用**（FT は WIR/WRR が無く生成されない） |
| `parts` | 個片結果（Bin, X/Y 座標）。CP=ダイ / FT=パッケージ |
| `test_data` | テスト測定値（PTR/MPR/FTR 統合）。MPR は 1 レコードをピン数分の行に展開し `pin_num` / `pin_name` 列に PMR のピン情報を格納。PTR/FTR 行ではこれらは NULL。 |
| `chipid` | FT chiplet の die 出自トレース（GDR `EN-S0-CHIPID_R` をデコード）。**FT 専用** |

> 解析・歩留りは重複排除済みの `parts_final` / `test_data_final` / `chipid_final`
> ビューを使う（詳細は `docs/schema.md` / `docs/sample_queries.md`）。

### Parquet パーティション構造

```
data/
└── {table}/
    └── product={product}/
        └── test_category={CP|FT}/
            └── sub_process={CP11|FT2}/
                └── lot_id={lot_id}/
                    └── (wafer_id={id}/retest={n}/)  ← wafers/parts/test_data/chipid
                        └── data.parquet
```

---

## リテスト対応

| フィールド | 取得元 |
|-----------|--------|
| `sub_process` | STDF MIR.TEST_COD / CLI `-s` |
| `test_rev` | ファイル名（Rev04等） |
| `retest_num` | 既存データから自動計算（0=初回, 1,2…=リテスト） |

---

## 開発環境分離 (`--env`)

```bash
stdf --env dev ingest-all ./test_data -p SCT101A  # data-dev/ に保存
stdf --env dev db lots
rm -rf data-dev/   # リセット
```

---

## WSL2 での注意点

| 問題 | 対策 |
|---|---|
| メモリ不足でワーカーが強制終了 | `.wslconfig` で `memory=8GB` を設定、`--workers 2` に減らす |
| `/mnt/c/` からのファイルが遅い | STDF ファイルを Linux 側（`~/`）にコピーしてから実行 |
| 中断後の再開 | `ingest-all` を再実行するだけ（成功済みは自動スキップ） |

---

## ライセンス

MIT License
