# stdf2pq

半導体テストデータ（STDF V4）を **Parquet + ClickHouse** に変換する高速ETLパイプライン。  
Pure Python パーサー × ThreadPoolExecutor による並列処理で、100ファイル以上のバッチ取り込みに対応。  
FastAPI + Alpine.js + Plotly.js の Web UI でデータを分析・CSV エクスポート。

---

## クイックスタート

```bash
# 1. 依存インストール
uv sync

# 2. ClickHouse 起動（Web UI 使用時に必要）
docker compose up -d

# 3. config.yaml を設定（example をコピー）
cp config.yaml.example config.yaml

# 4. データ取り込み
stdf2pq ingest-all ./downloads -p YOUR_PRODUCT

# 5. Web UI 起動
stdf2pq web   # → http://localhost:8000
```

---

## アーキテクチャ

```
STDF ファイル
     ↓
parse_stdf()  ←  Pure Python (struct.Struct 最適化)
     ↓
 ┌───┴───────────────────────────┐
 │                               │
Parquet                    ClickHouse
(Hive パーティション)        (MergeTree エンジン)
data/{table}/               lots / wafers
product={}/                 parts / test_data
test_category={}/                  ↓
lot_id={}/                   Web UI / query.py
data.parquet
     ↓
DuckDB ビュー（CLI / query.py フォールバック）
```

### バッチ処理の仕組み

```
100 ファイル
     ↓
ThreadPoolExecutor (max_workers=N)
├── Thread 1 → subprocess(_ingest_worker) → Parquet + ClickHouse
├── Thread 2 → subprocess(_ingest_worker) → Parquet + ClickHouse
└── Thread N → subprocess(_ingest_worker) → Parquet + ClickHouse
```

- 各 subprocess はメモリ分離（クラッシュしても他のワーカーに影響しない）
- 成功済みファイルを `data/ingest_history.json` に記録 → 中断後の再実行で自動スキップ

---

## インストール

```bash
uv sync
```

PostgreSQL を使う場合（オプション）:

```bash
uv pip install "psycopg2-binary>=2.9.0"
```

---

## 使用方法

### データ取り込み

```bash
# 単ファイル
stdf2pq ingest sample.stdf --product SCT101A

# パスから product 自動推定（.../SCT101A/CP/... 構造）
stdf2pq ingest ./downloads/SCT101A/CP/lot001.stdf --from-path

# ディレクトリ一括（推奨）— 中断後の再実行は自動で続きから
stdf2pq ingest-all ./downloads -p SCT101A
stdf2pq ingest-all ./downloads -p SCT101A --workers 8 --timeout 600
stdf2pq ingest-all ./downloads -p SCT101A --force   # 全ファイル強制再取り込み
```

### Web UI

```bash
stdf2pq web   # http://localhost:8000
```

| タブ | 機能 |
|---|---|
| **Dashboard** | ロット別歩留まりバーチャート、KPI（総部品数 / 良品数 / 平均歩留まり） |
| **Wafer** | ウェハー別歩留まり + ウェハーマップ（Pass/Fail or Bin 色分け） |
| **Tests** | テストパラメーター一覧（AG Grid）+ 分布ヒストグラム + フェール Pareto |
| **Export** | CSV ダウンロード（Pivot形式 / Long形式）、lot・wafer・test_num フィルター |

### SQL クエリ（VS Code）

```bash
# VS Code で query.py を開いて Shift+Enter でセルを実行
# ClickHouse が起動していれば自動的に CH を使用、なければ DuckDB/Parquet にフォールバック
```

### CLI クエリ

```bash
stdf2pq db lots                          # ロット一覧
stdf2pq db query "SELECT * FROM wafers"  # SQL 直接実行
stdf2pq db shell                         # DuckDB シェル
```

### 分析コマンド

```bash
stdf2pq analyze yield LOT001       # Wafer 歩留まり
stdf2pq analyze test-fail LOT001   # フェールテスト上位
stdf2pq analyze bins LOT001        # Bin 分布
```

### FTP 差分同期

```bash
stdf2pq fetch                   # config.yaml に従って差分取得
stdf2pq fetch -p SCT101A        # 製品指定
stdf2pq fetch --force           # 強制再ダウンロード
stdf2pq fetch --no-ingest       # ダウンロードのみ
stdf2pq fetch --reingest        # DL済み未 ingest を再試行（FTP接続なし）
```

---

## データ構造

### 4 テーブル

| テーブル | エンジン (CH) | 説明 |
|---------|---|------|
| `lots` | ReplacingMergeTree | ロット情報（product, test_category, sub_process） |
| `wafers` | ReplacingMergeTree | ウェハー歩留まり（リテスト追跡含む） |
| `parts` | MergeTree | 個片結果（Bin, X/Y 座標） |
| `test_data` | MergeTree | テスト測定値（PTR/MPR/FTR 統合） |

### Parquet パーティション構造

```
data/
└── {table}/
    └── product={product}/
        └── test_category={CP|FT}/
            └── sub_process={CP11|FT2}/
                └── lot_id={lot_id}/
                    └── (wafer_id={id}/retest={n}/)  ← wafers のみ
                        └── data.parquet
```

### ClickHouse ソートキー

| テーブル | ORDER BY | 最適化されるクエリ |
|---|---|---|
| `lots` | `lot_id` | ロット検索 |
| `wafers` | `(lot_id, wafer_id, retest_num)` | ウェハー検索・リテスト |
| `parts` | `(lot_id, wafer_id, part_id)` | 個片検索・ウェハーマップ |
| `test_data` | `(lot_id, test_num, part_id)` | テスト集計・Fail率 |

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
stdf2pq --env dev ingest-all ./test_data -p SCT101A  # data-dev/ に保存
stdf2pq --env dev db lots
rm -rf data-dev/   # リセット
```

---

## ClickHouse

Web UI と `query.py` のバックエンド。OLAP に最適化されたカラム型データベース。

```bash
docker compose up -d clickhouse   # 起動
# HTTP interface: http://localhost:8123
# Native TCP:     localhost:9000 (DBeaver, clickhouse-client)
```

デフォルト接続情報:

| 項目 | 値 |
|---|---|
| Host | `localhost` |
| HTTP Port | `8123` |
| TCP Port | `9000` |
| Database | `stdf` |
| User | `default` |
| Password | （空） |

`config.yaml` の `clickhouse:` セクションで変更可能。

---

## PostgreSQL（オプション）

JMP / Excel / BI ツールから ODBC/JDBC で直接接続する場合に使用。

```bash
docker compose up -d postgres
```

| 項目 | 値 |
|---|---|
| Host | サーバーの IP アドレス |
| Port | `5432` |
| Database | `stdf` |
| User | `stdf_reader` |
| Password | `stdf_read_only` |

---

## WSL2 での注意点

| 問題 | 対策 |
|---|---|
| メモリ不足でワーカーが強制終了 | `.wslconfig` で `memory=8GB` を設定、`--workers 2` に減らす |
| `/mnt/c/` からのファイルが遅い | STDFファイルを Linux 側（`~/`）にコピーしてから実行 |
| 中断後の再開 | `ingest-all` を再実行するだけ（成功済みは自動スキップ） |

---

## ライセンス

MIT License
