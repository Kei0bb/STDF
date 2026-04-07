# stdf2pq

半導体テストデータ（STDF V4）を Parquet + DuckDB に変換する高速ETLパイプライン。
Pure Python パーサー × **ProcessPoolExecutor** による並列処理で、100ファイル以上のバッチ取り込みにも対応。

## インストール

```bash
cd /path/to/STDF
uv sync
```

---

## 使用方法

### 単ファイルの取り込み

```bash
# product を指定して取り込み（sub_process は STDF MIR.TEST_COD から自動取得）
stdf2pq ingest sample.stdf --product SCT101A

# パスから product 自動推定（.../SCT101A/CP/... の構造を検出）
stdf2pq ingest ./downloads/SCT101A/CP/lot001.stdf --from-path

# dev 環境に取り込み（本番データと分離）
stdf2pq --env dev ingest test.stdf --product SCT101A
```

### ディレクトリ一括取り込み（推奨）

```bash
# デフォルト 4 ワーカーで並列処理
stdf2pq ingest-all ./downloads -p SCT101A

# ワーカー数とタイムアウトを調整
stdf2pq ingest-all ./downloads -p SCT101A --workers 8 --timeout 600

# ファイルパターン指定
stdf2pq ingest-all ./downloads -p SCT101A --glob "*.stdf.gz"
```

### DB 操作

```bash
stdf2pq db lots                          # ロット一覧
stdf2pq db query "SELECT * FROM wafers"  # SQL 直接実行
stdf2pq db shell                         # DuckDB シェル
```

### 分析

```bash
stdf2pq analyze yield LOT001          # Wafer 歩留まり
stdf2pq analyze test-fail LOT001      # フェールテスト上位
stdf2pq analyze bins LOT001           # Bin 分布
```

### エクスポート

```bash
stdf2pq export csv "SELECT * FROM test_data" out.csv
stdf2pq export lot LOT001 LOT002 results.csv
```

### FTP 差分同期

```bash
stdf2pq fetch                       # config.yaml に従って差分取得
stdf2pq fetch -p SCT101A            # 製品指定
stdf2pq fetch --force               # 強制再ダウンロード
stdf2pq fetch --no-ingest           # ダウンロードのみ
stdf2pq fetch --reingest            # DL済み未ingestを再試行（FTP接続なし）
```

### Web UI

```bash
stdf2pq web  # http://localhost:8501
```

---

## アーキテクチャ

```
STDF ファイル
     ↓
parse_stdf()  ← Pure Python (struct.Struct 最適化)
     ↓
STDFData
     ↓
ParquetStorage  ← Hive パーティション分割
     ↓
data/{table}/product={}/test_category={}/sub_process={}/lot_id={}/...
     ↓
DuckDB ビュー  ← SQL 分析
     ↓
CLI / Web UI
```

### バッチ処理の仕組み

```
100 ファイル
     ↓
ThreadPoolExecutor (max_workers=N)
├── Worker 1 → subprocess(_ingest_worker) → Parquet
├── Worker 2 → subprocess(_ingest_worker) → Parquet
├── Worker 3 → subprocess(_ingest_worker) → Parquet
└── Worker 4 → subprocess(_ingest_worker) → Parquet
```

- 各 subprocess はメモリ分離された独立プロセス（クラッシュしても他に影響しない）
- ワーカー数はデフォルト 4、`--workers N` で調整可能

---

## データ構造

### テーブル

| テーブル | 説明 |
|---------|------|
| lots | ロット情報（product, test_category, sub_process） |
| wafers | ウェハー歩留まり（リテスト追跡含む） |
| parts | 個片結果（Bin, X/Y 座標） |
| test_data | テスト結果と定義（統合テーブル） |

### Parquet パーティション

```
data/lots/product=SCT101A/test_category=CP/sub_process=CP11/lot_id=LOT001/data.parquet
data/wafers/...lot_id=LOT001/wafer_id=W01/retest=0/data.parquet
data/parts/...
data/test_data/...
```

---

## リテスト対応

| フィールド | 取得元 | 備考 |
|-----------|--------|------|
| sub_process | STDF MIR.TEST_COD / CLI `-s` | CLI 指定が最優先 |
| test_rev | ファイル名 (Rev04等) | |
| retest_num | 既存データから自動計算 | 0=初回, 1,2…=リテスト |

---

## 開発環境分離 (`--env`)

```bash
# dev 環境に取り込み（data-dev/ に保存）
stdf2pq --env dev ingest test.stdf --product SCT101A

# dev 環境のデータ確認
stdf2pq --env dev db lots

# dev 環境リセット
rm -rf data-dev/
```

| | デフォルト | `--env dev` |
|---|-----------|-------------|
| Parquet 保存先 | `data/` | `data-dev/` |
| DuckDB | `data/stdf.duckdb` | `data-dev/stdf.duckdb` |
| sync_history | 記録する | 記録しない |

---

## PostgreSQL（オプション）

複数ユーザーが JMP 等から直接 SQL クエリできます。

```bash
# 依存インストール
uv pip install "psycopg2-binary>=2.9.0"

# PostgreSQL 起動
docker compose up -d
```

| 項目 | 値 |
|------|-----|
| Host | サーバーの IP アドレス |
| Port | `5432` |
| Database | `stdf` |
| User | `stdf_reader` |
| Password | `stdf_read_only` |

---

## ライセンス

MIT License
