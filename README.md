# STDF Data Platform

半導体テストデータ（STDF）をETL処理し、DuckDB+Parquetベースで分析できるプラットフォーム。

## インストール

```bash
cd /path/to/STDF
uv sync
```

## 使用方法

### 1. STDFファイルの取り込み

```bash
# 基本（パスからproduct/test_type自動推定）
stdf-platform ingest ./downloads/SCT101A/CP/lot001.stdf

# 明示的に指定
stdf-platform ingest sample.stdf --product SCT101A --test-type CP
```

Parquetファイルが `./data/` に、DuckDBファイルが `./data/stdf.duckdb` に保存されます。

**ディレクトリ構造:**
```
data/test_results/product=SCT101A/test_type=CP/lot_id=XXX/wafer_id=YYY/data.parquet
```

### 2. CLI分析コマンド

```bash
# ロット一覧（product/test_type含む）
stdf-platform lots

# Wafer毎の歩留まり
stdf-platform analyze yield LOT001

# 上位10フェールテスト
stdf-platform analyze test-fail LOT001 --top 10

# Bin分布
stdf-platform analyze bins LOT001

# SQLクエリ実行
stdf-platform query "SELECT * FROM wafers LIMIT 10"
```

---

## FTPからの自動取得（差分同期）

### config.yaml設定

```yaml
filters:
  - product: SCT101A
    test_types: [CP]      # SCT101AはCPのみ
  - product: ABC200B
    test_types: [CP, FT]  # ABC200Bは両方

# filtersが空または未設定の場合、全製品・全テストタイプをダウンロード
```

### fetchコマンド

```bash
# config.yaml の filters に従って差分取得
stdf-platform fetch

# CLIで上書き指定
stdf-platform fetch -p SCT101A -t CP

# 強制再ダウンロード
stdf-platform fetch --force

# ダウンロードのみ（ingestしない）
stdf-platform fetch --no-ingest

# 最大10ファイルまで
stdf-platform fetch -n 10
```

> 履歴は `./downloads/.sync_history.json` に保存されます。

---

## クライアントからの接続

### DBeaver（推奨）

1. **DBeaver起動** → **データベース** → **新しい接続**
2. **DuckDB** を選択
3. **パス設定**:
   ```
   \\wsl$\Ubuntu\home\<user>\path\to\STDF\data\stdf.duckdb
   ```

### DuckDB CLI

```bash
duckdb ./data/stdf.duckdb

# テーブル確認
SHOW TABLES;

# クエリ実行
SELECT lot_id, product, test_type, COUNT(*) as parts FROM parts GROUP BY ALL;
```

---

## Web UI（ブラウザ）

```bash
stdf-platform web
```

ブラウザで http://localhost:8501 にアクセス。

**機能:**
- Product/Test Type フィルタリング
- Lot/Wafer/Parameter選択
- データプレビュー
- CSVダウンロード（JMP対応Pivot形式）

---

## データ構造

| テーブル | 説明 |
|---------|------|
| lots | ロット情報（product, test_type, job等） |
| wafers | ウェハー情報と歩留まり |
| parts | 個片結果（Bin, X/Y座標） |
| tests | テスト定義（リミット、単位） |
| test_results | テスト結果（Parametric/Functional） |

---

## 将来のロードマップ: Rust 移行

大量ファイル処理のパフォーマンス向上のため、STDFパーサーのRust移行を計画中。

### 期待される効果
- **10-30x の高速化**
- メモリ効率向上
- 並列処理対応

### アーキテクチャ
```
Python (CLI/Web/API)
       ↓ PyO3
Rust (STDF Parser → Parquet)
```

詳細: [Rust Architecture Design](docs/rust_architecture.md) *(計画中)*

---

## ライセンス

MIT License
