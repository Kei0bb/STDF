# stdf2pq DB

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
stdf2pq ingest ./downloads/SCT101A/CP/lot001.stdf

# 明示的に指定
stdf2pq ingest sample.stdf --product SCT101A --test-type CP
```

Parquetファイルが `./data/` に、DuckDBファイルが `./data/stdf.duckdb` に保存されます。

**ディレクトリ構造:**
```
data/test_data/product=SCT101A/test_category=CP/sub_process=CP11/lot_id=XXX/wafer_id=YYY/data.parquet
```

### 2. CLI分析コマンド

```bash
# ロット一覧（product/test_type含む）
stdf2pq lots

# Wafer毎の歩留まり
stdf2pq analyze yield LOT001

# 上位10フェールテスト
stdf2pq analyze test-fail LOT001 --top 10

# Bin分布
stdf2pq analyze bins LOT001

# SQLクエリ実行
stdf2pq query "SELECT * FROM wafers LIMIT 10"
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
stdf2pq fetch

# CLIで上書き指定
stdf2pq fetch -p SCT101A -t CP

# 強制再ダウンロード
stdf2pq fetch --force

# ダウンロードのみ（ingestしない）
stdf2pq fetch --no-ingest

# 最大10ファイルまで
stdf2pq fetch -n 10
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
stdf2pq web
```

ブラウザで http://localhost:8501 にアクセス。

**機能:**
- Product/Test Type フィルタリング
- Lot/Wafer/Parameter選択
- データプレビュー
- CSVダウンロード（JMP対応Pivot形式）

---

## データ構造

### テーブル

| テーブル | 説明 |
|---------|------|
| lots | ロット情報（product, test_category, sub_process等） |
| wafers | ウェハー情報と歩留まり（リテスト追跡含む） |
| parts | 個片結果（Bin, X/Y座標） |
| test_data | テスト結果と定義（統合テーブル） |

### パーティション階層

```
data/wafers/product=SCT101A/test_category=CP/sub_process=CP11/
            lot_id=E6A773.00/wafer_id=WS100/retest=0/data.parquet
                                           /retest=1/data.parquet  ← リテスト
```

---

## リテスト対応

### ファイル名パターン

```
E6A773.00-01_01_SCT101A_L000_CP11_5_Rev04_00_008@WS100#202601150243.stdf.gz
                             │     │  │
                             │     │  └─ Rev04 = test_rev
                             │     └─ CP11 = sub_process
                             └─ SCT101A = product
```

### 判定ルール

| フィールド | 取得元 | 優先順位 |
|-----------|--------|----------|
| sub_process (CP11等) | ファイル名 or ディレクトリ | ファイル名優先 |
| test_code (CP1等) | STDF MIR.TEST_COD | STDF優先 |
| test_rev (Rev04等) | ファイル名 | - |
| retest_num | 既存データから自動計算 | 0=初回, 1,2...=リテスト |

### wafersテーブル拡張フィールド

| 列名 | 説明 |
|------|------|
| test_code | 小工程（CP1等）- STDFから取得 |
| test_rev | テストバージョン（Rev04等）- ファイル名から |
| retest_num | リテスト番号（0=初回） |
| source_file | 元ファイル名 |

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
