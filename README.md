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
stdf-platform ingest sample.stdf
```

Parquetファイルが `./data/` に、DuckDBファイルが `./data/stdf.duckdb` に保存されます。

### 2. CLI分析コマンド

```bash
# ロット一覧
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

## クライアントからの接続方法

### DBeaver（推奨）

1. **DBeaver起動** → **データベース** → **新しい接続**
2. **DuckDB** を選択
3. **パス設定**（WSL上のファイルにWindowsからアクセス）:
   ```
   \\wsl$\Ubuntu\home\<user>\path\to\STDF\data\stdf.duckdb
   ```
4. 接続後、テーブル一覧が表示されます

### DuckDB CLI（WSL内）

```bash
# DuckDBシェル起動
duckdb ./data/stdf.duckdb

# テーブル確認
SHOW TABLES;

# クエリ実行
SELECT lot_id, COUNT(*) as parts FROM parts GROUP BY lot_id;
```

### Python（クライアント側）

```python
import duckdb

# ネットワークパス経由でParquetを直接クエリ
conn = duckdb.connect()
df = conn.execute("""
    SELECT * FROM read_parquet('//wsl$/Ubuntu/home/<user>/STDF/data/test_results/**/*.parquet', 
                               hive_partitioning=true)
    WHERE lot_id = 'LOT001'
""").fetchdf()
```

---

## データ構造

| テーブル | 説明 |
|---------|------|
| lots | ロット情報（job, operator等） |
| wafers | ウェハー情報と歩留まり |
| parts | 個片結果（Bin, X/Y座標） |
| tests | テスト定義（リミット、単位） |
| test_results | テスト結果（Parametric/Functional） |

## 設定（config.yaml）

```yaml
ftp:
  host: "ftp.example.com"
  username: "${FTP_USER}"
  password: "${FTP_PASSWORD}"

storage:
  data_dir: "./data"
  database: "./data/stdf.duckdb"
```

## ライセンス

MIT License
