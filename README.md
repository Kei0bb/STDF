# stdf2pq

半導体テストデータ（STDF）を Parquet + DuckDB に変換する高速ETLツール。
コアパーサーは **Rust (PyO3)** で実装し、Python フォールバックも内蔵。

## インストール

```bash
cd /path/to/STDF
uv sync
```

### Rust パーサーのセットアップ（推奨）

Rust パーサーを使うと **10〜30倍高速** にSTDFファイルを処理できます。
未インストールでも Python パーサーで動作するので、必須ではありません。

#### Step 1: Rust のインストール

まだ Rust が入っていない場合:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

ターミナルを再起動するか、以下を実行して Rust コマンドを有効化:

```bash
source "$HOME/.cargo/env"
```

確認:

```bash
rustc --version   # rustc 1.x.x と表示されれば OK
cargo --version   # cargo 1.x.x と表示されれば OK
```

#### Step 2: ビルドツール (maturin) の追加

```bash
cd /path/to/STDF
uv add --dev maturin
```

#### Step 3: Rust パーサーのビルド

```bash
cd rust
uv run maturin develop --features python --release
```

> `--release` を付けると最適化ビルドになり大幅に速くなります。
> 開発中は省略しても OK です。

#### 確認

```bash
cd /path/to/STDF
uv run python -c "from stdf_platform.parser import _USE_RUST; print(f'Rust parser: {_USE_RUST}')"
# → "Rust parser: True" と表示されれば成功
```

---

## 使用方法

### STDF ファイルの取り込み

```bash
# 基本（パスから product/test_type 自動推定）
stdf2pq ingest ./downloads/SCT101A/CP/lot001.stdf

# 明示的に指定
stdf2pq ingest sample.stdf --product SCT101A --test-type CP

# dev環境に取り込み（本番DBと分離）
stdf2pq --env dev ingest test.stdf --product SCT101A --test-type CP
```

### DB 操作

```bash
stdf2pq db lots                          # ロット一覧
stdf2pq db query "SELECT * FROM wafers"  # SQL 直接実行
stdf2pq db shell                         # DuckDB シェル

# dev環境のDBを参照
stdf2pq --env dev db lots
```

### 分析

```bash
stdf2pq analyze yield LOT001          # Wafer 歩留まり
stdf2pq analyze test-fail LOT001      # フェールテスト上位
stdf2pq analyze bins LOT001           # Bin 分布
```

### エクスポート

```bash
stdf2pq export csv "SELECT * FROM test_data" out.csv   # SQL → CSV
stdf2pq export lot E6A773.00 E6A774.00 results.csv     # Lot → CSV (JMP対応)
```

### FTP 差分同期

```bash
stdf2pq fetch                # config.yaml に従って差分取得
stdf2pq fetch -p SCT101A     # 製品指定
stdf2pq fetch --force         # 強制再ダウンロード
stdf2pq fetch --no-ingest     # ダウンロードのみ
```

### Web UI

```bash
stdf2pq web  # http://localhost:8501
```

---

## データ構造

### テーブル

| テーブル | 説明 |
|---------|------|
| lots | ロット情報（product, test_category, sub_process） |
| wafers | ウェハー歩留まり（リテスト追跡含む） |
| parts | 個片結果（Bin, X/Y 座標） |
| test_data | テスト結果と定義（統合テーブル） |

### パーティション

```
data/<table>/product=…/test_category=…/sub_process=…/lot_id=…/wafer_id=…/data.parquet
```

詳細: [docs/schema.md](docs/schema.md)

---

## リテスト対応

| フィールド | 取得元 | 備考 |
|-----------|--------|------|
| sub_process | STDF MIR.TEST_COD / CLI `-s` | CLI 指定が最優先 |
| test_rev | ファイル名 (Rev04等) | |
| retest_num | 既存データから自動計算 | 0=初回, 1,2…=リテスト |

## 開発・テスト環境 (`--env`)

本番データと分離してテスト用STDFを管理できます。

```bash
# テスト用ファイルを手動配置（フォルダ構成は自由）
cp test.stdf downloads-dev/

# dev環境にingest（data-dev/ に保存、sync_historyに記録しない）
stdf2pq --env dev ingest downloads-dev/test.stdf --product SCT101A --test-type CP

# dev環境のデータを確認
stdf2pq --env dev db lots
stdf2pq --env dev db query "SELECT * FROM test_data LIMIT 10"

# dev環境をリセット
rm -rf data-dev/
```

| | デフォルト | `--env dev` |
|---|-----------|-------------|
| DB保存先 | `data/` | `data-dev/` |
| DuckDB | `data/stdf.duckdb` | `data-dev/stdf.duckdb` |
| sync_history | 記録する | 記録しない |
| fetch連携 | あり | なし（手動配置のみ） |

---

## アーキテクチャ

```
CLI / Web UI (Python)
      ↓
  parse_stdf()
      ↓
┌─────────────────┐
│ stdf2pq_rs      │ ← Rust (PyO3), 高速
│ (auto-fallback) │
│ Python parser   │ ← フォールバック
└─────────────────┘
      ↓
  STDFData → Parquet → DuckDB
```

詳細: [docs/rust_architecture.md](docs/rust_architecture.md)

---

## ライセンス

MIT License
