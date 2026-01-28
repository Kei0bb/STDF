# Rust + Python ハイブリッドアーキテクチャ設計

## 概要

大量のSTDFファイルを高速処理するために、パフォーマンスクリティカルな部分をRustで実装し、PythonからPyO3経由で呼び出すアーキテクチャ。

## アーキテクチャ図

```
┌─────────────────────────────────────────────────────────────┐
│                    Python Layer                              │
├─────────────────────────────────────────────────────────────┤
│  CLI (click)  │  Web UI (Streamlit)  │  API (FastAPI)       │
├───────────────┴──────────────────────┴──────────────────────┤
│              Orchestration & Business Logic                  │
│  • FTP Client      • Config Management                      │
│  • Sync Manager    • Database Queries                       │
│  • Export Logic    • Visualization                          │
├─────────────────────────────────────────────────────────────┤
│              Python ↔ Rust Bridge (PyO3)                     │
│  stdf_parser_rs  (Rust crate with Python bindings)          │
├─────────────────────────────────────────────────────────────┤
│                    Rust Layer                                │
├─────────────────────────────────────────────────────────────┤
│  stdf-parser       │  Arrow/Parquet Writer                  │
│  • Binary parsing  │  • Zero-copy conversion                │
│  • Record decode   │  • Direct Parquet output               │
└─────────────────────────────────────────────────────────────┘
```

## コンポーネント詳細

### 1. Rust Crate: `stdf-parser-rs`

```
stdf-parser-rs/
├── Cargo.toml
├── src/
│   ├── lib.rs           # Main library
│   ├── records.rs       # STDF record definitions
│   ├── parser.rs        # Binary parsing
│   ├── arrow.rs         # Arrow RecordBatch conversion
│   └── python.rs        # PyO3 bindings
└── python/
    └── stdf_parser_rs/  # Python package
        └── __init__.py
```

### 2. Rust API設計

```rust
// Rust側の構造体
#[pyclass]
pub struct STDFData {
    pub lot_id: String,
    pub parts: Vec<PartRecord>,
    pub tests: HashMap<u32, TestRecord>,
    pub test_results: Vec<TestResult>,
}

#[pymethods]
impl STDFData {
    // Python から呼び出し可能
    fn to_arrow_parts(&self) -> PyResult<PyArrowArray> { ... }
    fn to_arrow_tests(&self) -> PyResult<PyArrowArray> { ... }
    fn to_arrow_results(&self) -> PyResult<PyArrowArray> { ... }
}

// パース関数
#[pyfunction]
fn parse_stdf(path: &str) -> PyResult<STDFData> { ... }

// 直接Parquet出力 (最速)
#[pyfunction]
fn parse_to_parquet(
    stdf_path: &str,
    output_dir: &str,
    product: &str,
    test_type: &str,
) -> PyResult<ParseStats> { ... }
```

### 3. Python側の使用例

```python
from stdf_parser_rs import parse_stdf, parse_to_parquet

# 方法1: Pythonでデータ操作が必要な場合
data = parse_stdf("file.stdf")
parts_df = data.to_arrow_parts().to_pandas()

# 方法2: 直接Parquet出力 (最速、推奨)
stats = parse_to_parquet(
    "file.stdf",
    "./data",
    product="SCT101A",
    test_type="CP1"
)
print(f"Parsed: {stats.parts} parts, {stats.tests} tests")
```

## 処理フロー

### バッチ処理フロー

```
1. Python: FTPからファイルリスト取得
2. Python: 未処理ファイルをフィルタ (SyncManager)
3. Python: 並列処理キュー作成
4. Rust:  STDF → Parquet 直接変換 (並列)
5. Python: メタデータ更新 (SyncManager)
6. Python: DuckDB ビュー更新
```

### 並列処理設計

```python
from concurrent.futures import ProcessPoolExecutor
from stdf_parser_rs import parse_to_parquet

def process_file(args):
    path, product, test_type = args
    return parse_to_parquet(path, "./data", product, test_type)

with ProcessPoolExecutor(max_workers=8) as executor:
    results = list(executor.map(process_file, file_list))
```

## パフォーマンス比較予測

| シナリオ | Python現在 | Rust移行後 | 高速化 |
|---------|-----------|-----------|--------|
| 1ファイル (100MB) | 20秒 | 1秒 | 20x |
| 100ファイル (並列) | 30分 | 2分 | 15x |
| 1000ファイル (日次) | 5時間 | 20分 | 15x |

## 開発ロードマップ

### Phase 1: Core Parser (2週間)
- [ ] Rust STDF パーサー基本実装
- [ ] PTR, MPR, FTR レコード対応
- [ ] PyO3 バインディング

### Phase 2: Arrow Integration (1週間)
- [ ] Arrow RecordBatch 出力
- [ ] Python pandas 連携

### Phase 3: Direct Parquet (1週間)
- [ ] Parquet 直接出力
- [ ] パーティション対応

### Phase 4: Integration (1週間)
- [ ] 既存 CLI との統合
- [ ] ベンチマーク・検証

## 依存関係

### Rust
```toml
[dependencies]
pyo3 = { version = "0.20", features = ["extension-module"] }
arrow = "50"
parquet = "50"
rayon = "1.8"  # 並列処理
```

### Python
```toml
[build-system]
requires = ["maturin>=1.4"]
build-backend = "maturin"
```

## ビルド・配布

```bash
# 開発ビルド
cd stdf-parser-rs
maturin develop

# Wheel ビルド
maturin build --release

# Wheel のインストール
pip install target/wheels/stdf_parser_rs-*.whl
```

## 検討事項

### メリット
- ✅ 10-30x の高速化
- ✅ メモリ効率向上
- ✅ 既存Pythonコードとの互換性維持
- ✅ 段階的移行が可能

### デメリット
- ⚠️ ビルド環境の複雑化 (Rust toolchain)
- ⚠️ デバッグの難易度上昇
- ⚠️ Windows/Linux/Mac 用バイナリのクロスコンパイル
- ⚠️ 開発者の Rust 学習コスト

## 推奨

**Phase 1 から段階的に開始**することを推奨。

まず小さなプロトタイプで効果を検証し、本格実装を判断。
