# STDF to Parquet Converter

半導体テストデータのSTDF（Standard Test Data Format）ファイルをApache Parquet形式に変換するCLIツール。

## インストール

```bash
# uvを使用
uv sync

# または pip
pip install .
```

## 使用方法

### 基本的な変換

```bash
stdf2parquet convert input.stdf ./output/
```

出力ディレクトリにレコードタイプごとのParquetファイルが生成されます：

```
output/
├── FAR.parquet
├── MIR.parquet
├── PIR.parquet
├── PTR.parquet
├── PRR.parquet
└── ...
```

### 特定レコードタイプのみ変換

```bash
stdf2parquet convert input.stdf ./output/ --records PTR,PIR,PRR
```

### サポートされているレコードタイプの一覧

```bash
stdf2parquet list-records
```

### オプション

| オプション | 説明 |
|-----------|------|
| `--records`, `-r` | 変換するレコードタイプ（カンマ区切り） |
| `--verbose`, `-v` | 詳細なエラー出力 |
| `--no-progress` | 進捗バーを非表示 |

## Parquetファイルの読み込み

```python
import pyarrow.parquet as pq
import pandas as pd

# PyArrowで読み込み
table = pq.read_table("output/PTR.parquet")

# Pandasで読み込み
df = pd.read_parquet("output/PTR.parquet")
print(df.head())
```

## 対応レコードタイプ

| タイプ | 説明 |
|-------|------|
| FAR | File Attributes Record |
| MIR | Master Information Record |
| MRR | Master Results Record |
| PCR | Part Count Record |
| HBR | Hardware Bin Record |
| SBR | Software Bin Record |
| PMR | Pin Map Record |
| PGR | Pin Group Record |
| PLR | Pin List Record |
| RDR | Retest Data Record |
| SDR | Site Description Record |
| WIR | Wafer Information Record |
| WRR | Wafer Results Record |
| WCR | Wafer Configuration Record |
| PIR | Part Information Record |
| PRR | Part Results Record |
| TSR | Test Synopsis Record |
| PTR | Parametric Test Record |
| MPR | Multiple-Result Parametric Record |
| FTR | Functional Test Record |
| BPS | Begin Program Section Record |
| EPS | End Program Section Record |
| GDR | Generic Data Record |
| DTR | Datalog Text Record |

## ライセンス

MIT License
