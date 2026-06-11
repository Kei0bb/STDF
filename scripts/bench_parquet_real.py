"""実データで Parquet エンコーディングを検証するベンチ（施錠端末で実行用）.

現行 v1.0+zstd と、最新フォーマット世代(v2.6) のエンコーディング
(BYTE_STREAM_SPLIT / DELTA_BINARY_PACKED) を、実データのスキーマ・分布で比較する。

★出力はファイルサイズと処理時間の集計値だけ。測定値そのものは一切出力しない★
→ 印字された表をそのまま共有して問題ない。

使い方（Windows / PowerShell の例）:
    uv run python scripts/bench_parquet_real.py data/test_data
    uv run python scripts/bench_parquet_real.py data/test_data --max-rows 20000000
    uv run python scripts/bench_parquet_real.py "data/test_data/product=SCT101A" --max-rows 0

引数:
    path        Parquet テーブルのディレクトリ or glob（再帰で *.parquet を拾う）
    --max-rows  読み込む最大行数（既定 2000万。0 で全件。大きいほど現実的だが要メモリ）

依存: pyarrow, duckdb のみ（リポジトリ本体に非依存）
"""
import argparse
import os
import sys
import time
import tempfile
import shutil
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# test_data_final の重複排除キー（database.py と同一）。
# 対象テーブルにこれらの列が無い場合は dedup 計測をスキップする。
DEDUP_COLS = ["lot_id", "wafer_id", "x_coord", "y_coord", "part_txt",
              "test_num", "pin_num", "retest_num"]


def build_glob(path: str) -> str:
    p = Path(path)
    if any(ch in path for ch in "*?[]"):
        return path
    if p.is_dir():
        return (p / "**" / "*.parquet").as_posix()
    return p.as_posix()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="Parquet テーブルのディレクトリ or glob")
    ap.add_argument("--max-rows", type=int, default=20_000_000,
                    help="読み込む最大行数（0=全件、既定 20,000,000）")
    args = ap.parse_args()

    glob = build_glob(args.path)
    print(f"source : {glob}")
    print(f"max-rows: {'ALL' if args.max_rows == 0 else f'{args.max_rows:,}'}")

    # --- 実データを読み込む（hive partition 込み） ---
    con0 = duckdb.connect()
    limit = "" if args.max_rows == 0 else f" LIMIT {args.max_rows}"
    t0 = time.perf_counter()
    try:
        tbl = con0.execute(
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true){limit}"
        ).fetch_arrow_table()
    except Exception as e:
        print(f"読み込み失敗: {e}")
        return 1
    con0.close()
    load_ms = (time.perf_counter() - t0) * 1000
    n = tbl.num_rows
    if n == 0:
        print("行が 0 件でした。path を確認してください。")
        return 1

    # --- スキーマから float / int / string 列を自動分類 ---
    floats, ints, strings = [], [], []
    for f in tbl.schema:
        if pa.types.is_floating(f.type):
            floats.append(f.name)
        elif pa.types.is_integer(f.type):
            ints.append(f.name)
        elif pa.types.is_string(f.type) or pa.types.is_large_string(f.type):
            strings.append(f.name)
    print(f"rows={n:,}  cols={len(tbl.schema)}  "
          f"floats={floats}  ints={ints}")
    print(f"(load {load_ms:.0f}ms)\n")

    bss = {c: "BYTE_STREAM_SPLIT" for c in floats}
    delta = {c: "DELTA_BINARY_PACKED" for c in ints}

    CONFIGS = {
        "v1.0_zstd (current)": dict(version="1.0", compression="zstd",
                                    use_dictionary=True),
        "v2.6_zstd_default":   dict(version="2.6", compression="zstd",
                                    use_dictionary=True),
        "v2.6_BSS_floats":     dict(version="2.6", compression="zstd",
                                    use_dictionary=strings or True,
                                    column_encoding=dict(bss)),
        "v2.6_DELTA_ints":     dict(version="2.6", compression="zstd",
                                    use_dictionary=strings or True,
                                    column_encoding=dict(delta)),
        "v2.6_BSS+DELTA":      dict(version="2.6", compression="zstd",
                                    use_dictionary=strings or True,
                                    column_encoding={**bss, **delta}),
    }

    has_dedup = all(c in tbl.schema.names for c in DEDUP_COLS)
    dedup_part = ", ".join(DEDUP_COLS[:-1])  # 末尾 retest_num は ORDER BY 側

    tmpdir = tempfile.mkdtemp(prefix="pqbench_real_")
    rows_out = []
    try:
        for name, kw in CONFIGS.items():
            path = os.path.join(tmpdir, name.split()[0].replace("/", "_") + ".parquet")
            t0 = time.perf_counter()
            try:
                pq.write_table(tbl, path, write_statistics=True,
                               coerce_timestamps="ms",
                               allow_truncated_timestamps=True, **kw)
            except Exception as e:
                print(f"{name:24s} WRITE FAILED: {str(e)[:90]}")
                continue
            write_ms = (time.perf_counter() - t0) * 1000
            size_mb = os.path.getsize(path) / 1e6

            con = duckdb.connect()
            t0 = time.perf_counter()
            con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchall()
            scan_ms = (time.perf_counter() - t0) * 1000

            dedup_ms = float("nan")
            if has_dedup:
                t0 = time.perf_counter()
                con.execute(f"""
                    SELECT count(*) FROM (
                      SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY {dedup_part} ORDER BY retest_num DESC) rn
                      FROM read_parquet('{path}')) WHERE rn=1
                """).fetchall()
                dedup_ms = (time.perf_counter() - t0) * 1000

            t0 = time.perf_counter()
            con.execute(f"SELECT * FROM read_parquet('{path}')").fetch_arrow_table()
            fetch_ms = (time.perf_counter() - t0) * 1000
            con.close()

            rows_out.append((name, size_mb, write_ms, scan_ms, dedup_ms, fetch_ms))
            dtxt = f"{dedup_ms:6.0f}" if has_dedup else "   n/a"
            print(f"{name:24s} size={size_mb:8.1f}MB write={write_ms:7.0f}ms "
                  f"scan={scan_ms:6.0f}ms dedup={dtxt}ms fetch={fetch_ms:7.0f}ms")

        if rows_out:
            base = rows_out[0]
            print("\n=== 現行(v1.0_zstd) を 100% とした相対値 ===")
            for name, size, w, s, d, f in rows_out:
                dpct = f"{d/base[4]*100:5.0f}%" if has_dedup else "  n/a"
                print(f"{name:24s} size={size/base[1]*100:5.0f}%  "
                      f"fetch={f/base[5]*100:5.0f}%  dedup={dpct}  "
                      f"write={w/base[2]*100:5.0f}%")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    print("\ndone (tmp 削除済み)。上の表をそのまま共有可。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
