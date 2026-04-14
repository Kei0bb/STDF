# %% [markdown]
# # stdf2pq — SQL Query Playground
#
# VS Code で開いて「Run Cell」または Shift+Enter で各セルを実行。
# Python Interactive Window (Jupyter 拡張) が必要です。
#
# **参考:** `docs/sample_queries.md` にクエリ集あり
#
# ## 主な関数
# - `q(sql, limit=100)` — プレビュー用 DataFrame 返却（limit=0 で全件）
# - `to_csv(sql, output, chunk_size)` — メモリ効率的な CSV 書き出し

# %% Setup — DuckDB（デフォルト）または ClickHouse（config.yaml に host 設定時）
import csv
import duckdb
import pandas as pd
from pathlib import Path

def _load_config() -> dict:
    config_path = Path(__file__).parent / "config.yaml"
    if config_path.exists():
        try:
            import yaml
            return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except Exception:
            pass
    return {}

_cfg = _load_config()

# ── DuckDB (primary) ──────────────────────────────────────────────────────────
_storage = _cfg.get("storage", {})
DATA_DIR = Path(_storage.get("data_dir", "./data"))
print(f"Data dir: {DATA_DIR}")

con = duckdb.connect(":memory:")
for table in ["lots", "wafers", "parts", "test_data"]:
    path = DATA_DIR / table
    if path.exists():
        con.execute(f"""
            CREATE OR REPLACE VIEW {table} AS
            SELECT * FROM read_parquet('{path.as_posix()}/**/*.parquet', hive_partitioning=true)
        """)
        print(f"  ✓ {table}")
    else:
        print(f"  - {table}  (not found)")
USE_CH = False
ch = None

# ── ClickHouse (optional — set clickhouse.host in config.yaml to enable) ───────
_ch_cfg = _cfg.get("clickhouse", {})
_ch_host = _ch_cfg.get("host", "")
if _ch_host:
    try:
        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=_ch_host,
            port=_ch_cfg.get("http_port", 8123),
            database=_ch_cfg.get("database", "stdf"),
            username=_ch_cfg.get("username", "default"),
            password=_ch_cfg.get("password", ""),
            connect_timeout=5,
        )
        ch.ping()
        print(f"✓ ClickHouse connected: {_ch_host}")
        USE_CH = True
    except Exception as e:
        print(f"ClickHouse unavailable ({e}), using DuckDB")


def q(sql: str, limit: int = 100, parameters: dict | None = None) -> pd.DataFrame:
    """SQL を実行して DataFrame を返す（プレビュー用）。limit=0 で全件取得。

    大量データの場合は to_csv() を使用してください。
    """
    if limit > 0 and "LIMIT" not in sql.upper():
        sql = sql.rstrip("; \n") + f"\nLIMIT {limit}"
    if USE_CH:
        return ch.query_df(sql, parameters=parameters or {})
    return con.execute(sql).fetchdf()


def to_csv(
    sql: str,
    output: str | Path = "output.csv",
    chunk_size: int = 100_000,
) -> None:
    """SQL 結果を CSV ファイルに書き出す（メモリ効率版）。

    DuckDB:
        COPY TO 文でエンジンが直接ファイルに書く。
        Python 側のメモリ消費はゼロ。大規模データに最適。

    ClickHouse:
        chunk_size 行ずつ LIMIT/OFFSET で分割取得し逐次追記。
        Python メモリは常に chunk_size 行分のみ保持。

    Args:
        sql:        実行する SELECT 文
        output:     出力先 CSV パス（デフォルト: output.csv）
        chunk_size: ClickHouse 使用時の1チャンクあたりの行数
    """
    output = Path(output)
    clean_sql = sql.rstrip("; \n")

    if not USE_CH:
        # DuckDB: COPY TO でゼロコピー書き出し
        # COPY は完了時に書き出し行数を返す
        result = con.execute(
            f"COPY ({clean_sql}) TO '{output.as_posix()}' (HEADER, DELIMITER ',')"
        )
        rows = result.fetchone()[0]
        print(f"Exported {rows:,} rows → {output}")
        return

    # ClickHouse: LIMIT/OFFSET チャンク分割で逐次追記
    total = 0
    first_chunk = True
    offset = 0
    while True:
        chunk_sql = (
            f"SELECT * FROM ({clean_sql}) AS _q "
            f"LIMIT {chunk_size} OFFSET {offset}"
        )
        df = ch.query_df(chunk_sql)
        if df.empty:
            break

        df.to_csv(
            output,
            mode="w" if first_chunk else "a",
            header=first_chunk,
            index=False,
        )
        total += len(df)
        print(f"  writing... {total:,} rows", end="\r")

        first_chunk = False
        offset += chunk_size
        if len(df) < chunk_size:
            break  # 最終チャンク

    print(f"Exported {total:,} rows → {output}        ")


backend = "ClickHouse" if USE_CH else "DuckDB"
print(f"\nReady ({backend}).  q(sql) でプレビュー、to_csv(sql) で CSV 書き出し。")


# %% ロット一覧
q("""
SELECT
    lot_id,
    product,
    test_category,
    sub_process,
    start_time,
    job_name,
    job_rev
FROM lots
ORDER BY start_time DESC
""")


# %% ロット別歩留まりサマリ
q("""
WITH latest_wafers AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY lot_id, wafer_id
               ORDER BY retest_num DESC
           ) AS rn
    FROM wafers
)
SELECT
    l.lot_id,
    l.product,
    l.sub_process,
    COUNT(DISTINCT w.wafer_id)                              AS wafer_count,
    SUM(w.part_count)                                       AS total_parts,
    SUM(w.good_count)                                       AS good_parts,
    ROUND(SUM(w.good_count) * 100.0 / SUM(w.part_count), 2) AS yield_pct
FROM lots l
JOIN latest_wafers w ON l.lot_id = w.lot_id AND w.rn = 1
GROUP BY l.lot_id, l.product, l.sub_process
ORDER BY l.lot_id
""")


# %% ウェーハ別歩留まり（ロットIDを変更して使用）
LOT_ID = "LOT001"   # ← 変更してください

q(f"""
WITH latest AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY lot_id, wafer_id
               ORDER BY retest_num DESC
           ) AS rn
    FROM wafers
    WHERE lot_id = '{LOT_ID}'
)
SELECT
    wafer_id,
    part_count,
    good_count,
    ROUND(good_count * 100.0 / part_count, 2) AS yield_pct,
    retest_num
FROM latest
WHERE rn = 1
ORDER BY wafer_id
""")


# %% テスト項目一覧（ロット内）
q(f"""
SELECT DISTINCT
    test_num,
    test_name,
    rec_type,
    units,
    lo_limit,
    hi_limit
FROM test_data
WHERE lot_id = '{LOT_ID}'
ORDER BY test_num
""")


# %% Fail テストランキング（ワースト上位）
q(f"""
SELECT
    test_num,
    test_name,
    COUNT(*)                                                    AS total,
    SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)              AS fail_cnt,
    ROUND(
        SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                                           AS fail_pct
FROM test_data
WHERE lot_id = '{LOT_ID}'
GROUP BY test_num, test_name
HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
ORDER BY fail_pct DESC
""")


# %% 特定テストの統計 + Cp / Cpk
TEST_NAME = "YOUR_TEST_NAME"   # ← 変更してください

q(f"""
SELECT
    test_num,
    test_name,
    units,
    lo_limit,
    hi_limit,
    COUNT(*)                    AS n,
    ROUND(AVG(result), 4)       AS mean,
    ROUND(STDDEV(result), 4)    AS sigma,
    ROUND(MIN(result), 4)       AS min_val,
    ROUND(MAX(result), 4)       AS max_val,
    ROUND(MEDIAN(result), 4)    AS median,
    -- Cp = (USL - LSL) / (6σ)
    ROUND((hi_limit - lo_limit) / (6 * STDDEV(result)), 3) AS cp,
    -- Cpk = min((USL-μ)/3σ, (μ-LSL)/3σ)
    ROUND(LEAST(
        (hi_limit - AVG(result)) / (3 * STDDEV(result)),
        (AVG(result) - lo_limit) / (3 * STDDEV(result))
    ), 3) AS cpk
FROM test_data
WHERE lot_id    = '{LOT_ID}'
  AND test_name = '{TEST_NAME}'
  AND lo_limit IS NOT NULL
  AND hi_limit IS NOT NULL
  AND rec_type IN ('PTR', 'MPR')
GROUP BY test_num, test_name, units, lo_limit, hi_limit
""")


# %% ビン分布（ロット全体）
q(f"""
SELECT
    hard_bin,
    soft_bin,
    COUNT(*) AS die_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM parts
WHERE lot_id = '{LOT_ID}'
GROUP BY hard_bin, soft_bin
ORDER BY die_count DESC
""")


# %% ビンとフェールテストの紐付け
q(f"""
SELECT
    p.hard_bin,
    p.soft_bin,
    td.test_num,
    td.test_name,
    COUNT(*) AS fail_count
FROM parts p
JOIN test_data td
    ON  p.lot_id   = td.lot_id
    AND p.wafer_id = td.wafer_id
    AND p.part_id  = td.part_id
WHERE p.lot_id  = '{LOT_ID}'
  AND p.passed  = FALSE
  AND td.passed = 'F'
GROUP BY p.hard_bin, p.soft_bin, td.test_num, td.test_name
ORDER BY fail_count DESC
""")


# %% CSV 書き出し — メモリ効率版（to_csv を使用）
# DuckDB: COPY TO で Python メモリ消費ゼロ
# ClickHouse: 100,000 行チャンクで逐次書き出し

to_csv(
    f"""
    SELECT
        p.lot_id, p.wafer_id, p.part_id,
        p.x_coord, p.y_coord,
        p.hard_bin, p.soft_bin, p.passed AS die_passed,
        td.test_num, td.test_name,
        td.result, td.lo_limit, td.hi_limit, td.units, td.passed AS test_passed
    FROM parts p
    JOIN test_data td
        ON  p.lot_id   = td.lot_id
        AND p.wafer_id = td.wafer_id
        AND p.part_id  = td.part_id
    WHERE p.lot_id = '{LOT_ID}'
    ORDER BY p.wafer_id, p.part_id, td.test_num
    """,
    output="output.csv",
)


# %% 任意 SQL → CSV（ここを書き換えて使用）
# 例: 複数ロットの全テストデータを出力
# to_csv("""
#     SELECT * FROM test_data
#     WHERE lot_id IN ('LOT001', 'LOT002')
# """, output="test_data_export.csv")

# 例: プレビューのみ（メモリに全件ロード — 小規模クエリ向け）
q("""
SELECT 1 AS hello
""")
