# %% [markdown]
# # stdf — SQL Query Playground
#
# VS Code で開いて「Run Cell」または Shift+Enter で各セルを実行。
# Python Interactive Window (Jupyter 拡張) が必要です。
#
# **参考:** `docs/sample_queries.md` にクエリ集あり
#
# ## 主な関数
# - `q(sql, limit=100)` — プレビュー用 DataFrame 返却（limit=0 で全件）
# - `to_csv(sql, output, chunk_size)` — メモリ効率的な CSV 書き出し

# %% Setup — DuckDB
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

if (DATA_DIR / "parts").exists():
    con.execute("""
        CREATE OR REPLACE VIEW parts_final AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY lot_id, wafer_id, x_coord, y_coord
                ORDER BY retest_num DESC
            ) AS rn FROM parts
        ) WHERE rn = 1
    """)
    print("  ✓ parts_final")

if (DATA_DIR / "test_data").exists():
    con.execute("""
        CREATE OR REPLACE VIEW test_data_final AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY lot_id, wafer_id, part_id, test_num, test_name, rec_type, pin_num, pin_name
                ORDER BY retest_num DESC
            ) AS rn FROM test_data
        ) WHERE rn = 1
    """)
    print("  ✓ test_data_final")

print("\nReady (DuckDB).  q(sql) でプレビュー、to_csv(sql) で CSV 書き出し。")


def q(sql: str, limit: int = 100) -> pd.DataFrame:
    """SQL を実行して DataFrame を返す（プレビュー用）。limit=0 で全件取得。

    大量データの場合は to_csv() を使用してください。
    """
    if limit > 0 and "LIMIT" not in sql.upper():
        sql = sql.rstrip("; \n") + f"\nLIMIT {limit}"
    return con.execute(sql).fetchdf()


def to_csv(sql: str, output: str | Path = "output.csv") -> None:
    """SQL 結果を CSV ファイルに書き出す（メモリ効率版）。

    DuckDB COPY TO 文でエンジンが直接ファイルに書く。
    Python 側のメモリ消費はゼロ。大規模データに最適。

    Args:
        sql:    実行する SELECT 文
        output: 出力先 CSV パス（デフォルト: output.csv）
    """
    output = Path(output)
    clean_sql = sql.rstrip("; \n")
    result = con.execute(
        f"COPY ({clean_sql}) TO '{output.as_posix()}' (HEADER, DELIMITER ',')"
    )
    rows = result.fetchone()[0]
    print(f"Exported {rows:,} rows → {output}")

# %%
LOT_ID = "E6B292.00"

# %% [CP]Test_failrate
q(f"""
    SELECT
        test_num,
        test_name,
        wafer_id,
        sub_process,
        COUNT(*) as total,
        SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) as fails,
        ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) as fail_rate
    FROM test_data_final
    WHERE lot_id = '{LOT_ID}'
    GROUP BY test_num, test_name, wafer_id, sub_process
    HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
    ORDER BY fail_rate DESC
""")

# %%
to_csv(f"""
    SELECT
        test_num,
        test_name,
        wafer_id,
        sub_process,
        COUNT(*) as total,
        SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) as fails,
        ROUND(100.0 * SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) as fail_rate
    FROM test_data_final
    WHERE lot_id = '{LOT_ID}'
    GROUP BY test_num, test_name, wafer_id, sub_process
    HAVING SUM(CASE WHEN passed = 'F' THEN 1 ELSE 0 END) > 0
    ORDER BY fail_rate DESC
"""
,"./Query/TestFail.csv")

# %%
q(f"""
    SELECT
        lot_id,
        wafer_id,
        test_name,
        pin_num,
        pin_name,
        x_coord,
        y_coord,
        result,
    FROM test_data_final
    WHERE lot_id = 'E6B155.00'
    AND test_name = 'VDF_OBS:ALL_FIXL::'
""")

# %%
to_csv(f"""
    SELECT
        lot_id,
        wafer_id,
        part_id,
        sub_process,
        test_name,
        x_coord,
        y_coord,
        result
    FROM test_data
    WHERE lot_id = 'E6B155.00'
       AND wafer_id = 'E6B155-04A4'
    AND (test_name LIKE '%DTS2ACC1VL%' or test_name LIKE '%DTS2ACC1VT%' or test_name LIKE '%DTS2ACC1VH%')
    ORDER BY
        wafer_id,
        part_id,
        sub_process
    """
,"./Query/CP_DTS2.csv")

# %%
to_csv(f"""
    SELECT
        lot_id,
        part_id,
        sub_process,
        test_name,
        pin_num,
        pin_name,
        x_coord,
        y_coord,
        result
    FROM test_data_final
    WHERE lot_id IN ('2618-X00','2618-X01','2618-X02')
    AND test_name LIKE '%DTS2ACC1V%' 
    ORDER BY
        part_id,
    """
,"./Query/FT_DTS2.csv")


# %%
q(f"""
SELECT
    lot_id,
    wafer_id,
    part_id,
    x_coord,
    y_coord,
    test_num,
    test_name,
    pin_num,
    pin_name,
    result,
    retest_num
FROM test_data_final
WHERE lot_id = 'E6B155.00'
  AND wafer_id = 'E6B155-02B6'
  AND part_id = 'E6B155.00_E6B155-02B6_1'
  AND test_name = 'VDF_OBS:ALL_FIXL::'
ORDER BY pin_num;
""")

# %%
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
FROM test_data_final
WHERE lot_id = '{LOT_ID}'
ORDER BY test_num
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
FROM test_data_final
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
FROM parts_final
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
FROM parts_final p
JOIN test_data_final td
    ON  p.lot_id   = td.lot_id
    AND p.wafer_id = td.wafer_id
    AND p.part_id  = td.part_id
WHERE p.lot_id  = '{LOT_ID}'
  AND p.passed  = FALSE
  AND td.passed = 'F'
GROUP BY p.hard_bin, p.soft_bin, td.test_num, td.test_name
ORDER BY fail_count DESC
""")

# %%
to_csv(f"""
SELECT
    *
FROM test_data_final
WHERE wafer_id = 'E6B355-01D1'
  AND test_name like '%PDFREQ%'
ORDER BY part_id , test_num ASC
"""

,"./Query/PDFREQ.csv")

# %%
to_csv(f"""
    SELECT
        lot_id,
        part_id,
        sub_process,
        test_num,
        test_name,
        result
    FROM test_data_final
    WHERE part_id LIKE ('2618-X00%')
    AND test_name LIKE '%DTS2%' 
    ORDER BY
        part_id, test_num
    """
,"./Query/FT_2618.csv")
# %%
