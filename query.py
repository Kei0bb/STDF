# %% [markdown]
# # stdf — SQL Query Playground
#
# VS Code で開いて「Run Cell」または Shift+Enter で各セルを実行。
# Python Interactive Window (Jupyter 拡張) が必要です。
#
# **参考:** `docs/sample_queries.md` にクエリ集あり
#
# ## 主な関数
# - `q(sql, limit=100, as_arrow=False)` — プレビュー用 DataFrame 返却
#   （limit=0 で全件 / as_arrow=True で pyarrow.Table を高速取得）
# - `to_csv(sql, output)` — メモリ効率的な CSV 書き出し（DuckDB COPY）
# - `use_lot('LOT_ID')` — 対象ロットの *_final を materialize（以降 ~45x 高速）
# - `use_all()` — *_final を全ロット対象のビューに復元

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

from stdf_platform.views import _DEDUP_UNIT, setup_views

con = duckdb.connect(":memory:")
for _name in setup_views(con, DATA_DIR):
    print(f"  ✓ {_name}")

# retest 重複排除ビュー（全ロット）。ROW_NUMBER() ウィンドウを毎クエリ計算するため
# 大規模だと重い → 1 ロットに絞って作業するなら use_lot() で materialize 推奨。
_FINAL_SPECS = {
    "parts_final":     ("parts",     f"PARTITION BY lot_id, {_DEDUP_UNIT}"),
    "test_data_final": ("test_data", f"PARTITION BY lot_id, {_DEDUP_UNIT}, test_num, pin_num"),
    "chipid_final":    ("chipid",    "PARTITION BY lot_id, efuse_raw"),
}


def _make_final_views() -> None:
    """全ロット対象の *_final ビューを (再)作成。"""
    for name, (base, part) in _FINAL_SPECS.items():
        if (DATA_DIR / base).exists():
            con.execute(f"""
                CREATE OR REPLACE VIEW {name} AS
                SELECT * EXCLUDE (rn) FROM (
                    SELECT *, ROW_NUMBER() OVER ({part} ORDER BY retest_num DESC) AS rn
                    FROM {base}
                ) WHERE rn = 1
            """)
            print(f"  ✓ {name}")


_make_final_views()

LOT_ID: str | None = None   # use_lot() が設定。各セルでも上書き可。

print("\nReady (DuckDB).  q(sql) でプレビュー、to_csv(sql) で CSV 書き出し。")
print("大量データは use_lot('LOT_ID') で対象ロットを materialize（以降 ~45x 高速）。")


def use_lot(lot: str) -> None:
    """対象ロットの *_final を temp テーブルに materialize し、ビューを差し替える。

    以降 parts_final / test_data_final / chipid_final への q() / to_csv() は、
    そのロットだけを保持する小さな表を引くため、ROW_NUMBER() ウィンドウの
    毎回再計算が消えて大幅に高速化（実測 ~45x）。全ロットに戻すには use_all()。
    """
    global LOT_ID
    LOT_ID = lot
    done = []
    for name, (base, part) in _FINAL_SPECS.items():
        if not (DATA_DIR / base).exists():
            continue
        cache = f"_lot_{name}"
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {cache} AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER ({part} ORDER BY retest_num DESC) AS rn
                FROM {base} WHERE lot_id = ?
            ) WHERE rn = 1
        """, [lot])
        con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {cache}")
        n = con.execute(f"SELECT COUNT(*) FROM {cache}").fetchone()[0]
        done.append(f"{name}={n:,}")
    print(f"use_lot({lot!r}): " + ", ".join(done) + "  (use_all() で全ロットに復元)")


def use_all() -> None:
    """*_final を全ロット対象のビューに戻す（dedup を毎クエリ再計算）。"""
    global LOT_ID
    LOT_ID = None
    _make_final_views()
    print("use_all(): 全ロットの *_final ビューを復元しました。")


def q(sql: str, limit: int = 100, as_arrow: bool = False):
    """SQL を実行して結果を返す（プレビュー用）。limit=0 で全件取得。

    大量データを取得する場合:
      - as_arrow=True で pyarrow.Table を返す（pandas 変換を省き ~3.5x 高速）
      - もしくは to_csv() でファイルへ直接書き出し（Python メモリ消費ゼロ）
    """
    if limit > 0 and "LIMIT" not in sql.upper():
        sql = sql.rstrip("; \n") + f"\nLIMIT {limit}"
    res = con.execute(sql)
    return res.fetch_arrow_table() if as_arrow else res.fetchdf()


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
FROM test_data_final
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
FROM test_data_final
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


# %% CSV 書き出し — メモリ効率版（to_csv を使用）
# DuckDB COPY TO で Python メモリ消費ゼロ

to_csv(
    f"""
    SELECT
        p.lot_id, p.wafer_id, p.part_id,
        p.x_coord, p.y_coord,
        p.hard_bin, p.soft_bin, p.passed AS die_passed,
        td.test_num, td.test_name,
        td.result, td.lo_limit, td.hi_limit, td.units, td.passed AS test_passed
    FROM parts_final p
    JOIN test_data_final td
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
