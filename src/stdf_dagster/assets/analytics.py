"""Analytics assets: yield summary, bin distribution, and test fail ranking."""

import pandas as pd

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from stdf_dagster.resources.duckdb_resource import DuckDBResource


@asset(
    description="Product × Lot 単位の歩留まりサマリ。最新リテストのみ使用",
    group_name="analytics",
    deps=["duckdb_views"],
    kinds={"duckdb", "pandas"},
)
def yield_summary(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Compute yield summary across all products and lots.

    Joins lots and wafers tables, using only the latest retest
    for each wafer. Returns product-level and lot-level yield metrics.
    """
    db = duckdb.get_database()
    with db:
        try:
            df = db.query_df("""
                SELECT
                    l.product,
                    l.test_category,
                    l.sub_process,
                    l.lot_id,
                    l.job_name,
                    COUNT(DISTINCT w.wafer_id) AS wafer_count,
                    SUM(w.part_count) AS total_parts,
                    SUM(w.good_count) AS good_parts,
                    ROUND(100.0 * SUM(w.good_count) / NULLIF(SUM(w.part_count), 0), 2) AS yield_pct
                FROM lots l
                LEFT JOIN (
                    SELECT *, ROW_NUMBER() OVER(
                        PARTITION BY lot_id, wafer_id ORDER BY retest_num DESC
                    ) AS rn
                    FROM wafers
                ) w ON l.lot_id = w.lot_id AND w.rn = 1
                GROUP BY l.product, l.test_category, l.sub_process, l.lot_id, l.job_name
                ORDER BY l.product, l.test_category, l.lot_id
            """)
        except Exception as e:
            context.log.warning(f"Could not compute yield summary: {e}")
            return MaterializeResult(
                metadata={"error": MetadataValue.text(str(e))}
            )

    context.log.info(f"Yield summary: {len(df)} lots")

    # Log product-level summary
    if not df.empty:
        product_summary = (
            df.groupby("product")
            .agg(
                lots=("lot_id", "nunique"),
                avg_yield=("yield_pct", "mean"),
                min_yield=("yield_pct", "min"),
                max_yield=("yield_pct", "max"),
            )
            .round(2)
        )
        context.log.info(f"\nProduct Summary:\n{product_summary.to_string()}")

        # Save as CSV for downstream use
        output_path = f"./data/analytics/yield_summary.csv"
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        context.log.info(f"Saved to {output_path}")

    return MaterializeResult(
        metadata={
            "lot_count": MetadataValue.int(len(df)),
            "product_count": MetadataValue.int(df["product"].nunique() if not df.empty else 0),
            "avg_yield_pct": MetadataValue.float(float(df["yield_pct"].mean()) if not df.empty else 0.0),
            "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if not df.empty else "_No data_"),
        }
    )


@asset(
    description="Soft Bin 分布集計。Product × Lot 単位",
    group_name="analytics",
    deps=["duckdb_views"],
    kinds={"duckdb", "pandas"},
)
def bin_distribution(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Compute soft bin distribution across all lots.

    Groups by product, lot_id, and soft_bin to show die counts
    and percentages per bin.
    """
    db = duckdb.get_database()
    with db:
        try:
            df = db.query_df("""
                SELECT
                    p.product,
                    p.lot_id,
                    p.soft_bin,
                    COUNT(*) AS die_count,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(
                        PARTITION BY p.product, p.lot_id
                    ), 2) AS pct
                FROM parts p
                GROUP BY p.product, p.lot_id, p.soft_bin
                ORDER BY p.product, p.lot_id, die_count DESC
            """)
        except Exception as e:
            context.log.warning(f"Could not compute bin distribution: {e}")
            return MaterializeResult(
                metadata={"error": MetadataValue.text(str(e))}
            )

    context.log.info(f"Bin distribution: {len(df)} rows")

    if not df.empty:
        output_path = "./data/analytics/bin_distribution.csv"
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(len(df)),
            "unique_bins": MetadataValue.int(int(df["soft_bin"].nunique()) if not df.empty else 0),
            "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if not df.empty else "_No data_"),
        }
    )


@asset(
    description="テスト項目別フェール率ランキング。フェール率が高い順にソート",
    group_name="analytics",
    deps=["duckdb_views"],
    kinds={"duckdb", "pandas"},
)
def test_fail_ranking(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Compute test fail rate ranking across all lots.

    Identifies the top failing test items ordered by fail rate,
    useful for yield improvement analysis.
    """
    db = duckdb.get_database()
    with db:
        try:
            df = db.query_df("""
                SELECT
                    td.product,
                    td.lot_id,
                    td.test_num,
                    td.test_name,
                    COUNT(*) AS total,
                    SUM(CASE WHEN td.passed = 'F' THEN 1 ELSE 0 END) AS fails,
                    ROUND(100.0 * SUM(CASE WHEN td.passed = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) AS fail_rate
                FROM test_data td
                GROUP BY td.product, td.lot_id, td.test_num, td.test_name
                HAVING SUM(CASE WHEN td.passed = 'F' THEN 1 ELSE 0 END) > 0
                ORDER BY fail_rate DESC
            """)
        except Exception as e:
            context.log.warning(f"Could not compute test fail ranking: {e}")
            return MaterializeResult(
                metadata={"error": MetadataValue.text(str(e))}
            )

    context.log.info(f"Test fail ranking: {len(df)} failing test items")

    if not df.empty:
        output_path = "./data/analytics/test_fail_ranking.csv"
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

    return MaterializeResult(
        metadata={
            "failing_tests": MetadataValue.int(len(df)),
            "avg_fail_rate": MetadataValue.float(float(df["fail_rate"].mean()) if not df.empty else 0.0),
            "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if not df.empty else "_No data_"),
        }
    )
