import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    from stdf_platform.analysis import AnalysisSession

    # for_notebook() は DuckDB 既定接続にビュー(再検解決済みの *_final)と資源制限を掛ける。
    s = AnalysisSession.for_notebook()
    return alt, mo, s


@app.cell
def _(mo):
    mo.md("""
    # CP1 コア冗長 — ロット別トレンド

    **サンプル。このファイルは直接編集しない。** 「名前を付けて保存」で
    `workspace/<自分の名前>/` にコピーしてから使う。

    ダイごとの `PASS2_JUDGE_PSDN_CPLT` / `PASS2_JUDGE_HNS_CPLT`(再検は最新結果)を
    ロット単位で数え、1つのグラフに重ねる:

    - **pass**: ハードビン pass のダイ数
    - **PSDN = HNS = 0**: 両方とも 0
    - **PSDN, HNS ≤ 6**: 両方とも 6 以下(0 のダイを含む)

    対象は開始から**直近 N か月**のロットだけ(既定 3 か月)。集計 SQL はこのノートブック内
    (`s.q(...)` のセル)にある。gross die は適用しない(probed = 実際に測ったダイ数)。
    """)
    return


@app.cell
def _(mo, s):
    products = sorted(s.lots(test_category="CP")["product"].dropna().unique().tolist())
    product = mo.ui.dropdown(options=["(すべて)"] + products, value="(すべて)", label="製品")
    sub_process = mo.ui.text(value="CP1", label="工程")
    # 取得範囲の上限。大きくするほど Parquet の読み込み量が増える。
    months = mo.ui.slider(1, 6, value=3, label="直近(か月)", show_value=True)
    mode = mo.ui.radio(options=["個数", "割合 (% of probed)"], value="個数", inline=True)
    mo.hstack([product, sub_process, months, mode], justify="start", gap=2)
    return mode, months, product, sub_process


@app.cell
def _():
    PSDN = "PASS2_JUDGE_PSDN_CPLT"
    HNS = "PASS2_JUDGE_HNS_CPLT"
    MAX_N = 6  # 「共に 6 以下」
    return HNS, MAX_N, PSDN


@app.cell
def _(months, product, s, sub_process):
    # 直近 N か月に開始したロット(lots.start_time = そのロットの最初の run)。
    # ここで先に絞るので、下の集計は対象ロットのパーティションしか読まない。
    recent = s.q(
        """
        SELECT lot_id, product, start_time
        FROM lots
        WHERE test_category = 'CP' AND sub_process = ?
          AND start_time >= current_timestamp - to_months(?)
        """,
        [sub_process.value, months.value],
    )
    if product.value != "(すべて)":
        recent = recent[recent["product"] == product.value]
    return (recent,)


@app.cell
def _(HNS, MAX_N, PSDN, mo, months, recent, s, sub_process):
    mo.stop(
        recent.empty,
        mo.callout(
            mo.md(f"工程 `{sub_process.value}` に、直近 {months.value} か月に開始した CP ロットがありません。"),
            kind="warn",
        ),
    )
    lot_ids = recent["lot_id"].drop_duplicates().tolist()
    marks = ",".join("?" * len(lot_ids))  # 値ではなく ? だけを組み立てる(値は bind)

    # 集計: ロット別に、ダイ単位の判定値(PSDN, HNS)で数える。
    #   parts_final / test_data_final は再検解決済み(最新の結果)。ダイは
    #   lot_id + wafer_id + die_key で結ぶ(part_id では結ばない)。
    #   ループ測定で1ダイに複数行あっても MAX で1値に畳む。
    #   pass = ハードビン pass / judged = 両方の判定値がある / NULL は条件を満たさない扱い。
    df = s.q(
        f"""
        WITH judge AS (
            SELECT lot_id, wafer_id, die_key,
                   MAX(result) FILTER (WHERE test_name = ?) AS psdn,
                   MAX(result) FILTER (WHERE test_name = ?) AS hns
            FROM test_data_final
            WHERE test_category = 'CP' AND sub_process = ?
              AND lot_id IN ({marks})
              AND test_name IN (?, ?)
            GROUP BY lot_id, wafer_id, die_key
        ),
        die AS (
            SELECT p.lot_id, p.product, p.passed, j.psdn, j.hns
            FROM parts_final p
            LEFT JOIN judge j
              ON  p.lot_id = j.lot_id AND p.wafer_id = j.wafer_id AND p.die_key = j.die_key
            WHERE p.test_category = 'CP' AND p.sub_process = ?
              AND p.lot_id IN ({marks})
        )
        SELECT lot_id, product,
               COUNT(*)                                                    AS probed,
               COUNT(*) FILTER (WHERE passed)                              AS pass_cnt,
               COUNT(*) FILTER (WHERE psdn IS NOT NULL AND hns IS NOT NULL) AS judged,
               COUNT(*) FILTER (WHERE psdn = 0 AND hns = 0)                AS both_zero,
               COUNT(*) FILTER (WHERE psdn <= ? AND hns <= ?)              AS both_le_n
        FROM die
        GROUP BY lot_id, product
        """,
        [PSDN, HNS, sub_process.value, *lot_ids, PSDN, HNS,
         sub_process.value, *lot_ids, MAX_N, MAX_N],
    )
    # 開始時刻順(古い → 新しい)に並べる。
    df = (df.merge(recent.groupby(["lot_id", "product"], as_index=False)["start_time"].min(),
                   on=["lot_id", "product"])
            .sort_values(["start_time", "lot_id"], ignore_index=True))
    return (df,)


@app.cell
def _(df, mo):
    # 判定値が 1 ダイも取れていない = テスト名か工程が違う。0 件のグラフを黙って出さない。
    mo.stop(
        int(df["judged"].sum()) == 0,
        mo.callout(
            mo.md(
                "ダイは見つかりましたが `PASS2_JUDGE_PSDN_CPLT` / `PASS2_JUDGE_HNS_CPLT` の"
                "測定値が 1 件もありません。テスト名を確認: "
                "`s.q(\"SELECT DISTINCT test_name FROM test_data_final WHERE sub_process = ? "
                "AND test_name LIKE '%JUDGE%'\", [\"CP1\"])`"
            ),
            kind="warn",
        ),
    )
    return


@app.cell
def _(MAX_N, alt, df, mode, mo):
    le = f"PSDN, HNS ≤ {MAX_N}"
    series = {
        "pass_cnt": "pass(ハードビン)",
        "both_zero": "PSDN = HNS = 0",
        "both_le_n": le,
    }
    long = df.melt(
        id_vars=["lot_id", "start_time", "probed"],
        value_vars=list(series),
        var_name="key",
        value_name="count",
    )
    long["series"] = long["key"].map(series)
    pct = mode.value.startswith("割合")
    long["value"] = long["count"] / long["probed"] * 100 if pct else long["count"]

    lot_order = df["lot_id"].tolist()  # start_time 昇順(古い → 新しい)
    names = list(series.values())
    # 色 + 線種の二重符号化(色だけに頼らない)。青 / オレンジ / アクア。
    scale_c = alt.Scale(domain=names, range=["#2a78d6", "#eb6834", "#1baf7a"])
    scale_d = alt.Scale(domain=names, range=[[1, 0], [6, 3], [2, 2]])

    base = alt.Chart(long).encode(
        x=alt.X("lot_id:N", sort=lot_order, title="ロット(開始時刻順)",
                axis=alt.Axis(labelAngle=-45)),
        y=alt.Y("value:Q", title="ダイの割合 (%)" if pct else "ダイ数",
                scale=alt.Scale(zero=True)),
        color=alt.Color("series:N", scale=scale_c, title=None,
                        legend=alt.Legend(orient="top")),
        strokeDash=alt.StrokeDash("series:N", scale=scale_d, legend=None),
        tooltip=[
            alt.Tooltip("lot_id:N", title="ロット"),
            alt.Tooltip("series:N", title="系列"),
            alt.Tooltip("count:Q", title="ダイ数", format=","),
            alt.Tooltip("probed:Q", title="probed", format=","),
            alt.Tooltip("value:Q", title="値", format=",.2f" if pct else ",.0f"),
        ],
    )
    chart = (base.mark_line(strokeWidth=2) + base.mark_point(size=60, filled=True, opacity=1)
             ).properties(height=380, width="container")
    mo.ui.altair_chart(chart)
    return


@app.cell
def _(df):
    # 同じ数字の表(グラフ外でも読める / CSV にコピーできる)
    out = df.copy()
    out["pass_%"] = (out["pass_cnt"] / out["probed"] * 100).round(2)
    out["both_zero_%"] = (out["both_zero"] / out["probed"] * 100).round(2)
    out["both_le_6_%"] = (out["both_le_n"] / out["probed"] * 100).round(2)
    out
    return


if __name__ == "__main__":
    app.run()
