import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    from stdf_platform.analysis import AnalysisSession

    s = AnalysisSession.for_notebook()
    return alt, mo, s


@app.cell
def _(mo):
    mo.md("""
    # SCT101B CP1 コア冗長 — ロット別トレンド

    **サンプル。直接編集せず、「名前を付けて保存」で `workspace/<自分の名前>/` にコピーして使う。**

    直近 3 か月のロットについて、ダイごとの `PASS2_JUDGE_PSDN_CPLT` / `PASS2_JUDGE_HNS_CPLT`
    (再検は最新結果)を数える。pass = ハードビン pass、probed = 実際に測ったダイ数(gross die なし)。
    """)
    return


@app.cell
def _():
    PRODUCT = "SCT101B"
    SUB = "CP1"
    MONTHS = 3
    MAX_N = 6  # 「共に 6 以下」
    PSDN = "PASS2_JUDGE_PSDN_CPLT"
    HNS = "PASS2_JUDGE_HNS_CPLT"
    return HNS, MAX_N, MONTHS, PRODUCT, PSDN, SUB


@app.cell
def _(HNS, MAX_N, MONTHS, PRODUCT, PSDN, SUB, mo, s):
    # 先に対象ロットを絞る(下の集計は、そのロットのパーティションしか読まない)。
    recent = s.q(
        """
        SELECT lot_id, MIN(start_time) AS start_time
        FROM lots
        WHERE test_category = 'CP' AND product = ? AND sub_process = ?
          AND start_time >= current_timestamp - to_months(?)
        GROUP BY lot_id
        """,
        [PRODUCT, SUB, MONTHS],
    )
    mo.stop(recent.empty, mo.callout(mo.md(
        f"{PRODUCT} / {SUB} に、直近 {MONTHS} か月のロットがありません。"), kind="warn"))

    lot_ids = recent["lot_id"].tolist()
    marks = ",".join("?" * len(lot_ids))  # IN の ? だけを組み立てる(値は bind)

    # parts_final / test_data_final は再検解決済み。ダイは lot_id + wafer_id + die_key で結ぶ。
    # ループ測定で 1 ダイに複数行あっても MAX で 1 値にする。NULL は条件を満たさない扱い。
    counts = s.q(
        f"""
        WITH judge AS (
            SELECT lot_id, wafer_id, die_key,
                   MAX(result) FILTER (WHERE test_name = ?) AS psdn,
                   MAX(result) FILTER (WHERE test_name = ?) AS hns
            FROM test_data_final
            WHERE test_category = 'CP' AND product = ? AND sub_process = ?
              AND lot_id IN ({marks}) AND test_name IN (?, ?)
            GROUP BY lot_id, wafer_id, die_key
        )
        SELECT p.lot_id,
               COUNT(*)                                      AS probed,
               COUNT(*) FILTER (WHERE p.passed)              AS pass_cnt,
               COUNT(*) FILTER (WHERE j.psdn = 0 AND j.hns = 0)        AS both_zero,
               COUNT(*) FILTER (WHERE j.psdn <= ? AND j.hns <= ?)      AS both_le_n,
               COUNT(*) FILTER (WHERE j.psdn IS NOT NULL AND j.hns IS NOT NULL) AS judged
        FROM parts_final p
        LEFT JOIN judge j
          ON p.lot_id = j.lot_id AND p.wafer_id = j.wafer_id AND p.die_key = j.die_key
        WHERE p.test_category = 'CP' AND p.product = ? AND p.sub_process = ?
          AND p.lot_id IN ({marks})
        GROUP BY p.lot_id
        """,
        [PSDN, HNS, PRODUCT, SUB, *lot_ids, PSDN, HNS, MAX_N, MAX_N,
         PRODUCT, SUB, *lot_ids],
    )
    df = counts.merge(recent, on="lot_id").sort_values(["start_time", "lot_id"], ignore_index=True)

    # 判定値が 1 ダイも無い = テスト名が違う。空のグラフを黙って出さない。
    mo.stop(int(df["judged"].sum()) == 0, mo.callout(mo.md(
        "ダイはありますが PSDN / HNS の測定値が 1 件もありません。テスト名を確認: "
        "`s.q(\"SELECT DISTINCT test_name FROM test_data_final "
        "WHERE sub_process = 'CP1' AND test_name LIKE '%JUDGE%'\")`"), kind="warn"))
    return (df,)


@app.cell
def _(MAX_N, alt, df, mo):
    names = {"pass_cnt": "pass(ハードビン)", "both_zero": "PSDN = HNS = 0",
             "both_le_n": f"PSDN, HNS ≤ {MAX_N}"}
    long = df.melt(id_vars=["lot_id"], value_vars=list(names), var_name="key", value_name="dies")
    long["series"] = long["key"].map(names)

    domain = list(names.values())
    chart = alt.Chart(long).mark_line(point=True, strokeWidth=2).encode(
        x=alt.X("lot_id:N", sort=df["lot_id"].tolist(), title="ロット(開始順)",
                axis=alt.Axis(labelAngle=-45)),
        y=alt.Y("dies:Q", title="ダイ数"),
        color=alt.Color("series:N", title=None, legend=alt.Legend(orient="top"),
                        scale=alt.Scale(domain=domain, range=["#2a78d6", "#eb6834", "#1baf7a"])),
        strokeDash=alt.StrokeDash("series:N", legend=None,
                                  scale=alt.Scale(domain=domain, range=[[1, 0], [6, 3], [2, 2]])),
        tooltip=["lot_id:N", "series:N", "dies:Q"],
    ).properties(height=380, width="container")
    mo.ui.altair_chart(chart)
    return (long,)


@app.cell
def _(df):
    df  # グラフと同じ数字(CSV にコピーできる)
    return


if __name__ == "__main__":
    app.run()
