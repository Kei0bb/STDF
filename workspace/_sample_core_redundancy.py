import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly.graph_objects as go
    from stdf_platform.analysis import AnalysisSession

    s = AnalysisSession.for_notebook()
    return go, mo, s


@app.cell
def _(mo):
    mo.md("""
    # SCT101B CP1 コア冗長 — ロット別トレンド

    **サンプル。直接編集せず、「名前を付けて保存」で `workspace/<自分の名前>/` にコピーして使う。**

    直近 3 か月のロットについて、ダイごとの `PASS2_JUDGE_PSDN_CPLT` / `PASS2_JUDGE_HNS_CPLT`
    (再検は最新結果)から、次の順に進める: **ダイ表 → ロット表 → グラフ**。
    冗長の割合は **ハードビン pass かつ 冗長条件**(fail のダイは数えない)。PSDN は
    `PSDN_LINES` に並べたしきい値ごとに 1 本ずつ線になり、HNS は `MAX_HNS` で共通。
    `EXCLUDE_LOTS` に書いたロットは集計から外す。
    割合の**母数は gross die**: ウェハーごとに max(probed, gross_die) を求め、ロット内で合計する
    (`config.yaml` の `products.<製品>.gross_die`。未設定の製品は probed)。測っていないダイは
    母数に入り、分子には入らない。
    """)
    return


@app.cell
def _():
    PRODUCT = "SCT101B"
    SUB = "CP1"
    MONTHS = 3
    PSDN_LINES = [2, 4, 6]  # PSDN のしきい値。1 つにつき 1 本の線(PSDN ≤ n)
    MAX_HNS = 2  # HNS のしきい値(全ラインで共通)
    EXCLUDE_LOTS = []  # 除外するロット。例: ["A12345", "A12346"]
    PSDN = "PASS2_JUDGE_PSDN_CPLT"
    HNS = "PASS2_JUDGE_HNS_CPLT"
    return EXCLUDE_LOTS, HNS, MAX_HNS, MONTHS, PRODUCT, PSDN, PSDN_LINES, SUB


@app.cell
def _(EXCLUDE_LOTS, HNS, MONTHS, PRODUCT, PSDN, SUB, mo, s):
    # 1) query: 直近 MONTHS か月のロットを先に絞り(下の集計はそのパーティションしか読まない)、
    #    除外ロットを外してから、ダイごとの判定値を取る。
    #    parts_final / test_data_final は再検解決済み(最新の結果)。
    #    ダイは lot_id + wafer_id + die_key で結ぶ(part_id では結ばない)。
    #    ループ測定で 1 ダイに複数行あっても MAX で 1 値にする。
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
    skip = recent["lot_id"].isin(EXCLUDE_LOTS)
    # 除外の指定が期間内のロットに当たったか(打ち間違いに気づけるよう、両方を表示する)
    exclude_note = (
        f"除外したロット: {', '.join(recent.loc[skip, 'lot_id']) or 'なし'}"
        + (f" / 期間内に無かった指定: {', '.join(sorted(set(EXCLUDE_LOTS) - set(recent['lot_id'])))}"
           if set(EXCLUDE_LOTS) - set(recent["lot_id"]) else "")
    )
    recent = recent[~skip]
    mo.stop(recent.empty, mo.callout(mo.md(
        f"{PRODUCT} / {SUB} に、直近 {MONTHS} か月で除外後に残るロットがありません。"
        f"({exclude_note})"), kind="warn"))

    lot_ids = recent["lot_id"].tolist()
    marks = ",".join("?" * len(lot_ids))  # IN の ? だけを組み立てる(値は bind)

    die = s.q(
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
        SELECT p.lot_id, p.wafer_id, p.x_coord AS x, p.y_coord AS y,
               p.hard_bin, p.passed, j.psdn, j.hns
        FROM parts_final p
        LEFT JOIN judge j
          ON p.lot_id = j.lot_id AND p.wafer_id = j.wafer_id AND p.die_key = j.die_key
        WHERE p.test_category = 'CP' AND p.product = ? AND p.sub_process = ?
          AND p.lot_id IN ({marks})
        """,
        [PSDN, HNS, PRODUCT, SUB, *lot_ids, PSDN, HNS, PRODUCT, SUB, *lot_ids],
    )
    # 開始時刻順(古い → 新しい)。ロット表・グラフはこの順に並ぶ。
    die = (die.merge(recent, on="lot_id")
              .sort_values(["start_time", "lot_id", "wafer_id", "y", "x"], ignore_index=True))

    # 判定値が 1 ダイも無い = テスト名が違う。空のグラフを黙って出さない。
    mo.stop(not (die["psdn"].notna() & die["hns"].notna()).any(), mo.callout(mo.md(
        "ダイはありますが PSDN / HNS の測定値が 1 件もありません。テスト名を確認: "
        "`s.q(\"SELECT DISTINCT test_name FROM test_data_final "
        "WHERE sub_process = 'CP1' AND test_name LIKE '%JUDGE%'\")`"), kind="warn"))
    return die, exclude_note


@app.cell
def _(die, exclude_note, mo):
    # 2) ダイごとの core 冗長表(psdn / hns が NULL = そのダイの判定値なし)
    mo.vstack([mo.md(f"**ダイごとの core 冗長**({len(die):,} ダイ。{exclude_note})"), die])
    return


@app.cell
def _(MAX_HNS, PRODUCT, PSDN_LINES, die, mo, s):
    # 3) ロット表: yield %、PASS & 0 redun %、PSDN の各しきい値ごとの PASS & PSDN≤n & HNS≤MAX_HNS %。
    #    母数(total)= ウェハーごとの max(probed, gross_die) の合計。
    #    分子は測ったダイだけで、冗長の 2 種はハードビン pass も必須(NULL は条件を満たさない)。
    YIELD, ZERO = "yield %", "PASS & 0 redun %"
    LINE_NAMES = [f"PASS & PSDN≤{n} & HNS≤{MAX_HNS} %" for n in sorted(PSDN_LINES)]
    gd = s.q("SELECT gross_die FROM gross_die WHERE product = ?", [PRODUCT])
    GROSS = int(gd["gross_die"].iloc[0]) if not gd.empty else None

    passed = die["passed"].fillna(False).astype(bool)
    hit = {YIELD: passed, ZERO: passed & (die["psdn"] == 0) & (die["hns"] == 0)}
    for _name, _n in zip(LINE_NAMES, sorted(PSDN_LINES)):
        hit[_name] = passed & (die["psdn"] <= _n) & (die["hns"] <= MAX_HNS)
    flags = die[["lot_id", "wafer_id", "start_time"]].assign(**hit)

    probed_w = flags.groupby(["lot_id", "wafer_id"], sort=False).size()  # ウェハーごとの probed
    total_w = probed_w.clip(lower=GROSS) if GROSS else probed_w
    by_lot = flags.groupby("lot_id", sort=False)  # die は開始時刻順なので、その順を保つ
    lot = by_lot.agg(start_time=("start_time", "first"))
    lot["probed"] = by_lot.size()
    lot["total"] = total_w.groupby("lot_id", sort=False).sum()
    for _name in hit:
        lot[_name] = (by_lot[_name].sum() / lot["total"] * 100).round(2)
    lot = lot.reset_index()

    note = (mo.md(f"`{PRODUCT}` の gross_die が未設定なので、母数は probed。") if GROSS is None
            else mo.md(f"母数 = ウェハーごとの max(probed, gross_die = {GROSS}) の合計。"))
    mo.vstack([
        mo.md("**ロット別**(yield = ハードビン pass、`PASS & 0 redun` = pass かつ PSDN, HNS とも 0、"
              f"`PASS & PSDN≤n & HNS≤{MAX_HNS}` = pass かつ PSDN が n 以下かつ HNS が {MAX_HNS} 以下)"),
        note, lot,
    ])
    return LINE_NAMES, YIELD, ZERO, lot


@app.cell
def _(LINE_NAMES, YIELD, ZERO, go, lot):
    # 4) グラフ: 全系列を % で 1 つに。yield と 0 redun は固有の色、PSDN のしきい値は
    #    1 色(緑)の濃淡 = しきい値が大きいほど濃く、線種も変える(色だけに頼らない)。
    _greens = ["#8fd9b8", "#4cc596", "#1baf7a", "#12805a", "#0b5a3f"]
    _greens = _greens[max(0, len(_greens) - len(LINE_NAMES)):]
    _dashes = ["dot", "dashdot", "longdash", "longdashdot", "solid"]
    style = {YIELD: ("#2a78d6", "solid"), ZERO: ("#eb6834", "dash")}
    style.update({name: (_greens[i % len(_greens)], _dashes[i % len(_dashes)])
                  for i, name in enumerate(LINE_NAMES)})
    fig = go.Figure([
        go.Scatter(x=lot["lot_id"], y=lot[name], name=name, mode="lines+markers",
                   line=dict(color=color, dash=dash, width=2), marker=dict(size=8),
                   hovertemplate="%{y:.2f} %")
        for name, (color, dash) in style.items()
    ])
    fig.update_layout(
        template="plotly_white", height=420, hovermode="x unified",
        legend=dict(orientation="h", y=1.15, x=0),
        margin=dict(l=60, r=20, t=60, b=80),
        xaxis=dict(title="ロット(開始順)", type="category", tickangle=-45,
                   categoryorder="array", categoryarray=lot["lot_id"].tolist()),
        yaxis=dict(title="% of 母数", rangemode="tozero"),
    )
    fig
    return (fig,)


if __name__ == "__main__":
    app.run()
