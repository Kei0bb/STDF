import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from stdf_platform.analysis import AnalysisSession

    s = AnalysisSession.for_notebook()
    return mo, s


@app.cell
def _(mo):
    mo.md("""
    # FT: bin とサンプル数の突き合わせ

    **サンプル。直接編集せず、「名前を付けて保存」で `workspace/<自分の名前>/` にコピーして使う。**

    別 DB と **母数 / fail bin 数が合わない**ときに、数え方のどこで差が出ているかを見る。
    FT ロット(× sub_process)を選ぶと、次の順に出る。

    1. **母数の内訳** — 同じロットを「数え方」別に並べる(全挿入 / 初回のみ / 最新=本プラットフォーム)
    2. **bin × 数え方** — bin ごとに同じ 3 通りの数を横に並べる
    3. **再検で bin が変わったダイ** — 初回 bin → 最新 bin の遷移(差の主因になりやすい)
    4. **サンプル一覧** — 選んだ bin に入ったパッケージを 1 件ずつ(再検の出現も全部)

    本プラットフォームの数は **最新=`retest_flag = 0`**(`parts_final` と同じ)。FT のパッケージ同一性は
    eFuse の `chip_key` のみで、eFuse が無いパッケージは再検でも同一と見なせず**別個に数えられる**
    (下の「eFuse 無し」の件数がその分)。別 DB が「初回のみ」「全挿入」「バーコード単位」のどれで
    数えているかを、この表と見比べて当てる。
    """)
    return


@app.cell
def _(mo, s):
    ft_lots = s.q(
        """
        SELECT lot_id, sub_process, product, MIN(start_time) AS start_time
        FROM lots
        WHERE test_category = 'FT'
        GROUP BY lot_id, sub_process, product
        ORDER BY start_time DESC
        """
    )
    mo.stop(ft_lots.empty, mo.callout(mo.md("FT のロットがありません"), kind="warn"))
    # 同じロットが sub_process 違いで複数あるので「ロット / sub_process」を 1 つの選択肢にする
    # (混ぜない)。値は (lot_id, sub_process) のタプル。
    options = {
        f"{r.lot_id} / {r.sub_process}  ({r.product})": (r.lot_id, r.sub_process)
        for r in ft_lots.itertuples()
    }
    lot_picker = mo.ui.dropdown(options=options, value=next(iter(options)), label="FT ロット")
    bin_kind = mo.ui.radio(
        options={"ハードビン": "hard_bin", "ソフトビン": "soft_bin"},
        value="ハードビン",
        label="bin の種類",
        inline=True,
    )
    mo.hstack([lot_picker, bin_kind], justify="start", gap=2)
    return bin_kind, lot_picker


@app.cell
def _(lot_picker, mo, s):
    mo.stop(lot_picker.value is None)
    lot, sub = lot_picker.value

    # 1) 母数の内訳。`parts` を直接読む(`parts_final` は最新だけなので比較にならない)。
    #    retest_flag は ingest 時に付く: 0 = そのパッケージの最新の出現。
    pop = s.q(
        """
        SELECT
            COUNT(*)                                          AS all_rows,
            COUNT(DISTINCT die_key)                           AS distinct_pkg,
            COUNT(*) FILTER (WHERE retest_num = 0)            AS first_rows,
            COUNT(*) FILTER (WHERE retest_flag = 0)           AS final_rows,
            COUNT(*) FILTER (WHERE retest_flag = 0 AND passed)          AS final_pass,
            COUNT(*) FILTER (WHERE retest_flag = 0 AND NOT passed)      AS final_fail,
            COUNT(*) FILTER (WHERE retest_flag = 0 AND passed IS NULL)  AS final_unknown,
            COUNT(*) FILTER (WHERE retest_flag IS NULL)       AS flag_null,
            COUNT(*) FILTER (WHERE chip_key IS NULL OR chip_key = '')   AS no_efuse_all,
            COUNT(*) FILTER (WHERE retest_flag = 0
                              AND (chip_key IS NULL OR chip_key = ''))  AS no_efuse_final
        FROM (
            SELECT *, CASE WHEN x_coord = -32768 AND y_coord = -32768
                      THEN COALESCE(NULLIF(chip_key, ''),
                                    'NOEF|' || CAST(retest_num AS VARCHAR) || '|' || part_id)
                      END AS die_key
            FROM parts
            WHERE test_category = 'FT' AND lot_id = ? AND sub_process = ?
        )
        """,
        [lot, sub],
    ).iloc[0]

    files = s.q(
        """
        SELECT retest_num, source_file, node_name, job_name, job_rev, start_time
        FROM runs
        WHERE test_category = 'FT' AND lot_id = ? AND sub_process = ?
        ORDER BY retest_num
        """,
        [lot, sub],
    )

    n = int(pop["all_rows"])
    summary = mo.md(f"""
    ## 1. 母数の内訳 — `{lot}` / `{sub}`

    | 数え方 | 母数 | 説明 |
    |---|---:|---|
    | **全挿入** | **{n}** | `parts` の全行。再検・再挿入を全部足した数(ファイル {len(files)} 個) |
    | **初回のみ** | **{int(pop['first_rows'])}** | `retest_num = 0` の行だけ |
    | **最新(本 DB)** | **{int(pop['final_rows'])}** | `retest_flag = 0`。= `parts_final`。pass {int(pop['final_pass'])} / fail {int(pop['final_fail'])} / 合否無効 {int(pop['final_unknown'])} |
    | 参考: 一意パッケージ | {int(pop['distinct_pkg'])} | 全挿入を `chip_key`(eFuse)で重ねた数。eFuse 無しは重ならず別個に数える |

    **eFuse 無し** の行: 全挿入 {int(pop['no_efuse_all'])} / うち最新 {int(pop['no_efuse_final'])}
    {"— これは再検しても同一パッケージと判定できないので、「最新」の母数に**再検の分が二重に入り得る**。" if int(pop['no_efuse_final']) else ""}
    {"" if int(pop['flag_null']) == 0 else f"**⚠ retest_flag が NULL の行が {int(pop['flag_null'])} 件**(古いコードで ingest されたファイル。wipe + 再 ingest が必要)。"}
    """)
    mo.vstack([summary, mo.ui.table(files, selection=None, label="このロットの取り込みファイル(retest_num 順)")])
    return lot, sub


@app.cell
def _(bin_kind, lot, mo, s, sub):
    # 2) bin × 数え方。bin の種類は radio の固定値(hard_bin / soft_bin)だけを列名として
    #    使う。ロット等の値は bind。
    col = bin_kind.value
    assert col in ("hard_bin", "soft_bin")
    by_bin = s.q(
        f"""
        SELECT {col} AS bin,
               COUNT(*)                                           AS "全挿入",
               COUNT(*) FILTER (WHERE retest_num = 0)             AS "初回のみ",
               COUNT(*) FILTER (WHERE retest_flag = 0)            AS "最新(本DB)",
               COUNT(*) FILTER (WHERE retest_flag = 0 AND passed)         AS "最新 pass",
               COUNT(*) FILTER (WHERE retest_flag = 0 AND NOT passed)     AS "最新 fail",
               COUNT(*) FILTER (WHERE retest_flag = 0 AND passed IS NULL) AS "最新 合否無効"
        FROM parts
        WHERE test_category = 'FT' AND lot_id = ? AND sub_process = ?
        GROUP BY {col}
        ORDER BY {col}
        """,
        [lot, sub],
    )
    total = by_bin.drop(columns="bin").sum()
    total_row = {"bin": "合計", **{k: int(v) for k, v in total.items()}}
    by_bin_with_total = by_bin.copy()
    by_bin_with_total["bin"] = by_bin_with_total["bin"].astype("string")
    by_bin_with_total.loc[len(by_bin_with_total)] = total_row

    # 「最新 fail」は PRR の合否フラグ基準。bin が pass bin 以外なのに passed=True(またはその逆)
    # の bin があれば、別 DB が bin 番号で fail を数えているときにずれる。
    mixed = by_bin[(by_bin["最新 pass"] > 0) & (by_bin["最新 fail"] > 0)]
    note = (
        mo.callout(
            mo.md(f"bin {', '.join(map(str, mixed['bin']))} は **pass と fail が混在**しています"
                  "(PRR の合否フラグと bin 番号が一致しない)。bin 番号で fail を数える DB とは合いません。"),
            kind="warn",
        )
        if not mixed.empty
        else mo.md("")
    )
    mo.vstack([
        mo.md(f"## 2. bin × 数え方 — `{lot}` / `{sub}`({'ハードビン' if col == 'hard_bin' else 'ソフトビン'})"),
        mo.ui.table(by_bin_with_total, selection=None, page_size=50),
        note,
    ])
    return by_bin, col


@app.cell
def _(col, lot, mo, s, sub):
    # 3) 再検でパッケージの bin が変わったもの。初回の出現と最新の出現を eFuse で突き合わせる
    #    (eFuse 無しは突き合わせられないので対象外)。
    moved = s.q(
        f"""
        WITH occ AS (
            SELECT chip_key, {col} AS bin, retest_num, retest_flag,
                   ROW_NUMBER() OVER (PARTITION BY chip_key ORDER BY retest_num, part_seq) AS first_rank
            FROM parts
            WHERE test_category = 'FT' AND lot_id = ? AND sub_process = ?
              AND chip_key IS NOT NULL AND chip_key <> ''
        ),
        pair AS (
            SELECT f.chip_key, f.bin AS first_bin, l.bin AS final_bin
            FROM occ f
            JOIN occ l ON l.chip_key = f.chip_key AND l.retest_flag = 0
            WHERE f.first_rank = 1
        )
        SELECT first_bin AS "初回 bin", final_bin AS "最新 bin", COUNT(*) AS "パッケージ数"
        FROM pair
        WHERE first_bin IS DISTINCT FROM final_bin
        GROUP BY ALL
        ORDER BY "パッケージ数" DESC
        """,
        [lot, sub],
    )
    mo.vstack([
        mo.md("## 3. 再検で bin が変わったパッケージ(初回 → 最新)\n\n"
              "別 DB が初回 bin で数えていれば、ここに出た分だけ bin ごとの数がずれる。"),
        mo.ui.table(moved, selection=None) if not moved.empty else mo.md("*bin が変わったパッケージはありません*"),
    ])
    return


@app.cell
def _(by_bin, mo):
    bins = by_bin["bin"].dropna().astype(int).tolist()
    bin_picker = mo.ui.dropdown(
        options={str(b): b for b in bins},
        value=str(bins[0]) if bins else None,
        label="サンプルを見る bin",
    )
    only_final = mo.ui.checkbox(label="最新(retest_flag=0)の行だけ", value=False)
    mo.vstack([mo.md("## 4. サンプル一覧"), mo.hstack([bin_picker, only_final], justify="start", gap=2)])
    return bin_picker, only_final


@app.cell
def _(bin_picker, col, lot, mo, only_final, s, sub):
    mo.stop(bin_picker.value is None)
    # 選んだ bin に入った行を 1 件ずつ。再検の出現も出す(採用 = 本 DB が数える行)。
    # 取り込みファイルは (lot, sub_process, retest_num) で runs に結ぶ(FT は wafer_id='' で 1 run/ファイル)。
    samples = s.q(
        f"""
        SELECT p.retest_num, p.retest_flag = 0 AS "採用", p.chip_key, p.part_txt, p.part_serial,
               p.site_num, p.hard_bin, p.soft_bin, p.passed, p.chip_status, r.source_file
        FROM parts p
        LEFT JOIN runs r
          ON r.lot_id = p.lot_id AND r.test_category = p.test_category
         AND r.sub_process = p.sub_process AND r.wafer_id = p.wafer_id
         AND r.retest_num = p.retest_num
        WHERE p.test_category = 'FT' AND p.lot_id = ? AND p.sub_process = ?
          AND p.{col} = ?
          {"AND p.retest_flag = 0" if only_final.value else ""}
        ORDER BY p.chip_key, p.retest_num, p.part_seq
        """,
        [lot, sub, bin_picker.value],
    )
    mo.vstack([
        mo.md(f"bin **{bin_picker.value}** — {len(samples)} 行(うち採用 {int(samples['採用'].sum())})"),
        mo.ui.table(samples, selection=None, page_size=25),
    ])
    return


if __name__ == "__main__":
    app.run()
