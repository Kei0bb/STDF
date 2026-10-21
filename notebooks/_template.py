import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from stdf_platform.analysis import AnalysisSession

    # 引数なし = config.yaml (または環境変数 STDF_CONFIG) の storage.data_dir を読む。
    s = AnalysisSession()
    con = s.conn

    # 共有マシンで同時に何人も動かすので、1カーネルが使うメモリ/スレッドを絞る。
    # 値は共有マシンの搭載量に合わせて調整する(docs/marimo-server.md)。
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 4")
    return con, mo, s


@app.cell
def _(mo):
    mo.md("""
    # stdf 解析ノートブック

    `cp notebooks/_template.py notebooks/<自分の名前>/<題材>.py` して使う。
    **自分のフォルダ以外のノートブックは編集しない。**
    使い回すクエリは `sql/` に `.sql` として昇格させる(`sql/README.md`)。
    """)
    return


@app.cell
def _(mo, s):
    lots = s.lots()
    lot_picker = mo.ui.dropdown(
        options=lots["lot_id"].tolist(),
        value=lots["lot_id"].iloc[0] if len(lots) else None,
        label="ロット",
    )
    lot_picker
    return (lot_picker,)


@app.cell
def _(lot_picker, mo, s):
    mo.stop(lot_picker.value is None, mo.md("ロットを選んでください"))
    # 名前付きクエリ(sql/)。条件は引数で渡す。s.queries() で一覧が見える。
    s.run("fail_ranking", lot=lot_picker.value)
    return


@app.cell
def _(con, lot_picker, mo):
    mo.stop(lot_picker.value is None)
    lot = lot_picker.value
    # SQL セル: *_final ビュー(retest 解決済み)をそのまま読める。
    wafer_yield = mo.sql(
        f"""
        SELECT * FROM wafer_yield_final
        WHERE lot_id = '{lot}'
        ORDER BY wafer_id
        """,
        engine=con,
    )
    return


@app.cell
def _(s):
    # 使えるビュー / 名前付きクエリの一覧
    s.queries()
    return


if __name__ == "__main__":
    app.run()
