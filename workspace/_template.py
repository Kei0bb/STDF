import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from stdf_platform.analysis import AnalysisSession

    # 引数なし = config.yaml (または環境変数 STDF_CONFIG) の storage.data_dir を読む。
    # for_notebook() は DuckDB の既定接続(mo.sql が engine 無しで使う接続)にビューと
    # 資源制限(config.yaml の notebook: 節)を掛けるので、SQL セルでも *_final を読める。
    s = AnalysisSession.for_notebook()
    return mo, s


@app.cell
def _(mo):
    mo.md("""
    # stdf 解析ノートブック

    **このファイルは直接編集しない。** 「名前を付けて保存」で
    `workspace/<自分の名前>/<題材>.py` にコピーしてから使う。
    **自分のフォルダ以外のノートブックは編集しない。**
    使い回すクエリは `sql/` に `.sql` として昇格させる(`sql/README.md`)。
    """)
    return


@app.cell
def _(mo, s):
    lots = s.lots()
    # CP/FT 両方に同じロット ID があり得るので重複を除く。
    lot_ids = lots["lot_id"].drop_duplicates().tolist()
    lot_picker = mo.ui.dropdown(
        options=lot_ids,
        value=lot_ids[0] if lot_ids else None,
        label="ロット",
    )
    lot_picker
    return (lot_picker,)


@app.cell
def _(lot_picker, mo, s):
    mo.stop(lot_picker.value is None, mo.md("ロットを選んでください"))
    # 名前付きクエリ(sql/)。条件は引数(bind 変数)で渡す — SQL に値を埋め込まない。
    # s.queries() で一覧と受け取れる変数が見える。
    s.run("fail_ranking", lot=lot_picker.value)
    return


@app.cell
def _(lot_picker, mo, s):
    mo.stop(lot_picker.value is None)
    s.run("wafer_yield", lot=lot_picker.value)
    return


@app.cell
def _(mo):
    # SQL セル: engine を指定しなくても、for_notebook() が載せたビュー(lots / *_final
    # など、retest 解決済み)を読める。値を絞るときは f-string で埋めず、上のように
    # s.run() か s.q("... WHERE x = ?", [値]) を使う。
    mo.sql(
        """
        SELECT lot_id, product, test_category, start_time
        FROM lots
        ORDER BY start_time DESC
        LIMIT 10
        """
    )
    return


@app.cell
def _(s):
    # 使えるビュー / 名前付きクエリの一覧
    s.queries()
    return


if __name__ == "__main__":
    app.run()
