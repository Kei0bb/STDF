"""sql/ — 名前付きクエリライブラリ。

1ファイル1クエリ。ファイルは**自己完結**していて、エディタで開いてそのまま
流しても動く(冒頭の `SET VARIABLE` が既定値を与える)。同じファイルを
`AnalysisSession.run()` から呼ぶと、渡したパラメータが既定値を上書きする。

    -- sql/03_test/fail_ranking.sql
    -- ロット内 Fail テストランキング
    SET VARIABLE lot = 'LOT001';      -- ← 開いて実行するときはここを書き換える

    SELECT ... WHERE lot_id = getvariable('lot') ...

    >>> s.run("03_test/fail_ranking", lot="LOT002")     # 既定値を上書き
    >>> s.run("05_export/die_test", lot="LOT002", out="x.csv")   # COPY TO で直接 CSV

パラメータは必ずバインド変数として渡す(f-string 埋め込みをしない)ので、
クォートを含むロット名でも壊れないし、SQL 断片を渡されても値としてしか
解釈されない。
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

# リポジトリ同梱の sql/ (src/stdf_platform/analysis/library.py → リポジトリルート)
DEFAULT_SQL_DIR = Path(__file__).resolve().parents[3] / "sql"


def resolve_sql_dir(sql_dir: Path | str | None = None) -> Path:
    return Path(sql_dir) if sql_dir is not None else DEFAULT_SQL_DIR


def list_queries(sql_dir: Path | str | None = None) -> pd.DataFrame:
    """`name` / `description` の一覧(フォルダ順)を返す。"""
    root = resolve_sql_dir(sql_dir)
    rows = []
    for f in sorted(root.rglob("*.sql")):
        rows.append({
            "name": f.relative_to(root).with_suffix("").as_posix(),
            "description": describe(f),
        })
    return pd.DataFrame(rows, columns=["name", "description"])


def describe(path: Path) -> str:
    """先頭の `--` コメント行を説明として返す。"""
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("--"):
            return line.lstrip("-").strip()
        if line:
            break
    return ""


def find_query(name: str, sql_dir: Path | str | None = None) -> Path:
    """"03_test/fail_ranking" でも "fail_ranking" でも引けるようにする。"""
    root = resolve_sql_dir(sql_dir)
    if not root.exists():
        raise FileNotFoundError(f"クエリライブラリが見つかりません: {root}")
    stem = name[:-4] if name.endswith(".sql") else name
    exact = root / f"{stem}.sql"
    if exact.exists():
        return exact
    matches = [f for f in root.rglob("*.sql") if f.stem == Path(stem).name]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        available = ", ".join(list_queries(root)["name"]) or "(空)"
        raise FileNotFoundError(f"クエリ '{name}' がありません。利用可能: {available}")
    dup = ", ".join(m.relative_to(root).with_suffix("").as_posix() for m in matches)
    raise ValueError(f"'{name}' は複数あります。フォルダ込みで指定してください: {dup}")


def run_query(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    out: Path | str | None = None,
    sql_dir: Path | str | None = None,
    **params: object,
) -> pd.DataFrame | int:
    """クエリファイルを実行する。out を渡すと CSV に直接書き出す。

    ファイル内の `SET VARIABLE` は既定値なので**先に**流し、そのあとに
    params を当てる — 呼び出し側が必ず勝つ。最後の文が結果を返す文。
    out を指定した場合は最後の文を COPY() で包むので、DuckDB が直接ファイルに
    書き、Python 側にデータが載らない(大きなエクスポート向け)。
    """
    path = find_query(name, sql_dir)
    statements = duckdb.extract_statements(path.read_text(encoding="utf-8"))
    if not statements:
        raise ValueError(f"{path} に実行できる文がありません")

    for st in statements[:-1]:            # 既定値の SET VARIABLE など
        conn.execute(st.query)
    for key, value in params.items():     # 呼び出し側の指定で上書き
        conn.execute(f"SET VARIABLE {_identifier(key)} = ?", [value])

    final = statements[-1].query.rstrip("; \n")
    if out is None:
        return conn.execute(final).fetchdf()
    out = Path(out)
    rows = conn.execute(
        f"COPY ({final}) TO '{sql_literal(out.as_posix())}' (HEADER, DELIMITER ',')"
    ).fetchone()[0]
    return rows


def sql_literal(value: str) -> str:
    """SQL シングルクォート文字列リテラルに埋め込めるよう ' を '' にする。

    パス等を f-string で SQL に埋め込む箇所専用(値のバインドが可能な場所では
    バインドを使うこと)。
    """
    return value.replace("'", "''")


def _identifier(name: str) -> str:
    """変数名として安全なものだけ通す(値はバインドするのでここだけ検査)。"""
    if not name.isidentifier():
        raise ValueError(f"パラメータ名が不正です: {name!r}")
    return name
