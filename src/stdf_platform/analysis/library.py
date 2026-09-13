"""名前付きクエリライブラリ。1ファイル1クエリ、Python の `AnalysisSession.run()` から呼ぶ。

置き場所は2つ。同じ名前なら個人用が勝つ:

  - 個人用  <リポジトリ>/sql/              gitignore。本番機で手元改造するクエリ
  - 同梱    src/stdf_platform/sql/         git 管理。テストとコードが使う定番クエリ

変数(条件)は呼び出し側がキーワード引数で渡す。いくつでも同時に渡せる:

    -- sql/10_mine/lot_filter.sql
    -- 条件付きロット一覧
    SET VARIABLE product  = NULL;       -- 既定値 NULL = 任意条件(渡さなければ絞らない)
    SET VARIABLE category = NULL;

    SELECT * FROM lots
    WHERE opt_eq(product,       getvariable('product'))
      AND opt_eq(test_category, getvariable('category'))

    >>> s.run("lot_filter", product="P1", category="CP")   # 2条件
    >>> s.run("lot_filter", product="P1")                  # product だけ
    >>> s.run("wafer_yield", lot="LOT002", out="x.csv")    # COPY TO で直接 CSV

規約:
  - `getvariable('x')` を使っていて既定値が無い変数は**必須**。渡し忘れは TypeError
    (以前は NULL と比較されて 0 行が黙って返った)
  - クエリが使わない名前を渡したら TypeError(`prodcut=` の typo を黙って無視しない)
  - 変数は run() ごとにリセットする。前の run() で渡した値は次のクエリに残らない
    (DuckDB の変数は接続単位なので、放っておくと別クエリの条件に紛れ込む)
  - `opt_eq(col, v)` は v が NULL なら TRUE。DuckDB は getvariable() を計画時に
    定数化するので、指定した条件は Parquet スキャンまで押し込まれ、未指定の
    条件は計画から消える

値は必ずバインド変数として渡す(f-string 埋め込みをしない)ので、クォートを
含むロット名でも壊れないし、SQL 断片を渡されても値としてしか解釈されない。
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

# 同梱クエリ(src/stdf_platform/analysis/library.py → src/stdf_platform/sql)
PACKAGE_SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
# 個人用クエリ(リポジトリ直下の sql/。gitignore)
PERSONAL_SQL_DIR = Path(__file__).resolve().parents[3] / "sql"

_OPT_EQ_MACRO = "CREATE OR REPLACE TEMP MACRO opt_eq(col, v) AS (v IS NULL OR col = v)"


def _search_path(sql_dir: Path | str | None) -> list[tuple[str, Path]]:
    """探す順の (source, dir)。明示された sql_dir があればそこだけ。"""
    if sql_dir is not None:
        return [("explicit", Path(sql_dir))]
    return [("personal", PERSONAL_SQL_DIR), ("package", PACKAGE_SQL_DIR)]


def _catalog(sql_dir: Path | str | None) -> dict[str, tuple[str, Path]]:
    """name → (source, path)。先に見つかった方(個人用)が勝つ。"""
    found: dict[str, tuple[str, Path]] = {}
    for source, root in _search_path(sql_dir):
        if not root.is_dir():
            continue
        for f in sorted(root.rglob("*.sql")):
            name = f.relative_to(root).with_suffix("").as_posix()
            found.setdefault(name, (source, f))
    return found


def list_queries(sql_dir: Path | str | None = None) -> pd.DataFrame:
    """`name` / `description` / `params` / `source` の一覧(フォルダ順)を返す。

    params は必須変数をそのまま、任意変数を `name=既定値` で並べる。
    """
    rows = []
    for name, (source, path) in sorted(_catalog(sql_dir).items()):
        params = query_params(path)
        rows.append({
            "name": name,
            "description": describe(path),
            "params": ", ".join(k if v is None else f"{k}={v}" for k, v in params.items()),
            "source": source,
        })
    return pd.DataFrame(rows, columns=["name", "description", "params", "source"])


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
    dirs = _search_path(sql_dir)
    if sql_dir is not None and not Path(sql_dir).exists():
        raise FileNotFoundError(f"クエリライブラリが見つかりません: {sql_dir}")
    catalog = _catalog(sql_dir)
    stem = name[:-4] if name.endswith(".sql") else name
    if stem in catalog:
        return catalog[stem][1]
    matches = [n for n in catalog if Path(n).name == Path(stem).name]
    if len(matches) == 1:
        return catalog[matches[0]][1]
    if not matches:
        available = ", ".join(catalog) or "(空)"
        searched = ", ".join(str(d) for _, d in dirs)
        raise FileNotFoundError(
            f"クエリ '{name}' がありません。利用可能: {available}(探した場所: {searched})")
    raise ValueError(f"'{name}' は複数あります。フォルダ込みで指定してください: "
                     f"{', '.join(matches)}")


def query_params(path: Path) -> dict[str, str | None]:
    """クエリが受け取る変数 → 既定値の SQL 表記(必須なら None)。

    `getvariable('x')` の参照と、結果文より前の `SET VARIABLE x = ...` を
    DuckDB のトークナイザで拾う。コメントや文字列リテラルの中は数えない。
    順序は登場順。
    """
    statements = duckdb.extract_statements(path.read_text(encoding="utf-8"))
    defaults: dict[str, str] = {}
    for st in statements[:-1]:
        parsed = _parse_set_variable(st.query)
        if parsed is not None:
            defaults[parsed[0]] = parsed[1]
    params: dict[str, str | None] = {}
    for st in statements:
        for var in _getvariable_refs(st.query):
            params.setdefault(var, defaults.get(var))
    for var, default in defaults.items():         # SET だけして参照しない変数も受け付ける
        params.setdefault(var, default)
    return params


def run_query(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    out: Path | str | None = None,
    sql_dir: Path | str | None = None,
    **params: object,
) -> pd.DataFrame | int:
    """クエリファイルを実行する。out を渡すと CSV に直接書き出す。

    ファイル内の `SET VARIABLE`(既定値)を**先に**流し、そのあとに params を
    当てる — 呼び出し側が必ず勝つ。最後の文が結果を返す文。out を指定した場合は
    最後の文を COPY() で包むので、DuckDB が直接ファイルに書き、Python 側に
    データが載らない(大きなエクスポート向け)。
    """
    path = find_query(name, sql_dir)
    statements = duckdb.extract_statements(path.read_text(encoding="utf-8"))
    if not statements:
        raise ValueError(f"{path} に実行できる文がありません")

    for key in params:
        _identifier(key)
    accepted = query_params(path)
    unknown = [k for k in params if k not in accepted]
    if unknown:
        usable = ", ".join(accepted) or "(変数なし)"
        raise TypeError(f"クエリ '{name}' は {', '.join(unknown)} を受け取りません。"
                        f"使える変数: {usable}")
    missing = [k for k, v in accepted.items() if v is None and k not in params]
    if missing:
        raise TypeError(f"クエリ '{name}' には {', '.join(missing)} が必要です"
                        f"(例: s.run({name!r}, {missing[0]}=...))")

    # 前の run() の値を持ち越さない。このクエリが読む変数はすべて、ここで
    # 既定値か呼び出し側の値で置き直される。
    for var in accepted:
        conn.execute(f"RESET VARIABLE {var}")
    conn.execute(_OPT_EQ_MACRO)
    for st in statements[:-1]:            # 既定値の SET VARIABLE など
        conn.execute(st.query)
    for key, value in params.items():     # 呼び出し側の指定で上書き
        conn.execute(f"SET VARIABLE {key} = ?", [value])

    final = statements[-1].query.rstrip("; \n")
    if out is None:
        return conn.execute(final).fetchdf()
    out = Path(out)
    rows = conn.execute(
        f"COPY ({final}) TO '{sql_literal(out.as_posix())}' (HEADER, DELIMITER ',')"
    ).fetchone()[0]
    return rows


def _tokens(sql: str) -> tuple[bytes, list[tuple[str, int, int]]]:
    """DuckDB トークナイザで (text, start, end) の列にする(コメントは現れない)。

    tokenize() の位置は UTF-8 のバイトオフセットなので、バイト列で切り出す。
    文字列リテラルは1トークンになるので、'getvariable(...)' という文字列の
    中身を参照と取り違えない。
    """
    raw = sql.encode("utf-8")
    spans = duckdb.tokenize(sql)
    out = []
    for i, (pos, kind) in enumerate(spans):
        end = spans[i + 1][0] if i + 1 < len(spans) else len(raw)
        out.append((_token_text(raw[pos:end].decode("utf-8"), kind), pos, end))
    return raw, out


def _token_text(chunk: str, kind) -> str:
    """次のトークンの開始までの切れ端から、トークン本体だけを取り出す。

    コメントはトークンにならないので、直前のトークンの切れ端に空白や
    `-- ...` がくっついて届く。文字列は閉じクォートまで('' はエスケープ)、
    それ以外は空白かコメント開始の手前まで。
    """
    if kind == duckdb.token_type.string_const:
        i = 1
        while i < len(chunk):
            if chunk[i] == "'":
                if i + 1 < len(chunk) and chunk[i + 1] == "'":
                    i += 2
                    continue
                return chunk[:i + 1]
            i += 1
        return chunk.strip()
    head = chunk.split(None, 1)[0] if chunk.strip() else ""
    for marker in ("--", "/*"):
        if marker in head[1:]:
            head = head[:head.index(marker, 1)]
    return head


def _getvariable_refs(sql: str) -> list[str]:
    _, toks = _tokens(sql)
    text = [t for t, _, _ in toks]
    refs = []
    for i, t in enumerate(text):
        if (t.lower() == "getvariable" and i + 2 < len(text) and text[i + 1] == "("
                and text[i + 2].startswith("'")):
            refs.append(text[i + 2][1:-1].replace("''", "'"))
    return refs


def _parse_set_variable(sql: str) -> tuple[str, str] | None:
    """`SET VARIABLE x = expr` / `SET VARIABLE x TO expr` → (x, expr)。それ以外は None。"""
    raw, toks = _tokens(sql)
    if len(toks) < 4 or toks[0][0].upper() != "SET" or toks[1][0].upper() != "VARIABLE":
        return None
    name = toks[2][0]
    if toks[3][0] != "=" and toks[3][0].upper() != "TO":
        return None
    body = [t for t in toks[4:] if t[0] != ";"]
    if not body:
        return None
    # 最後のトークンの「中身」までで切る(後ろに続く行末コメントを含めない)
    last_text, last_start, _ = body[-1]
    expr = raw[body[0][1]:last_start + len(last_text.encode("utf-8"))].decode("utf-8")
    return name, expr.strip()


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
