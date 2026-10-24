# workspace — 個人の一時解析層 / 共有 marimo のノートブック置き場

ここは各自の自由帳です。`query.py`(`cp query.py.example query.py`)や
任意の *.py / *.sql を置いてください。**README.md・query.py.example・`_template.py` 以外は
git 追跡されません。**

共有 marimo サーバ(`scripts/start_marimo.ps1`、運用は `docs/marimo-server.md`)は
この `workspace/` を配信します。ブラウザからは次のルールで使います。

- `_template.py` — marimo ノートブックの出発点(git 管理)。**直接編集しない**(保存で作業ツリーが
  dirty になり `git pull` が衝突する)。「名前を付けて保存」か `cp _template.py <自分の名前>/<題材>.py`
  でコピーしてから使う。
- `<自分の名前>/` — 個人のノートブック。**自分のフォルダ以外は編集しない。**
- 値は SQL に埋め込まない(f-string 禁止)。`s.run(name, 変数=値)` か `s.q("... ?", [値])`。
- パッケージは UI から入れない(`var/venv-notebook` にだけ入り、次の更新で消える)。
- **注意**: 共有サーバは `workspace/` 直下の `.py` も一覧に出すので、共有マシンに置いた
  `query.py` などの個人ファイルは他のメンバーからも見える。見せたくないものは `workspace/` の外に置く。

## SQL の熟成ラダー(昇格ルール)

1. **書き捨て(ここ)** — セル実行で探索。`q("SELECT ...")`。
2. **2回使った SQL → リポジトリ直下の `sql/` へ** — 該当フォルダに `名前.sql` を作る。
   1行目を `-- 説明` に、条件は `getvariable('lot')`(任意条件は `SET VARIABLE x = NULL;`
   + `opt_eq()`)。実行: `s.run("名前", lot=..., product=...)` / `out=` で CSV。
   `sql/` は git 管理。本番機で編集して commit / push する。
   ビルドも中間生成物も要らない。詳細は `sql/README.md`。
3. **計算が重くて毎回待てない → 事前計算を検討** — ここまで来るクエリは多くない。
   実測で遅いと分かってから初めて考えればよい(現状そういうクエリは無い)。

使われなくなったクエリは 2 に留めるか消す。
