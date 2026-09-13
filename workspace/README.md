# workspace — 個人の一時解析層

ここは各自の自由帳です。`query.py`(`cp query.py.example query.py`)や
任意の *.py / *.sql を置いてください。**README.md と query.py.example 以外は
git 追跡されません。**

## SQL の熟成ラダー(昇格ルール)

1. **書き捨て(ここ)** — セル実行で探索。`q("SELECT ...")`。
2. **2回使った SQL → リポジトリ直下の `sql/` へ** — 該当フォルダに `名前.sql` を作る。
   1行目を `-- 説明` に、条件は `getvariable('lot')`(任意条件は `SET VARIABLE x = NULL;`
   + `opt_eq()`)。実行: `s.run("名前", lot=..., product=...)` / `out=` で CSV。
   `sql/` は git 管理外なので本番機で気兼ねなく改造してよい。みんなで使う定番に
   なったら `src/stdf_platform/sql/` に移してコミットする。
   ビルドも中間生成物も要らない。詳細は `src/stdf_platform/sql/README.md`。
3. **計算が重くて毎回待てない → 事前計算を検討** — ここまで来るクエリは多くない。
   実測で遅いと分かってから初めて考えればよい(現状そういうクエリは無い)。

使われなくなったクエリは 2 に留めるか消す。
