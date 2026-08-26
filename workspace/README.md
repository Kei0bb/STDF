# workspace — 個人の一時解析層

ここは各自の自由帳です。`query.py`(`cp query.py.example query.py`)や
任意の *.py / *.sql を置いてください。**README.md と query.py.example 以外は
git 追跡されません。**

## SQL の熟成ラダー(昇格ルール)

1. **書き捨て(ここ)** — セル実行で探索。
2. **2回使った SQL → `dbt/analyses/` へ** — 名前を付けて git へ。
   `stdf db query -f` はファイルを生 SQL として読むだけで Jinja を解決しないため、
   `{{ ref(...) }}` は使えない — マウント済みのビュー/マート名(`parts_final` /
   `lot_yield_summary` 等)を直接書いたプレーンな SQL にする。
   実行: `stdf db query -f dbt/analyses/名前.sql` またはブラウザコンソール。
3. **定着 / 同僚も使う / 計算が重い → `dbt/models/marts/` へ** — モデル化して
   `stdf build` で毎晩 Parquet 実体化。テストと docs を付ける。

使われなくなったマートは 2 に降格して維持コストを下げる。
