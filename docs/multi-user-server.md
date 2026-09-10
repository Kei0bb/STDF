# マルチユーザー解析 — stdf query server 運用ガイド

数人のメンバーが各自のPCからアドホック解析するための構成。データと歩留まり定義
(mounts.py)は共有マシンに置いたまま、読み取り専用のHTTP APIだけをLANに公開する。
メンバー側は VSCode + 薄クライアント、またはブラウザだけの2通りで接続できる。

```
[共有マシン]  ingest/fetch(従来どおり)─▶ data/ (Parquet)
              stdf serve  ─▶ http://<共有マシン>:8555
[各PC]        ブラウザ(SQLコンソール、追加インストール不要)
              または VSCode + client/stdf_client.py(依存: requests, pandas のみ)
```

- 生データは共有マシンから出ない(APIはクエリ結果だけを返す)
- 1リクエスト = 1つの :memory: DuckDBセッション。ユーザー間の干渉なし
- ユーザーSQLは **単一のSELECTのみ**。ファイルアクセスは data_dir 配下に制限
  (DuckDB `allowed_directories` + `enable_external_access=false` + `lock_configuration`)
- 結果は `server.max_rows`(既定10,000行)で切り詰め(`truncated` フラグ付き)

## 共有マシン側のセットアップ

1. `config.yaml` に `server:` 節を追加(config.yaml.example 参照)
2. 起動確認:

   ```
   uv run stdf serve
   # → stdf query server → http://0.0.0.0:8555
   ```

3. Windowsファイアウォールで受信ポート(既定8555)を許可
4. サービス確認:
   - `GET /` — ブラウザ SQL コンソール(下記)
   - `GET /api` — 旧来のプレーンテキスト索引(エンドポイント一覧)
   - `GET /health` — 生存確認
5. 常駐化(Windowsタスクスケジューラ):
   - タスクスケジューラ → 基本タスクの作成
   - トリガー: 「スタートアップ時」/ 操作: プログラムの開始
     - プログラム: `<repoパス>\.venv\Scripts\stdf.exe`(または `uv` 経由のbat)
     - 引数: `serve` / 開始(作業)フォルダ: repoのルート(config.yamlのある場所)
   - 「ユーザーがログオンしているかどうかにかかわらず実行する」を選択
   - 更新手順: `git pull` → タスクを再起動

## メンバー側のセットアップ

インストール不要のブラウザコンソールと、pandas 連携が欲しい人向けの VSCode 薄クライアント、
2通りの入口がある。どちらも同じ `/api/query` を叩くので、見えるビューは同一。

### ブラウザ SQL コンソール(追加インストール不要)

`http://<共有マシン>:8555/` をブラウザで開くだけ。単一ファイルの自己完結 HTML
(`stdf_platform/server/console.html`、外部 CDN 依存なし)で、SQL 入力・実行・結果テーブル
表示・CSV ダウンロードに加えて、`GET /api/schema` から取得したテーブル/ビュー一覧と列名を
サイドバーに表示する(スキーマ取得に失敗したビューはサイドバー上でエラー表示になるだけで、
ページ全体は落ちない)。VSCode もブラウザもない環境(タブレット等)からの確認にも使える。

- `GET /` — このコンソール本体
- `GET /api` — 旧来のプレーンテキスト索引(エンドポイント一覧、スクリプト向け)
- `GET /api/views` — 利用可能なビュー名一覧(JSON)
- `GET /api/schema` — テーブル/ビューごとの列名(JSON)

### VSCode 薄クライアント(pandas 連携)

1. `pip install requests pandas`
2. `client/stdf_client.py` を作業フォルダにコピー(repo全体は不要)
3. サーバURLを環境変数で設定: `STDF_SERVER=http://<共有マシン>:8555`
4. VSCodeのセル(`# %%`)から:

   ```python
   # %%
   from stdf_client import q, to_csv, views

   views()                                          # 使えるビュー一覧
   df = q("SELECT * FROM wafer_yield_final WHERE lot_id = 'ABC123'")
   to_csv("SELECT * FROM test_data_final WHERE lot_id = 'ABC123'", "abc123.csv")
   ```

利用できるビューは共有マシン上の解析と完全に同一(`runs` / `lots` / `parts_final` /
`test_data_final` / `wafer_yield_final` などの実行時ビューは `mounts.py` に一本化)。
定番の集計は `sql/` に名前付きで置いてある — クエリ例は docs/sample_queries.md と
`sql/README.md` を参照。

## パワーユーザー向けの代替: 共有フォルダ直読み

大量の生データ加工(max_rowsを超える処理)をローカルでやりたい場合は、repoを
自PCに入れて `config.yaml` の `data_dir` を読み取り専用共有のUNCパスに向ければ、
workspace/query.py / AnalysisSession がローカルDuckDBでそのまま動く(サーバ不要・併用可):

```yaml
storage:
  data_dir: "//SHARED-PC/stdf-data"   # forward slash のUNCパス
```

## 将来の拡張(設計済み・未実装)

- 既存のFastAPI+Reactダッシュボードへの統合: サーバ本体は `APIRouter` として
  実装済み(`stdf_platform.server.router`)。ダッシュボード側で
  `app.include_router(stdf_router)` するだけで同じAPIを提供できる
- 認証・監査ログ: 社内LAN・数人の間は省略。必要になったらリバースプロキシ
  またはトークンを追加する
