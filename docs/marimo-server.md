# 共有 marimo サーバ — ブラウザだけで SQL/Python 解析

共有マシンで `marimo edit` を1つ常駐させ、メンバーはブラウザでアクセスするだけで
SQL / Python のノートブックを書ける構成。メンバー側のインストールは不要。
独自のサーバコードは無く、marimo(OSS)と既存の `AnalysisSession` をそのまま使う。

```
 メンバー(ブラウザのみ)          共有マシン(Windows)
 +-----------------+  HTTP      +--------------------------------------+
 | http://host:2718| ---------> | marimo edit notebooks/               |
 +-----------------+  + token   |   notebooks/<name>/*.py              |
                                |   1 ノートブック = 1 カーネル        |
                                |        | import                      |
                                |        v                             |
                                |   AnalysisSession (DuckDB :memory:)  |
                                |        | views = mounts.py           |
                                |        v                             |
                                |   var/data/ (Parquet)                |
                                |        ^                             |
                                |   stdf fetch / ingest-all            |
                                +--------------------------------------+
```

`stdf serve`(SELECT のみのサンドボックス)とは別系統で、**併用も撤去も自由**。
こちらは任意の Python を実行できる代わりに、メンバー側の準備が要らない。

## 共有マシンのセットアップ

1. 依存を入れる(marimo は `notebook` グループ):

   ```
   uv sync --group notebook
   ```

2. アクセストークンを作る(1行のパスワード。`var/` は git 管理外):

   ```
   echo <十分長いランダム文字列> > var\marimo_token.txt
   ```

3. 起動確認:

   ```
   powershell -ExecutionPolicy Bypass -File scripts\start_marimo.ps1
   # → http://<共有マシン>:2718  をブラウザで開き、トークンを入力
   ```

4. Windows ファイアウォールで受信ポート 2718 を **社内ネットワークのみ**許可。
5. 常駐化(タスクスケジューラ): 基本タスクの作成 →
   トリガー「スタートアップ時」/ プログラム `powershell.exe` /
   引数 `-ExecutionPolicy Bypass -NoProfile -NonInteractive -File "<repo>\scripts\start_marimo.ps1"`。
   更新手順: `git pull` → タスクを再起動。

`start_marimo.ps1` が渡している主なオプション(確認済み: marimo 0.25.2):

| オプション | 理由 |
|---|---|
| `--token-password-file` | トークンをコマンドラインに出さない。トークン無しの API は 401 |
| `--session-ttl 3600` | ブラウザを閉じて1時間後にカーネルとメモリを解放(放置対策) |
| `--no-sandbox` | ノートブックを repo の環境(stdf_platform 入り)で動かす。sandbox は専用 venv を作ってしまう |
| `--host 0.0.0.0` | LAN 公開。ファイアウォールで社内に限定すること |

## メンバーの使い方

1. `http://<共有マシン>:2718` を開いてトークンを入力。
2. `notebooks/_template.py` を開いて「名前を付けて保存」→ `notebooks/<自分の名前>/<題材>.py`
   (または共有マシンで `cp`)。
3. SQL セル(`mo.sql(..., engine=con)`)で `lots` / `wafer_yield_final` / `test_data_final` など
   の実行時ビュー(定義は `mounts.py`)を直接読める。名前付きクエリは
   `s.run("fail_ranking", lot=...)` / 一覧は `s.queries()`。
4. 2回使った SQL は `sql/` に `.sql` として昇格(`sql/README.md`)。ブラウザからは git が
   使えないので、保守者が共有マシンのノートブックを見て commit / push する。

運用ルール: **自分のフォルダ以外のノートブックは編集しない。**
同じファイルを2人目が開くと、その人は「reader(閲覧のみ)」としてつながり、1人目と
同じカーネルを共有する(確認済み)。画面の **Take over** を押すと編集権が移り、
前の人は閲覧側に回る。他人のノートブックを開いても壊れはしないが、Take over は押さない。

## リソース

- 各カーネルが DuckDB を1つ持つ。`_template.py` の冒頭で `memory_limit` / `threads` を
  絞っている(共有マシンの搭載量に合わせて調整)。ユーザーが上書きできるので目安。
- ingest / fetch と同じマシンなので、取り込み中は重くなりうる。重いクエリは取り込み時間帯を避ける。

## 未対応のリスク(意図的に後回し)

この構成では**ユーザーのコードが、データの持ち主と同じ権限で共有マシン上で動く**。
Windows アカウントを分けられない環境のため、次は現状**防げない**:

| リスク | 内容 |
|---|---|
| データの破損 | ノートブックから `var/data/` の Parquet を削除・上書きできる |
| 秘密情報の読み取り | 同じアカウントの環境変数・ファイル(FTP 認証情報など)を読める |
| 他人のノートブック | 閲覧・編集できる |
| 負荷 | 重いクエリで共有マシン(と ingest)が遅くなる |
| 監査 | トークンが共通なので誰の操作か分からない |

信頼できるエンジニアだけが使う前提で運用する。対策を足す場合の候補(コスト順):

1. **`var/data/` の定期バックアップ**(robocopy 等)。最後の砦。元の STDF を `fetch` の
   cleanup で消していると Parquet が唯一のコピーになるので、特に重要。
2. **FTP は読み取り専用の認証情報にする**。
3. **marimo にはレプリカ(`var/data/` の取り込み後コピー)だけを見せる**。
   専用 `config.yaml` を `STDF_CONFIG` で渡す。事故は防げるが容量が倍になり、
   意図的な操作は防げない。
4. **マシンを分ける**(ingest 用と marimo 用)。確実な境界。

## 他の入口との使い分け

| 入口 | 向く用途 |
|---|---|
| 共有 marimo(このドキュメント) | SQL/Python で探索、ノートブックの共有。ブラウザだけ |
| `stdf serve` のブラウザコンソール | 1本の SELECT を投げて CSV を取る。サンドボックス付き |
| `stdf serve` + VSCode 薄クライアント | 自 PC の pandas で加工したい人 |
| `workspace/query.py` | 共有マシン上での個人スクラッチ |
