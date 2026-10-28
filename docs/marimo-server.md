# 共有 marimo サーバ — ブラウザだけで SQL/Python 解析

共有マシンで `marimo edit` を1つ常駐させ、メンバーはブラウザでアクセスするだけで
SQL / Python のノートブックを書ける構成。メンバー側のインストールは不要。
独自のサーバコードは無く、marimo(OSS)と既存の `AnalysisSession` をそのまま使う。

```
 メンバー(ブラウザのみ)          共有マシン(Windows)
 +-----------------+  HTTP      +--------------------------------------+
 | http://host:2718| ---------> | var\venv-notebook\...\marimo edit    |
 +-----------------+  + token   |   workspace/<name>/*.py              |
                                |   1 ノートブック = 1 カーネル        |
                                |        | AnalysisSession.for_notebook|
                                |        v                             |
                                |   DuckDB 既定接続 (views = mounts.py)|
                                |        v                             |
                                |   var/data/ (Parquet)               |
                                |        ^                             |
                                |   stdf fetch / ingest-all (.venv)   |
                                +--------------------------------------+
```

任意の Python を実行できる代わりに、メンバー側の準備が要らない。

## 共有マシンのセットアップ

marimo は**専用の仮想環境 `var\venv-notebook`** から**直接**起動する(`uv run` を通さない)。
理由: `uv run` 配下で起動すると、marimo は UI の「パッケージを入れる/消す」ボタンを
`uv add` / `uv remove` として実行し、追跡対象の `pyproject.toml` / `uv.lock` と、夜間の
`stdf fetch` が使う `.venv` を書き換えてしまう。専用 venv から直接起動すれば、UI からの
インストールは `var\venv-notebook` にしか届かない(`var\` は git 管理外)。

1. アクセストークンを作る(1行のパスワード)。**`echo ... > file` は使わない**: Windows PowerShell 5.1 では
   UTF-16 で書き出され、marimo が読めず即終了する(`-Encoding UTF8` も BOM がトークンに混ざる)。
   起動スクリプトが検出してメッセージを出す:

   ```
   Set-Content -Path var\marimo_token.txt -Value '<十分長いランダム文字列>' -Encoding ascii
   ```

2. 起動確認(専用 venv が無ければ `uv sync --group notebook --frozen` で自動作成。
   初回はパッケージインデックスへのネットワークが要る。`--frozen` は `uv.lock` どおりに
   入れるだけで再解決しない — 社内 TLS 傍受環境でもロック済みの wheel だけが入る):

   ```
   powershell -ExecutionPolicy Bypass -File scripts\start_marimo.ps1
   # → http://<共有マシン>:2718  をブラウザで開き、トークンを入力
   ```

3. Windows ファイアウォールの受信ポート 2718 を許可し、**RemoteAddress を利用者の IP /
   サブネットに限定する**(「社内のみ」ではなく具体的な範囲で):

   ```
   New-NetFirewallRule -DisplayName "STDF marimo" -Direction Inbound -Protocol TCP `
     -LocalPort 2718 -Action Allow -RemoteAddress <利用者のIP or サブネット>
   ```

4. 常駐化(登録は管理者の PowerShell で1回。**タスク自体は昇格なし(`-RunLevel Limited`)で動く** —
   カーネルは利用者のコードをそのまま実行するので、管理者トークンを渡さない):

   ```
   powershell -ExecutionPolicy Bypass -File scripts\register_marimo_task.ps1
   ```

   スタートアップ時に起動、ログオフ中も実行、**実行時間制限なし**(`schtasks` の既定は
   72 時間で停止するため使わない)、失敗時は自動再起動。実行ユーザーのパスワードを聞かれる。

5. 更新手順: `git pull` → `powershell -File scripts\start_marimo.ps1 -Sync`
   (専用 venv を `uv.lock` に合わせ直す。ブラウザが開いていない時間帯に)→ タスクを再起動。
   タスク停止後は `Get-NetTCPConnection -LocalPort 2718` でポートが空いたことを確かめてから起動する。

`start_marimo.ps1` が渡している主なオプション(確認済み: marimo 0.25.2):

| オプション | 理由 |
|---|---|
| (`--quiet` は付けない) | 付けると何も出力されず、起動していても止まって見える。代わりに起動バナー(`?access_token=<トークン>` を含む)を、スクリプトが**トークンを `***` に伏せて**コンソールとログへ出す |
| `--token-password-file` | トークンをコマンドラインに出さない。トークン無しの API は 401 |
| `--session-ttl 3600` | ブラウザの接続が切れて1時間後にカーネルとメモリを解放。**タブを開いたままだと解放されない** |
| `--no-sandbox` | ノートブックを専用 venv(stdf_platform 入り)で動かす。sandbox はノートブックごとに venv を作る |
| `--host 0.0.0.0` | LAN 公開。ファイアウォールで利用者の範囲に限定すること |

marimo の**実行場所(カレントディレクトリ)と配信先は `workspace\`**。ノートブック内の相対パス
(`s.run(..., out="x.csv")` など)は `workspace\` を基準に解決される。設定(`config.yaml`)は
`workspace\` から見つからなければリポジトリ直下のものが使われる。

出力はコンソールにも出る(起動・URL・終了コード・エラー)。トークンは `***` に伏せる。
**起動後は Ctrl+C まで画面が止まって見えるのが正常**(サーバが動き続けている)。`URL: http://…` の行が出たら起動済み。ログは
`var\logs\marimo-YYYYMMDD.log`(UTF-8、14 日で削除)。タスクスケジューラ経由ではコンソールが無いので、
起動しない・落ちるときはこのログを見る。`start_marimo.ps1` は PowerShell 5.1 向けに ASCII のみで書く
(BOM なし UTF-8 の日本語は ANSI として読まれて化ける)。

## つながらないとき

| 症状 | 原因と確認 |
|---|---|
| 共有マシン自身で `localhost` / `0.0.0.0` が開けないが IP なら開ける | サーバは IPv4 の `0.0.0.0` で待ち受ける。`localhost` は IPv6(`::1`)に解決されて失敗することがあり、`0.0.0.0` は待ち受けアドレスで接続先としては使えない(Windows)。**`http://127.0.0.1:2718`**(同じ PC)か、起動時に表示される IP を使う。確認: `curl.exe -i http://127.0.0.1:2718/api/status` が `401` を返せばサーバは動いている |
| 他の PC からトークン入力までは進むが、ノートブックを開くと `WebAssembly.instantiateStreaming(): expected … bytes, fell off end` | 画面を構成する wasm ファイル(`loro_wasm_bg-*.wasm`、3,240,077 バイト)が**途中で切れて**届いている。サーバ側は全バイトを正しく返すことを確認済み(ローカルと LAN の IP の両方でハッシュ一致)ので、**経路上**(プロキシ、セキュリティ製品、ブラウザ拡張など)で切れている。下の手順で切り分ける |

wasm の切り分けは、**接続元の PC**で `scripts\diag_marimo_remote.ps1` を実行する(リポジトリは不要、
このファイル1つだけをコピーして使える。何もインストール・変更せず、`%TEMP%` に2ファイルをダウンロードして
サイズと SHA-256 を既知の値と比べ、結果の読み方まで表示する。出力にトークンは含まれない):

```
powershell -ExecutionPolicy Bypass -File diag_marimo_remote.ps1 -HostAddress <共有マシンのIP>
```

`curl.exe`(システムのプロキシ設定を無視)と `Invoke-WebRequest`(プロキシ設定を使う)の2通りで、
wasm と、それより大きい非 wasm ファイル(対照)を比べるので、プロキシ・content-type フィルタ・
経路全般のどれかを切り分けられる。手動でやる場合は次のとおり(サイズが `3240077`、SHA-256 が
`75ff0754c7681ec2c83345ea191e7ebf079cdf099fadea7da6401cd031217b67` なら正常):

```
curl.exe -s -o loro.wasm http://<共有マシンのIP>:2718/assets/loro_wasm_bg-CSolyDSZ.wasm
(Get-Item loro.wasm).Length
(Get-FileHash loro.wasm -Algorithm SHA256).Hash
```

**実績**: 接続元 PC で `curl.exe` と `Invoke-WebRequest` は wasm を完全に取得でき(プロキシなし)、
**Chrome では開けたが Edge では同じエラーが出た**。この場合は経路ではなく **Edge 固有**の要因: 拡張機能、
会社配布の Edge ポリシー(`edge://policy`)、Enhanced Security(`edge://settings/privacy`)。
InPrivate ウィンドウ(拡張機能なし)で開けるかが最初の切り分け。当面は Chrome を使えば回避できる。

- サイズが小さい: 経路で切られている。**ホスト名ではなく IP で**開く、ブラウザ/OS のプロキシ設定の
  除外リストに共有マシンを入れる、別のブラウザ・シークレットウィンドウ(拡張機能なし)で試す、
  を順に。それでも切れるなら、ネットワーク側のセキュリティ製品が大きな `application/wasm` を
  通していないので、情報システム部門への確認が要る。
- サイズが正しい(CLI では届く): ブラウザ側(拡張機能・キャッシュ)。ハードリロード(Ctrl+F5)を試す。
- 別のポートで試す(`start_marimo.ps1` の `$Port`)と、ポート単位のフィルタかを切り分けられる。

## メンバーの使い方

1. `http://<共有マシン>:2718` を開いてトークンを入力。
2. `workspace/_template.py` を開き、**編集する前に**「名前を付けて保存」→
   `workspace/<自分の名前>/<題材>.py`(または共有マシンで `cp`)。テンプレートを直接
   編集すると保存で作業ツリーが dirty になり、`git pull` が衝突する。
3. セッションは `AnalysisSession.for_notebook()` で作る(テンプレートのとおり)。
   `mo.sql(...)` は engine を指定しなくても、`lots` / `wafer_yield_final` /
   `test_data_final` などの実行時ビュー(定義は `mounts.py`)を読める。名前付きクエリは
   `s.run("fail_ranking", lot=...)`、一覧は `s.queries()`。
4. **値は SQL に埋め込まない**(f-string 禁止)。条件は `s.run(name, 変数=値)` か
   `s.q("... WHERE x = ?", [値])` で渡す。
5. 2回使った SQL は `sql/` に `.sql` として昇格(`sql/README.md`)。ブラウザからは git が
   使えないので、保守者が共有マシンのノートブックを見て commit / push する。
6. **パッケージを入れない。** UI から入れても `var\venv-notebook` にだけ入り、次の `-Sync` で消える。
   必要な依存は保守者が `pyproject.toml` の `notebook` グループに足して `uv.lock` を更新する。

運用ルール: **自分のフォルダ以外のノートブックは編集しない。** 配信されるのは `workspace/` 全体なので、
共有マシンに置いた個人ファイル(`query.py` など)も他のメンバーから見える。見せたくないものは `workspace/` の外に置く。

同じファイルを2人目が開くと、その人は「reader(閲覧のみ)」としてつながり、1人目と
同じカーネルを共有する(確認済み)。画面の **Take over** を押すと編集権が移り、
前の人は閲覧側に回る。他人のノートブックを開いても壊れはしないが、Take over は押さない。

AI 補完は `pyproject.toml` の `[tool.marimo.ai]` / `[tool.marimo.completion]` で**無効に固定**
している。全員が同じ Windows アカウントで動くため、個人の API キーや設定を持たせられず、
コードや出力が社外の AI サービスへ送られるのを防ぐ。

## リソース

- 各カーネルが DuckDB の既定接続を1つ持つ。`config.yaml` の `notebook:` 節
  (`memory_limit` 既定 4GB / `threads` 既定 4 / `temp_directory` 既定 `var/tmp`)を
  `AnalysisSession.for_notebook()` が掛ける。ユーザーが `SET` で上書きできるので目安であり、
  強制ではない。
- **予算の見積り**: 同時カーネル数 N × `memory_limit` に、各カーネルの Python ヒープ上の
  pandas 結果(`memory_limit` の対象外)を足した分を、共有マシンの搭載量から
  取り込み(ingest)の分を引いた範囲に収める。3 人で各 1〜2 ノートブックなら N = 3〜6。
  搭載量に合わせて `notebook:` を調整する。
- ingest / fetch と同じマシンなので、取り込み中は重くなりうる。

## ingest との同居(要検証 — 採用前に Win11 で1回)

ingest は retest の古いファイルを降格するとき、新しい Parquet を `os.replace` で上書きする
(`storage.py`、約3秒だけリトライして例外)。**Windows では、別プロセス(= marimo のカーネル)が
同じ Parquet を読んでいる間は、この置換が失敗する可能性がある**(未確認)。失敗すると新旧の
retest が両方 `retest_flag=0` のまま残り、`test_data_final` に重複が永続する(移行手段は無い)。

採用前に Win11 で次を実行し、PASS / FAIL を確認する(実データは使わない):

```
uv run python scripts/diag_parquet_lock.py
```

- `PASS`: スキャン中でも置換できた。現状の運用で問題ない。
- `FAIL`: 置換が拒否された。**ingest の時間帯(夜間 fetch の前後)は marimo のタスクを止める**
  運用にする(`Stop-ScheduledTask STDF_Marimo` → ingest 完了後に `Start-ScheduledTask`)。

(Linux では常に PASS になる。意味があるのは Windows での結果。)

## 未対応のリスク(意図的に後回し)

この構成では**ユーザーのコードが、データの持ち主と同じ権限で共有マシン上で動く**。
Windows アカウントを分けられない環境のため、次は現状**防げない**:

| リスク | 内容 |
|---|---|
| データの破損 | ノートブックから `var/data/` の Parquet を削除・上書きできる |
| 秘密情報の読み取り | 同じアカウントの環境変数・ファイル(FTP 認証情報など)を読める |
| 他人のノートブック | 閲覧・編集できる |
| 負荷 | 重いクエリで共有マシン(と ingest)が遅くなる。marimo にはクエリのタイムアウトが無い |
| 監査 | トークンが共通なので誰の操作か分からない |
| 平文の通信 | HTTP で、トークンは URL(`?access_token=`)でも渡る。社内 LAN で受け入れる前提 |

`var/` 配下の専用 venv 化と、出力のトークン伏せ字により、**ブラウザ操作が `.venv` / 追跡ファイルを書き換える**
経路と、**トークンがログに残る**経路は塞いである。

信頼できるエンジニアだけが使う前提で運用する。対策を足す場合の候補(コスト順):

1. **`var/data/` と `workspace/` の定期バックアップ**(robocopy 等)。最後の砦。元の STDF を
   `fetch` の cleanup で消していると Parquet が唯一のコピーになる。`workspace/` は git 管理外で
   履歴が無いので、これが唯一のコピー。
2. **FTP は読み取り専用の認証情報にする**。同一アカウント内では秘密を隔離できない。
3. **ファイアウォールを利用者の IP に限定**(セットアップ手順 3)。
4. **marimo にはレプリカ(`var/data/` の取り込み後コピー)だけを見せる**。
   専用 `config.yaml` を `STDF_CONFIG` で渡す。事故は防げるが容量が倍になり、
   意図的な操作は防げない。
5. **書き込み禁止の起動**(未検証の候補): marimo を低整合性レベルで起動すると `var/data` への
   書き込みを防げる可能性がある。`workspace/` と `var/logs` に低整合性ラベルが要る。試す場合は
   まず検証用の環境で。
6. **マシンを分ける**(ingest 用と marimo 用)。確実な境界。

## 他の入口との使い分け

| 入口 | 向く用途 |
|---|---|
| 共有 marimo(このドキュメント) | SQL/Python で探索、ノートブックの共有。ブラウザだけ |
| `workspace/query.py` | 共有マシン上での個人スクラッチ |
