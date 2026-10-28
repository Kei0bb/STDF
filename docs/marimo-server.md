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

起動は **`scripts\start_marimo.bat`**(ダブルクリックか、`cmd.exe` でリポジトリ直下から)。
PowerShell スクリプトは使わない。

marimo は**専用の仮想環境 `var\venv-notebook`** から**直接**起動する(`uv run` を通さない)。
理由: `uv run` 配下で起動すると、marimo は UI の「パッケージを入れる/消す」ボタンを
`uv add` / `uv remove` として実行し、追跡対象の `pyproject.toml` / `uv.lock` と、夜間の
`stdf fetch` が使う `.venv` を書き換えてしまう。専用 venv から直接起動すれば、UI からの
インストールは `var\venv-notebook` にしか届かない(`var\` は git 管理外)。

1. 専用 venv を作る: **`scripts\marimo_sync.bat`**(初回と、`git pull` で `uv.lock` が変わったとき。
   marimo を止めてから)。`uv.lock` どおりに入れるだけで再解決しない(`--frozen`) — 社内 TLS
   傍受環境でもロック済みの wheel だけが入る。`start_marimo.bat` は venv が無ければ自動で呼ぶ。
   中身は `set UV_PROJECT_ENVIRONMENT=var\venv-notebook` → `uv sync --group notebook --frozen`。
   (`set` はこのバッチの中だけで効く。窓に手で打つと、その窓の `uv sync` が専用 venv に入ってしまう。)

2. アクセストークン `var\marimo_token.txt` は、無ければ `start_marimo.bat` が**ランダムに作る**
   (Python で ASCII の1行を書く)。自分で作る場合も同じ形式にする: PowerShell 5.1 の
   `echo > file` は UTF-16、`-Encoding UTF8` は BOM がトークンに混ざり、どちらも marimo が読めない。

3. 起動: **`scripts\start_marimo.bat`**。`uv run` を使わず、`UV` / `VIRTUAL_ENV` を消して `workspace\` で
   `var\venv-notebook\Scripts\marimo.exe edit . …`(オプションは下の表)を実行する。
   起動バナーの `URL: http://…` が出れば起動済み。**共有マシン自身では `http://127.0.0.1:2718`、
   他の PC からは `http://<共有マシンのIP>:2718`**(バッチが先頭に一覧を表示する)を開き、
   トークン(`var\marimo_token.txt` の中身)を入力する。バナーにトークンが出るので、画面を
   共有・貼り付けしない。

4. Windows ファイアウォールの受信ポート 2718 を許可し、**RemoteAddress を利用者の IP /
   サブネットに限定する**(「社内のみ」ではなく具体的な範囲で。管理者の PowerShell で1回):

   ```
   New-NetFirewallRule -DisplayName "STDF marimo" -Direction Inbound -Protocol TCP `
     -LocalPort 2718 -Action Allow -RemoteAddress <利用者のIP or サブネット>
   ```

5. 更新手順: `git pull` → 起動中の窓で Ctrl+C → `marimo_sync.bat` → `start_marimo.bat`。ブラウザが開いていない時間帯に。
   起動できないときは、前のプロセスがポートを握っていないか `netstat -ano | findstr :2718` で確認する。

**常駐化はしていない**(ログオン中の窓で動かし、再起動後は手で起動し直す)。サービス化が必要に
なったら、タスクスケジューラに `scripts\start_marimo.bat` を登録する。そのとき「タスクを停止するまでの時間」を
**無効**にする(既定の 72 時間で止まる)。**昇格なし(最上位の特権で実行しない)**で動かす —
カーネルは利用者のコードをそのまま実行するので、管理者トークンを渡さない。

`start_marimo.bat` が渡している主なオプション(確認済み: marimo 0.25.2):

| オプション | 理由 |
|---|---|
| (`--quiet` は付けない) | 付けると何も出力されず、起動していても止まって見える |
| `--token-password-file` | トークンをコマンドラインに出さない。トークン無しの API は 401 |
| `--session-ttl 3600` | ブラウザの接続が切れて1時間後にカーネルとメモリを解放。**タブを開いたままだと解放されない** |
| `--no-sandbox` | ノートブックを専用 venv(stdf_platform 入り)で動かす。sandbox はノートブックごとに venv を作る |
| `--host 0.0.0.0` | LAN 公開。ファイアウォールで利用者の範囲に限定すること |

marimo の**実行場所(カレントディレクトリ)と配信先は `workspace\`**。ノートブック内の相対パス
(`s.run(..., out="x.csv")` など)は `workspace\` を基準に解決される。設定(`config.yaml`)は
`workspace\` から見つからなければリポジトリ直下のものが使われる。

**起動後は Ctrl+C まで画面が止まって見えるのが正常**(サーバが動き続けている)。出力はコンソールだけで、
ログファイルは作らない。止めるとき Ctrl+C は1回で止まる(バッチが `Terminate batch job (Y/N)?` と聞いたら Y)。効かない場合:

- 窓のタイトルが「選択」で始まっていないか見る(窓をクリックすると QuickEdit の選択状態になり、
  プロセスが一時停止する)。Esc か Enter で解除する。
- 開いているブラウザのタブを閉じる(接続とカーネルの終了を待つため)。
- Ctrl+Break、それでも駄目なら別の窓で `taskkill /F /T /IM marimo.exe`。

## つながらないとき

| 症状 | 原因と確認 |
|---|---|
| 共有マシン自身で `localhost` / `0.0.0.0` が開けないが IP なら開ける | サーバは IPv4 の `0.0.0.0` で待ち受ける。`localhost` は IPv6(`::1`)に解決されて失敗することがあり、`0.0.0.0` は待ち受けアドレスで接続先としては使えない(Windows)。**`http://127.0.0.1:2718`**(同じ PC)か、起動時に表示される IP を使う。確認: `curl.exe -i http://127.0.0.1:2718/api/status` が `401` を返せばサーバは動いている |
| 他の PC からトークン入力までは進むが、ノートブックを開くと `WebAssembly.instantiateStreaming(): expected … bytes, fell off end` | 画面を構成する wasm ファイル(`loro_wasm_bg-*.wasm`、3,240,077 バイト)が**途中で切れて**届いている。サーバ側は全バイトを正しく返すことを確認済み(ローカルと LAN の IP の両方でハッシュ一致)ので、**経路上**(プロキシ、セキュリティ製品、ブラウザ拡張など)で切れている。下の手順で切り分ける |

wasm の切り分けは、**接続元の PC**(`cmd.exe`)で wasm を直接ダウンロードして、サイズと
SHA-256 を既知の値と比べる(`curl.exe` はシステムのプロキシ設定を使わず、サーバへ直接つなぐ)。
サイズが `3240077`、SHA-256 が `75ff0754c7681ec2c83345ea191e7ebf079cdf099fadea7da6401cd031217b67`
なら正常(marimo 0.25.2 の値。更新後はファイル名とともに変わる):

```
curl.exe -s -o loro.wasm http://<共有マシンのIP>:2718/assets/loro_wasm_bg-CSolyDSZ.wasm
dir loro.wasm
certutil -hashfile loro.wasm SHA256
```

**実績**: 接続元 PC で wasm を完全に取得でき(プロキシなし)、**Chrome では開けたが Edge では同じ
エラーが出た**。この場合は経路ではなく **Edge 固有**の要因: 拡張機能、会社配布の Edge ポリシー
(`edge://policy`)、Enhanced Security(`edge://settings/privacy`)。InPrivate ウィンドウ(拡張機能なし)で
開けるかが最初の切り分け。当面は Chrome を使えば回避できる。

- サイズが小さい: 経路で切られている。**ホスト名ではなく IP で**開く、ブラウザ/OS のプロキシ設定の
  除外リストに共有マシンを入れる、別のブラウザ・シークレットウィンドウ(拡張機能なし)で試す、
  を順に。それでも切れるなら、ネットワーク側のセキュリティ製品が大きな `application/wasm` を
  通していないので、情報システム部門への確認が要る。
- サイズが正しい(CLI では届く): ブラウザ側(拡張機能・キャッシュ)。ハードリロード(Ctrl+F5)を試す。
- 別のポートで試す(起動コマンドの `--port`)と、ポート単位のフィルタかを切り分けられる。

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
6. **パッケージを入れない。** UI から入れても `var\venv-notebook` にだけ入り、次に venv を作り直すと消える。
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
- `FAIL`: 置換が拒否された。**ingest の時間帯(夜間 fetch の前後)は marimo を止める**
  運用にする(marimo を Ctrl+C で止める → ingest 完了後に起動し直す)。

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

`var/` 配下の専用 venv 化により、**ブラウザ操作が `.venv` / 追跡ファイルを書き換える**経路は塞いである。
トークンは起動した窓にだけ出る(ログファイルは作らない)。

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
