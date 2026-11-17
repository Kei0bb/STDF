# stdf

半導体テストデータ（STDF V4）を **Parquet + DuckDB** に変換する高速 ETL パイプライン。  
Pure Python パーサー × ThreadPoolExecutor による並列処理で、1000 ファイル以上のバッチ取り込みに対応。  
数人での共同解析は共有 marimo サーバ（ブラウザだけで SQL / Python ノートブック）で対応。  
**Docker 不要 — `uv sync` のみでセットアップ完了。**

---

## クイックスタート

> **Python は `.python-version` の 3.13 に固定**（`uv sync` が自動で用意する）。
> 全マシンで検証済みの処理系を揃えるための固定で、`requires-python` は 3.11 以上（DuckDB 2.x の要件）。

```bash
# 1. 依存インストール
uv sync

# 2. config.yaml を設定（example をコピー）
cp config.yaml.example config.yaml

# 3. データ取り込み
stdf ingest-all ./var/downloads -p YOUR_PRODUCT

# 4. 解析（VS Code）— 名前付きクエリは sql/
cp workspace/query.py.example workspace/query.py
#    query.py を開いて  s.queries()  /  s.run("fail_ranking", lot="LOT001")

# 5. アドホックな SQL
stdf db query "SELECT * FROM lots ORDER BY start_time DESC LIMIT 10"

# 6. マルチユーザー: ブラウザだけで SQL/Python ノートブック(共有 marimo サーバ → docs/marimo-server.md)
scripts\start_marimo.bat   # 専用 venv (var\venv-notebook) を必要なら作って起動 → docs/marimo-server.md
```

---

## アーキテクチャ

### 全体データフロー

```
FTP サーバー
     ↓ ftp_client.py + sync_manager.py
var/downloads/
     ↓ worker.py (ThreadPoolExecutor)
     ↓   ├── Thread 1 → subprocess(_ingest_worker) ─┐
     ↓   ├── Thread 2 → subprocess(_ingest_worker) ─┤→ Parquet (Hive パーティション)
     ↓   └── Thread N → subprocess(_ingest_worker) ─┘     var/data/{table}/product={P}/
     ↓                    ↑                                 test_category={CP|FT}/
     ↓               parser.py                              sub_process={CP11|FT2}/
     ↓               storage.py                             lot_id={L}/wafer_id={W}/
     ↓                                                      retest={n}/data.parquet
     ↓
DuckDB glob ビュー（クエリごとに fs スキャン、mounts.py）
     ↓
     ├── stdf db query / lots / programs  (CLI)
     ├── workspace/query.py + 名前付きクエリ  (VS Code インタラクティブ)
     └── 共有 marimo サーバ (workspace/、ブラウザだけで SQL / Python)
```

---

### リポジトリ構成

追跡されるのはソースとドキュメントだけで、**動かすと増えるものはすべて `var/` の下**に集約する。

```
src/stdf_platform/   コアライブラリ
sql/                 名前付きクエリ（1ファイル1クエリ）
tests/               pytest
docs/                スキーマ・サンプルクエリ・共有 marimo 運用
scripts/             Windows タスクスケジューラ用 .ps1/.bat、診断スクリプト
workspace/           個人用スクラッチ兼 共有 marimo のノートブック置き場（README・.example・_template.py 以外は git 管理外）
config.yaml.example  → config.yaml にコピーして使う

var/                 ランタイム生成物（.gitignore で丸ごと除外）
├── data/            Parquet ストア = storage.data_dir
├── data-dev/        --env dev の隔離ストア（data_dir の兄弟として自動導出）
├── downloads/       FTP 取得先 = storage.download_dir
├── logs/            daily_fetch.ps1 の実行ログ（30日で自動削除）
├── test_data/       tests/make_test_stdf.py が生成する合成 STDF（再生成可）
└── .pytest_cache/ .ruff_cache/
```

`var/` 配下の場所は `config.yaml` の `storage.*` で変更できる。`--env dev` は
`data_dir` の兄弟ディレクトリを作るので（`./var/data` → `./var/data-dev`）、
`data_dir` を移すと dev ストアも一緒についてくる。

### モジュール構成

#### Ingest パイプライン

| モジュール | 役割 |
|---|---|
| `cli.py` | Click CLI — `ingest` / `ingest-all` / `fetch` / `db` / `export` コマンド |
| `worker.py` | `ThreadPoolExecutor` でファイルごとに subprocess を起動・タイムアウト管理 |
| `_ingest_worker.py` | 独立 subprocess — 1ファイルを parse → Parquet 書き込みして JSON を stdout に出力 |
| `parser.py` | Pure Python STDF V4 パーサー（`struct.Struct` 最適化、FAR/MIR/SDR/WIR/WRR/PIR/PRR/PTR/MPR/FTR/PMR/GDR）。part はマルチサイトの (head, site) ごとに管理。FAR/MIR の無いファイル・途中で切れたファイルは `STDFFormatError` |
| `storage.py` | PyArrow で Hive パーティション Parquet に書き込み、リテスト番号の採番と `retest_flag` の再計算 |
| `ftp_client.py` | `ftplib` FTP 差分ダウンロード（`.stdf.gz` 自動展開） |
| `sync_manager.py` | `sync_history.json` で FTP 取得済み・ingest 済みを追跡 |
| `ingest_history.py` | `ingest_history.json` でローカル ingest 済みファイルを追跡（`ingest-all` の再開用） |

> **Subprocess 分離の理由:** パーサーがクラッシュしても他のワーカーに影響しない。プロセスごとにメモリが解放されるため、1000+ ファイルのバッチ処理でもメモリリークが蓄積しない。

#### ストレージ層

| モジュール | 役割 |
|---|---|
| `storage.py` | Parquet Hive パーティション書き込み（`runs` / `parts` / `test_data` の 3 テーブル） |
| `chipid.py` | FT 2-die パッケージの CHIP ID（eFuse 64bit）デコードと検査 |
| `mounts.py`（旧 `views.py`） | `die_key_expr()` / `store_fingerprint()` / `setup_views(conn, data_dir, gross_die_map)` の単一ソース。ビューは glob なので、登録後に ingest したデータも再登録なしで見える |
| `config.py` | `config.yaml` 読み込み（FTP / Storage / Server 設定、`${ENV_VAR}` 展開対応） |

#### 名前付きクエリ層

1ファイル1クエリ。`AnalysisSession.run()` から呼ぶ。ビルドも中間生成物も無く、生ビューを直接読む。
置き場所はリポジトリ直下の `sql/` だけ（git 管理）。本番機で編集して commit / push する。

| クエリ | 内容 |
|---|---|
| `01_lots/` | ロット一覧 / ロット別歩留まりサマリ（product / test_category / sub_process で任意に絞れる） |
| `02_wafer/` | ウェーハ別歩留まり |
| `03_test/` | テスト項目一覧 / Fail ランキング / Cp・Cpk |
| `04_bin/` | ビン分布 / ビン×Fail テスト |
| `05_export/` | ダイ×テスト明細（CSV 書き出し前提） |

条件（変数）はキーワード引数で複数同時に渡せる。クエリが必須とする変数の渡し忘れと、
クエリが使わない名前（typo）は `TypeError` になり、0 行を黙って返さない。任意条件は
`SET VARIABLE x = NULL;` と `opt_eq(col, getvariable('x'))` で書く。値はバインド変数として
渡すので、SQL への文字列連結は不要。書き方と追加手順は `sql/README.md`。

#### 個人解析層（`workspace/`）

`README.md` と `query.py.example` のみ git 追跡。SQL の熟成ラダー（書き捨て →
名前付きクエリ → 必要なら事前計算）は `workspace/README.md` を参照。

#### マルチユーザー層

| モジュール | 役割 |
|---|---|
| `analysis/session.py` | `AnalysisSession.for_notebook()` — 共有 marimo カーネル用セッション（DuckDB の default 接続 = `mo.sql` と同じ、`notebook:` 設定の上限を適用） |
| `scripts/start_marimo.bat` | 専用 venv（`var/venv-notebook`）で共有 marimo サーバを起動。運用は `docs/marimo-server.md` |

---

### バッチ処理の仕組み

```
N ファイル
     ↓
ThreadPoolExecutor (max_workers=N)
├── Thread 1 → subprocess(_ingest_worker) → Parquet
├── Thread 2 → subprocess(_ingest_worker) → Parquet
└── Thread N → subprocess(_ingest_worker) → Parquet
```

- 各 subprocess はメモリ分離（クラッシュしても他のワーカーに影響しない）
- 同じロットのファイルは並列にせず、ファイル名のタイムスタンプ順に直列で処理する
- 成功したファイルは1件ごとに `var/data/ingest_history.json` に記録 → 中断後の再実行で自動スキップ
- 失敗（STDF でない・途中で切れている・壊れた .gz・タイムアウトで SIGKILL）は記録せず次ファイルへ継続し、最後に終了コード 1
- 書き込み中のロット/ウェーハは `var/data/_pending/` に記録し、中断で `retest_flag` が古いまま残ったものはバッチの最初と最後に再計算して直す（残っている間はビュー登録時に警告）
- ingest バッチはデータディレクトリごとに1つずつ（`var/data/.ingest.lock`）。夜間バッチ中に手動 ingest すると終わるまで待つ
- 同じファイルを再 ingest すると、そのファイル自身の `retest` 番号を上書きする（重複しない。`X.stdf.gz` と `X.stdf` は同じファイル扱い）

---

## インストール

```bash
uv sync
```

---

## 使用方法

### データ取り込み

```bash
# 単ファイル
stdf ingest sample.stdf --product SCT101A

# パスから product 自動推定（.../SCT101A/CP/... 構造）
stdf ingest ./var/downloads/SCT101A/CP/lot001.stdf --from-path

# sub_process を MIR.TEST_COD ではなく指定値にする
stdf ingest sample.stdf -p SCT101A -s CP2

# ディレクトリ一括（推奨）— 中断後の再実行は自動で続きから
stdf ingest-all ./var/downloads -p SCT101A
stdf ingest-all ./var/downloads -p SCT101A -r        # 下位フォルダも（_ で始まるフォルダは除外）
stdf ingest-all ./var/downloads -p SCT101A --workers 8 --timeout 600
stdf ingest-all ./var/downloads -p SCT101A --force   # 全ファイル強制再取り込み
stdf ingest-all ./var/downloads -p SCT101A --allow-truncated  # 途中で切れたファイルも完全なレコードまで取り込む
```

- 対象は `*.stdf` / `*.std` / `*.stdf.gz` / `*.std.gz`（大文字小文字を区別しない。`--glob` で上書き可）。
  `.gz` は一時ファイルに展開して読み、`runs.source_file` には `.gz` を外した名前が入る（fetch と同じ）。
- 失敗が1件でもあれば終了コード 1（単ファイルの `ingest` も同じ）。

**FT の CHIP ID を ingest 前に確かめる**（読むだけ。実データ機で、suite 名のバイト列・site 番号の
振り方・C0/C1 の取れ方を確認する）:

```bash
uv run python scripts/diag_chipid.py <FTファイル.stdf.gz> --part 1
```

### SQL クエリ（VS Code）

`workspace/query.py` は個人のスクラッチ領域（`workspace/` は README.md と
`query.py.example` 以外 gitignore 対象）です。初回、またはリポジトリ更新でテンプレートの
定義が変わったときはコピーしてください:

```bash
cp workspace/query.py.example workspace/query.py
```

VS Code で `workspace/query.py` を開き、各セル (`# %%`) を Shift+Enter で実行（DuckDB）。
定番の集計は `sql/` の名前付きクエリにあるので、まずそこから引く:

```python
s.queries()                                   # クエリ一覧（name / 説明 / 受け取る変数）
s.run("fail_ranking", lot="LOT001")           # → DataFrame
s.run("cpk", lot="LOT001", test_name="Vth%")  # 追加パラメータ
s.run("lot_list", product="P1", test_category="CP")  # 条件は複数同時に渡せる
print(s.show("cpk"))                          # SQL 本文を見る（改造の出発点）

# CSV 書き出し（DuckDB COPY、メモリに載せない）。返り値は書き出した行数
s.run("die_test_export", lot="LOT001", out="LOT001.csv")

# 書き捨ての探索は生ビュー（*_final）を直接
q("SELECT * FROM test_data_final WHERE lot_id = 'E6A773.00'")
```

同じロットへ繰り返しクエリするなら `use_lot("LOT001")` で `*_final` を
メモリ上に materialize できる（`use_all()` で全ロットに復元）。

> 同じ SQL を2回使ったら `sql/` の該当フォルダへ昇格させる（1行目を `-- 説明` に、
> 条件は `getvariable('lot')`）。
> 詳細は `sql/README.md` と `workspace/README.md` の熟成ラダーを参照。

### 分析（`AnalysisSession`）

`AnalysisSession` はリテスト重複排除済みビュー(`*_final`)を持つ DuckDB セッションで、歩留まり・Cpk の
定義が名前付きクエリや CLI（`stdf db lots` / `stdf db query`）と完全に一致します。

```python
from stdf_platform.analysis import AnalysisSession

s = AnalysisSession()                              # config.yaml を自動読み込み
s.queries()                                        # sql/ の名前付きクエリ一覧
s.run("fail_ranking", lot="L001")                  # DataFrame
s.run("lot_yield", product="SCT101A", out="y.csv") # out= で CSV に直接書く
```

`workspace/query.py` は `# %%` セル形式（VS Code / Jupytext）で、Shift+Enter で逐次実行できます。

### CLI クエリ

```bash
stdf db lots                                 # ロット一覧（TP 混在ロットは Job 列に ⚠×N）
stdf db programs --lot LOT001                # wafer / retest ごとのテストプログラム履歴
stdf db query "SELECT * FROM lots"               # SQL 直接実行
stdf db query -f path/to/query.sql               # ファイルから SQL を読む（単一 SELECT）
stdf db query "SELECT * FROM wafer_yield_final" -o out.csv  # 結果を CSV に書き出す
stdf db shell                                # DuckDB シェル（2回目以降はカタログ再利用で即起動 / --refresh で再登録）
```

### マルチユーザー解析（共有 marimo サーバ）

共有マシンで `marimo edit` を1つ常駐させ、メンバーはブラウザでアクセスするだけで SQL / Python の
ノートブックを書けます（メンバー側のインストール不要）。セットアップ・運用ルール・リスクは
`docs/marimo-server.md`、ノートブックの置き場所と作法は `workspace/README.md` を参照。

### FTP 差分同期

```bash
stdf fetch                   # config.yaml に従って差分取得
stdf fetch -p SCT101A        # 製品指定
stdf fetch --force           # 強制再ダウンロード
stdf fetch --no-ingest       # ダウンロードのみ
stdf fetch --reingest        # DL済み未 ingest を再試行（FTP接続なし）
```

### 定期実行（Windows 11 — Task Scheduler）

毎日朝 6:00 に `stdf fetch` を自動実行するスクリプトを同梱しています。

```cmd
REM セットアップ（管理者権限で1回だけ実行）
scripts\register_task.bat

REM 動作テスト（即時実行）
schtasks /Run /TN STDF_DailyFetch

REM ログ確認
type var\logs\fetch_*.log

REM 登録解除
scripts\unregister_task.bat
```

| ファイル | 役割 |
|---------|------|
| `scripts/daily_fetch.ps1` | メインスクリプト — `uv run stdf fetch --verbose` 実行、`var/logs/fetch_YYYYMMDD_HHMMSS.log` に記録、30日超のログを自動削除 |
| `scripts/register_task.bat` | Task Scheduler にタスク登録（毎日 06:00 トリガー） |
| `scripts/unregister_task.bat` | タスク登録解除 |

---

## データ構造

### テーブル

| テーブル | 説明 |
|---------|------|
| `runs` | STDF ファイル × wafer 単位の MIR/MRR メタデータと WRR 値（CP のみ）。CP は wafer ごと、FT は FT ロットの実行ごと（`wafer_id=''`）に 1 行。テストプログラム（`job_name` / `job_rev`）がロット内で変わってもここに残る |
| `lots` | ロット情報（product, test_category, sub_process）。Parquet ではなく **`runs` から集約した DuckDB ビュー**（1 ロット 1 行）。`job_mixed` / `job_variant_count` でロット内の TP 混在を判定できる |
| `parts` | 個片結果（Bin, X/Y 座標）。CP=ダイ / FT=パッケージ。FT の CHIP ID（GDR `EN-S0-CHIPID_R` をデコード）は `c0_*` / `c1_*` / `chip_status` 列 |
| `test_data` | テスト測定値（PTR/MPR/FTR 統合）。MPR は 1 レコードをピン数分の行に展開し `pin_num` / `pin_name` 列に PMR のピン情報を格納。PTR/FTR 行ではこれらは NULL。 |

> 解析・歩留りは重複排除済みの `parts_final` / `test_data_final` / `wafer_yield_final`
> ビューを使う（詳細は `docs/schema.md` / `docs/sample_queries.md`）。同じロットの CP1 / CP2
> （FT1 / FT2）は別の測定なので混ぜない（dedup・歩留りとも sub_process ごと）。

### Parquet パーティション構造

```
var/data/          ← storage.data_dir（config.yaml で変更可）
└── {table}/
    └── product={product}/
        └── test_category={CP|FT}/
            └── sub_process={CP11|FT2}/
                └── lot_id={lot_id}/
                    └── wafer_id={id}/retest={n}/     ← runs / parts / test_data
                        └── data.parquet
```

> `lots` は Parquet を持たない（`runs` 由来のビュー）。旧ストアの `data/lots/`（= data_dir 直下の `lots/`）が
> 残っていると `setup_views()` がエラーで停止するので、wipe して再 ingest する。
> 旧ストアの `wafers/` / `chipid/` ディレクトリは無視される。

### wipe と再 ingest

移行コードは持たない。ingest 時に書く列が変わったら、ストアを作り直す。
`fetch` は ingest に成功したファイルを既定で削除する（`--cleanup`）ので、元ファイルは FTP から取り直す:

```cmd
schtasks /Change /TN STDF_DailyFetch /DISABLE   REM 夜間バッチを止める
rmdir /s /q var\data                            REM Parquet と sync_history / ingest_history をまとめて消す
uv run stdf fetch --verbose                     REM 全ファイルを取り直して ingest（ロット内は時刻順）
schtasks /Change /TN STDF_DailyFetch /ENABLE
```

`sync_history.json` も `var/data` にあるので一緒に消える（＝全ファイルが未取得扱いになり、取り直しになる）。
手元に元ファイルが残っているなら、`var/data` を消してから `stdf ingest-all <dir> -p <製品> -r` でもよい。

---

## リテスト対応

| フィールド | 取得元 |
|-----------|--------|
| `sub_process` | STDF MIR.TEST_COD / CLI `-s` |
| `test_rev` | ファイル名（Rev04等） |
| `retest_num` | 既存データから自動計算（0=初回, 1,2…=リテスト）。同じ `source_file` の再 ingest は自分の番号を上書き |
| `parts.retest_flag` / `test_data.retest_flag` | ingest 時に自動算出（0=そのダイの最新の測定）。run の MIR 開始時刻 → `retest_num` → ファイル内 PIR 順で決まり、ingest 順に依存しない。`parts_final` / `test_data_final` は `retest_flag = 0` の単純フィルタ。`test_data_final` は勝った run の行だけ（古い run で補わない） |
| ダイの同一性（`die_key`） | CP = ウェーハ + X/Y。FT = eFuse の CHIP ID（C0、なければ C1。同じファイルに同じ C0+C1 が 2 回あれば再挿入として後の part）。eFuse の無い FT パッケージはリテスト間で重ねない（初回・リテストの両方が残る）。バーコード（`part_txt`）は使わない |
| `test_data.exec_seq` | ingest 時に自動算出（run 内 0 始まり出現順）。ループ計測（OTP ダンプ等）の各回を区別 |

---

## 開発環境分離 (`--env`)

```bash
stdf --env dev ingest-all ./var/test_data -p SCT101A  # var/data-dev/ に保存
stdf --env dev db lots
rm -rf var/data-dev/   # リセット
```

---

## WSL2 での注意点

| 問題 | 対策 |
|---|---|
| メモリ不足でワーカーが強制終了 | `.wslconfig` で `memory=8GB` を設定、`--workers 2` に減らす |
| `/mnt/c/` からのファイルが遅い | STDF ファイルを Linux 側（`~/`）にコピーしてから実行 |
| 中断後の再開 | `ingest-all` を再実行するだけ（成功済みは自動スキップ） |

---

## ライセンス

MIT License
