# stdf

半導体テストデータ（STDF V4）を **Parquet + DuckDB** に変換する高速 ETL パイプライン。  
Pure Python パーサー × ThreadPoolExecutor による並列処理で、1000 ファイル以上のバッチ取り込みに対応。  
数人での共同解析は共有 marimo サーバ（ブラウザだけで SQL / Python ノートブック）で対応。  
**Docker 不要 — `uv sync` のみでセットアップ完了。**

---

## クイックスタート

> **Python は `.python-version` の 3.13 に固定**（`uv sync` が自動で用意する）。
> 全マシンで検証済みの処理系を揃えるための固定で、`requires-python` は 3.10 以上。

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
powershell -File scripts/start_marimo.ps1   # 専用 venv (var/venv-notebook) を自動作成して起動
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
     ↓               storage.py                             lot_id={L}/
     ↓                                                      data.parquet
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
| `parser.py` | Pure Python STDF V4 パーサー（`struct.Struct` 最適化、FAR/MIR/WIR/PIR/PRR/PTR/MPR/FTR/PMR対応） |
| `storage.py` | PyArrow で Hive パーティション Parquet に書き込み、リテスト番号の自動採番 |
| `ftp_client.py` | `ftplib` FTP 差分ダウンロード（`.stdf.gz` 自動展開） |
| `sync_manager.py` | `sync_history.json` で FTP 取得済み・ingest 済みを追跡 |
| `ingest_history.py` | `ingest_history.json` でローカル ingest 済みファイルを追跡（`ingest-all` の再開用） |

> **Subprocess 分離の理由:** パーサーがクラッシュしても他のワーカーに影響しない。プロセスごとにメモリが解放されるため、1000+ ファイルのバッチ処理でもメモリリークが蓄積しない。

#### ストレージ層

| モジュール | 役割 |
|---|---|
| `storage.py` | Parquet Hive パーティション書き込み（5テーブル） |
| `mounts.py`（旧 `views.py`） | `_DEDUP_UNIT` / `store_fingerprint()` / `setup_views(conn, data_dir, gross_die_map)` の単一ソース。ビューは glob なので、登録後に ingest したデータも再登録なしで見える |
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
- 成功済みファイルを `var/data/ingest_history.json` に記録 → 中断後の再実行で自動スキップ
- タイムアウト超過時は SIGKILL → 次ファイルへ継続

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

# ディレクトリ一括（推奨）— 中断後の再実行は自動で続きから
stdf ingest-all ./var/downloads -p SCT101A
stdf ingest-all ./var/downloads -p SCT101A --workers 8 --timeout 600
stdf ingest-all ./var/downloads -p SCT101A --force   # 全ファイル強制再取り込み
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
stdf db query "SELECT * FROM wafers" -o out.csv  # 結果を CSV に書き出す
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
| `runs` | STDF ファイル × wafer 単位の MIR/MRR メタデータ。CP は wafer ごと、FT は FT ロットの実行ごと（`wafer_id=''`）に 1 行。テストプログラム（`job_name` / `job_rev`）がロット内で変わってもここに残る |
| `lots` | ロット情報（product, test_category, sub_process）。Parquet ではなく **`runs` から集約した DuckDB ビュー**（1 ロット 1 行）。`job_mixed` / `job_variant_count` でロット内の TP 混在を判定できる |
| `wafers` | ウェハー歩留まり（リテスト追跡含む）。**CP 専用**（FT は WIR/WRR が無く生成されない） |
| `parts` | 個片結果（Bin, X/Y 座標）。CP=ダイ / FT=パッケージ |
| `test_data` | テスト測定値（PTR/MPR/FTR 統合）。MPR は 1 レコードをピン数分の行に展開し `pin_num` / `pin_name` 列に PMR のピン情報を格納。PTR/FTR 行ではこれらは NULL。 |
| `chipid` | FT chiplet の die 出自トレース（GDR `EN-S0-CHIPID_R` をデコード）。**FT 専用** |

> 解析・歩留りは重複排除済みの `parts_final` / `test_data_final` / `chipid_final`
> ビューを使う（詳細は `docs/schema.md` / `docs/sample_queries.md`）。

### Parquet パーティション構造

```
var/data/          ← storage.data_dir（config.yaml で変更可）
└── {table}/
    └── product={product}/
        └── test_category={CP|FT}/
            └── sub_process={CP11|FT2}/
                └── lot_id={lot_id}/
                    └── (wafer_id={id}/retest={n}/)  ← runs/wafers/parts/test_data/chipid
                        └── data.parquet
```

> `lots` は Parquet を持たない（`runs` 由来のビュー）。旧ストアの `data/lots/`（= data_dir 直下の `lots/`）が
> 残っていると `setup_views()` がエラーで停止するので、wipe して再 ingest する。

---

## リテスト対応

| フィールド | 取得元 |
|-----------|--------|
| `sub_process` | STDF MIR.TEST_COD / CLI `-s` |
| `test_rev` | ファイル名（Rev04等） |
| `retest_num` | 既存データから自動計算（0=初回, 1,2…=リテスト） |
| `test_data.retest_flag` | ingest 時に自動算出（0=そのキーの最新 run）。`test_data_final` は `retest_flag = 0` の単純フィルタ |
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
