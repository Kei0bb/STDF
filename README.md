# stdf

半導体テストデータ（STDF V4）を **Parquet + DuckDB** に変換する高速 ETL パイプライン。  
Pure Python パーサー × ThreadPoolExecutor による並列処理で、1000 ファイル以上のバッチ取り込みに対応。  
数人での共同解析は読み取り専用クエリサーバ（`stdf serve`）+ 薄クライアントで対応。  
**Docker 不要 — `uv sync` のみでセットアップ完了。**

---

## クイックスタート

```bash
# 1. 依存インストール
uv sync

# 2. config.yaml を設定（example をコピー）
cp config.yaml.example config.yaml

# 3. データ取り込み
stdf ingest-all ./downloads -p YOUR_PRODUCT

# 4. dbt マート構築(歩留まりサマリ・Fail ランキング・Cpk 等を Parquet に実体化)
stdf build

# 5. クエリ / 解析
stdf db query "SELECT * FROM lot_yield_summary ORDER BY start_time DESC LIMIT 10"

# 6. マルチユーザー: 読み取り専用クエリサーバ(ブラウザ SQL コンソール、または各メンバーの VSCode+薄クライアント)
stdf serve    # → docs/multi-user-server.md
```

---

## アーキテクチャ

### 全体データフロー

```
FTP サーバー
     ↓ ftp_client.py + sync_manager.py
downloads/
     ↓ worker.py (ThreadPoolExecutor)
     ↓   ├── Thread 1 → subprocess(_ingest_worker) ─┐
     ↓   ├── Thread 2 → subprocess(_ingest_worker) ─┤→ Parquet (Hive パーティション)
     ↓   └── Thread N → subprocess(_ingest_worker) ─┘     data/{table}/product={P}/
     ↓                    ↑                                 test_category={CP|FT}/
     ↓               parser.py                              sub_process={CP11|FT2}/
     ↓               storage.py                             lot_id={L}/
     ↓                                                      data.parquet
     ↓
DuckDB glob ビュー（クエリごとに fs スキャン、mounts.py）
     ↓
     ↓ stdf build (dbt run + dbt test) ─▶ data/marts/*.parquet ─┐
     ↓                                                          │ mounts.py が自動マウント
     ↓◀─────────────────────────────────────────────────────────┘
     ├── stdf db query / lots / programs  (CLI)
     ├── workspace/query.py  (VS Code インタラクティブ)
     └── stdf serve (読み取り専用 HTTP API)
          ├── ブラウザ SQL コンソール（GET /、追加インストール不要）
          └── 各メンバーの PC → client/stdf_client.py (VS Code) / 将来: ダッシュボード
```

---

### モジュール構成

#### Ingest パイプライン

| モジュール | 役割 |
|---|---|
| `cli.py` | Click CLI — `ingest` / `ingest-all` / `fetch` / `build` / `db` / `export` / `serve` コマンド |
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
| `mounts.py`（旧 `views.py`） | `_DEDUP_UNIT` 定数と `setup_views(conn, data_dir, gross_die_map)` の単一ソース。コアビュー登録に加え、`data/marts/*.parquet`（dbt が構築）をファイル名でビューとして自動マウント（コアビュー / `gross_die` と名前が衝突するマートはスキップ） |
| `build.py` | `stdf build [--select MODEL]` の実装 — `dbt run`（`--select` でモデルを絞れる）→ `dbt test`（**常に全件、`--select` の対象外**）を実行し、`data/.marts_build/` へ出力後、`data/marts/` へアトミックに差し替え（失敗時は旧 marts へロールバック） |
| `config.py` | `config.yaml` 読み込み（FTP / Storage / Server 設定、`${ENV_VAR}` 展開対応） |

#### 解析 SQL 層（`dbt/`）

| ディレクトリ | 役割 |
|---|---|
| `dbt/models/staging/` | `stg_parts_final` / `stg_test_data_final` / `stg_chipid_final` / `stg_lots` / `stg_wafer_yield` — 実行時ビューと同じセマンティクス（`tests/test_dbt_staging_parity.py` で機械検証） |
| `dbt/models/marts/` | `lot_yield_summary` / `fail_ranking` / `cpk_stats` / `bin_pareto` / `bin_fail_tests` — 外部実体化で `data/marts/*.parquet` に書き出し。`schema.yml` にテスト定義 |
| `dbt/macros/dedup_key.sql` | `_DEDUP_UNIT` 相当の重複排除キーマクロ |
| `dbt/tests/assert_*.sql` | 旧 `stdf db verify-flags` の4不変条件。`stdf build` の `dbt test` で毎回検証 |
| `dbt/analyses/` | 名前は付くが実体化しない SQL 階層（`stdf db query -f dbt/analyses/<name>.sql` で実行） |

#### 個人解析層（`workspace/`）

`README.md` と `query.py.example` のみ git 追跡。SQL の熟成ラダー（書き捨て →
`dbt/analyses/` → `dbt/models/marts/`）は `workspace/README.md` を参照。

#### マルチユーザー層

| モジュール | 役割 |
|---|---|
| `server/app.py` | 読み取り専用 HTTP クエリ API（`stdf serve`）。`APIRouter` 実装で将来のダッシュボード統合に対応。`GET /` はブラウザ SQL コンソール（`server/console.html`、外部 CDN 依存なし）、旧来のプレーンテキスト索引は `GET /api` に移動、`GET /api/schema` はビューごとの列名を返す（DESCRIBE に失敗したビューは 500 ではなく `error` エントリになる） |
| `client/stdf_client.py` | メンバー配布用の薄いクライアント（依存: requests + pandas のみ、単体1ファイル） |

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
- 成功済みファイルを `data/ingest_history.json` に記録 → 中断後の再実行で自動スキップ
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
stdf ingest ./downloads/SCT101A/CP/lot001.stdf --from-path

# ディレクトリ一括（推奨）— 中断後の再実行は自動で続きから
stdf ingest-all ./downloads -p SCT101A
stdf ingest-all ./downloads -p SCT101A --workers 8 --timeout 600
stdf ingest-all ./downloads -p SCT101A --force   # 全ファイル強制再取り込み
```

### SQL クエリ（VS Code）

`workspace/query.py` は個人のスクラッチ領域（`workspace/` は README.md と
`query.py.example` 以外 gitignore 対象）です。初回、またはリポジトリ更新でテンプレートの
定義が変わったときはコピーしてください:

```bash
cp workspace/query.py.example workspace/query.py
```

VS Code で `workspace/query.py` を開き、各セル (`# %%`) を Shift+Enter で実行（DuckDB）。
クエリ集は `docs/sample_queries.md` を参照。定番の集計（歩留まりサマリ・Fail ランキング・
Cpk・ビン分布）は dbt マートに済んでいるので、まずそこから引く:

```python
q("SELECT * FROM lot_yield_summary ORDER BY start_time DESC")   # ロット一覧（歩留まり付き）
q("SELECT * FROM fail_ranking WHERE lot_id = ? ORDER BY fail_pct DESC", ["LOT001"])
q("SELECT * FROM cpk_stats WHERE lot_id = ? ORDER BY cpk ASC LIMIT 20", ["LOT001"])

# マートに無い集計は生ビュー（*_final）を直接叩く
q("SELECT * FROM test_data_final WHERE lot_id = 'E6A773.00'")

# ファイル出力（DuckDB COPY、メモリに載せない）
to_csv("SELECT * FROM test_data_final WHERE lot_id = 'E6A773.00'")
```

> 同じ SQL を2回使ったら `dbt/analyses/` へ昇格（`{{ ref(...) }}` でモデル参照、
> スキーマ変更はコンパイルエラーで検知）、定着したら `dbt/models/marts/` へモデル化して
> `stdf build` で毎晩実体化する。詳細は `workspace/README.md` の熟成ラダーを参照。
> 旧実装にあった `use_lot()` / `use_all()`（ロット単位 materialize）はマート化により
> 不要になったため廃止された。

### 分析 API（`stdf_platform.analysis`）

`AnalysisSession` を通じてリテスト重複排除済みビューにアクセスし、DataFrame / Plotly figure を返す再利用可能な分析関数群。

```python
from stdf_platform.analysis import AnalysisSession, yield_by_lot, lot_trend, cp_ft_yield, zone_yield, trend_fig

s = AnalysisSession()                              # config.yaml を自動読み込み

# ロット間比較
df = yield_by_lot(s, product="SCT101A", lot_ids=["L001", "L002"], test_category="CP")

# トレンド（MIR start_time 順、mean±3σ コントロールライン付き）
fig = trend_fig(lot_trend(s, product="SCT101A", test_category="CP"))

# CP↔FT 歩留まり相関
df = cp_ft_yield(s, product="SCT101A")

# CP ウェハー面内ゾーン別歩留まり（n_zones=3 で Center / Middle / Edge）
df = zone_yield(s, product="SCT101A", lot_id="L001")
```

すべての関数は `*_final` ビューを使用するため、歩留まり・Cpk の定義が dbt マート
（`lot_yield_summary` / `cpk_stats` 等）や CLI（`stdf db lots` / `stdf db query`）と
完全に一致します。`workspace/query.py` は `# %%` セル形式（VS Code / Jupytext）で、
Shift+Enter で逐次実行可能。

### CLI クエリ

```bash
stdf db lots                                 # ロット一覧（TP 混在ロットは Job 列に ⚠×N）
stdf db programs --lot LOT001                # wafer / retest ごとのテストプログラム履歴
stdf db query "SELECT * FROM lot_yield_summary"  # SQL 直接実行（マート／生ビュー両方引ける）
stdf db query -f dbt/analyses/some_query.sql     # ファイルから SQL を読む
stdf db query "SELECT * FROM wafers" -o out.csv  # 結果を CSV に書き出す
stdf db shell                                # DuckDB シェル
```

### dbt マート構築

```bash
stdf build                    # dbt run + dbt test → data/marts/ をアトミックに更新
stdf build --select cpk_stats # 特定モデルだけ dbt run で再構築（dbt test は常に全件実行）
```

構築されるマート: `lot_yield_summary`（ロット別歩留まりサマリ）/ `fail_ranking`
（ロット×テスト別 Fail 率）/ `cpk_stats`（ロット×テスト別 Cp/Cpk）/ `bin_pareto`
（ロット別 bin 分布）/ `bin_fail_tests`（Fail ダイのビン×Failテスト紐付け）。
`data/marts/*.parquet` は `mounts.py` が同名のビューとして自動マウントするので、
`stdf db query` / `workspace/query.py` / `stdf serve` から即座に `SELECT * FROM <マート名>`
で使える。

### マルチユーザー解析（`stdf serve`）

データと歩留まり定義は共有マシンに置いたまま、読み取り専用の HTTP API だけを社内 LAN に公開。
メンバーはブラウザだけでも解析できます（`GET /` の自己完結 SQL コンソール、追加インストール不要）。
pandas 連携が欲しい場合は repo 不要 — `client/stdf_client.py` 1ファイルと
`pip install requests pandas` だけで VS Code のセルから解析できます。

```bash
# 共有マシン側（1回だけ）
stdf serve        # config.yaml の server: 節（host / port / max_rows）に従って起動
```

```
# メンバー側 A: ブラウザで http://<共有マシン>:8555/ を開く（インストール不要）
```

```python
# メンバー側 B（VS Code セル、STDF_SERVER=http://<共有マシン>:8555 を設定）
from stdf_client import q, to_csv, views

views()                                                    # 使えるビュー・マート一覧
df = q("SELECT * FROM wafer_yield_final WHERE lot_id = 'ABC123'")
to_csv("SELECT * FROM test_data_final WHERE lot_id = 'ABC123'", "abc123.csv")
```

- ユーザー SQL は**単一 SELECT のみ**。ファイルアクセスは data_dir 配下に制限
  （DuckDB `allowed_directories` + `enable_external_access=false`）
- 結果は `server.max_rows`（既定 10,000 行）で切り詰め（`truncated` フラグ付き）
- 1リクエスト = 1 DuckDB セッション。全員が `mounts.py` の同一定義で計算
- 常駐化（Task Scheduler）・ブラウザコンソールの詳細・メンバー配布手順 → `docs/multi-user-server.md`

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
type logs\fetch_*.log

REM 登録解除
scripts\unregister_task.bat
```

| ファイル | 役割 |
|---------|------|
| `scripts/daily_fetch.ps1` | メインスクリプト — `uv run stdf fetch --verbose` 実行、`logs/fetch_YYYYMMDD_HHMMSS.log` に記録、30日超のログを自動削除 |
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
data/
└── {table}/
    └── product={product}/
        └── test_category={CP|FT}/
            └── sub_process={CP11|FT2}/
                └── lot_id={lot_id}/
                    └── (wafer_id={id}/retest={n}/)  ← runs/wafers/parts/test_data/chipid
                        └── data.parquet
```

> `lots` は Parquet を持たない（`runs` 由来のビュー）。旧ストアの `data/lots/` が
> 残っていると `setup_views()` がエラーで停止するので、wipe して再 ingest する。

---

## リテスト対応

| フィールド | 取得元 |
|-----------|--------|
| `sub_process` | STDF MIR.TEST_COD / CLI `-s` |
| `test_rev` | ファイル名（Rev04等） |
| `retest_num` | 既存データから自動計算（0=初回, 1,2…=リテスト） |
| `test_data.retest_flag` | ingest 時に自動算出（0=そのキーの最新 run）。`test_data_final` は `retest_flag = 0` の単純フィルタ（整合性は `stdf build` が実行する dbt テスト `dbt/tests/assert_*.sql` でチェック） |
| `test_data.exec_seq` | ingest 時に自動算出（run 内 0 始まり出現順）。ループ計測（OTP ダンプ等）の各回を区別 |

---

## 開発環境分離 (`--env`)

```bash
stdf --env dev ingest-all ./test_data -p SCT101A  # data-dev/ に保存
stdf --env dev db lots
rm -rf data-dev/   # リセット
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
