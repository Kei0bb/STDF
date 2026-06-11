# 複数人利用・レポート自動化 設計書

日付: 2026-06-11
ステータス: ユーザー承認待ち

## 背景と目的

stdf プラットフォームを (1) 複数人での利用(Web UI 閲覧 + CLI/query.py 解析)、
(2) 解析・レポートの定型化・自動化に対応させる。
データ取り込み(ingest)の実行者は限定される前提(同一ロットの同時 ingest 競合は設計対象外、
ただし履歴ファイルの破損防止は行う)。

3 フェーズに分割し、各フェーズを spec → 実装計画 → 実装のサイクルで進める。

---

## Phase 1: 基盤リファクタリング

レポートエンジンを載せる前の地ならし。すべて低〜中リスク。

### 1.1 死にコード・破損コードの除去
- `src/stdf_platform/app.py`(旧 Streamlit UI、485 行)を削除。
  依存(`streamlit`, `st_aggrid`)が pyproject.toml になく import 不能。旧スキーマ参照。
- `src/tests/test_timeout.py` を削除(`_ingest_worker.py` を上書きする危険なスクリプト)。
  タイムアウト動作は通常の pytest として書き直す(モック化した遅い worker を使う)。
- `src/tests/test_ingest.py` は pytest 形式に書き直すか `scripts/` へ移動。

### 1.2 `export lot` の修正
- `cli.py:726-755` が存在しないテーブル(`test_results`, `tests`)を参照。
  `test_data_final` + `parts_final` を使う SQL に書き直す。回帰テストを追加。

### 1.3 ビュー定義の単一ソース化
- 新モジュール `src/stdf_platform/views.py` を作成:
  - `_DEDUP_UNIT` 定数(現在 `database.py:17` と `web/api/deps.py:21` に重複)
  - `setup_views(conn, data_dir)`(現在 `database.py:56-103` と `deps.py:26-77` に重複)
- `database.py`、`web/api/deps.py`、`query.py` はこれを import する。
- パスは `.as_posix()` に統一(`database.py` 側の Windows 非互換を解消)。

### 1.4 履歴ファイルのアトミック書き込み
- `SyncManager._save()` と `IngestHistory._save()` を
  「一時ファイルに書いて `os.replace`」方式に変更。並行プロセスでの破損を防止。

### 1.5 Web サーバの複数人対応
- 共有 DuckDB 接続 + グローバル `threading.Lock` を廃止し、
  リクエスト毎に `duckdb.connect(":memory:")` + `setup_views()` を行う依存に変更。
  Parquet が真実のソースなので接続生成は軽量。閲覧の直列化ボトルネックを解消。
- `/health` エンドポイント追加。

### 1.6 設定解決の堅牢化
- `Config.load()` の探索順: 明示引数 → `STDF_CONFIG` 環境変数 → cwd の `config.yaml`。
  cron / Task Scheduler 起動時の「黙ってデフォルトにフォールバック」問題を解消。

### 1.7 ingest 失敗の構造化記録
- バッチ ingest 終了時、失敗ファイルとエラーを `data/ingest_failures.json` に書き出す
  (アトミック書き込み)。自動化スクリプトが機械的に失敗を検出できるようにする。

### 1.8 軽微な整理
- `ProcessingConfig.batch_size`(未使用)を削除。
- `cli.py:785` の未使用 `STDF_DB_PATH` 環境変数を削除。
- CLAUDE.md の `ProcessPoolExecutor` 記述を実態(`ThreadPoolExecutor` + subprocess)に修正。
- `data/ingest_worker.log` にサイズ上限付きローテーションを導入。

---

## Phase 2: レポートエンジン

### 方式(承認済み)
Jinja2 + Plotly 埋め込みによる自己完結 HTML 1 ファイル生成(案 A)。
plotly.js 本体を HTML に同梱しオフライン閲覧可能にする(1 ファイル ≒ 4MB 前後、許容)。

### モジュール構成
```
src/stdf_platform/reporting/
├── queries.py     # 解析クエリ層。views.py(Phase 1)のビュー定義の上に構築
├── sections.py    # セクション = 関数(conn, product, lot_id) → SectionResult(タイトル, 図, 表)
├── render.py      # Jinja2 テンプレート + plotly.js 同梱で単一 HTML を生成
└── generator.py   # generate_lot_report(product, lot_id) → 出力パス
```

### 生成契機
- CLI: `stdf report --lot X -p PROD`(単発)、`stdf report --pending`(未生成・更新分一括)
- 自動: `ingest` / `ingest-all` / `fetch` 完了後、取り込んだロットのレポートを再生成。
  レポート生成失敗は ingest を失敗させない(警告ログ + ingest_failures.json とは別管理)。

### 出力レイアウト
```
data/reports/product={P}/test_category={CP|FT}/lot_id={L}/report.html
```
retest を含め常に最新状態で上書き再生成。レポートは Parquet から再生成可能な
使い捨てキャッシュとして扱う(真実のソースは Parquet)。

### レポート構成(CP ロット)
1. **ヘッダ** — MIR 由来メタデータ(製品、テストプログラム、装置、日時)、retest 回数
2. **歩留まりサマリ** — ウェハ別歩留まり表 + 棒グラフ、ロット合計(`parts_final` ベース)
3. **ビンパレート** — ソフトビン集計のパレート図、ウェハ別積み上げ
4. **ウェハマップ** — 全ウェハのビンマップをグリッド表示
5. **パラメトリック** — フェイル率上位テスト表 + ヒストグラム(規格線・Cpk 付き)
6. **リテスト履歴** — retest > 0 の場合のみ

FT ロットは 4 を省略し、6 に ChipID 集計を追加。

### ヒストグラム対象テストの選定(承認済み)
- デフォルト: フェイル率上位 N(N=20、config で変更可)
- 加えて config.yaml に製品別の「常に表示するテストリスト」を指定可能:
  ```yaml
  reporting:
    histogram_top_n: 20
    always_include_tests:
      PRODUCT_A: [1234, 5678]   # test_num のリスト
  ```

### Web UI 統合
- 「Reports」タブを追加。
- `GET /api/reports` — `data/reports/` をスキャンして一覧(product / category / lot / 更新日時)。
- レポート HTML は FastAPI の静的配信でそのまま閲覧。

---

## Phase 3: 解析テンプレート集(概要のみ、詳細設計は Phase 2 完了後)

- `query.py` を `analysis/` パッケージに昇格。
- ロット間比較、トレンド、パラメトリック相関などを reporting/queries.py と
  同じクエリ層の上に関数として整備。Phase 2 のセクション部品を再利用。

---

## テスト方針

- Phase 1: 既存テスト(32 関数)を維持しつつ、views.py 統合・export lot・
  アトミック書き込み・per-request 接続のそれぞれに回帰テストを追加。
- Phase 2: queries.py の各クエリを合成 STDF(make_test_stdf.py)で検証。
  render は「HTML が生成され必須セクション・図 JSON を含む」ことをスモークテスト。
  ingest 後フックは「失敗してもingestが成功する」ことをテスト。

## 実装体制

実装は Opus/Sonnet サブエージェントに委譲し、本セッションの主モデルは
設計・レビュー監督に専念する(トークンコスト削減)。
