# Phase 3: 解析テンプレート集 設計書

日付: 2026-06-11
ステータス: ユーザー委任(テンプレート選定はClaude提案で進める指示)
親スペック: 2026-06-11-multiuser-reporting-design.md

## 目的

query.py 的なアドホック解析を、再利用可能な解析パッケージ + Jupyter/cell-script
テンプレートとして整備する。複数人が同じ関数・同じ定義(retest考慮済みビュー)で
解析できるようにし、「人によって歩留まり定義が違う」事故を防ぐ。

## アーキテクチャ

```
src/stdf_platform/analysis/
├── __init__.py      # 公開API再エクスポート
├── session.py       # AnalysisSession: 接続+views登録+q()/lots()等の基本操作
├── compare.py       # ロット間比較
├── trend.py         # トレンド(時系列)
├── correlation.py   # CP↔FT相関・テスト間相関
└── spatial.py       # ウェハ面内分析

templates/analysis/  # リポジトリ直下。# %% セルスクリプト(VSCode/Jupytext互換)
├── 01_lot_compare.py
├── 02_trend.py
├── 03_cp_ft_correlation.py
└── 04_wafer_spatial.py
```

設計原則:
- 全関数は **pandas DataFrame を返す**(表示・加工は呼び出し側)。図が要るものは
  plotly `go.Figure` を返す `*_fig` 関数を併設。
- データ定義は Phase 1 の `views.py`(parts_final 等)と Phase 2 の
  `reporting/queries.py` を再利用。SQL重複は作らない。
- すべてバインドパラメータ。f-string への値埋め込み禁止。
- 接続は `AnalysisSession(data_dir=None)` が config 解決(STDF_CONFIG対応)込みで担う。
  `with` 対応。`query.py` は薄いラッパとして残す(後方互換)。

## 関数仕様

### session.py
- `AnalysisSession(data_dir: Path | None = None)` — None なら Config.load() の storage.data_dir
- `.q(sql, params=None) -> DataFrame` — 生SQL逃げ道
- `.lots(product=None, test_category=None) -> DataFrame` — lots一覧(メタデータ付き)
- `.conn` — DuckDB接続(views登録済み)

### compare.py(ロット間比較)
- `yield_by_lot(s, product, lot_ids, test_category) -> DataFrame`
  — lot_id × {total, good, yield_pct}(parts_finalベース、CPはウェハ別行も)
- `bin_pareto_by_lot(s, product, lot_ids, test_category) -> DataFrame`
  — lot_id × soft_bin × count(+構成比)
- `test_stats_by_lot(s, product, lot_ids, test_category, test_nums=None) -> DataFrame`
  — lot_id × test_num × {n, mean, std, cpk, lo_limit, hi_limit}。
    test_nums=None はフェイル率上位20(reporting設定流用)
- `test_distribution_fig(s, product, lot_ids, test_category, test_num) -> go.Figure`
  — ロット別オーバーレイヒストグラム+規格線

### trend.py(時系列)
- `lot_trend(s, product, test_category, last_n=30) -> DataFrame`
  — MIR start_t 順に lot_id × {start_t, yield_pct, total, good}
- `test_trend(s, product, test_category, test_num, last_n=30) -> DataFrame`
  — ロット順の test_num の {mean, std, cpk}
- `trend_fig(df, y, control_limits=True) -> go.Figure`
  — 折れ線+全期間 mean±3σ の管理線

### correlation.py
- `cp_ft_yield(s, product, lot_ids=None) -> DataFrame`
  — ロット単位で CP yield と FT yield を突き合わせ(lot_id join)
- `die_cp_ft_join(s, product, lot_id) -> DataFrame`
  — ChipID 経由のダイレベル結合: FT の chipid_final(lot/wafer/x/y デコード値)を
    CP の parts_final(x_coord/y_coord)へ結合し、FT part_id ↔ CP die を対応付け
- `test_correlation(s, product, lot_id, test_category, test_nums) -> DataFrame`
  — part_id でピボットした上で相関行列(pandas corr)

### spatial.py(CP専用)
- `zone_yield(s, product, lot_id, n_zones=3) -> DataFrame`
  — ダイ座標を正規化半径で center/mid/edge ゾーンに分け、ゾーン別 yield
    (ウェハ中心・半径はダイ座標の min/max から推定)
- `radial_profile(s, product, lot_id, test_num) -> DataFrame`
  — 正規化半径ビン × 測定値 {n, mean, std}
- `param_wafermap_fig(s, product, lot_id, wafer_id, test_num) -> go.Figure`
  — 測定値ヒートマップ(x/y 散布、カラースケール)

### テンプレート(templates/analysis/*.py)
各テンプレートは `# %%` セル構成: パラメータセル(PRODUCT/LOTS等を編集)→
セッション生成 → 各関数呼び出し+fig.show()。コメントで使い方を説明。

## テスト方針

既存の合成 Parquet フィクスチャ(test_reporting_queries.py の _write_cp/_write_ft
パターン)を拡張し、各モジュールの数値の正しさ(yield、Cpk、ゾーン分割、CP-FT結合の
キー一致)をアサート。図関数は go.Figure 型と trace 数のスモーク。
templates/ は import 不要のスクリプトなので構文チェック(compile)のみ。

## YAGNI(やらないこと)

- GUI/Webへの組み込み(必要になればPhase 4)
- 統計的検定・SPCルール(Western Electric等)の自動判定
- ノートブック(.ipynb)形式での提供(セルスクリプトで代替、Jupytextで変換可能)
