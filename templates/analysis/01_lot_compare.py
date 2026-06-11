# %% [markdown]
# # ロット間比較 (lot compare)
# `analysis.compare` を使ったロット横断の歩留まり/ビン/テスト統計。
# VS Code で「Run Cell」または Shift+Enter。下のパラメータセルを編集して実行。

# %% パラメータ（編集してください）
PRODUCT = "PROD"
LOTS = ["LOT1", "LOT2"]
TEST_CATEGORY = "CP"
TEST_NUM = 1001

# %% セッション
from stdf_platform.analysis import (
    AnalysisSession, yield_by_lot, bin_pareto_by_lot,
    test_stats_by_lot, test_distribution_fig,
)

s = AnalysisSession()        # config.yaml / STDF_CONFIG からデータディレクトリ解決

# %% 歩留まり（ロット×ウェハ）
yield_by_lot(s, PRODUCT, LOTS, TEST_CATEGORY)

# %% ビン構成比
bin_pareto_by_lot(s, PRODUCT, LOTS, TEST_CATEGORY)

# %% テスト統計（test_nums=None でフェイル上位を自動選択）
test_stats_by_lot(s, PRODUCT, LOTS, TEST_CATEGORY)

# %% 分布オーバーレイ
test_distribution_fig(s, PRODUCT, LOTS, TEST_CATEGORY, TEST_NUM).show()
