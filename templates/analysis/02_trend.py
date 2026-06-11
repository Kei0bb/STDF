# %% [markdown]
# # トレンド (trend) — MIR start_time 順
# `analysis.trend`。ロット歩留まり/テスト統計の時系列 + mean±3σ 管理線。

# %% パラメータ（編集してください）
PRODUCT = "PROD"
TEST_CATEGORY = "CP"
TEST_NUM = 1001
LAST_N = 30

# %% セッション
from stdf_platform.analysis import AnalysisSession, lot_trend, test_trend, trend_fig

s = AnalysisSession()

# %% ロット歩留まりトレンド
df_lot = lot_trend(s, PRODUCT, TEST_CATEGORY, last_n=LAST_N)
df_lot

# %% 歩留まりトレンド図（管理線つき）
trend_fig(df_lot, "yield_pct", control_limits=True).show()

# %% テスト統計トレンド
df_test = test_trend(s, PRODUCT, TEST_CATEGORY, TEST_NUM, last_n=LAST_N)
trend_fig(df_test, "mean", control_limits=True).show()
