# %% [markdown]
# # ウェハ面内分析 (spatial, CP 専用)
# `analysis.spatial`。正規化半径によるゾーン歩留まり、半径方向プロファイル、
# パラメトリック・ウェハマップ。半径はダイ座標の min/max 中点を中心に正規化。

# %% パラメータ（編集してください）
PRODUCT = "PROD"
LOT = "G1"
WAFER = "W1"
TEST_NUM = 7
N_ZONES = 3

# %% セッション
from stdf_platform.analysis import (
    AnalysisSession, zone_yield, radial_profile, param_wafermap_fig,
)

s = AnalysisSession()

# %% ゾーン別歩留まり（center/mid/edge）
zone_yield(s, PRODUCT, LOT, n_zones=N_ZONES)

# %% 半径方向プロファイル
radial_profile(s, PRODUCT, LOT, TEST_NUM)

# %% パラメトリック・ウェハマップ
param_wafermap_fig(s, PRODUCT, LOT, WAFER, TEST_NUM).show()
