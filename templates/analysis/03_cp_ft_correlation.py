# %% [markdown]
# # CP↔FT 相関 (correlation)
# `analysis.correlation`。ロット単位の CP/FT 歩留まり、ChipID 経由のダイ結合、
# テスト間相関。die_cp_ft_join は FT ロットを指定し、ChipID デコード由来の
# CP origin (lot/wafer/x/y) で CP parts_final に結合します。

# %% パラメータ（編集してください）
PRODUCT = "CHIP"
FT_LOT = "FT1"
CP_LOT = "HKPFJK"
TEST_NUMS = [1, 2]

# %% セッション
from stdf_platform.analysis import (
    AnalysisSession, cp_ft_yield, die_cp_ft_join, test_correlation,
)

s = AnalysisSession()

# %% ロット単位 CP/FT 歩留まり
cp_ft_yield(s, PRODUCT)

# %% ダイレベル CP↔FT 結合（ChipID origin）
die_cp_ft_join(s, PRODUCT, FT_LOT)

# %% テスト間相関行列
test_correlation(s, PRODUCT, CP_LOT, "CP", TEST_NUMS)
