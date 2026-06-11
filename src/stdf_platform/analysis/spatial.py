"""Spatial / wafermap analysis functions: zone yield, radial profile, wafermap figure.

Full implementations land in Task 5. These stubs expose the correct signatures
so the package imports cleanly in Task 1.
"""

from __future__ import annotations

import pandas as pd


def zone_yield(s, product, lot_id, n_zones=3) -> pd.DataFrame:
    """Return yield broken down by radial zone (center / middle / edge)."""
    raise NotImplementedError


def radial_profile(s, product, lot_id, test_num) -> pd.DataFrame:
    """Return mean test value binned by radial distance from wafer centre."""
    raise NotImplementedError


def param_wafermap_fig(s, product, lot_id, wafer_id, test_num):
    """Return a plotly Figure of a parametric wafermap for a single test."""
    raise NotImplementedError
