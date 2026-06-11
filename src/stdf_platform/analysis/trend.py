"""Time-series trend functions: lot yield trend, test mean trend, trend figure.

Full implementations land in Task 3. These stubs expose the correct signatures
so the package imports cleanly in Task 1.
"""

from __future__ import annotations

import pandas as pd


def lot_trend(s, product, test_category, last_n=30) -> pd.DataFrame:
    """Return lot-ordered yield trend (last_n lots by start_time)."""
    raise NotImplementedError


def test_trend(s, product, test_category, test_num, last_n=30) -> pd.DataFrame:
    """Return lot-ordered test-mean / σ trend for a single test number."""
    raise NotImplementedError


def trend_fig(df, y, control_limits=True):
    """Return a plotly Figure of a trend column with optional ±3σ UCL/LCL lines."""
    raise NotImplementedError
