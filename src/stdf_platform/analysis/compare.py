"""Cross-lot comparison functions: yield, bin pareto, test stats, distributions.

Full implementations land in Task 2. These stubs expose the correct signatures
so the package imports cleanly in Task 1.
"""

from __future__ import annotations

import pandas as pd


def yield_by_lot(s, product, lot_ids, test_category) -> pd.DataFrame:
    """Return per-lot yield summary (pass rate, wafer count, part count)."""
    raise NotImplementedError


def bin_pareto_by_lot(s, product, lot_ids, test_category) -> pd.DataFrame:
    """Return soft-bin failure pareto grouped by lot."""
    raise NotImplementedError


def test_stats_by_lot(s, product, lot_ids, test_category, test_nums=None) -> pd.DataFrame:
    """Return per-lot × per-test statistics (mean, σ, Cpk, fail rate)."""
    raise NotImplementedError


def test_distribution_fig(s, product, lot_ids, test_category, test_num):
    """Return a plotly Figure of the test-value distribution overlaid by lot."""
    raise NotImplementedError
