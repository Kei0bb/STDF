"""CP↔FT correlation functions: yield correlation, die-level join, test correlation.

Full implementations land in Task 4. These stubs expose the correct signatures
so the package imports cleanly in Task 1.
"""

from __future__ import annotations

import pandas as pd


def cp_ft_yield(s, product, lot_ids=None) -> pd.DataFrame:
    """Return per-lot CP yield vs FT yield for correlation analysis."""
    raise NotImplementedError


def die_cp_ft_join(s, product, lot_id) -> pd.DataFrame:
    """Return die-level CP↔FT join using decoded ChipID origin coordinates."""
    raise NotImplementedError


def test_correlation(s, product, lot_id, test_category, test_nums) -> pd.DataFrame:
    """Return pairwise Pearson correlation matrix for the given test numbers."""
    raise NotImplementedError
