"""Retest-aware analysis API over the Parquet store.

    from stdf_platform.analysis import AnalysisSession
    s = AnalysisSession()
    s.run("fail_ranking", lot="LOT001")     # named queries live in sql/
"""

from .session import AnalysisSession

__all__ = ["AnalysisSession"]
