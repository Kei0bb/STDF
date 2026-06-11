"""Report listing endpoint — scans data/reports/ partitions."""

import logging
from datetime import datetime, timezone
from urllib.parse import quote

from fastapi import APIRouter

from .deps import get_data_dir

logger = logging.getLogger(__name__)
router = APIRouter(tags=["reports"])


@router.get("/reports")
def list_reports() -> list[dict]:
    """List generated reports with product/category/lot and modified time."""
    reports_dir = get_data_dir() / "reports"
    out = []
    if not reports_dir.exists():
        return out
    for report in reports_dir.glob(
        "product=*/test_category=*/lot_id=*/report.html"
    ):
        try:
            parts = {
                p.split("=", 1)[0]: p.split("=", 1)[1]
                for p in report.parts if "=" in p
            }
            mtime = datetime.fromtimestamp(
                report.stat().st_mtime, tz=timezone.utc
            ).isoformat()
            rel = report.relative_to(reports_dir).as_posix()
            out.append({
                "product": parts.get("product", ""),
                "test_category": parts.get("test_category", ""),
                "lot_id": parts.get("lot_id", ""),
                "modified": mtime,
                "url": "/reports/" + quote(rel),
            })
        except Exception as e:  # noqa: BLE001
            logger.warning("skipping report %s: %s", report, e)
    out.sort(key=lambda r: (r["product"], r["test_category"], r["lot_id"]))
    return out
