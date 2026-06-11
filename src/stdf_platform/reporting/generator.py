"""Generate per-lot HTML reports. Reports are a disposable cache regenerated
from Parquet; output is always overwritten."""

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from stdf_platform.views import setup_views
from stdf_platform.storage import ParquetStorage
from .sections import build_sections
from .render import render_report

_CATS = ["CP", "FT", "OTHER"]


def report_path(data_dir: Path, product: str, test_category: str, lot_id: str) -> Path:
    s = ParquetStorage._sanitize
    return (
        data_dir / "reports"
        / f"product={s(product)}"
        / f"test_category={s(test_category)}"
        / f"lot_id={s(lot_id)}"
        / "report.html"
    )


def generate_lot_report(config, product: str, test_category: str, lot_id: str) -> Path:
    data_dir = config.storage.data_dir
    conn = duckdb.connect(":memory:")
    try:
        setup_views(conn, data_dir)
        sections = build_sections(conn, product, test_category, lot_id, config)
    finally:
        conn.close()
    html = render_report(
        product, test_category, lot_id, sections,
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    )
    out = report_path(data_dir, product, test_category, lot_id)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    return out


def _scan_lots(data_dir: Path) -> list[tuple[str, str, str]]:
    """Discover (product, test_category, lot_id) from data/lots partitions."""
    lots_root = data_dir / "lots"
    found: set[tuple[str, str, str]] = set()
    if not lots_root.exists():
        return []
    for prod_dir in lots_root.glob("product=*"):
        product = prod_dir.name[len("product="):]
        for cat_dir in prod_dir.glob("test_category=*"):
            category = cat_dir.name[len("test_category="):]
            for lot_dir in cat_dir.glob("**/lot_id=*"):
                lot_id = lot_dir.name[len("lot_id="):]
                found.add((product, category, lot_id))
    return sorted(found)


def _newest_parquet_mtime(data_dir: Path, product: str, category: str, lot_id: str) -> float:
    """Newest data.parquet mtime across all tables for this lot."""
    s = ParquetStorage._sanitize
    newest = 0.0
    for table in ["lots", "parts", "test_data", "wafers", "chipid"]:
        base = (data_dir / table / f"product={s(product)}"
                / f"test_category={s(category)}")
        if not base.exists():
            continue
        for pq_file in base.glob(f"**/lot_id={s(lot_id)}/**/*.parquet"):
            newest = max(newest, pq_file.stat().st_mtime)
        # lots has no wafer/retest depth; also catch the direct lot dir
        for pq_file in base.glob(f"**/lot_id={s(lot_id)}/*.parquet"):
            newest = max(newest, pq_file.stat().st_mtime)
    return newest


def pending_lots(config) -> list[tuple[str, str, str]]:
    """Lots whose report is missing or older than their newest Parquet."""
    data_dir = config.storage.data_dir
    pending = []
    for product, category, lot_id in _scan_lots(data_dir):
        report = report_path(data_dir, product, category, lot_id)
        src_mtime = _newest_parquet_mtime(data_dir, product, category, lot_id)
        if not report.exists() or report.stat().st_mtime < src_mtime:
            pending.append((product, category, lot_id))
    return pending


def resolve_lot_categories(config, product: str, lot_id: str) -> list[str]:
    """All categories a (product, lot_id) appears under (CP and/or FT)."""
    data_dir = config.storage.data_dir
    return [c for (p, c, l) in _scan_lots(data_dir) if p == product and l == lot_id]


def generate_reports_for_lots(config, lots, warn=None) -> list[Path]:
    """Generate reports, isolating failures: a failed lot warns and continues."""
    written = []
    for product, category, lot_id in lots:
        try:
            written.append(generate_lot_report(config, product, category, lot_id))
        except Exception as e:  # noqa: BLE001 — report failure must not propagate
            if warn:
                warn(f"report generation failed for {product}/{category}/{lot_id}: {e}")
    return written
