"""Render section results into a single self-contained HTML file.

plotly.js is embedded inline via plotly.offline.get_plotlyjs() so the report
opens offline with no network access. That inline bundle is ~4MB, which
dominates the file size — acceptable per the approved design (offline viewing
is the requirement). Only ONE copy of the bundle is embedded per HTML.
"""

import json

from jinja2 import Environment, PackageLoader, select_autoescape
from plotly.offline import get_plotlyjs

from .sections import SectionResult

_env = Environment(
    loader=PackageLoader("stdf_platform.reporting", "templates"),
    autoescape=select_autoescape(["html", "xml", "j2"]),
)


def render_report(product, test_category, lot_id, sections: list[SectionResult],
                  generated_at: str) -> str:
    # Per-section JSON array of figure specs (each section's figures[] are
    # already fig.to_json() strings -> wrap into a JS array literal).
    figures_json = [
        "[" + ",".join(s.figures) + "]" for s in sections
    ]
    template = _env.get_template("report.html.j2")
    return template.render(
        product=product,
        test_category=test_category,
        lot_id=lot_id,
        sections=sections,
        figures_json=figures_json,
        generated_at=generated_at,
        plotly_js=get_plotlyjs(),
    )
