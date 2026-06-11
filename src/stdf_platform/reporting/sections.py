"""Report sections: each builds Plotly figures + tables from queries.

A section function returns a SectionResult, or None when it does not apply
(e.g. retest history for a lot with no retests, wafer maps for FT, ChipID
for CP). build_sections() assembles the ordered CP/FT list.
"""

from dataclasses import dataclass, field
from statistics import mean, pstdev

import plotly.graph_objects as go

from . import queries as q

_HIST_FIG_CAP = 24          # max parametric histograms rendered per report
_WAFER_MAP_CAP = 30         # max wafer-map figures per report


@dataclass
class SectionResult:
    title: str
    figures: list[str] = field(default_factory=list)   # fig.to_json() strings
    tables: list[dict] = field(default_factory=list)    # {caption, columns, rows}
    notes: list[str] = field(default_factory=list)


def _fig_json(fig: go.Figure) -> str:
    return fig.to_json()


def _cpk(values, lo, hi) -> float | None:
    """Process capability. Needs at least one finite limit and >1 sample."""
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return None
    sigma = pstdev(vals)
    if sigma == 0:
        return None
    mu = mean(vals)
    cands = []
    if hi is not None:
        cands.append((hi - mu) / (3 * sigma))
    if lo is not None:
        cands.append((mu - lo) / (3 * sigma))
    return round(min(cands), 3) if cands else None


# ── header ───────────────────────────────────────────────────────────────────

def header_section(conn, product, test_category, lot_id, cfg) -> SectionResult:
    h = q.lot_header(conn, product, test_category, lot_id)
    retests = q.max_retest(conn, product, test_category, lot_id)
    rows = [
        ["Lot ID", h.get("lot_id", lot_id)],
        ["Product", h.get("product", product)],
        ["Category", h.get("test_category", test_category)],
        ["Sub-process", h.get("sub_process", "")],
        ["Part type", h.get("part_type", "")],
        ["Test program", f"{h.get('job_name', '')} ({h.get('job_rev', '')})"],
        ["Tester", h.get("tester_type", "")],
        ["Operator", h.get("operator", "")],
        ["Start", str(h.get("start_time", ""))],
        ["Finish", str(h.get("finish_time", ""))],
        ["Retest count", str(retests)],
    ]
    return SectionResult(
        title="Header",
        tables=[{"caption": "Lot metadata", "columns": ["Field", "Value"], "rows": rows}],
    )


# ── yield summary ────────────────────────────────────────────────────────────

def yield_section(conn, product, test_category, lot_id, cfg) -> SectionResult:
    wy = q.wafer_yield(conn, product, test_category, lot_id)
    total = q.lot_yield_total(conn, product, test_category, lot_id)
    labels = [r["wafer_id"] or "(lot)" for r in wy]
    fig = go.Figure(go.Bar(
        x=labels,
        y=[r["yield_pct"] for r in wy],
        marker_color="#6366f1",
        text=[f"{r['yield_pct']:.1f}%" for r in wy],
        textposition="outside",
    ))
    fig.update_layout(
        title="Yield by wafer", yaxis_title="Yield %",
        yaxis_range=[0, 105], template="plotly_dark",
        margin=dict(l=40, r=20, t=40, b=40),
    )
    rows = [[r["wafer_id"] or "(lot)", int(r["total"]), int(r["good"]),
             f"{r['yield_pct']:.2f}"] for r in wy]
    rows.append(["TOTAL", total["total"], total["good"], f"{total['yield_pct']:.2f}"])
    return SectionResult(
        title="Yield summary",
        figures=[_fig_json(fig)],
        tables=[{"caption": "Per-wafer yield",
                 "columns": ["Wafer", "Total", "Good", "Yield %"], "rows": rows}],
    )


# ── bin pareto ───────────────────────────────────────────────────────────────

def bin_pareto_section(conn, product, test_category, lot_id, cfg) -> SectionResult:
    data = q.bin_pareto(conn, product, test_category, lot_id)
    wafers = sorted({r["wafer_id"] for r in data})
    bins = sorted({r["soft_bin"] for r in data},
                  key=lambda b: -sum(r["count"] for r in data if r["soft_bin"] == b))
    lookup = {(r["soft_bin"], r["wafer_id"]): r["count"] for r in data}
    fig = go.Figure()
    for w in wafers:
        fig.add_bar(name=(w or "(lot)"), x=[str(b) for b in bins],
                    y=[lookup.get((b, w), 0) for b in bins])
    fig.update_layout(
        barmode="stack", title="Soft-bin pareto (stacked by wafer)",
        xaxis_title="Soft bin", yaxis_title="Parts",
        template="plotly_dark", margin=dict(l=40, r=20, t=40, b=40),
    )
    rows = [[str(b), sum(r["count"] for r in data if r["soft_bin"] == b)] for b in bins]
    return SectionResult(
        title="Bin pareto",
        figures=[_fig_json(fig)],
        tables=[{"caption": "Soft-bin totals", "columns": ["Soft bin", "Count"], "rows": rows}],
    )


# ── wafer maps grid (CP only) ────────────────────────────────────────────────

def wafer_maps_section(conn, product, test_category, lot_id, cfg) -> SectionResult | None:
    ids = q.wafer_ids(conn, product, test_category, lot_id)
    if not ids:
        return None
    figures = []
    capped = ids[:_WAFER_MAP_CAP]
    for wid in capped:
        dies = q.wafer_map(conn, product, test_category, lot_id, wid)
        fig = go.Figure(go.Scatter(
            x=[d["x_coord"] for d in dies],
            y=[d["y_coord"] for d in dies],
            mode="markers",
            marker=dict(
                size=8, symbol="square",
                color=[d["soft_bin"] for d in dies],
                colorscale="Viridis", showscale=True,
                colorbar=dict(title="Soft bin"),
            ),
            text=[f"bin {d['soft_bin']}" for d in dies],
        ))
        fig.update_layout(
            title=f"Wafer {wid}", template="plotly_dark",
            yaxis=dict(autorange="reversed", scaleanchor="x", scaleratio=1),
            margin=dict(l=30, r=20, t=40, b=30),
        )
        figures.append(_fig_json(fig))
    notes = []
    if len(ids) > _WAFER_MAP_CAP:
        notes.append(f"Showing {_WAFER_MAP_CAP} of {len(ids)} wafers (capped).")
    return SectionResult(title="Wafer maps", figures=figures, notes=notes)


# ── parametric (top fail tests + histograms) ─────────────────────────────────

def _select_histogram_tests(conn, product, test_category, lot_id, cfg, fails):
    top = [f["test_num"] for f in fails]
    always = list(cfg.reporting.always_include_tests.get(product, []))
    ordered = top + [t for t in always if t not in top]
    return ordered[:_HIST_FIG_CAP]


def parametric_section(conn, product, test_category, lot_id, cfg) -> SectionResult:
    fails = q.top_fail_tests(conn, product, test_category, lot_id,
                             cfg.reporting.histogram_top_n)
    table_rows = [[f["test_num"], f["test_name"], int(f["total"]),
                   int(f["fail_count"]), f"{f['fail_rate']:.2f}"] for f in fails]
    figures = []
    for tn in _select_histogram_tests(conn, product, test_category, lot_id, cfg, fails):
        v = q.test_values(conn, product, test_category, lot_id, tn)
        if not v or not v["values"]:
            continue
        cpk = _cpk(v["values"], v["lo_limit"], v["hi_limit"])
        fig = go.Figure(go.Histogram(x=v["values"], nbinsx=40, marker_color="#38bdf8"))
        for lim, name in ((v["lo_limit"], "LSL"), (v["hi_limit"], "USL")):
            if lim is not None:
                fig.add_vline(x=lim, line_color="#ef4444", line_dash="dash",
                              annotation_text=name)
        cpk_txt = f"  Cpk={cpk}" if cpk is not None else ""
        fig.update_layout(
            title=f"{v['test_name']} (#{tn}) [{v['units']}]{cpk_txt}",
            template="plotly_dark", bargap=0.02,
            margin=dict(l=40, r=20, t=40, b=40),
        )
        figures.append(_fig_json(fig))
    return SectionResult(
        title="Parametric",
        figures=figures,
        tables=[{"caption": "Top failing tests",
                 "columns": ["Test #", "Name", "Total", "Fails", "Fail %"],
                 "rows": table_rows}],
    )


# ── retest history (only when retests > 0) ───────────────────────────────────

def retest_section(conn, product, test_category, lot_id, cfg) -> SectionResult | None:
    if q.max_retest(conn, product, test_category, lot_id) == 0:
        return None
    hist = q.retest_history(conn, product, test_category, lot_id)
    rows = [[int(r["retest_num"]), int(r["parts"]), int(r["good"]),
             f"{r['yield_pct']:.2f}"] for r in hist]
    return SectionResult(
        title="Retest history",
        tables=[{"caption": "Per-retest counts",
                 "columns": ["Retest", "Parts", "Good", "Yield %"], "rows": rows}],
    )


# ── ChipID summary (FT only) ─────────────────────────────────────────────────

def chipid_section(conn, product, test_category, lot_id, cfg) -> SectionResult | None:
    summary = q.chipid_summary(conn, product, test_category, lot_id)
    if not summary:
        return None
    rows = [[r["origin_fab"], int(r["dies"]), int(r["valid_dies"]),
             int(r["origin_lots"]), int(r["origin_wafers"])] for r in summary]
    fig = go.Figure(go.Bar(
        x=[r["origin_fab"] for r in summary],
        y=[r["dies"] for r in summary], marker_color="#22c55e",
    ))
    fig.update_layout(title="Dies by origin fab", template="plotly_dark",
                      yaxis_title="Dies", margin=dict(l=40, r=20, t=40, b=40))
    return SectionResult(
        title="ChipID summary",
        figures=[_fig_json(fig)],
        tables=[{"caption": "Origin fab breakdown",
                 "columns": ["Origin fab", "Dies", "Valid", "Origin lots", "Origin wafers"],
                 "rows": rows}],
    )


def build_sections(conn, product, test_category, lot_id, cfg) -> list[SectionResult]:
    """Ordered section list. CP: header, yield, bins, wafer maps, parametric,
    retest. FT: header, yield, bins, parametric, retest, ChipID (no wafer maps)."""
    builders = [header_section, yield_section, bin_pareto_section]
    if test_category != "FT":
        builders.append(wafer_maps_section)
    builders.append(parametric_section)
    builders.append(retest_section)
    if test_category == "FT":
        builders.append(chipid_section)
    out = []
    for b in builders:
        res = b(conn, product, test_category, lot_id, cfg)
        if res is not None:
            out.append(res)
    return out
