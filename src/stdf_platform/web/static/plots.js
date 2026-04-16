/** Plotly chart helpers for stdf */

const PLOTLY_LAYOUT_BASE = {
  paper_bgcolor: 'transparent',
  plot_bgcolor: '#0f172a',
  font: { color: '#94a3b8', size: 11 },
  margin: { t: 10, r: 10, b: 40, l: 50 },
  xaxis: { gridcolor: '#334155', linecolor: '#475569', tickcolor: '#475569' },
  yaxis: { gridcolor: '#334155', linecolor: '#475569', tickcolor: '#475569' },
  legend: { bgcolor: 'transparent', font: { color: '#94a3b8' } },
};

const PLOTLY_CONFIG = {
  displayModeBar: true,
  modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d'],
  displaylogo: false,
  responsive: true,
};

// Yield bar chart (by lot)
function drawYieldBar(elementId, rows) {
  if (!rows || rows.length === 0) return;
  const colors = rows.map(r => {
    const y = r.yield_pct || 0;
    return y >= 90 ? '#4ade80' : y >= 80 ? '#facc15' : '#f87171';
  });
  Plotly.newPlot(elementId, [{
    type: 'bar',
    x: rows.map(r => r.lot_id),
    y: rows.map(r => r.yield_pct || 0),
    marker: { color: colors },
    text: rows.map(r => `${(r.yield_pct||0).toFixed(1)}%`),
    textposition: 'outside',
    hovertemplate: '<b>%{x}</b><br>Yield: %{y:.2f}%<br>Parts: %{customdata[0]:,}<extra></extra>',
    customdata: rows.map(r => [r.total_parts]),
  }], {
    ...PLOTLY_LAYOUT_BASE,
    yaxis: { ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Yield (%)', range: [0, 110] },
    margin: { t: 20, r: 10, b: 50, l: 55 },
  }, PLOTLY_CONFIG);
}

// Wafer yield horizontal bar
function drawWaferYield(elementId, rows) {
  if (!rows || rows.length === 0) return;
  const colors = rows.map(r => {
    const y = r.yield_pct || 0;
    return y >= 90 ? '#4ade80' : y >= 80 ? '#facc15' : '#f87171';
  });
  Plotly.newPlot(elementId, [{
    type: 'bar',
    orientation: 'h',
    y: rows.map(r => r.wafer_id),
    x: rows.map(r => r.yield_pct || 0),
    marker: { color: colors },
    text: rows.map(r => `${(r.yield_pct||0).toFixed(1)}%`),
    textposition: 'outside',
    hovertemplate: '<b>%{y}</b><br>Yield: %{x:.2f}%<br>Good: %{customdata[0]} / %{customdata[1]}<extra></extra>',
    customdata: rows.map(r => [r.good, r.total]),
  }], {
    ...PLOTLY_LAYOUT_BASE,
    xaxis: { ...PLOTLY_LAYOUT_BASE.xaxis, title: 'Yield (%)', range: [0, 110] },
    margin: { t: 10, r: 60, b: 40, l: 45 },
  }, PLOTLY_CONFIG);
}

// Wafer map scatter plot
function drawWaferMap(elementId, points, colorBy) {
  if (!points || points.length === 0) return;

  let colors, colorTitle, colorscale;
  if (colorBy === 'passed') {
    colors = points.map(p => p.passed ? 0 : 1);
    colorscale = [[0, '#4ade80'], [1, '#f87171']];
    colorTitle = 'Pass=0 Fail=1';
  } else {
    const field = colorBy === 'soft_bin' ? 'soft_bin' : 'hard_bin';
    colors = points.map(p => p[field]);
    colorscale = 'Viridis';
    colorTitle = colorBy === 'soft_bin' ? 'Soft Bin' : 'Hard Bin';
  }

  Plotly.newPlot(elementId, [{
    type: 'scattergl',
    mode: 'markers',
    x: points.map(p => p.x_coord),
    y: points.map(p => p.y_coord),
    marker: {
      color: colors,
      colorscale,
      size: 6,
      opacity: 0.85,
      colorbar: { title: colorTitle, thickness: 12, len: 0.8 },
    },
    hovertemplate: 'X:%{x} Y:%{y}<br>Soft Bin:%{customdata[0]}<br>Hard Bin:%{customdata[1]}<extra></extra>',
    customdata: points.map(p => [p.soft_bin, p.hard_bin]),
  }], {
    ...PLOTLY_LAYOUT_BASE,
    xaxis: { ...PLOTLY_LAYOUT_BASE.xaxis, scaleanchor: 'y', scaleratio: 1 },
    margin: { t: 10, r: 70, b: 40, l: 45 },
  }, PLOTLY_CONFIG);
}

// Histogram with limit lines
function drawDistribution(elementId, dist) {
  if (!dist || !dist.values || dist.values.length === 0) return;

  const traces = [{
    type: 'histogram',
    x: dist.values,
    nbinsx: 40,
    marker: { color: '#6366f1', opacity: 0.8 },
    name: dist.test_name || `Test ${dist.test_num}`,
    hovertemplate: 'Range: %{x}<br>Count: %{y}<extra></extra>',
  }];

  const shapes = [];
  if (dist.lo_limit != null) {
    shapes.push({ type: 'line', x0: dist.lo_limit, x1: dist.lo_limit, y0: 0, y1: 1, yref: 'paper', line: { color: '#f87171', width: 2, dash: 'dot' } });
  }
  if (dist.hi_limit != null) {
    shapes.push({ type: 'line', x0: dist.hi_limit, x1: dist.hi_limit, y0: 0, y1: 1, yref: 'paper', line: { color: '#f87171', width: 2, dash: 'dot' } });
  }

  const units = dist.units ? ` (${dist.units})` : '';
  Plotly.newPlot(elementId, traces, {
    ...PLOTLY_LAYOUT_BASE,
    xaxis: { ...PLOTLY_LAYOUT_BASE.xaxis, title: `${dist.test_name || ''}${units}` },
    yaxis: { ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Count' },
    shapes,
    margin: { t: 10, r: 10, b: 45, l: 55 },
  }, PLOTLY_CONFIG);
}

// Fail pareto horizontal bar
function drawPareto(elementId, rows) {
  if (!rows || rows.length === 0) {
    Plotly.newPlot(elementId, [], {
      ...PLOTLY_LAYOUT_BASE,
      annotations: [{
        text: 'No failures in selection', x: 0.5, y: 0.5,
        xref: 'paper', yref: 'paper', showarrow: false,
        font: { color: '#64748b', size: 13 },
      }],
    }, PLOTLY_CONFIG);
    return;
  }
  const sorted = [...rows].sort((a, b) => a.fail_rate - b.fail_rate);
  Plotly.newPlot(elementId, [{
    type: 'bar',
    orientation: 'h',
    y: sorted.map(r => r.test_name || `#${r.test_num}`),
    x: sorted.map(r => r.fail_rate),
    marker: { color: '#f87171' },
    text: sorted.map(r => `${r.fail_rate.toFixed(1)}%`),
    textposition: 'outside',
    hovertemplate: '<b>%{y}</b><br>Fail rate: %{x:.2f}%<br>Count: %{customdata}<extra></extra>',
    customdata: sorted.map(r => r.fail_count),
  }], {
    ...PLOTLY_LAYOUT_BASE,
    xaxis: { ...PLOTLY_LAYOUT_BASE.xaxis, title: 'Fail Rate (%)' },
    margin: { t: 10, r: 60, b: 40, l: 130 },
  }, PLOTLY_CONFIG);
}
