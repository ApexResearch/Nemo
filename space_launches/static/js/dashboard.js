'use strict';

// ── State ──────────────────────────────────────────────────────────────────
const state = {
  data: null,
  selectedYear: '2025',
  activeCountries: null,   // null = all
  lastHash: null,
  pollInterval: null,
};

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const YEARS  = ['2023','2024','2025','2026'];

// ── Plotly base layout ─────────────────────────────────────────────────────
const BASE_LAYOUT = {
  paper_bgcolor: 'transparent',
  plot_bgcolor:  'transparent',
  font: { color: '#e2e8f0', family: 'Inter, sans-serif', size: 12 },
  margin: { t: 10, r: 10, b: 40, l: 45 },
  colorway: ['#3b82f6','#ef4444','#f97316','#22c55e','#a855f7','#ec4899','#f59e0b','#06b6d4','#6b7280'],
  xaxis: {
    gridcolor: '#2d3748', linecolor: '#2d3748',
    tickfont: { size: 11 }, showgrid: true,
  },
  yaxis: {
    gridcolor: '#2d3748', linecolor: '#2d3748',
    tickfont: { size: 11 }, showgrid: true,
  },
  legend: {
    bgcolor: 'rgba(0,0,0,0)',
    bordercolor: '#2d3748',
    font: { size: 11 },
    orientation: 'h',
    x: 0, y: -0.25,
  },
  hoverlabel: { bgcolor: '#1f2937', bordercolor: '#3b82f6', font: { size: 12 } },
};

const PLOTLY_CONFIG = { responsive: true, displayModeBar: false };

// ── Helpers ────────────────────────────────────────────────────────────────
function getCountries() {
  return state.data.countries;
}

function monthlyArr(year, countryId) {
  return state.data.launches[year]?.countries[countryId] ?? Array(12).fill(0);
}

function yearTotal(year, countryId) {
  return monthlyArr(year, countryId).reduce((a, b) => a + b, 0);
}

function allCountriesTotals(year) {
  const result = {};
  getCountries().forEach(c => { result[c.id] = yearTotal(year, c.id); });
  return result;
}

function globalTotal(year, onlyActual = false) {
  const meta = state.data.launches[year];
  let sum = 0;
  getCountries().forEach(c => {
    const arr = monthlyArr(year, c.id);
    const limit = onlyActual ? (meta?.actual_months ?? 12) : 12;
    for (let i = 0; i < limit; i++) sum += (arr[i] ?? 0);
  });
  return sum;
}

function activeCountryList() {
  if (!state.activeCountries) return getCountries();
  return getCountries().filter(c => state.activeCountries.has(c.id));
}

function yoyChange(year, countryId) {
  const prev = YEARS[YEARS.indexOf(year) - 1];
  if (!prev) return null;
  const cy = yearTotal(year, countryId);
  const py = yearTotal(prev, countryId);
  if (py === 0) return null;
  return Math.round(((cy - py) / py) * 100);
}

// ── Init ───────────────────────────────────────────────────────────────────
async function init() {
  const res = await fetch('/api/data');
  const json = await res.json();
  state.data   = json.launches;
  state.lastHash = json.hash;
  updateLastUpdated(json.updated_at);

  // default: all countries active
  state.activeCountries = null;

  buildYearTabs();
  buildCountryChips();
  renderAll();
  startPolling();
}

// ── Year Tabs ──────────────────────────────────────────────────────────────
function buildYearTabs() {
  const container = document.getElementById('year-tabs');
  container.innerHTML = '';
  YEARS.forEach(yr => {
    const btn = document.createElement('button');
    btn.className = 'year-btn' + (yr === '2026' ? ' predicted-tab' : '') + (yr === state.selectedYear ? ' active' : '');
    btn.textContent = yr + (yr === '2026' ? ' (partial)' : '');
    btn.onclick = () => { state.selectedYear = yr; buildYearTabs(); renderAll(); };
    container.appendChild(btn);
  });
}

// ── Country Chips ──────────────────────────────────────────────────────────
function buildCountryChips() {
  const container = document.getElementById('country-chips');
  container.innerHTML = '';

  const allBtn = document.createElement('button');
  allBtn.className = 'country-chip' + (!state.activeCountries ? ' active' : '');
  allBtn.textContent = 'All';
  allBtn.style.background = !state.activeCountries ? '#3b82f6' : '';
  allBtn.onclick = () => { state.activeCountries = null; buildCountryChips(); renderAll(); };
  container.appendChild(allBtn);

  getCountries().forEach(c => {
    const isActive = !state.activeCountries || state.activeCountries.has(c.id);
    const btn = document.createElement('button');
    btn.className = 'country-chip' + (isActive ? ' active' : '');
    btn.textContent = c.flag + ' ' + c.id;
    if (isActive) btn.style.background = c.color;
    btn.onclick = () => toggleCountry(c.id);
    container.appendChild(btn);
  });
}

function toggleCountry(id) {
  if (!state.activeCountries) {
    state.activeCountries = new Set([id]);
  } else {
    if (state.activeCountries.has(id)) {
      state.activeCountries.delete(id);
      if (state.activeCountries.size === 0) state.activeCountries = null;
    } else {
      state.activeCountries.add(id);
    }
  }
  buildCountryChips();
  renderAll();
}

// ── Render All ─────────────────────────────────────────────────────────────
function renderAll() {
  const yr = state.selectedYear;
  renderStatCards(yr);
  renderMap(yr);
  renderPie(yr);
  renderMonthlyBar(yr);
  renderTrends();
  renderSeasonHeatmap();
  render2026Tracker();
}

// ── Stat Cards ─────────────────────────────────────────────────────────────
function renderStatCards(yr) {
  const meta   = state.data.launches[yr];
  const total  = globalTotal(yr);
  const actual = globalTotal(yr, true);
  const totals = allCountriesTotals(yr);
  const topC   = getCountries().reduce((a, b) => totals[a.id] > totals[b.id] ? a : b);

  // Peak month (sum across countries)
  const monthly = Array(12).fill(0);
  getCountries().forEach(c => {
    monthlyArr(yr, c.id).forEach((v, i) => { monthly[i] += v; });
  });
  const peakIdx   = monthly.indexOf(Math.max(...monthly));
  const peakCount = monthly[peakIdx];

  const prevYr = YEARS[YEARS.indexOf(yr) - 1];
  const prevTotal = prevYr ? globalTotal(prevYr) : null;
  const yoyPct = prevTotal ? Math.round(((total - prevTotal) / prevTotal) * 100) : null;

  const setCard = (id, value, sub, change) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.querySelector('.value').textContent = value;
    el.querySelector('.sub').textContent   = sub;
    const ch = el.querySelector('.change');
    if (change !== null && ch) {
      ch.textContent  = (change >= 0 ? '▲ +' : '▼ ') + change + '%';
      ch.className    = 'change ' + (change >= 0 ? 'up' : 'down');
      ch.style.display = '';
    } else if (ch) { ch.style.display = 'none'; }
  };

  const displayTotal = (yr === '2026') ? `${actual} / ~${total}` : total;
  setCard('card-total',    displayTotal,         yr === '2026' ? 'actual / full-year est.' : 'orbital launches', yoyPct);
  setCard('card-top',      topC.flag + ' ' + topC.id, totals[topC.id] + ' launches',                   null);
  setCard('card-peak',     MONTHS[peakIdx] + ' ' + yr, peakCount + ' launches',                          null);
  setCard('card-countries',getCountries().filter(c => totals[c.id] > 0).length, 'active launch nations', null);
}

// ── World Map ──────────────────────────────────────────────────────────────
function renderMap(yr) {
  const totals = allCountriesTotals(yr);
  const locations = [];
  const values    = [];
  const texts     = [];

  getCountries().forEach(c => {
    if (!c.iso3 || !totals[c.id]) return;
    locations.push(c.iso3);
    values.push(totals[c.id]);
    texts.push(`<b>${c.flag} ${c.label}</b><br>${totals[c.id]} launches`);
  });

  const trace = {
    type: 'choropleth',
    locationmode: 'ISO-3',
    locations,
    z: values,
    text: texts,
    hovertemplate: '%{text}<extra></extra>',
    colorscale: [
      [0,   '#0c1445'],
      [0.1, '#1e3a8a'],
      [0.3, '#1d4ed8'],
      [0.6, '#3b82f6'],
      [0.8, '#60a5fa'],
      [1,   '#bfdbfe'],
    ],
    colorbar: {
      title: { text: 'Launches', font: { size: 11, color: '#94a3b8' } },
      tickfont: { color: '#94a3b8', size: 10 },
      len: 0.7, thickness: 12,
    },
    zmin: 0,
    zmax: Math.max(...values),
    showscale: true,
  };

  const layout = {
    ...BASE_LAYOUT,
    margin: { t: 0, r: 0, b: 0, l: 0 },
    geo: {
      bgcolor:       'transparent',
      showframe:     false,
      showcoastlines:true,
      coastlinecolor:'#2d3748',
      showland:      true,
      landcolor:     '#1f2937',
      showocean:     true,
      oceancolor:    '#0d1424',
      showlakes:     true,
      lakecolor:     '#0d1424',
      showcountries: true,
      countrycolor:  '#374151',
      projection:    { type: 'natural earth' },
    },
  };

  Plotly.react('chart-map', [trace], layout, PLOTLY_CONFIG);
}

// ── Pie Chart ─────────────────────────────────────────────────────────────
function renderPie(yr) {
  const totals = allCountriesTotals(yr);
  const countries = getCountries().filter(c => totals[c.id] > 0);

  const trace = {
    type: 'pie',
    labels:  countries.map(c => c.flag + ' ' + c.id),
    values:  countries.map(c => totals[c.id]),
    marker:  { colors: countries.map(c => c.color) },
    hole:    0.45,
    textinfo: 'percent',
    hovertemplate: '<b>%{label}</b><br>%{value} launches (%{percent})<extra></extra>',
    textfont: { size: 11, color: '#fff' },
  };

  const layout = {
    ...BASE_LAYOUT,
    margin: { t: 0, r: 10, b: 60, l: 10 },
    showlegend: true,
    legend: {
      ...BASE_LAYOUT.legend,
      orientation: 'h',
      x: 0, y: -0.1,
      font: { size: 10 },
    },
    annotations: [{
      text: '<b>' + globalTotal(yr) + '</b><br>total',
      x: 0.5, y: 0.5, xref: 'paper', yref: 'paper',
      showarrow: false,
      font: { size: 14, color: '#e2e8f0' },
    }],
  };

  Plotly.react('chart-pie', [trace], layout, PLOTLY_CONFIG);
}

// ── Monthly Bar ────────────────────────────────────────────────────────────
function renderMonthlyBar(yr) {
  const meta = state.data.launches[yr];
  const actualMonths = meta?.actual_months ?? 12;
  const countries = activeCountryList();

  const traces = countries.map(c => {
    const vals = monthlyArr(yr, c.id);
    // Split into actual + predicted segments for 2026
    const actualVals    = vals.map((v, i) => i < actualMonths ? v : null);
    const predictedVals = vals.map((v, i) => i < actualMonths ? null : v);

    const base = {
      type: 'bar',
      name: c.flag + ' ' + c.id,
      x: MONTHS,
      marker: { color: c.color },
      hovertemplate: `<b>${c.flag} ${c.id}</b>: %{y} (%{x} ${yr})<extra></extra>`,
    };

    if (yr !== '2026') {
      return { ...base, y: vals };
    }

    return [
      { ...base, y: actualVals, showlegend: true, legendgroup: c.id },
      {
        ...base, y: predictedVals, showlegend: false, legendgroup: c.id,
        marker: { color: c.color, opacity: 0.4, pattern: { shape: '/', size: 4, fgcolor: c.color, bgcolor: 'transparent' } },
        name: c.id + ' (pred)',
      },
    ];
  });

  const flatTraces = traces.flat();

  const shapes = (yr === '2026') ? [{
    type: 'line',
    x0: MONTHS[actualMonths - 1], x1: MONTHS[actualMonths - 1],
    y0: 0, y1: 1, yref: 'paper',
    line: { color: '#a78bfa', width: 2, dash: 'dot' },
  }] : [];

  const annotations = (yr === '2026') ? [{
    x: MONTHS[actualMonths - 0.5],
    y: 1, yref: 'paper',
    text: '← actual | predicted →',
    showarrow: false,
    font: { size: 10, color: '#a78bfa' },
    xanchor: 'center',
  }] : [];

  const layout = {
    ...BASE_LAYOUT,
    barmode: 'stack',
    bargap: 0.15,
    shapes,
    annotations,
    xaxis: { ...BASE_LAYOUT.xaxis, tickangle: -30 },
  };

  Plotly.react('chart-monthly', flatTraces, layout, PLOTLY_CONFIG);
}

// ── Trend Lines ────────────────────────────────────────────────────────────
function renderTrends() {
  const countries = activeCountryList();
  const traces = countries.map(c => ({
    type: 'scatter',
    mode: 'lines+markers',
    name: c.flag + ' ' + c.id,
    x: YEARS,
    y: YEARS.map(yr => yearTotal(yr, c.id)),
    line:   { color: c.color, width: 2.5 },
    marker: { color: c.color, size: 7, symbol: 'circle' },
    hovertemplate: `<b>${c.flag} ${c.id}</b><br>%{x}: %{y} launches<extra></extra>`,
    connectgaps: true,
  }));

  // Add total trace
  traces.push({
    type: 'scatter',
    mode: 'lines+markers',
    name: '🌐 Total',
    x: YEARS,
    y: YEARS.map(yr => globalTotal(yr)),
    line:   { color: '#ffffff', width: 2, dash: 'dot' },
    marker: { color: '#ffffff', size: 6, symbol: 'diamond' },
    hovertemplate: '<b>Total</b><br>%{x}: %{y} launches<extra></extra>',
  });

  // Dashed segment for 2026 predicted
  const x0 = '2025', x1 = '2026';
  const shapes = [{
    type: 'rect',
    x0: '2025.5', x1: '2026.5',
    y0: 0, y1: 1, yref: 'paper',
    fillcolor: 'rgba(167,139,250,0.06)',
    line: { width: 0 },
  }];

  const layout = {
    ...BASE_LAYOUT,
    shapes,
    xaxis: { ...BASE_LAYOUT.xaxis, type: 'category' },
    annotations: [{
      x: '2026', y: 1, yref: 'paper',
      text: '2026 partial',
      showarrow: false,
      font: { size: 10, color: '#a78bfa' },
    }],
  };

  Plotly.react('chart-trends', traces, layout, PLOTLY_CONFIG);
}

// ── Seasonal Heatmap ───────────────────────────────────────────────────────
function renderSeasonHeatmap() {
  // Sum all countries by month × year
  const matrix = YEARS.map(yr => {
    return MONTHS.map((_, mi) => {
      return getCountries().reduce((sum, c) => sum + (monthlyArr(yr, c.id)[mi] ?? 0), 0);
    });
  });

  const trace = {
    type: 'heatmap',
    z: matrix,
    x: MONTHS,
    y: YEARS,
    colorscale: [
      [0,   '#0c1445'],
      [0.15,'#1e3a8a'],
      [0.4, '#2563eb'],
      [0.7, '#60a5fa'],
      [1,   '#bfdbfe'],
    ],
    hoverongaps: false,
    hovertemplate: '<b>%{y} %{x}</b>: %{z} launches<extra></extra>',
    colorbar: {
      title: { text: 'Launches', font: { size: 11, color: '#94a3b8' } },
      tickfont: { color: '#94a3b8', size: 10 },
      len: 0.8, thickness: 12,
    },
    xgap: 2,
    ygap: 2,
    text: matrix.map(row => row.map(v => v.toString())),
    texttemplate: '%{text}',
    textfont: { size: 11, color: '#fff' },
  };

  const layout = {
    ...BASE_LAYOUT,
    margin: { t: 10, r: 80, b: 40, l: 55 },
    xaxis: { ...BASE_LAYOUT.xaxis, type: 'category', tickangle: -30 },
    yaxis: { ...BASE_LAYOUT.yaxis, type: 'category', autorange: 'reversed' },
  };

  Plotly.react('chart-heatmap', [trace], layout, PLOTLY_CONFIG);
}

// ── 2026 Tracker ───────────────────────────────────────────────────────────
function render2026Tracker() {
  const meta = state.data.launches['2026'];
  const actualMonths = meta?.actual_months ?? 4;
  const countries = getCountries();

  // Monthly totals actual vs full-year
  const actualMonthly    = Array(12).fill(0);
  const predictedMonthly = Array(12).fill(0);

  countries.forEach(c => {
    const arr = monthlyArr('2026', c.id);
    arr.forEach((v, i) => {
      if (i < actualMonths) actualMonthly[i] += v;
      else predictedMonthly[i] += v;
    });
  });

  // For 2025 comparison line
  const monthly2025 = Array(12).fill(0);
  countries.forEach(c => {
    monthlyArr('2025', c.id).forEach((v, i) => { monthly2025[i] += v; });
  });

  const traces = [
    {
      type: 'bar',
      name: '2026 Actual',
      x: MONTHS.slice(0, actualMonths),
      y: actualMonthly.slice(0, actualMonths),
      marker: { color: '#3b82f6' },
      hovertemplate: '<b>2026 Actual</b> %{x}: %{y}<extra></extra>',
    },
    {
      type: 'bar',
      name: '2026 Predicted',
      x: MONTHS.slice(actualMonths),
      y: predictedMonthly.slice(actualMonths),
      marker: { color: '#a855f7', opacity: 0.5 },
      hovertemplate: '<b>2026 Predicted</b> %{x}: %{y}<extra></extra>',
    },
    {
      type: 'scatter',
      mode: 'lines+markers',
      name: '2025 Reference',
      x: MONTHS,
      y: monthly2025,
      line:   { color: '#f59e0b', width: 2, dash: 'dash' },
      marker: { color: '#f59e0b', size: 5 },
      hovertemplate: '<b>2025 Ref</b> %{x}: %{y}<extra></extra>',
    },
  ];

  const shapes = [{
    type: 'line',
    x0: MONTHS[actualMonths - 1], x1: MONTHS[actualMonths - 1],
    y0: 0, y1: 1, yref: 'paper',
    line: { color: '#a78bfa', width: 2, dash: 'dot' },
  }];

  const layout = {
    ...BASE_LAYOUT,
    barmode: 'overlay',
    shapes,
    xaxis: { ...BASE_LAYOUT.xaxis, tickangle: -30 },
    annotations: [{
      x: MONTHS[actualMonths],
      y: 0.98, yref: 'paper',
      text: 'predicted →',
      showarrow: false,
      font: { size: 10, color: '#a78bfa' },
      xanchor: 'left',
    }],
  };

  Plotly.react('chart-2026', traces, layout, PLOTLY_CONFIG);
}

// ── Polling ────────────────────────────────────────────────────────────────
function startPolling() {
  state.pollInterval = setInterval(async () => {
    try {
      const res  = await fetch('/api/hash');
      const json = await res.json();
      if (json.hash !== state.lastHash) {
        state.lastHash = json.hash;
        const dataRes  = await fetch('/api/data');
        const dataJson = await dataRes.json();
        state.data     = dataJson.launches;
        updateLastUpdated(dataJson.updated_at);
        renderAll();
        showToast('🚀 Data updated — charts refreshed!');
      }
    } catch (e) {
      console.warn('Poll failed:', e);
    }
  }, 30_000);
}

function updateLastUpdated(ts) {
  const el = document.getElementById('last-updated');
  if (el) el.textContent = 'Last sync: ' + (ts || 'unknown');
}

function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 4000);
}

// ── Boot ───────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', init);
