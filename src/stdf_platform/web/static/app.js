/** Alpine.js app — state management + API calls + AG Grid */

function app() {
  return {
    // ── Filter state ────────────────────────────────────────────
    products: [],
    testCategories: [],
    selProducts: [],
    selCategories: [],
    lots: [],
    selLots: new Set(),
    wafers: [],
    selWafers: new Set(),
    hasWafers: false,
    loading: false,
    applied: false,

    // ── Tab ─────────────────────────────────────────────────────
    tabs: [
      { id: 'dashboard', label: 'Dashboard' },
      { id: 'wafer',     label: 'Wafer' },
      { id: 'tests',     label: 'Tests' },
      { id: 'export',    label: '⬇ Export' },
    ],
    activeTab: 'dashboard',

    // ── Dashboard ───────────────────────────────────────────────
    summaryRows: [],

    // ── Wafer ───────────────────────────────────────────────────
    selectedLotForWafer: '',
    selectedWaferForMap: '',
    wafermapColor: 'passed',

    // ── Tests ───────────────────────────────────────────────────
    tests: [],               // full list from API
    selTestNums: new Set(),
    testSearch: '',
    selectedTestForDist: null,
    failRows: [],
    _grid: null,             // AG Grid instance (outside Alpine reactive scope)

    // ── Export ──────────────────────────────────────────────────
    exportFormat: 'pivot',
    exportFilename: 'stdf_export.csv',
    exporting: false,
    exportStatus: null,
    exportPreview: { parts: 0, tests: 0, long_rows: 0, pivot_rows: 0 },

    // ── Computed ────────────────────────────────────────────────
    get totalParts() { return this.summaryRows.reduce((s, r) => s + (r.total_parts || 0), 0); },
    get totalGood()  { return this.summaryRows.reduce((s, r) => s + (r.good_parts  || 0), 0); },
    get avgYield()   {
      if (!this.summaryRows.length) return 0;
      const vals = this.summaryRows.map(r => r.yield_pct || 0).filter(v => v > 0);
      return vals.length ? vals.reduce((s, v) => s + v, 0) / vals.length : 0;
    },

    // ── Init ────────────────────────────────────────────────────
    async init() {
      await this.loadFilters();
    },

    async loadFilters() {
      this.loading = true;
      try {
        const [prods, cats] = await Promise.all([
          fetch('/api/products').then(r => r.json()),
          fetch('/api/test-categories').then(r => r.json()),
        ]);
        this.products = prods;
        this.testCategories = cats;
        await this.loadLots();
      } finally {
        this.loading = false;
      }
    },

    async loadLots() {
      const params = new URLSearchParams();
      this.selProducts.forEach(p => params.append('product', p));
      this.selCategories.forEach(c => params.append('category', c));
      this.lots = await fetch('/api/lots?' + params).then(r => r.json());
      this.selLots = new Set(this.lots);
    },

    // ── Filter interactions ─────────────────────────────────────
    onProductChange(e) {
      this.selProducts = Array.from(e.target.selectedOptions).map(o => o.value);
      this.loadLots();
    },

    toggleCategory(cat) {
      const idx = this.selCategories.indexOf(cat);
      if (idx >= 0) this.selCategories.splice(idx, 1);
      else this.selCategories.push(cat);
      this.loadLots();
    },

    toggleLot(lot) {
      if (this.selLots.has(lot)) this.selLots.delete(lot);
      else this.selLots.add(lot);
      this.selLots = new Set(this.selLots); // trigger reactivity
    },

    selectAllLots() { this.selLots = new Set(this.lots); },
    clearLots()     { this.selLots = new Set(); },

    toggleWafer(w) {
      if (this.selWafers.has(w)) this.selWafers.delete(w);
      else this.selWafers.add(w);
      this.selWafers = new Set(this.selWafers);
    },

    selectAllWafers() { this.selWafers = new Set(this.wafers); },
    clearWafers()     { this.selWafers = new Set(); },

    // ── Apply ───────────────────────────────────────────────────
    async applyFilters() {
      if (this.selLots.size === 0) return;
      this.loading = true;
      this.applied = true;
      try {
        await Promise.all([
          this.loadWaferList(),
          this.loadSummary(),
        ]);
        await this.loadTests();
        await this.loadFails();
        await this.refreshPreview();
        this.setTab(this.activeTab); // re-render current tab charts
      } finally {
        this.loading = false;
      }
    },

    async loadWaferList() {
      const params = new URLSearchParams();
      [...this.selLots].forEach(l => params.append('lot', l));
      const data = await fetch('/api/wafers?' + params).then(r => r.json());
      this.wafers = data.wafer_ids;
      this.hasWafers = data.has_wafers;
      this.selWafers = new Set(this.wafers);
      this.selectedLotForWafer = [...this.selLots][0] || '';
      this.selectedWaferForMap = this.wafers[0] || '';
    },

    async loadSummary() {
      const params = new URLSearchParams();
      [...this.selLots].forEach(l => params.append('lot', l));
      this.summaryRows = await fetch('/api/summary?' + params).then(r => r.json());
      this.$nextTick(() => drawYieldBar('chart-yield-lot', this.summaryRows));
    },

    async loadWaferYield() {
      if (!this.selectedLotForWafer) return;
      const data = await fetch(`/api/wafer-yield?lot=${encodeURIComponent(this.selectedLotForWafer)}`).then(r => r.json());
      drawWaferYield('chart-wafer-yield', data);
    },

    async loadWaferMap() {
      if (!this.selectedLotForWafer || !this.selectedWaferForMap) return;
      const params = new URLSearchParams({
        lot: this.selectedLotForWafer,
        wafer: this.selectedWaferForMap,
      });
      const data = await fetch('/api/wafermap?' + params).then(r => r.json());
      drawWaferMap('chart-wafermap', data, this.wafermapColor);
    },

    async loadTests() {
      const params = new URLSearchParams();
      [...this.selLots].forEach(l => params.append('lot', l));
      this.tests = await fetch('/api/tests?' + params).then(r => r.json());
      this.selTestNums = new Set(this.tests.map(t => t.test_num));
      this.$nextTick(() => this.initTestGrid());
    },

    async loadFails() {
      const params = new URLSearchParams();
      [...this.selLots].forEach(l => params.append('lot', l));
      this.failRows = await fetch('/api/fails?' + params).then(r => r.json());
    },

    async loadDistribution() {
      if (!this.selectedTestForDist) return;
      const params = new URLSearchParams({ test_num: this.selectedTestForDist });
      [...this.selLots].forEach(l => params.append('lot', l));
      const data = await fetch('/api/distribution?' + params).then(r => r.json());
      drawDistribution('chart-distribution', data);
    },

    async refreshPreview() {
      if (this.selLots.size === 0) return;
      const body = {
        lots: [...this.selLots],
        wafers: this.hasWafers ? [...this.selWafers] : null,
        test_nums: this.selTestNums.size > 0 ? [...this.selTestNums] : null,
        format: this.exportFormat,
        filename: this.exportFilename,
      };
      this.exportPreview = await fetch('/api/export/preview', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      }).then(r => r.json()).catch(() => ({ parts: 0, tests: 0, long_rows: 0, pivot_rows: 0 }));
    },

    // ── Tab switching ───────────────────────────────────────────
    setTab(id) {
      this.activeTab = id;
      this.$nextTick(() => {
        if (id === 'dashboard' && this.summaryRows.length)
          drawYieldBar('chart-yield-lot', this.summaryRows);
        if (id === 'wafer' && this.hasWafers) {
          this.loadWaferYield();
          this.loadWaferMap();
        }
        if (id === 'tests') {
          this.initTestGrid();
          if (this.failRows.length) drawPareto('chart-pareto', this.failRows);
          if (this.selectedTestForDist) this.loadDistribution();
        }
      });
    },

    // ── AG Grid ─────────────────────────────────────────────────
    initTestGrid() {
      const el = document.getElementById('test-grid');
      if (!el) return;
      if (this._grid) {
        this._grid.destroy();
        this._grid = null;
      }

      const self = this;
      const gridOptions = {
        columnDefs: [
          { headerCheckboxSelection: true, checkboxSelection: true, width: 44, pinned: 'left', resizable: false },
          { field: 'test_num',  headerName: 'Test#', width: 80, sort: 'asc' },
          { field: 'test_name', headerName: 'Name', flex: 2, minWidth: 160 },
          { field: 'rec_type',  headerName: 'Type', width: 65 },
          { field: 'units',     headerName: 'Units', width: 70 },
          { field: 'lo_limit',  headerName: 'Lo Limit', width: 95, type: 'numericColumn' },
          { field: 'hi_limit',  headerName: 'Hi Limit', width: 95, type: 'numericColumn' },
        ],
        rowData: this.tests,
        rowSelection: 'multiple',
        suppressRowClickSelection: false,
        defaultColDef: { sortable: true, resizable: true, filter: false },
        headerHeight: 34,
        rowHeight: 30,
        onSelectionChanged(e) {
          const rows = e.api.getSelectedRows();
          self.selTestNums = new Set(rows.map(r => r.test_num));
          if (self.selTestNums.size > 0) {
            self.selectedTestForDist = rows[0].test_num;
            self.loadDistribution();
          }
          self.refreshPreview();
        },
        onGridReady(params) {
          params.api.selectAll();
        },
      };

      this._grid = new agGrid.Grid(el, gridOptions);
    },

    filterTestGrid() {
      if (!this._grid) return;
      const term = this.testSearch.toLowerCase();
      this._grid.gridOptions.api.setQuickFilter(term);
    },

    selectAllTests() {
      if (this._grid) this._grid.gridOptions.api.selectAll();
    },

    clearTests() {
      if (this._grid) this._grid.gridOptions.api.deselectAll();
      this.selTestNums = new Set();
    },

    getTestName(testNum) {
      const t = this.tests.find(t => t.test_num === testNum);
      return t ? (t.test_name || `#${testNum}`) : `#${testNum}`;
    },

    // ── CSV Download ────────────────────────────────────────────
    async downloadCSV() {
      if (this.selLots.size === 0) return;
      this.exporting = true;
      this.exportStatus = null;

      const body = {
        lots: [...this.selLots],
        wafers: this.hasWafers && this.selWafers.size > 0 ? [...this.selWafers] : null,
        test_nums: this.selTestNums.size > 0 ? [...this.selTestNums] : null,
        format: this.exportFormat,
        filename: this.exportFilename,
      };

      try {
        const res = await fetch('/api/export/csv', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });

        if (!res.ok) {
          const msg = await res.text();
          this.exportStatus = { ok: false, msg: `Error: ${msg}` };
          return;
        }

        const rowCount = res.headers.get('X-Row-Count');
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = this.exportFilename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);

        this.exportStatus = {
          ok: true,
          msg: `✓ Downloaded ${rowCount ? Number(rowCount).toLocaleString() + ' rows' : ''} — ${this.exportFilename}`,
        };
      } catch (err) {
        this.exportStatus = { ok: false, msg: `Error: ${err.message}` };
      } finally {
        this.exporting = false;
      }
    },
  };
}
