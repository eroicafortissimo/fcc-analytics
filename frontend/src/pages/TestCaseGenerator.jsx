import { useState, useEffect, useCallback, useRef } from 'react'
import { testcasesApi } from '../api/testcasesApi'

// ── Colour maps ────────────────────────────────────────────────────────────────

const OUTCOME_COLOURS = {
  'Must Hit': 'bg-red-100 text-red-700',
  'Should Hit': 'bg-amber-100 text-amber-700',
  'Testing Purposes': 'bg-blue-100 text-blue-700',
  'Should Not Hit': 'bg-slate-100 text-slate-600',
}

const RESULT_COLOURS = {
  HIT: 'bg-emerald-100 text-emerald-700',
  MISS: 'bg-rose-100 text-rose-700',
}

const ENTITY_COLOURS = {
  individual: 'bg-violet-100 text-violet-700',
  entity: 'bg-sky-100 text-sky-700',
  vessel: 'bg-cyan-100 text-cyan-700',
  aircraft: 'bg-teal-100 text-teal-700',
  country: 'bg-orange-100 text-orange-700',
  unknown: 'bg-slate-100 text-slate-500',
}

const WATCHLIST_COLOURS = {
  OFAC_SDN: 'bg-red-500',
  OFAC_NON_SDN: 'bg-orange-400',
  EU: 'bg-blue-500',
  HMT: 'bg-violet-500',
  BIS: 'bg-amber-500',
  JAPAN: 'bg-rose-500',
}

// ── Small helpers ──────────────────────────────────────────────────────────────

function Badge({ text, colourClass }) {
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${colourClass}`}>
      {text}
    </span>
  )
}

function StatCard({ label, value, sub, colour = 'slate' }) {
  const colours = {
    slate: 'border-slate-200 text-slate-700',
    green: 'border-emerald-200 text-emerald-700',
    red: 'border-rose-200 text-rose-700',
    blue: 'border-blue-200 text-blue-700',
  }
  return (
    <div className={`bg-white rounded-lg border p-4 ${colours[colour]}`}>
      <p className="text-xs text-slate-500 mb-1">{label}</p>
      <p className="text-2xl font-bold">{value.toLocaleString()}</p>
      {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
    </div>
  )
}

// ── Theme group component ──────────────────────────────────────────────────────

function ThemeGroup({ theme, types, selected, onToggle, onToggleAll }) {
  const [open, setOpen] = useState(true)
  const allSelected = types.every(t => selected.has(t.type_id))
  const someSelected = types.some(t => selected.has(t.type_id))

  const outcomeOrder = ['Must Hit', 'Should Hit', 'Testing Purposes', 'Should Not Hit']

  return (
    <div className="border border-slate-200 rounded-lg overflow-hidden mb-2">
      {/* Theme header */}
      <div
        className="flex items-center gap-2 px-3 py-2 bg-slate-50 cursor-pointer hover:bg-slate-100 select-none"
        onClick={() => setOpen(o => !o)}
      >
        <span className="text-slate-400 text-xs w-4">{open ? '▼' : '▶'}</span>
        <input
          type="checkbox"
          checked={allSelected}
          ref={el => { if (el) el.indeterminate = !allSelected && someSelected }}
          onChange={e => { e.stopPropagation(); onToggleAll(types, e.target.checked) }}
          onClick={e => e.stopPropagation()}
          className="h-3.5 w-3.5 rounded border-slate-300 text-blue-600 cursor-pointer"
        />
        <span className="text-sm font-semibold text-slate-700 flex-1">{theme}</span>
        <span className="text-xs text-slate-400">
          {types.filter(t => selected.has(t.type_id)).length}/{types.length}
        </span>
      </div>

      {/* Type rows */}
      {open && (
        <div className="divide-y divide-slate-100">
          {types.map(t => (
            <label
              key={t.type_id}
              className="flex items-start gap-2 px-4 py-1.5 hover:bg-slate-50 cursor-pointer"
            >
              <input
                type="checkbox"
                checked={selected.has(t.type_id)}
                onChange={() => onToggle(t.type_id)}
                className="h-3.5 w-3.5 mt-0.5 rounded border-slate-300 text-blue-600 cursor-pointer flex-shrink-0"
              />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5 flex-wrap">
                  <span className="text-xs font-mono text-slate-400">{t.type_id}</span>
                  <span className="text-xs font-medium text-slate-700">{t.type_name}</span>
                  <span className={`inline-flex px-1.5 py-0.5 rounded text-[10px] font-medium ${OUTCOME_COLOURS[t.expected_outcome] || 'bg-slate-100 text-slate-500'}`}>
                    {t.expected_outcome}
                  </span>
                </div>
                <p className="text-[11px] text-slate-400 truncate mt-0.5">{t.description}</p>
              </div>
            </label>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Main page ──────────────────────────────────────────────────────────────────

export default function TestCaseGenerator() {
  // Types & selection
  const [types, setTypes] = useState([])
  const [selected, setSelected] = useState(new Set())

  // Generation settings
  const [countPerType, setCountPerType] = useState(250)
  const [cultureDist, setCultureDist] = useState('balanced')

  // State
  const [generating, setGenerating] = useState(false)
  const [genResult, setGenResult] = useState(null)
  const [genError, setGenError] = useState(null)

  // Stats
  const [stats, setStats] = useState(null)

  // Results table
  const [cases, setCases] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const PAGE_SIZE = 100
  const [tableFilter, setTableFilter] = useState({ expectedResult: '', entityType: '', search: '' })
  const [searchDraft, setSearchDraft] = useState('')
  const searchTimer = useRef(null)

  // ── Load types on mount ──────────────────────────────────────────────────────
  useEffect(() => {
    testcasesApi.types().then(r => {
      setTypes(r.data)
      // Default: all selected
      setSelected(new Set(r.data.map(t => t.type_id)))
    }).catch(() => {})
  }, [])

  // ── Load stats ───────────────────────────────────────────────────────────────
  const refreshStats = useCallback(() => {
    testcasesApi.stats().then(r => setStats(r.data)).catch(() => {})
  }, [])

  useEffect(() => { refreshStats() }, [refreshStats])

  // ── Load table ───────────────────────────────────────────────────────────────
  const loadTable = useCallback((p = 1, filter = tableFilter) => {
    testcasesApi.cases({
      page: p,
      pageSize: PAGE_SIZE,
      expectedResult: filter.expectedResult || undefined,
      entityType: filter.entityType || undefined,
      search: filter.search || undefined,
    }).then(r => {
      setCases(r.data.items)
      setTotal(r.data.total)
      setPage(p)
    }).catch(() => {})
  }, [tableFilter])

  useEffect(() => { loadTable(1) }, [tableFilter])

  // ── Toggle helpers ───────────────────────────────────────────────────────────
  const toggleType = (tid) => {
    setSelected(prev => {
      const next = new Set(prev)
      next.has(tid) ? next.delete(tid) : next.add(tid)
      return next
    })
  }

  const toggleTheme = (themeTypes, checked) => {
    setSelected(prev => {
      const next = new Set(prev)
      themeTypes.forEach(t => checked ? next.add(t.type_id) : next.delete(t.type_id))
      return next
    })
  }

  const selectAll = () => setSelected(new Set(types.map(t => t.type_id)))
  const clearAll = () => setSelected(new Set())

  // ── Group types by theme ─────────────────────────────────────────────────────
  const themeGroups = types.reduce((acc, t) => {
    if (!acc[t.theme]) acc[t.theme] = []
    acc[t.theme].push(t)
    return acc
  }, {})

  // ── Generate ─────────────────────────────────────────────────────────────────
  const generate = async () => {
    if (selected.size === 0) return
    setGenerating(true)
    setGenError(null)
    setGenResult(null)
    try {
      const r = await testcasesApi.generate({
        type_ids: [...selected],
        count_per_type: countPerType,
        culture_distribution: cultureDist,
        export_format: 'names_only',
      })
      setGenResult(r.data)
      refreshStats()
      loadTable(1)
    } catch (e) {
      setGenError(e?.response?.data?.detail || 'Generation failed')
    } finally {
      setGenerating(false)
    }
  }

  // ── Clear ─────────────────────────────────────────────────────────────────────
  const handleClear = async () => {
    if (!window.confirm('Delete all generated test cases?')) return
    await testcasesApi.clear()
    refreshStats()
    loadTable(1)
    setGenResult(null)
  }

  // ── Search debounce ───────────────────────────────────────────────────────────
  const handleSearchChange = (val) => {
    setSearchDraft(val)
    clearTimeout(searchTimer.current)
    searchTimer.current = setTimeout(() => {
      setTableFilter(f => ({ ...f, search: val }))
    }, 300)
  }

  const setFilter = (key, val) => setTableFilter(f => ({ ...f, [key]: val }))

  // ── Export URL ────────────────────────────────────────────────────────────────
  const exportUrl = testcasesApi.exportCsv({
    expectedResult: tableFilter.expectedResult || undefined,
    entityType: tableFilter.entityType || undefined,
  })

  // ── Render ────────────────────────────────────────────────────────────────────
  return (
    <div className="max-w-[1600px] mx-auto">
      {/* Page header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Test Case Generator</h1>
          <p className="text-sm text-slate-500 mt-0.5">
            Generate name variation test cases from the sanctions watchlists
          </p>
        </div>
        <div className="flex gap-2">
          {stats?.total > 0 && (
            <>
              <a
                href={exportUrl}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 bg-white text-sm text-slate-600 hover:bg-slate-50"
              >
                ↓ Export CSV
              </a>
              <button
                onClick={handleClear}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-rose-200 bg-white text-sm text-rose-600 hover:bg-rose-50"
              >
                ✕ Clear All
              </button>
            </>
          )}
        </div>
      </div>

      {/* Stats row */}
      {stats && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-5">
          <StatCard label="Total Generated" value={stats.total} colour="slate" />
          <StatCard
            label="Expected HIT"
            value={stats.by_result?.HIT ?? 0}
            sub={stats.total ? `${Math.round(((stats.by_result?.HIT ?? 0) / stats.total) * 100)}%` : undefined}
            colour="green"
          />
          <StatCard
            label="Expected MISS"
            value={stats.by_result?.MISS ?? 0}
            sub={stats.total ? `${Math.round(((stats.by_result?.MISS ?? 0) / stats.total) * 100)}%` : undefined}
            colour="red"
          />
          <StatCard
            label="Entity Types"
            value={Object.keys(stats.by_entity_type || {}).length}
            sub={
              Object.entries(stats.by_entity_type || {})
                .sort((a, b) => b[1] - a[1])
                .slice(0, 2)
                .map(([k, v]) => `${k}: ${v.toLocaleString()}`)
                .join(' · ')
            }
            colour="blue"
          />
        </div>
      )}

      {/* Generation result banner */}
      {genResult && (
        <div className="mb-4 p-3 rounded-lg border border-emerald-200 bg-emerald-50 text-sm text-emerald-800 flex items-center justify-between">
          <span>
            Generated <strong>{genResult.generated?.toLocaleString()}</strong> test cases
            across <strong>{Object.keys(genResult.by_type || {}).length}</strong> type(s).
            {genResult.skipped > 0 && ` (${genResult.skipped} skipped — names didn't qualify)`}
          </span>
          <button onClick={() => setGenResult(null)} className="text-emerald-500 hover:text-emerald-700 text-lg leading-none">×</button>
        </div>
      )}
      {genError && (
        <div className="mb-4 p-3 rounded-lg border border-rose-200 bg-rose-50 text-sm text-rose-800 flex items-center justify-between">
          <span>{genError}</span>
          <button onClick={() => setGenError(null)} className="text-rose-400 hover:text-rose-600 text-lg leading-none">×</button>
        </div>
      )}

      {/* Main split layout */}
      <div className="flex gap-4">

        {/* ── Left panel: type selection ───────────────────────────────────────── */}
        <div className="w-80 flex-shrink-0 flex flex-col gap-3">

          {/* Controls */}
          <div className="bg-white border border-slate-200 rounded-lg p-4 space-y-4">
            <div className="flex items-center justify-between">
              <span className="text-xs font-semibold text-slate-500 uppercase tracking-wide">
                Variation Types
              </span>
              <div className="flex gap-2">
                <button onClick={selectAll} className="text-xs text-blue-600 hover:underline">All</button>
                <span className="text-slate-300">|</span>
                <button onClick={clearAll} className="text-xs text-slate-500 hover:underline">None</button>
              </div>
            </div>

            <div className="text-sm text-slate-600">
              <span className="font-semibold text-slate-900">{selected.size}</span> / {types.length} types selected
            </div>

            {/* Count per type */}
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">
                Count per type
              </label>
              <div className="flex items-center gap-2">
                <input
                  type="range"
                  min={10} max={1000} step={10}
                  value={countPerType}
                  onChange={e => setCountPerType(Number(e.target.value))}
                  className="flex-1 h-1.5 accent-blue-600"
                />
                <input
                  type="number"
                  min={1} max={5000}
                  value={countPerType}
                  onChange={e => setCountPerType(Math.max(1, Number(e.target.value)))}
                  className="w-16 border border-slate-200 rounded px-2 py-1 text-sm text-right"
                />
              </div>
              <p className="text-xs text-slate-400 mt-1">
                Est. max: {(selected.size * countPerType).toLocaleString()} test cases
              </p>
            </div>

            {/* Culture distribution */}
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1.5">
                Culture distribution
              </label>
              <div className="flex gap-2">
                {['balanced', 'weighted'].map(opt => (
                  <button
                    key={opt}
                    onClick={() => setCultureDist(opt)}
                    className={`flex-1 py-1.5 rounded text-xs font-medium border transition-colors
                      ${cultureDist === opt
                        ? 'bg-blue-600 text-white border-blue-600'
                        : 'bg-white text-slate-600 border-slate-200 hover:bg-slate-50'
                      }`}
                  >
                    {opt.charAt(0).toUpperCase() + opt.slice(1)}
                  </button>
                ))}
              </div>
              <p className="text-[11px] text-slate-400 mt-1.5 leading-tight">
                {cultureDist === 'balanced'
                  ? 'Random sample equally from all entity types'
                  : 'Sample proportional to watchlist composition'}
              </p>
            </div>

            {/* Generate button */}
            <button
              onClick={generate}
              disabled={generating || selected.size === 0}
              className="w-full py-2 rounded-lg bg-blue-600 text-white text-sm font-semibold
                         hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed
                         flex items-center justify-center gap-2 transition-colors"
            >
              {generating ? (
                <>
                  <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
                  </svg>
                  Generating…
                </>
              ) : (
                `Generate ${selected.size > 0 ? `${selected.size} Types` : ''}`
              )}
            </button>
          </div>

          {/* Theme groups (scrollable) */}
          <div className="bg-white border border-slate-200 rounded-lg p-3 overflow-y-auto" style={{ maxHeight: '60vh' }}>
            <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-2 px-1">
              Themes & Types
            </p>
            {Object.entries(themeGroups).map(([theme, themeTypes]) => (
              <ThemeGroup
                key={theme}
                theme={theme}
                types={themeTypes}
                selected={selected}
                onToggle={toggleType}
                onToggleAll={toggleTheme}
              />
            ))}
          </div>
        </div>

        {/* ── Right panel: results table ───────────────────────────────────────── */}
        <div className="flex-1 min-w-0 flex flex-col gap-3">

          {/* Filter bar */}
          <div className="bg-white border border-slate-200 rounded-lg p-3 flex flex-wrap gap-2 items-center">
            <input
              type="text"
              placeholder="Search names or type…"
              value={searchDraft}
              onChange={e => handleSearchChange(e.target.value)}
              className="border border-slate-200 rounded px-3 py-1.5 text-sm flex-1 min-w-40"
            />
            <select
              value={tableFilter.expectedResult}
              onChange={e => setFilter('expectedResult', e.target.value)}
              className="border border-slate-200 rounded px-2 py-1.5 text-sm"
            >
              <option value="">All results</option>
              <option value="HIT">HIT</option>
              <option value="MISS">MISS</option>
            </select>
            <select
              value={tableFilter.entityType}
              onChange={e => setFilter('entityType', e.target.value)}
              className="border border-slate-200 rounded px-2 py-1.5 text-sm"
            >
              <option value="">All entity types</option>
              {['individual', 'entity', 'vessel', 'aircraft', 'country', 'unknown'].map(et => (
                <option key={et} value={et}>{et}</option>
              ))}
            </select>
            <span className="text-xs text-slate-400 ml-auto">
              {total.toLocaleString()} {total === 1 ? 'case' : 'cases'}
            </span>
          </div>

          {/* Table */}
          {cases.length === 0 ? (
            <div className="flex-1 flex flex-col items-center justify-center py-24 text-slate-400">
              <div className="text-5xl mb-4">🧪</div>
              <p className="text-lg font-medium text-slate-500">No test cases yet</p>
              <p className="text-sm mt-1">Select variation types and click Generate</p>
            </div>
          ) : (
            <>
              <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-slate-50 border-b border-slate-200">
                        <th className="px-3 py-2 text-left text-xs font-semibold text-slate-500 whitespace-nowrap">ID</th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-slate-500 whitespace-nowrap">Type</th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-slate-500">Original Name</th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-slate-500">Test Name</th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-slate-500 whitespace-nowrap">Entity</th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-slate-500 whitespace-nowrap">List</th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-slate-500 whitespace-nowrap">Expected</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {cases.map(c => (
                        <tr key={c.test_case_id} className="hover:bg-slate-50 group">
                          <td className="px-3 py-2 font-mono text-[11px] text-slate-400 whitespace-nowrap">
                            {c.test_case_id}
                          </td>
                          <td className="px-3 py-2 text-xs text-slate-600 whitespace-nowrap max-w-[160px] truncate">
                            <span title={c.test_case_type}>{c.test_case_type}</span>
                          </td>
                          <td className="px-3 py-2 text-xs text-slate-700 max-w-[200px]">
                            <span className="line-clamp-1" title={c.cleaned_original_name}>
                              {c.cleaned_original_name}
                            </span>
                          </td>
                          <td className="px-3 py-2 text-xs font-medium text-slate-900 max-w-[200px]">
                            <span className="line-clamp-1" title={c.test_name}>
                              {c.test_name}
                            </span>
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap">
                            <Badge
                              text={c.entity_type}
                              colourClass={ENTITY_COLOURS[c.entity_type] || 'bg-slate-100 text-slate-500'}
                            />
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap">
                            <span className="flex items-center gap-1.5">
                              <span className={`w-2 h-2 rounded-full flex-shrink-0 ${WATCHLIST_COLOURS[c.watchlist] || 'bg-slate-400'}`} />
                              <span className="text-xs text-slate-600">{c.watchlist}</span>
                            </span>
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap">
                            <Badge
                              text={c.expected_result}
                              colourClass={RESULT_COLOURS[c.expected_result] || 'bg-slate-100 text-slate-500'}
                            />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Pagination */}
              {total > PAGE_SIZE && (
                <div className="flex items-center justify-between px-1">
                  <span className="text-xs text-slate-500">
                    Page {page} of {Math.ceil(total / PAGE_SIZE)}
                  </span>
                  <div className="flex gap-2">
                    <button
                      onClick={() => loadTable(page - 1)}
                      disabled={page <= 1}
                      className="px-3 py-1.5 rounded border border-slate-200 text-xs disabled:opacity-40 hover:bg-slate-50"
                    >
                      ← Prev
                    </button>
                    <button
                      onClick={() => loadTable(page + 1)}
                      disabled={page >= Math.ceil(total / PAGE_SIZE)}
                      className="px-3 py-1.5 rounded border border-slate-200 text-xs disabled:opacity-40 hover:bg-slate-50"
                    >
                      Next →
                    </button>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )
}
