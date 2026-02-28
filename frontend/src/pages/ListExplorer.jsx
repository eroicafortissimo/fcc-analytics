import { useState, useEffect, useCallback, useRef } from 'react'
import {
  BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { listsApi } from '../api/listsApi'

// ── Constants ─────────────────────────────────────────────────────────────────

const WATCHLIST_COLORS = {
  OFAC_SDN:     '#ef4444',
  OFAC_NON_SDN: '#f97316',
  EU:           '#3b82f6',
  HMT:          '#8b5cf6',
  BIS:          '#10b981',
  JAPAN:        '#f59e0b',
}
const ENTITY_COLORS  = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#6b7280','#ec4899']
const CONF_COLORS    = { HIGH: '#10b981', MEDIUM: '#f59e0b', LOW: '#ef4444' }

const EMPTY_FILTERS = {
  watchlists: [],
  entity_types: [],
  nationalities: [],
  search: '',
  recently_modified_only: false,
}

// ── Tiny helpers ──────────────────────────────────────────────────────────────

function Badge({ label, color, onRemove }) {
  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium"
      style={{ background: color + '22', color }}>
      {label}
      {onRemove && (
        <button onClick={onRemove} className="ml-0.5 hover:opacity-70">×</button>
      )}
    </span>
  )
}

function StatCard({ label, value, sub, accent = '#3b82f6' }) {
  return (
    <div className="bg-white rounded-xl shadow-sm p-4 border-l-4" style={{ borderColor: accent }}>
      <p className="text-xs text-slate-500 font-medium uppercase tracking-wide">{label}</p>
      <p className="text-2xl font-bold text-slate-800 mt-1">{value?.toLocaleString() ?? '—'}</p>
      {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
    </div>
  )
}

// ── Multi-select dropdown ─────────────────────────────────────────────────────

function MultiSelect({ label, options, value, onChange, colorMap }) {
  const [open, setOpen] = useState(false)
  const ref = useRef()

  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const toggle = (opt) =>
    onChange(value.includes(opt) ? value.filter(v => v !== opt) : [...value, opt])

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between gap-2 px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white hover:border-slate-400 transition-colors"
      >
        <span className="truncate text-slate-600">
          {value.length === 0 ? label : `${label}: ${value.length} selected`}
        </span>
        <span className="text-slate-400 text-xs">{open ? '▲' : '▼'}</span>
      </button>
      {open && (
        <div className="absolute z-20 mt-1 w-56 bg-white border border-slate-200 rounded-xl shadow-lg max-h-60 overflow-y-auto">
          {options.length === 0 && (
            <div className="px-3 py-2 text-xs text-slate-400">No options</div>
          )}
          {options.map(opt => (
            <label key={opt}
              className="flex items-center gap-2 px-3 py-1.5 hover:bg-slate-50 cursor-pointer text-sm">
              <input type="checkbox" checked={value.includes(opt)} onChange={() => toggle(opt)}
                className="rounded accent-blue-600" />
              {colorMap && (
                <span className="w-2 h-2 rounded-full inline-block flex-shrink-0"
                  style={{ background: colorMap[opt] || '#6b7280' }} />
              )}
              <span className="truncate">{opt}</span>
            </label>
          ))}
          {value.length > 0 && (
            <button onClick={() => onChange([])}
              className="w-full text-left px-3 py-1.5 text-xs text-red-500 hover:bg-red-50 border-t border-slate-100">
              Clear all
            </button>
          )}
        </div>
      )}
    </div>
  )
}

// ── Charts ────────────────────────────────────────────────────────────────────

function WatchlistChart({ data, onFilterClick }) {
  return (
    <div className="bg-white rounded-xl shadow-sm p-4">
      <h3 className="text-sm font-semibold text-slate-700 mb-1">By Watchlist</h3>
      <p className="text-xs text-slate-400 mb-3">Click a bar to filter</p>
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={data} margin={{ bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="name" tick={{ fontSize: 10 }} angle={-20} textAnchor="end" />
          <YAxis tick={{ fontSize: 10 }} />
          <Tooltip formatter={(v) => v.toLocaleString()} />
          <Bar dataKey="count" radius={[4,4,0,0]} cursor="pointer"
            onClick={(d) => onFilterClick('watchlists', d.name)}>
            {data.map((entry) => (
              <Cell key={entry.name} fill={WATCHLIST_COLORS[entry.name] || '#6b7280'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function EntityTypeChart({ data, onFilterClick }) {
  const RADIAN = Math.PI / 180
  const renderLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, name, percent }) => {
    if (percent < 0.04) return null
    const r = innerRadius + (outerRadius - innerRadius) * 0.5
    const x = cx + r * Math.cos(-midAngle * RADIAN)
    const y = cy + r * Math.sin(-midAngle * RADIAN)
    return <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central"
      fontSize={10}>{`${(percent * 100).toFixed(0)}%`}</text>
  }

  return (
    <div className="bg-white rounded-xl shadow-sm p-4">
      <h3 className="text-sm font-semibold text-slate-700 mb-1">By Entity Type</h3>
      <p className="text-xs text-slate-400 mb-3">Click a slice to filter</p>
      <ResponsiveContainer width="100%" height={180}>
        <PieChart>
          <Pie data={data} cx="50%" cy="50%" outerRadius={75} dataKey="value"
            labelLine={false} label={renderLabel}
            onClick={(d) => onFilterClick('entity_types', d.name)}>
            {data.map((_, i) => (
              <Cell key={i} fill={ENTITY_COLORS[i % ENTITY_COLORS.length]} cursor="pointer" />
            ))}
          </Pie>
          <Tooltip formatter={(v) => v.toLocaleString()} />
          <Legend iconSize={8} wrapperStyle={{ fontSize: 10 }} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  )
}

function NationalityChart({ data, onFilterClick }) {
  return (
    <div className="bg-white rounded-xl shadow-sm p-4 col-span-2">
      <h3 className="text-sm font-semibold text-slate-700 mb-1">Top 20 Nationalities / Regions</h3>
      <p className="text-xs text-slate-400 mb-3">Click a bar to filter</p>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data} layout="vertical" margin={{ left: 160, right: 30 }}>
          <CartesianGrid strokeDasharray="3 3" horizontal={false} />
          <XAxis type="number" tick={{ fontSize: 10 }} />
          <YAxis type="category" dataKey="name" tick={{ fontSize: 10 }} width={155} />
          <Tooltip formatter={(v) => v.toLocaleString()} />
          <Bar dataKey="count" fill="#6366f1" radius={[0,4,4,0]} cursor="pointer"
            onClick={(d) => onFilterClick('nationalities', d.name)} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function NameLengthChart({ data }) {
  return (
    <div className="bg-white rounded-xl shadow-sm p-4">
      <h3 className="text-sm font-semibold text-slate-700 mb-1">Name Length Distribution</h3>
      <p className="text-xs text-slate-400 mb-3">Character count buckets</p>
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="bucket" tick={{ fontSize: 10 }} />
          <YAxis tick={{ fontSize: 10 }} />
          <Tooltip formatter={(v) => v.toLocaleString()} />
          <Bar dataKey="count" fill="#0ea5e9" radius={[4,4,0,0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function TokenCountChart({ data }) {
  return (
    <div className="bg-white rounded-xl shadow-sm p-4">
      <h3 className="text-sm font-semibold text-slate-700 mb-1">Token Count Distribution</h3>
      <p className="text-xs text-slate-400 mb-3">Space-separated word count</p>
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="tokens" tick={{ fontSize: 10 }} />
          <YAxis tick={{ fontSize: 10 }} />
          <Tooltip formatter={(v) => v.toLocaleString()} />
          <Bar dataKey="count" fill="#8b5cf6" radius={[4,4,0,0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function ListExplorer() {
  const [filters, setFilters] = useState(EMPTY_FILTERS)
  const [searchDraft, setSearchDraft] = useState('')

  const [chartData, setChartData]   = useState(null)
  const [entries, setEntries]       = useState([])
  const [total, setTotal]           = useState(0)
  const [page, setPage]             = useState(1)
  const [filterOpts, setFilterOpts] = useState({ watchlists: [], entity_types: [], nationalities: [] })

  const [downloadStatus, setDownloadStatus] = useState([])
  const [inferResult, setInferResult]       = useState(null)
  const [inferStatus, setInferStatus]       = useState(null)

  const [loadingEntries, setLoadingEntries] = useState(false)
  const [loadingCharts, setLoadingCharts]   = useState(false)
  const [downloading, setDownloading]       = useState(false)
  const [inferring, setInferring]           = useState(false)

  // ── Data fetchers ──────────────────────────────────────────────────────────

  const fetchCharts = useCallback(async (f) => {
    setLoadingCharts(true)
    try {
      const { data } = await listsApi.chartData(f)
      setChartData(data)
    } catch (_) {}
    setLoadingCharts(false)
  }, [])

  const fetchEntries = useCallback(async (f, pg = 1) => {
    setLoadingEntries(true)
    try {
      const { data } = await listsApi.entries(f, pg)
      setEntries(data.items)
      setTotal(data.total)
      setPage(pg)
    } catch (_) {}
    setLoadingEntries(false)
  }, [])

  const fetchFilterOpts = useCallback(async () => {
    try {
      const { data } = await listsApi.filterOptions()
      setFilterOpts(data)
    } catch (_) {}
  }, [])

  const fetchInferStatus = useCallback(async () => {
    try {
      const { data } = await listsApi.inferStatus()
      setInferStatus(data)
    } catch (_) {}
  }, [])

  useEffect(() => {
    fetchCharts(EMPTY_FILTERS)
    fetchEntries(EMPTY_FILTERS)
    fetchFilterOpts()
    fetchInferStatus()
  }, [])

  // ── Filter changes ─────────────────────────────────────────────────────────

  const applyFilters = (newFilters) => {
    setFilters(newFilters)
    fetchCharts(newFilters)
    fetchEntries(newFilters, 1)
  }

  const updateFilter = (key, val) => {
    const updated = { ...filters, [key]: val }
    applyFilters(updated)
  }

  const handleChartClick = (filterKey, value) => {
    const current = filters[filterKey] || []
    const updated = current.includes(value)
      ? current.filter(v => v !== value)
      : [...current, value]
    const newFilters = { ...filters, [filterKey]: updated }
    applyFilters(newFilters)
  }

  const submitSearch = () => {
    const newFilters = { ...filters, search: searchDraft }
    applyFilters(newFilters)
  }

  const clearAllFilters = () => {
    setSearchDraft('')
    applyFilters(EMPTY_FILTERS)
  }

  const activeFilterCount =
    filters.watchlists.length + filters.entity_types.length +
    filters.nationalities.length + (filters.search ? 1 : 0) +
    (filters.recently_modified_only ? 1 : 0)

  // ── Actions ────────────────────────────────────────────────────────────────

  const handleDownload = async () => {
    setDownloading(true)
    setDownloadStatus([])
    try {
      const { data } = await listsApi.download()
      setDownloadStatus(data)
      await Promise.all([
        fetchCharts(filters),
        fetchEntries(filters, 1),
        fetchFilterOpts(),
        fetchInferStatus(),
      ])
    } catch (err) {
      setDownloadStatus([{ watchlist: 'all', status: 'failed', error: String(err) }])
    }
    setDownloading(false)
  }

  const handleInferNationalities = async () => {
    setInferring(true)
    setInferResult(null)
    try {
      const { data } = await listsApi.inferNationalities([], 1000, true)
      setInferResult(data)
      await Promise.all([fetchCharts(filters), fetchInferStatus(), fetchFilterOpts()])
    } catch (err) {
      setInferResult({ error: String(err) })
    }
    setInferring(false)
  }

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <div className="max-w-screen-2xl mx-auto space-y-5">

      {/* ── Header ── */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">List Explorer</h1>
          <p className="text-sm text-slate-500 mt-0.5">
            {chartData
              ? `${chartData.total.toLocaleString()} entries across ${chartData.by_watchlist.length} lists`
              : 'No data loaded — download lists to begin'}
          </p>
        </div>
        <div className="flex gap-2">
          <button onClick={handleDownload} disabled={downloading}
            className="px-4 py-2 text-sm font-medium rounded-lg bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white transition-colors">
            {downloading ? 'Downloading…' : 'Download / Refresh Lists'}
          </button>
          <button onClick={handleInferNationalities} disabled={inferring || !chartData?.total}
            className="px-4 py-2 text-sm font-medium rounded-lg bg-indigo-600 hover:bg-indigo-700 disabled:bg-indigo-400 text-white transition-colors">
            {inferring ? 'Inferring…' : 'Infer Nationalities'}
          </button>
        </div>
      </div>

      {/* ── Status cards ── */}
      {inferStatus && (
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
          <StatCard label="Total entries"    value={inferStatus.total}    accent="#3b82f6" />
          <StatCard label="Nationality inferred" value={inferStatus.inferred} accent="#10b981"
            sub={inferStatus.total ? `${((inferStatus.inferred/inferStatus.total)*100).toFixed(1)}%` : null} />
          <StatCard label="Pending inference" value={inferStatus.pending}  accent="#f59e0b" />
          <StatCard label="Via data lookup"  value={inferStatus.via_data}  accent="#10b981" />
          <StatCard label="Via heuristic"    value={inferStatus.via_heuristic} accent="#6366f1" />
          <StatCard label="Via LLM"          value={inferStatus.via_llm}   accent="#ec4899" />
        </div>
      )}

      {/* ── Download status row ── */}
      {downloadStatus.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {downloadStatus.map(s => (
            <div key={s.watchlist}
              className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs border ${
                s.status === 'success' ? 'bg-green-50 border-green-200 text-green-700' :
                s.status === 'cached'  ? 'bg-blue-50  border-blue-200  text-blue-700' :
                                         'bg-red-50   border-red-200   text-red-700'
              }`}>
              <span className="font-medium">{s.watchlist}</span>
              <span className="capitalize">{s.status}</span>
              {s.count > 0 && <span>({s.count.toLocaleString()})</span>}
              {s.error && <span title={s.error} className="truncate max-w-xs">⚠ {s.error}</span>}
            </div>
          ))}
        </div>
      )}

      {/* ── Infer result ── */}
      {inferResult && (
        <div className={`flex items-center gap-3 px-4 py-2 rounded-lg text-sm border ${
          inferResult.error ? 'bg-red-50 border-red-200 text-red-700' : 'bg-indigo-50 border-indigo-200 text-indigo-700'
        }`}>
          {inferResult.error
            ? `Inference error: ${inferResult.error}`
            : `Processed ${inferResult.processed} entries — `
              + Object.entries(inferResult.by_method || {}).map(([k,v]) => `${k}: ${v}`).join(', ')
              + (inferResult.processed < (inferStatus?.pending || 0) ? ' · Click again for next batch' : '')
          }
        </div>
      )}

      {/* ── Filter bar ── */}
      <div className="bg-white rounded-xl shadow-sm p-4 space-y-3">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
          {/* Search */}
          <div className="flex gap-2 lg:col-span-2">
            <input
              type="text"
              placeholder="Search names…"
              value={searchDraft}
              onChange={e => setSearchDraft(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && submitSearch()}
              className="flex-1 px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <button onClick={submitSearch}
              className="px-3 py-2 text-sm bg-slate-800 text-white rounded-lg hover:bg-slate-900">
              Search
            </button>
          </div>
          <MultiSelect label="Watchlist"    options={filterOpts.watchlists || []}
            value={filters.watchlists}    onChange={v => updateFilter('watchlists', v)}
            colorMap={WATCHLIST_COLORS} />
          <MultiSelect label="Entity Type"  options={filterOpts.entity_types || []}
            value={filters.entity_types}  onChange={v => updateFilter('entity_types', v)} />
          <MultiSelect label="Nationality"  options={filterOpts.nationalities || []}
            value={filters.nationalities} onChange={v => updateFilter('nationalities', v)} />
        </div>

        <div className="flex items-center justify-between">
          <label className="flex items-center gap-2 text-sm text-slate-600 cursor-pointer">
            <input type="checkbox" className="rounded accent-blue-600"
              checked={filters.recently_modified_only}
              onChange={e => updateFilter('recently_modified_only', e.target.checked)} />
            Recently modified only (last 90 days)
            {chartData && (
              <span className="text-slate-400">({chartData.recently_modified_count.toLocaleString()})</span>
            )}
          </label>

          {activeFilterCount > 0 && (
            <button onClick={clearAllFilters}
              className="text-xs text-red-500 hover:text-red-700 font-medium">
              Clear {activeFilterCount} filter{activeFilterCount !== 1 ? 's' : ''}
            </button>
          )}
        </div>

        {/* Active filter badges */}
        {activeFilterCount > 0 && (
          <div className="flex flex-wrap gap-1.5 pt-1">
            {filters.watchlists.map(w => (
              <Badge key={w} label={w} color={WATCHLIST_COLORS[w] || '#6b7280'}
                onRemove={() => updateFilter('watchlists', filters.watchlists.filter(v => v !== w))} />
            ))}
            {filters.entity_types.map(e => (
              <Badge key={e} label={e} color="#10b981"
                onRemove={() => updateFilter('entity_types', filters.entity_types.filter(v => v !== e))} />
            ))}
            {filters.nationalities.map(n => (
              <Badge key={n} label={n} color="#6366f1"
                onRemove={() => updateFilter('nationalities', filters.nationalities.filter(v => v !== n))} />
            ))}
            {filters.search && (
              <Badge label={`"${filters.search}"`} color="#0ea5e9"
                onRemove={() => { setSearchDraft(''); updateFilter('search', '') }} />
            )}
          </div>
        )}
      </div>

      {/* ── Charts ── */}
      {chartData && chartData.total > 0 && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <WatchlistChart data={chartData.by_watchlist}  onFilterClick={handleChartClick} />
          <EntityTypeChart data={chartData.by_entity_type} onFilterClick={handleChartClick} />
          <NameLengthChart data={chartData.name_length_hist} />
          <TokenCountChart data={chartData.token_count_hist} />
          {chartData.by_nationality.length > 0 && (
            <NationalityChart data={chartData.by_nationality} onFilterClick={handleChartClick} />
          )}
        </div>
      )}

      {/* ── Entries table ── */}
      <div className="bg-white rounded-xl shadow-sm overflow-hidden">
        <div className="px-4 py-3 border-b border-slate-100 flex items-center justify-between">
          <span className="text-sm font-medium text-slate-700">
            {loadingEntries ? 'Loading…' : `${total.toLocaleString()} entries`}
            {activeFilterCount > 0 && <span className="text-slate-400"> (filtered)</span>}
          </span>
          <div className="flex items-center gap-2 text-xs text-slate-500">
            <button onClick={() => fetchEntries(filters, Math.max(1, page - 1))}
              disabled={page === 1 || loadingEntries}
              className="px-2.5 py-1 rounded border border-slate-200 disabled:opacity-40 hover:bg-slate-50">
              ← Prev
            </button>
            <span>Page {page} of {Math.max(1, Math.ceil(total / 100))}</span>
            <button onClick={() => fetchEntries(filters, page + 1)}
              disabled={page * 100 >= total || loadingEntries}
              className="px-2.5 py-1 rounded border border-slate-200 disabled:opacity-40 hover:bg-slate-50">
              Next →
            </button>
          </div>
        </div>

        {loadingEntries ? (
          <div className="p-10 text-center text-slate-400 text-sm">Loading entries…</div>
        ) : entries.length === 0 ? (
          <div className="p-10 text-center text-slate-400 text-sm">
            {total === 0 && activeFilterCount === 0
              ? 'No data loaded. Click "Download / Refresh Lists" to fetch watchlists.'
              : 'No entries match the current filters.'}
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="bg-slate-50 border-b border-slate-200 sticky top-0">
                <tr>
                  {[
                    'Watchlist', 'Program', 'Sub-list 2',
                    'Cleaned Name', 'Original Name',
                    'P/AKA', 'Type', 'Tokens', 'Length',
                    'Nationality', 'Confidence', 'Date Listed', 'Recent',
                  ].map(h => (
                    <th key={h} className="px-2.5 py-2 text-left font-semibold text-slate-600 whitespace-nowrap">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {entries.map((e, i) => (
                  <tr key={e.uid || i} className="hover:bg-slate-50 transition-colors">
                    <td className="px-2.5 py-1.5">
                      <span className="px-1.5 py-0.5 rounded text-white text-xs font-medium"
                        style={{ background: WATCHLIST_COLORS[e.watchlist] || '#6b7280' }}>
                        {e.watchlist}
                      </span>
                    </td>
                    <td className="px-2.5 py-1.5 text-slate-500 max-w-[120px] truncate"
                      title={e.sub_watchlist_1}>{e.sub_watchlist_1 || '—'}</td>
                    <td className="px-2.5 py-1.5 text-slate-400 max-w-[100px] truncate"
                      title={e.sub_watchlist_2}>{e.sub_watchlist_2 || '—'}</td>
                    <td className="px-2.5 py-1.5 font-medium text-slate-800 max-w-[200px] truncate"
                      title={e.cleaned_name}>{e.cleaned_name}</td>
                    <td className="px-2.5 py-1.5 text-slate-500 max-w-[180px] truncate"
                      title={e.original_name}>{e.original_name}</td>
                    <td className="px-2.5 py-1.5">
                      <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                        e.primary_aka === 'primary'
                          ? 'bg-blue-100 text-blue-700'
                          : 'bg-slate-100 text-slate-500'
                      }`}>{e.primary_aka}</span>
                    </td>
                    <td className="px-2.5 py-1.5 text-slate-600 capitalize">{e.entity_type}</td>
                    <td className="px-2.5 py-1.5 text-center text-slate-500">{e.num_tokens}</td>
                    <td className="px-2.5 py-1.5 text-center text-slate-500">{e.name_length}</td>
                    <td className="px-2.5 py-1.5 text-slate-600 max-w-[140px] truncate"
                      title={e.nationality}>{e.nationality || '—'}</td>
                    <td className="px-2.5 py-1.5">
                      {e.nationality_confidence && (
                        <span className="px-1.5 py-0.5 rounded text-xs font-medium"
                          style={{
                            background: (CONF_COLORS[e.nationality_confidence] || '#6b7280') + '22',
                            color: CONF_COLORS[e.nationality_confidence] || '#6b7280',
                          }}>
                          {e.nationality_confidence}
                        </span>
                      )}
                    </td>
                    <td className="px-2.5 py-1.5 text-slate-400 whitespace-nowrap">
                      {e.date_listed || '—'}
                    </td>
                    <td className="px-2.5 py-1.5 text-center">
                      {e.recently_modified && (
                        <span className="text-amber-500" title="Modified in last 90 days">●</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
