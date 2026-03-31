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
  BIS:          '#10b981',
  EU:           '#3b82f6',
  HMT:          '#8b5cf6',
  JAPAN:        '#f59e0b',
}
const WATCHLIST_LABELS = {
  OFAC_SDN:     'OFAC SDN',
  OFAC_NON_SDN: 'OFAC NON-SDN',
  EU:           'EU',
  HMT:          'HMT',
  BIS:          'BIS',
  JAPAN:        'Japan',
}
const WATCHLIST_KEYS = Object.keys(WATCHLIST_COLORS)
const ENTITY_COLORS  = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#6b7280','#ec4899']
const CULTURE_CONF_COLORS = { High: '#10b981', Medium: '#f59e0b', Low: '#ef4444' }

const capFirst = s => s ? s.charAt(0).toUpperCase() + s.slice(1) : s

const EMPTY_FILTERS = {
  watchlists: [],
  entity_types: [],
  search: '',
  recently_modified_only: false,
  min_tokens: null,
  max_tokens: null,
}

// ── Profile URL builder ───────────────────────────────────────────────────────

function getProfileUrl(entry) {
  if (entry.watchlist === 'OFAC_SDN' || entry.watchlist === 'OFAC_NON_SDN') {
    // uid: OFAC_SDN_12345_primary  →  parts[2] = numeric id
    //      OFAC_NON_SDN_12345_...  →  parts[3] = numeric id
    const parts = entry.uid.split('_')
    const numericId = entry.watchlist === 'OFAC_SDN' ? parts[2] : parts[3]
    if (numericId && /^\d+$/.test(numericId))
      return `https://sanctionssearch.ofac.treas.gov/Details.aspx?id=${numericId}`
  }
  return null
}

// Strip watchlist prefix and _primary/_aka_N suffixes from UID for display
function displayUid(uid, watchlist) {
  if (!uid) return null
  const prefix = watchlist + '_'
  let id = uid.startsWith(prefix) ? uid.slice(prefix.length) : uid
  // Remove trailing _primary or _aka_<digits>
  id = id.replace(/_primary$/, '').replace(/_aka_\d+$/, '')
  return id
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

function MultiSelect({ label, options, value, onChange, colorMap, labelFn }) {
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
              <span className="truncate">{labelFn ? labelFn(opt) : opt}</span>
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
          <XAxis dataKey="name" tick={{ fontSize: 10 }} angle={-20} textAnchor="end"
            tickFormatter={k => WATCHLIST_LABELS[k] || k} />
          <YAxis tick={{ fontSize: 10 }} />
          <Tooltip formatter={(v) => v.toLocaleString()} labelFormatter={k => WATCHLIST_LABELS[k] || k} />
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
      <h3 className="text-sm font-semibold text-slate-700 mb-1">By Record Type</h3>
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

function TokenCountChart({ data, onFilterClick }) {
  return (
    <div className="bg-white rounded-xl shadow-sm p-4">
      <h3 className="text-sm font-semibold text-slate-700 mb-1">Token Count Distribution</h3>
      <p className="text-xs text-slate-400 mb-3">Click a bar to filter</p>
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="tokens" tick={{ fontSize: 10 }} />
          <YAxis tick={{ fontSize: 10 }} />
          <Tooltip formatter={(v) => v.toLocaleString()} />
          <Bar dataKey="count" fill="#8b5cf6" radius={[4,4,0,0]} cursor="pointer"
            onClick={(d) => onFilterClick(d.tokens)} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}


// ── Progress bar ──────────────────────────────────────────────────────────────

function ProgressBar({ value, color }) {
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-slate-200 rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-500 ease-out"
          style={{ width: `${value}%`, background: color }}
        />
      </div>
      <span className="text-xs tabular-nums text-slate-500 w-8 text-right">{value}%</span>
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
  const [filterOpts, setFilterOpts] = useState({ watchlists: [], entity_types: [] })

  const [downloadStatus, setDownloadStatus] = useState([])

  const [loadingEntries, setLoadingEntries] = useState(false)
  const [loadingCharts, setLoadingCharts]   = useState(false)
  const [downloading, setDownloading]       = useState(false)
  const [downloadProgress, setDownloadProgress] = useState(0)
  const [clearing, setClearing]             = useState(false)
  const [clearProgress, setClearProgress]   = useState(0)

  const [classifyingCulture, setClassifyingCulture] = useState(false)
  const [cultureProgress, setCultureProgress]       = useState(0)
  const [cultureStatus, setCultureStatus]           = useState(null)

  const [nlQuery, setNlQuery]             = useState('')
  const [nlLoading, setNlLoading]         = useState(false)
  const [nlExplanation, setNlExplanation] = useState('')
  const [nlError, setNlError]             = useState('')

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

  const fetchCultureStatus = useCallback(async () => {
    try {
      const { data } = await listsApi.inferCulturesStatus()
      setCultureStatus(data)
    } catch (_) {}
  }, [])

  useEffect(() => {
    fetchCharts(EMPTY_FILTERS)
    fetchEntries(EMPTY_FILTERS)
    fetchFilterOpts()
    fetchCultureStatus()
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
    (filters.search ? 1 : 0) + (filters.recently_modified_only ? 1 : 0) +
    (filters.min_tokens != null || filters.max_tokens != null ? 1 : 0)

  // ── Actions ────────────────────────────────────────────────────────────────

  const handleDownload = async () => {
    setDownloading(true)
    setDownloadProgress(0)
    setDownloadStatus([])
    const collected = []
    try {
      for (let i = 0; i < WATCHLIST_KEYS.length; i++) {
        const { data } = await listsApi.download([WATCHLIST_KEYS[i]])
        collected.push(...data)
        setDownloadStatus([...collected])
        setDownloadProgress(Math.round((i + 1) / WATCHLIST_KEYS.length * 100))
        await Promise.all([fetchCharts(filters), fetchFilterOpts()])
      }
      await fetchEntries(filters, 1)
    } catch (err) {
      setDownloadStatus([...collected, { watchlist: 'error', status: 'failed', error: String(err) }])
    }
    setDownloading(false)

    // Auto-poll culture classification (runs in background on server)
    setClassifyingCulture(true)
    setCultureProgress(0)
    try {
      const { data: initial } = await listsApi.inferCulturesStatus()
      setCultureStatus(initial)
      const totalToClassify = initial.pending ?? 0
      let noProgressStreak = 0
      while (true) {
        const { data: status } = await listsApi.inferCulturesStatus()
        setCultureStatus(status)
        const remaining = status.pending ?? 0
        const done = totalToClassify - remaining
        setCultureProgress(totalToClassify > 0 ? Math.min(99, Math.round(done / totalToClassify * 100)) : 100)
        if (remaining === 0) break
        if (done === 0 && noProgressStreak > 0) {
          noProgressStreak++
          if (noProgressStreak >= 10) break  // give up after ~30s of no progress
        } else {
          noProgressStreak = remaining === (totalToClassify - done) ? noProgressStreak + 1 : 0
        }
        await new Promise(r => setTimeout(r, 3000))
      }
      setCultureProgress(100)
      const { data: final } = await listsApi.inferCulturesStatus()
      setCultureStatus(final)
      await fetchEntries(filters, page)
    } catch (_) {}
    setClassifyingCulture(false)
  }

  const handleNlFilter = async () => {
    if (!nlQuery.trim()) return
    setNlLoading(true)
    setNlExplanation('')
    setNlError('')
    try {
      const { data } = await listsApi.nlFilter(nlQuery)
      const f = data.filters || {}
      const newFilters = {
        ...EMPTY_FILTERS,
        watchlists: f.watchlists || [],
        entity_types: f.entity_types || [],
        search: f.search || '',
        recently_modified_only: f.recently_modified_only || false,
        min_tokens: f.min_tokens != null ? parseInt(f.min_tokens, 10) : null,
        max_tokens: f.max_tokens != null ? parseInt(f.max_tokens, 10) : null,
      }
      applyFilters(newFilters)
      if (f.search) setSearchDraft(f.search)
      setNlExplanation(data.explanation || '')
    } catch (_) {}
    setNlLoading(false)
  }

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <div className="max-w-screen-2xl mx-auto space-y-5">

      {/* ── Header ── */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Watchlist Explorer</h1>
          <p className="text-sm text-slate-500 mt-0.5">
            {chartData
              ? `${chartData.total.toLocaleString()} entries across ${chartData.by_watchlist.length} lists`
              : 'No data loaded — download lists to begin'}
          </p>
        </div>
        <div className="flex gap-3">
          <div className="flex flex-col gap-1.5 min-w-[190px]">
            <button onClick={handleDownload} disabled={downloading || classifyingCulture}
              className="px-4 py-2 text-sm font-medium rounded-lg bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white transition-colors">
              {downloading
                ? `Downloading… ${downloadProgress}%`
                : classifyingCulture
                  ? `Classifying cultures… ${cultureProgress}%`
                  : 'Download / Refresh Lists'}
            </button>
            {downloading && <ProgressBar value={downloadProgress} color="#2563eb" />}
            {!downloading && classifyingCulture && <ProgressBar value={cultureProgress} color="#0d9488" />}
          </div>
          <div className="flex flex-col gap-1.5 min-w-[140px]">
            <button
              onClick={async () => {
                if (!window.confirm('Delete all watchlist data from the database? This cannot be undone.')) return
                setClearing(true)
                setClearProgress(0)
                const timer = setInterval(() => setClearProgress(p => Math.min(p + 20, 80)), 200)
                try {
                  await listsApi.clearDatabase()
                  clearInterval(timer)
                  setClearProgress(100)
                  setTimeout(() => window.location.reload(), 400)
                } catch {
                  clearInterval(timer)
                  setClearing(false)
                  setClearProgress(0)
                }
              }}
              disabled={clearing}
              className="px-4 py-2 text-sm font-medium rounded-lg bg-red-600 hover:bg-red-700 disabled:bg-red-300 text-white transition-colors">
              {clearing ? `Clearing… ${clearProgress}%` : 'Clear Database'}
            </button>
            {clearing && <ProgressBar value={clearProgress} color="#dc2626" />}
          </div>
        </div>
      </div>

      {/* ── Status cards ── */}
      {chartData && (
        <div className="flex gap-3 items-stretch">
          <div className="bg-white rounded-xl shadow-sm p-4 border-l-4 flex items-center gap-5 flex-wrap"
            style={{ borderColor: '#3b82f6' }}>
            <div className="flex-shrink-0">
              <p className="text-xs text-slate-500 font-medium uppercase tracking-wide">Total entries</p>
              <p className="text-2xl font-bold text-slate-800 mt-1">{chartData.total?.toLocaleString() ?? '—'}</p>
            </div>
            <div className="flex flex-wrap gap-x-4 gap-y-1.5 pl-4 border-l border-slate-100">
              {WATCHLIST_KEYS.map(wlKey => {
                const wlData = chartData?.by_watchlist?.find(w => w.name === wlKey)
                const count = wlData?.count ?? 0
                return (
                  <div key={wlKey} className="flex items-center gap-1.5">
                    <span className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                      style={{ background: WATCHLIST_COLORS[wlKey] }} />
                    <span className={`text-xs font-medium ${count > 0 ? 'text-slate-600' : 'text-slate-400'}`}>
                      {WATCHLIST_LABELS[wlKey]}
                    </span>
                    <span className="text-xs text-slate-400">{count.toLocaleString()}</span>
                  </div>
                )
              })}
            </div>
          </div>
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
              <span className="font-medium">{WATCHLIST_LABELS[s.watchlist] || s.watchlist}</span>
              <span className="capitalize">{s.status}</span>
              {s.count > 0 && <span>({s.count.toLocaleString()})</span>}
              {s.error && <span title={s.error} className="truncate max-w-xs">⚠ {s.error}</span>}
            </div>
          ))}
        </div>
      )}

      {/* ── Filter bar ── */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 space-y-3">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
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
          <MultiSelect label="Watchlist"   options={WATCHLIST_KEYS}
            value={filters.watchlists}   onChange={v => updateFilter('watchlists', v)}
            colorMap={WATCHLIST_COLORS}  labelFn={k => WATCHLIST_LABELS[k] || k} />
          <MultiSelect label="Record Type" options={filterOpts.entity_types || []}
            value={filters.entity_types} onChange={v => updateFilter('entity_types', v)}
            labelFn={capFirst} />
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
              <Badge key={w} label={WATCHLIST_LABELS[w] || w} color={WATCHLIST_COLORS[w] || '#6b7280'}
                onRemove={() => updateFilter('watchlists', filters.watchlists.filter(v => v !== w))} />
            ))}
            {filters.entity_types.map(e => (
              <Badge key={e} label={capFirst(e)} color="#10b981"
                onRemove={() => updateFilter('entity_types', filters.entity_types.filter(v => v !== e))} />
            ))}
            {filters.search && (
              <Badge label={`"${filters.search}"`} color="#0ea5e9"
                onRemove={() => { setSearchDraft(''); updateFilter('search', '') }} />
            )}
            {(filters.min_tokens != null || filters.max_tokens != null) && (
              <Badge
                label={
                  filters.min_tokens != null && filters.max_tokens != null && filters.min_tokens === filters.max_tokens
                    ? `${filters.min_tokens} token${filters.min_tokens !== 1 ? 's' : ''}`
                    : filters.min_tokens != null && filters.max_tokens != null
                    ? `${filters.min_tokens}–${filters.max_tokens} tokens`
                    : filters.min_tokens != null
                    ? `≥${filters.min_tokens} tokens`
                    : `≤${filters.max_tokens} tokens`
                }
                color="#8b5cf6"
                onRemove={() => applyFilters({ ...filters, min_tokens: null, max_tokens: null })}
              />
            )}
          </div>
        )}

        {/* Natural-language filter — distinct AI section */}
        <div className="mt-1 rounded-lg bg-indigo-50 border border-indigo-200 p-3">
          <p className="text-xs font-semibold text-indigo-700 mb-2 flex items-center gap-1">
            <span>✦</span> AI Search — describe what you're looking for
          </p>
          <div className="flex gap-2">
            <input
              type="text"
              placeholder='e.g. "find African vessels with two or more tokens"'
              value={nlQuery}
              onChange={e => setNlQuery(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleNlFilter()}
              className="flex-1 px-3 py-2 text-sm border border-indigo-200 bg-white rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-400"
            />
            <button
              onClick={handleNlFilter}
              disabled={nlLoading || !nlQuery.trim()}
              className="px-4 py-2 text-sm font-medium bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-colors"
            >
              {nlLoading ? 'Searching…' : 'Search'}
            </button>
          </div>
          {nlExplanation && (
            <p className="mt-1.5 text-xs text-indigo-700">{nlExplanation}</p>
          )}
          {nlError && (
            <p className="mt-1.5 text-xs text-red-600">{nlError}</p>
          )}
        </div>
      </div>

      {/* ── Charts ── */}
      {chartData && chartData.total > 0 && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <WatchlistChart data={chartData.by_watchlist}  onFilterClick={handleChartClick} />
          <EntityTypeChart data={chartData.by_entity_type} onFilterClick={handleChartClick} />
          <NameLengthChart data={chartData.name_length_hist} />
          <TokenCountChart data={chartData.token_count_hist}
            onFilterClick={(tokens) => {
              if (tokens === '11+') {
                // toggle: if already filtering 11+, clear; else set min=11
                const active = filters.min_tokens === 11 && filters.max_tokens == null
                applyFilters({ ...filters, min_tokens: active ? null : 11, max_tokens: active ? null : null })
              } else {
                const val = parseInt(tokens, 10)
                const active = filters.min_tokens === val && filters.max_tokens === val
                applyFilters({ ...filters, min_tokens: active ? null : val, max_tokens: active ? null : val })
              }
            }} />
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
          <div className="overflow-auto max-h-[65vh]">
            <table className="text-xs" style={{ tableLayout: 'fixed', width: '100%', minWidth: 1750 }}>
              <thead className="bg-slate-50 border-b border-slate-200 sticky top-0 z-10">
                <tr>
                  {[
                    ['Watchlist', 90], ['Program', 110],
                    ['UID', 80], ['Parent UID', 80],
                    ['Cleaned Name', 200], ['Original Name', 180],
                    ['P/AKA', 70], ['Record Type', 80],
                    ['Region', 170], ['Name Culture', 150], ['Confidence', 70],
                    ['Date Listed', 90],
                    ['Tokens', 55], ['Length', 55],
                  ].map(([h, w]) => (
                    <th key={h} style={{ width: w, resize: 'horizontal', overflow: 'hidden', minWidth: 40 }}
                      className="px-2.5 py-2 text-left font-semibold text-slate-600 whitespace-nowrap bg-slate-50 border-r border-slate-100 last:border-r-0">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {entries.map((e, i) => {
                  const profileUrl = getProfileUrl(e)
                  const shortUid = displayUid(e.uid, e.watchlist)
                  const shortParentUid = displayUid(e.parent_uid, e.watchlist)
                  const isPrimary = e.primary_aka === 'primary'
                  return (
                    <tr key={e.uid || i} className={`transition-colors ${isPrimary ? 'bg-gray-50 hover:bg-gray-100' : 'bg-white hover:bg-gray-50'}`}>
                      <td className="px-2.5 py-1.5 overflow-hidden">
                        <span className="px-1.5 py-0.5 rounded text-white text-xs font-medium"
                          style={{ background: WATCHLIST_COLORS[e.watchlist] || '#6b7280' }}>
                          {WATCHLIST_LABELS[e.watchlist] || e.watchlist}
                        </span>
                      </td>
                      <td className="px-2.5 py-1.5 text-slate-500 truncate overflow-hidden"
                        title={e.sub_watchlist_1}>{e.sub_watchlist_1 || '—'}</td>
                      <td className="px-2.5 py-1.5 text-slate-400 font-mono truncate overflow-hidden"
                        title={e.uid}>{shortUid}</td>
                      <td className="px-2.5 py-1.5 text-slate-400 font-mono truncate overflow-hidden"
                        title={e.parent_uid || ''}>{shortParentUid || '—'}</td>
                      <td className="px-2.5 py-1.5 font-medium truncate overflow-hidden"
                        title={e.cleaned_name}>
                        {profileUrl
                          ? <a href={profileUrl} target="_blank" rel="noopener noreferrer"
                              className="text-blue-600 hover:underline">{e.cleaned_name}</a>
                          : <span className="text-slate-800">{e.cleaned_name}</span>
                        }
                      </td>
                      <td className="px-2.5 py-1.5 text-slate-500 truncate overflow-hidden"
                        title={e.original_name}>{e.original_name}</td>
                      <td className="px-2.5 py-1.5 overflow-hidden">
                        <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                          isPrimary ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-500'
                        }`}>{e.primary_aka}</span>
                      </td>
                      <td className="px-2.5 py-1.5 text-slate-600 truncate overflow-hidden">{capFirst(e.entity_type)}</td>
                      <td className="px-2.5 py-1.5 text-slate-600 truncate overflow-hidden"
                        title={e.region}>{e.region || '—'}</td>
                      <td className="px-2.5 py-1.5 text-slate-600 truncate overflow-hidden"
                        title={e.name_culture}>{e.name_culture || '—'}</td>
                      <td className="px-2.5 py-1.5 overflow-hidden">
                        {e.culture_confidence && (
                          <span className="px-1.5 py-0.5 rounded text-xs font-medium"
                            style={{
                              background: (CULTURE_CONF_COLORS[e.culture_confidence] || '#6b7280') + '22',
                              color: CULTURE_CONF_COLORS[e.culture_confidence] || '#6b7280',
                            }}>
                            {e.culture_confidence}
                          </span>
                        )}
                      </td>
                      <td className="px-2.5 py-1.5 text-slate-400 whitespace-nowrap overflow-hidden">
                        {e.date_listed || '—'}
                      </td>
                      <td className="px-2.5 py-1.5 text-center text-slate-500 overflow-hidden">{e.num_tokens}</td>
                      <td className="px-2.5 py-1.5 text-center text-slate-500 overflow-hidden">{e.name_length}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
