import { useState, useRef, useCallback, useEffect } from 'react'
import { reconciliationApi } from '../api/reconciliationApi'

// Module-level store — survives navigation away and back
const _saved = {
  jobId:           null,
  jobStatus:       null,
  results:         null,
  tabGroup:        'results',
  activeSection:   'full_public',
  page:            1,
  filterWatchlist: '',
  filterType:      '',
  filterSearch:    '',
}

// "public_list" is a frontend alias for the full_public backend section
// (same data, different column rendering)
const SECTION_ALIAS = { public_list: 'full_public' }

const TAB_GROUPS = {
  results: {
    label: 'Reconciliation',
    defaultSection: 'full_public',
    sections: [
      { id: 'full_public',           label: 'Full Table',     active: 'bg-slate-700',   dot: 'bg-slate-500',   countKey: 'total_public' },
      { id: 'public_not_on_private', label: 'Public Gaps',    active: 'bg-red-600',     dot: 'bg-red-500',     countKey: 'unmatched_public' },
      { id: 'private_not_on_public', label: 'Private Extras', active: 'bg-amber-500',   dot: 'bg-amber-400',   countKey: 'unmatched_private' },
      { id: 'matches',               label: 'Matches',        active: 'bg-emerald-600', dot: 'bg-emerald-500', countKey: 'matched_total' },
    ],
  },
  inspect: {
    label: 'Input Lists',
    defaultSection: 'public_list',
    sections: [
      { id: 'public_list',  label: 'Public List',  active: 'bg-blue-600',   dot: 'bg-blue-500',   countKey: 'total_public' },
      { id: 'private_list', label: 'Private List', active: 'bg-indigo-600', dot: 'bg-indigo-500', countKey: 'total_private' },
    ],
  },
}

const WATCHLISTS = [
  { id: 'OFAC_SDN',     label: 'OFAC SDN',      color: 'bg-red-500' },
  { id: 'OFAC_NON_SDN', label: 'OFAC Non-SDN',   color: 'bg-orange-500' },
  { id: 'EU',           label: 'EU Consolidated', color: 'bg-blue-500' },
  { id: 'HMT',          label: 'UK HMT',          color: 'bg-purple-500' },
  { id: 'BIS',          label: 'BIS Entity List', color: 'bg-emerald-500' },
  { id: 'JAPAN',        label: 'Japan METI',      color: 'bg-amber-500' },
]

const TIER_LABELS = {
  exact:    { label: 'Exact',        cls: 'bg-emerald-100 text-emerald-700' },
  expanded: { label: 'Reordered',    cls: 'bg-blue-100 text-blue-700' },
  fuzzy:    { label: 'Fuzzy',        cls: 'bg-amber-100 text-amber-700' },
  ai:       { label: 'AI Assisted',  cls: 'bg-purple-100 text-purple-700' },
}

const ENTITY_TYPES = ['individual', 'entity', 'vessel', 'aircraft', 'unknown']

const WL_COLORS = {
  OFAC_SDN:     'bg-red-100 text-red-700',
  OFAC_NON_SDN: 'bg-orange-100 text-orange-700',
  EU:           'bg-blue-100 text-blue-700',
  HMT:          'bg-purple-100 text-purple-700',
  BIS:          'bg-emerald-100 text-emerald-700',
  JAPAN:        'bg-amber-100 text-amber-700',
}

function Badge({ children, cls }) {
  return (
    <span className={`inline-block text-xs font-medium px-2 py-0.5 rounded-full ${cls}`}>
      {children}
    </span>
  )
}

function SectionDivider({ label, color }) {
  return (
    <div className="flex items-center gap-3">
      <span className={`w-2.5 h-2.5 rounded-sm shrink-0 ${color}`} />
      <span className="text-xs font-bold text-slate-600 uppercase tracking-widest whitespace-nowrap">
        {label}
      </span>
      <div className="flex-1 h-px bg-slate-200" />
    </div>
  )
}

function StatCard({ label, value, sub, accent }) {
  return (
    <div className={`bg-white rounded-xl border border-slate-200 border-l-4 ${accent} p-4`}>
      <div className="text-2xl font-bold text-slate-900">{value?.toLocaleString() ?? '—'}</div>
      <div className="text-xs font-semibold text-slate-500 mt-0.5">{label}</div>
      {sub && <div className="text-xs text-slate-400 mt-1">{sub}</div>}
    </div>
  )
}

export default function ListReconciliation() {
  const [selectedWatchlists, setSelectedWatchlists] = useState(
    WATCHLISTS.map(w => w.id)
  )
  const [files, setFiles] = useState([])
  const [useAI, setUseAI] = useState(true)
  const [dragging, setDragging] = useState(false)

  const [jobId,         setJobId]         = useState(_saved.jobId)
  const [jobStatus,     setJobStatus]     = useState(_saved.jobStatus)
  const [results,       setResults]       = useState(_saved.results)
  const [tabGroup,      setTabGroup]      = useState(_saved.tabGroup)
  const [activeSection, setActiveSection] = useState(_saved.activeSection)
  const [page,          setPage]          = useState(_saved.page)
  const [loading,       setLoading]       = useState(false)
  const [error,         setError]         = useState(null)

  // Filters — restored from saved state
  const [filterWatchlist, setFilterWatchlist] = useState(_saved.filterWatchlist)
  const [filterType,      setFilterType]      = useState(_saved.filterType)
  const [filterSearch,    setFilterSearch]    = useState(_saved.filterSearch)

  const pollRef = useRef(null)
  const fileInputRef = useRef(null)

  // Keep _saved in sync so state survives navigation
  useEffect(() => { _saved.jobId           = jobId         }, [jobId])
  useEffect(() => { _saved.jobStatus       = jobStatus     }, [jobStatus])
  useEffect(() => { _saved.results         = results       }, [results])
  useEffect(() => { _saved.tabGroup        = tabGroup      }, [tabGroup])
  useEffect(() => { _saved.activeSection   = activeSection }, [activeSection])
  useEffect(() => { _saved.page            = page          }, [page])
  useEffect(() => { _saved.filterWatchlist = filterWatchlist }, [filterWatchlist])
  useEffect(() => { _saved.filterType      = filterType    }, [filterType])
  useEffect(() => { _saved.filterSearch    = filterSearch  }, [filterSearch])

  // -------------------------------------------------------------------------
  // File handling
  // -------------------------------------------------------------------------
  const addFiles = (incoming) => {
    const arr = Array.from(incoming).filter(f =>
      /\.(csv|xlsx|xls)$/i.test(f.name)
    )
    if (!arr.length) return
    setFiles(prev => {
      const existingNames = new Set(prev.map(f => f.name))
      return [...prev, ...arr.filter(f => !existingNames.has(f.name))]
    })
  }
  const removeFile = (name) => setFiles(prev => prev.filter(f => f.name !== name))

  const onDrop = useCallback((e) => {
    e.preventDefault()
    setDragging(false)
    addFiles(e.dataTransfer.files)
  }, [])

  // -------------------------------------------------------------------------
  // Watchlist toggle
  // -------------------------------------------------------------------------
  const toggleWatchlist = (id) => {
    setSelectedWatchlists(prev =>
      prev.includes(id) ? prev.filter(w => w !== id) : [...prev, id]
    )
  }

  // -------------------------------------------------------------------------
  // Run reconciliation
  // -------------------------------------------------------------------------
  const startJob = async () => {
    if (files.length === 0) { setError('Please upload at least one private list file.'); return }
    if (selectedWatchlists.length === 0) { setError('Select at least one watchlist.'); return }

    setError(null)
    setResults(null)
    setJobId(null)
    setJobStatus(null)
    setPage(1)
    setLoading(true)

    try {
      const { data } = await reconciliationApi.start(files, selectedWatchlists, useAI)
      const id = data.job_id
      setJobId(id)
      pollRef.current = setInterval(() => pollStatus(id), 1200)
    } catch (err) {
      setError(err.response?.data?.detail || err.message)
      setLoading(false)
    }
  }

  const pollStatus = async (id) => {
    try {
      const { data } = await reconciliationApi.status(id)
      setJobStatus(data)
      if (data.status === 'done') {
        clearInterval(pollRef.current)
        await fetchResults(id, activeSection, 1)
        setLoading(false)
      } else if (data.status === 'error') {
        clearInterval(pollRef.current)
        setError(data.error || 'Reconciliation failed.')
        setLoading(false)
      }
    } catch {
      clearInterval(pollRef.current)
      setLoading(false)
    }
  }

  // -------------------------------------------------------------------------
  // Fetch results page
  // -------------------------------------------------------------------------
  const fetchResults = async (id, section, pg, filters) => {
    try {
      const apiSection = SECTION_ALIAS[section] || section
      const { data } = await reconciliationApi.results(id, apiSection, pg, 50, filters ?? activeFilters())
      setResults(data)
      setActiveSection(section)
      setPage(pg)
    } catch (err) {
      setError(err.response?.data?.detail || err.message)
    }
  }

  const switchSection = (section) => {
    if (!jobId) return
    const filters = section === 'private_list' ? {} : activeFilters()
    fetchResults(jobId, section, 1, filters)
  }

  const switchTabGroup = (group) => {
    if (!jobId) return
    setTabGroup(group)
    const defaultSection = TAB_GROUPS[group].defaultSection
    switchSection(defaultSection)
  }

  const goPage = (pg) => {
    if (!jobId) return
    fetchResults(jobId, activeSection, pg)
  }

  const exportUrl = () => {
    const params = new URLSearchParams({ section: activeSection })
    const f = activeFilters()
    if (f.watchlist)   params.set('watchlist',   f.watchlist)
    if (f.entity_type) params.set('entity_type', f.entity_type)
    if (f.search)      params.set('search',      f.search)
    return `/api/reconciliation/export/${jobId}?${params}`
  }

  const applyFilters = () => {
    if (!jobId) return
    fetchResults(jobId, activeSection, 1, activeFilters())
  }

  // -------------------------------------------------------------------------
  // Filters helper
  // -------------------------------------------------------------------------
  const activeFilters = () => ({
    watchlist:   filterWatchlist || undefined,
    entity_type: filterType      || undefined,
    search:      filterSearch    || undefined,
  })

  const clearFilters = () => {
    setFilterWatchlist('')
    setFilterType('')
    setFilterSearch('')
  }

  const hasFilters = filterWatchlist || filterType || filterSearch

  // -------------------------------------------------------------------------
  // Reset
  // -------------------------------------------------------------------------
  const handleReset = () => {
    clearInterval(pollRef.current)
    setFiles([])
    setJobId(null)
    setJobStatus(null)
    setResults(null)
    setError(null)
    setPage(1)
    setLoading(false)
    setActiveSection('full_public')
    setSelectedWatchlists(WATCHLISTS.map(w => w.id))
    clearFilters()
    setTabGroup('results')
    // Clear persisted store immediately
    Object.assign(_saved, {
      jobId: null, jobStatus: null, results: null,
      tabGroup: 'results', activeSection: 'full_public', page: 1,
      filterWatchlist: '', filterType: '', filterSearch: '',
    })
  }

  // -------------------------------------------------------------------------
  // Render
  // -------------------------------------------------------------------------
  const stats = results?.stats
  const totalPages = results ? Math.ceil(results.total / results.page_size) : 0
  const canReset = files.length > 0 || !!jobId || !!results

  return (
    <div className="max-w-6xl mx-auto py-8 space-y-7">

      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">List Reconciliation</h1>
          <p className="text-sm text-slate-500 mt-1">
            Compare public watchlists against your private screening list.
            Identify gaps (public entries missing from your list) and extras (private entries not on any public list).
          </p>
        </div>
        {canReset && (
          <button
            onClick={handleReset}
            className="shrink-0 text-sm font-medium text-slate-500 hover:text-red-600 border border-slate-200 hover:border-red-300 px-4 py-2 rounded-lg transition-colors"
          >
            Reset
          </button>
        )}
      </div>

      {/* Setup */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-5">

        {/* Watchlist selector */}
        <div className="bg-white rounded-xl border border-slate-200 p-5">
          <h2 className="text-sm font-semibold text-slate-700 mb-3">Public Watchlists</h2>
          <div className="space-y-2">
            {WATCHLISTS.map(({ id, label, color }) => (
              <label key={id} className="flex items-center gap-3 cursor-pointer group">
                <input
                  type="checkbox"
                  className="w-4 h-4 rounded accent-blue-600"
                  checked={selectedWatchlists.includes(id)}
                  onChange={() => toggleWatchlist(id)}
                />
                <span className={`w-2.5 h-2.5 rounded-sm ${color}`} />
                <span className="text-sm text-slate-700 group-hover:text-slate-900">{label}</span>
              </label>
            ))}
          </div>
          <button
            className="mt-4 text-xs text-blue-600 hover:underline"
            onClick={() =>
              setSelectedWatchlists(
                selectedWatchlists.length === WATCHLISTS.length ? [] : WATCHLISTS.map(w => w.id)
              )
            }
          >
            {selectedWatchlists.length === WATCHLISTS.length ? 'Deselect all' : 'Select all'}
          </button>
        </div>

        {/* File upload + options */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 flex flex-col gap-4">
          <div>
            <h2 className="text-sm font-semibold text-slate-700 mb-2">Private List</h2>

            {/* Drop zone */}
            <div
              className={`border-2 border-dashed rounded-lg p-5 text-center cursor-pointer transition-colors
                ${dragging ? 'border-blue-400 bg-blue-50' : 'border-slate-300 hover:border-slate-400'}`}
              onDragOver={(e) => { e.preventDefault(); setDragging(true) }}
              onDragLeave={() => setDragging(false)}
              onDrop={onDrop}
              onClick={() => fileInputRef.current?.click()}
            >
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,.xlsx,.xls"
                multiple
                className="hidden"
                onChange={e => addFiles(e.target.files)}
              />
              <p className="text-sm text-slate-500">Drop files here or click to browse</p>
              <p className="text-xs text-slate-400 mt-1">CSV or Excel · multiple files supported</p>
            </div>

            {/* File list */}
            {files.length > 0 && (
              <ul className="mt-2 space-y-1">
                {files.map(f => (
                  <li key={f.name} className="flex items-center justify-between bg-slate-50 border border-slate-200 rounded-lg px-3 py-1.5 text-xs">
                    <span className="font-medium text-slate-700 truncate mr-2">{f.name}</span>
                    <span className="text-slate-400 shrink-0 mr-2">{(f.size / 1024).toFixed(0)} KB</span>
                    <button onClick={() => removeFile(f.name)} className="text-slate-400 hover:text-red-500 transition-colors shrink-0">✕</button>
                  </li>
                ))}
              </ul>
            )}

            <p className="text-xs text-slate-400 mt-2">
              Needs a <code className="bg-slate-100 px-1 rounded">name</code> column.
              Optional <code className="bg-slate-100 px-1 rounded">aka</code> column (pipe-separated aliases).
            </p>
          </div>

          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              className="w-4 h-4 rounded accent-purple-600"
              checked={useAI}
              onChange={e => setUseAI(e.target.checked)}
            />
            <span className="text-sm text-slate-700">Use AI matching</span>
            <span className="text-xs text-slate-400">(Claude Haiku — catches transliterations & variants)</span>
          </label>

          <button
            onClick={startJob}
            disabled={loading}
            className="mt-auto bg-slate-900 hover:bg-slate-700 disabled:opacity-50
                       text-white text-sm font-medium px-5 py-2.5 rounded-lg transition-colors"
          >
            {loading ? 'Running…' : 'Run Reconciliation'}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 text-sm rounded-lg px-4 py-3">
          {error}
        </div>
      )}

      {/* Progress */}
      {loading && jobStatus && (
        <div className="bg-white rounded-xl border border-slate-200 p-5">
          <div className="flex justify-between text-xs text-slate-500 mb-2">
            <span>{jobStatus.message}</span>
            <span>{jobStatus.progress}%</span>
          </div>
          <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
            <div
              className="h-full bg-blue-500 transition-all duration-500"
              style={{ width: `${jobStatus.progress}%` }}
            />
          </div>
        </div>
      )}

      {/* Results */}
      {stats && (
        <div className="space-y-5">
          <SectionDivider label="Results" color="bg-blue-600" />

          {/* Stat cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              label="Public Entities"
              value={stats.total_public}
              accent="border-l-blue-500"
            />
            <StatCard
              label="Private Entries"
              value={stats.total_private}
              accent="border-l-slate-400"
            />
            <StatCard
              label="Public Gaps"
              value={stats.unmatched_public}
              sub="public entries missing from your list"
              accent="border-l-red-500"
            />
            <StatCard
              label="Private Extras"
              value={stats.unmatched_private}
              sub="private entries not on any public list"
              accent="border-l-amber-500"
            />
          </div>

          {/* Match breakdown */}
          <div className="bg-white rounded-xl border border-slate-200 px-5 py-3 flex flex-wrap gap-5 text-sm text-slate-600">
            <span>
              Matched:{' '}
              <strong className="text-slate-900">
                {(stats.matched_exact + stats.matched_expanded + (stats.matched_fuzzy || 0) + stats.matched_ai).toLocaleString()}
              </strong>
            </span>
            <span className="text-slate-300">|</span>
            <span>Exact: <strong>{stats.matched_exact.toLocaleString()}</strong></span>
            <span>Reordered: <strong>{stats.matched_expanded.toLocaleString()}</strong></span>
            {(stats.matched_fuzzy > 0) && (
              <span>Fuzzy: <strong>{stats.matched_fuzzy.toLocaleString()}</strong></span>
            )}
            {useAI && (
              <span>AI: <strong>{stats.matched_ai.toLocaleString()}</strong></span>
            )}
            {results?.name_col && (
              <>
                <span className="text-slate-300">|</span>
                <span className="text-slate-400 text-xs">
                  Name col: <code className="bg-slate-100 px-1 rounded">{results.name_col}</code>
                  {results.aka_col && (
                    <> · AKA col: <code className="bg-slate-100 px-1 rounded">{results.aka_col}</code></>
                  )}
                </span>
              </>
            )}
          </div>

          {/* Filters */}
          <div className="bg-white rounded-xl border border-slate-200 px-5 py-4">
            <div className="flex flex-wrap items-end gap-3">
              {/* Search — always visible */}
              <div className="flex flex-col gap-1 min-w-[220px]">
                <label className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Search</label>
                <input
                  type="text"
                  value={filterSearch}
                  onChange={e => setFilterSearch(e.target.value)}
                  onKeyDown={e => e.key === 'Enter' && applyFilters()}
                  placeholder="Name or AKA…"
                  className="text-sm border border-slate-200 rounded-lg px-3 py-1.5 bg-white text-slate-700 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-300"
                />
              </div>
              {/* Watchlist + Type — hidden for private list */}
              {activeSection !== 'private_list' && <>
                <div className="flex flex-col gap-1 min-w-[140px]">
                  <label className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Watchlist</label>
                  <select
                    value={filterWatchlist}
                    onChange={e => setFilterWatchlist(e.target.value)}
                    className="text-sm border border-slate-200 rounded-lg px-3 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-300"
                  >
                    <option value="">All</option>
                    {WATCHLISTS.map(w => <option key={w.id} value={w.id}>{w.label}</option>)}
                  </select>
                </div>
                <div className="flex flex-col gap-1 min-w-[140px]">
                  <label className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Type</label>
                  <select
                    value={filterType}
                    onChange={e => setFilterType(e.target.value)}
                    className="text-sm border border-slate-200 rounded-lg px-3 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-300"
                  >
                    <option value="">All</option>
                    {ENTITY_TYPES.map(t => <option key={t} value={t}>{t.charAt(0).toUpperCase() + t.slice(1)}</option>)}
                  </select>
                </div>
              </>}
              <div className="flex gap-2 pb-0.5">
                <button
                  onClick={applyFilters}
                  className="text-sm font-medium bg-slate-900 hover:bg-slate-700 text-white px-4 py-1.5 rounded-lg transition-colors"
                >
                  Apply
                </button>
                {hasFilters && (
                  <button
                    onClick={() => { clearFilters(); fetchResults(jobId, activeSection, 1, {}) }}
                    className="text-sm font-medium text-slate-500 hover:text-red-600 border border-slate-200 hover:border-red-300 px-4 py-1.5 rounded-lg transition-colors"
                  >
                    Clear
                  </button>
                )}
              </div>
            </div>
          </div>

          {/* Two-level tabs */}
          <div className="space-y-2">
            {/* Main group tabs */}
            <div className="flex gap-2 border-b border-slate-200 pb-0">
              {Object.entries(TAB_GROUPS).map(([key, group]) => (
                <button
                  key={key}
                  onClick={() => switchTabGroup(key)}
                  className={`px-5 py-2 text-sm font-semibold rounded-t-lg border border-b-0 transition-colors -mb-px
                    ${tabGroup === key
                      ? 'bg-white border-slate-200 text-slate-900'
                      : 'bg-slate-50 border-transparent text-slate-500 hover:text-slate-700'}`}
                >
                  {group.label}
                </button>
              ))}
            </div>

            {/* Sub-tabs for active group */}
            <div className="flex flex-wrap gap-2 pt-1">
              {TAB_GROUPS[tabGroup].sections.map(({ id, label, active, dot, countKey }) => {
                const count = stats[countKey]
                return (
                  <button key={id}
                    onClick={() => switchSection(id)}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors
                      ${activeSection === id ? `${active} text-white` : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-50'}`}
                  >
                    {label}
                    <span className={`ml-2 text-xs px-1.5 py-0.5 rounded-full
                      ${activeSection === id ? dot : 'bg-slate-100 text-slate-500'}`}>
                      {count?.toLocaleString() ?? '—'}
                    </span>
                  </button>
                )
              })}
            </div>
          </div>

          {/* Section description */}
          <p className="text-xs text-slate-500 -mt-2">
            {activeSection === 'full_public'           && 'Complete public list with disposition, match type, and linked private entry for each record.'}
            {activeSection === 'public_not_on_private' && 'Public watchlist entries with no match in your private list — potential coverage gaps.'}
            {activeSection === 'private_not_on_public' && 'Private entries not found on any selected public watchlist.'}
            {activeSection === 'matches'               && 'Public entries that were matched to a private list entry, with match type and private name.'}
            {activeSection === 'public_list'           && 'All public watchlist entries loaded for this run — inspect names, types, and programs.'}
            {activeSection === 'private_list'          && 'All entries from your uploaded private list — inspect names and aliases as loaded.'}
          </p>

          {/* Table */}
          {results.entries.length === 0 ? (
            <div className="bg-white rounded-xl border border-slate-200 p-8 text-center text-slate-400 text-sm">
              No entries in this section.
            </div>
          ) : (
            <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
              <div className="flex justify-end px-4 pt-3 pb-1">
                <a
                  href={exportUrl()}
                  download
                  className="text-xs font-medium text-slate-500 hover:text-slate-900 border border-slate-200 hover:border-slate-400 px-3 py-1.5 rounded-lg transition-colors"
                >
                  Export CSV
                </a>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-slate-50 border-b border-slate-200 text-xs text-slate-500 uppercase tracking-wide">
                      {/* Public Gaps */}
                      {activeSection === 'public_not_on_private' && <>
                        <th className="text-left px-4 py-3 font-semibold">Name</th>
                        <th className="text-left px-4 py-3 font-semibold">AKAs</th>
                        <th className="text-left px-4 py-3 font-semibold">Watchlist</th>
                        <th className="text-left px-4 py-3 font-semibold">Type</th>
                        <th className="text-left px-4 py-3 font-semibold">Program</th>
                        <th className="text-left px-4 py-3 font-semibold">Date Listed</th>
                      </>}
                      {/* Private Extras */}
                      {activeSection === 'private_not_on_public' && <>
                        <th className="text-left px-4 py-3 font-semibold">Name</th>
                        <th className="text-left px-4 py-3 font-semibold">AKAs</th>
                      </>}
                      {/* Matches */}
                      {activeSection === 'matches' && <>
                        <th className="text-left px-4 py-3 font-semibold">Public Name</th>
                        <th className="text-left px-4 py-3 font-semibold">Watchlist</th>
                        <th className="text-left px-4 py-3 font-semibold">Type</th>
                        <th className="text-left px-4 py-3 font-semibold">Match Type</th>
                        <th className="text-left px-4 py-3 font-semibold">Public Key</th>
                        <th className="text-left px-4 py-3 font-semibold">Private Name</th>
                        <th className="text-left px-4 py-3 font-semibold">Private Key</th>
                      </>}
                      {/* Full Public List */}
                      {activeSection === 'full_public' && <>
                        <th className="text-left px-4 py-3 font-semibold">Name</th>
                        <th className="text-left px-4 py-3 font-semibold">P / AKA</th>
                        <th className="text-left px-4 py-3 font-semibold">Disposition</th>
                        <th className="text-left px-4 py-3 font-semibold">Match Type</th>
                        <th className="text-left px-4 py-3 font-semibold">Watchlist</th>
                        <th className="text-left px-4 py-3 font-semibold">Public Key</th>
                        <th className="text-left px-4 py-3 font-semibold">Private Name</th>
                        <th className="text-left px-4 py-3 font-semibold">Private Key</th>
                      </>}
                      {/* Public List (inspect) */}
                      {activeSection === 'public_list' && <>
                        <th className="text-left px-4 py-3 font-semibold">#</th>
                        <th className="text-left px-4 py-3 font-semibold">Name</th>
                        <th className="text-left px-4 py-3 font-semibold">AKAs</th>
                        <th className="text-left px-4 py-3 font-semibold">Watchlist</th>
                        <th className="text-left px-4 py-3 font-semibold">Type</th>
                        <th className="text-left px-4 py-3 font-semibold">Program</th>
                        <th className="text-left px-4 py-3 font-semibold">Date Listed</th>
                      </>}
                      {/* Private List (inspect) */}
                      {activeSection === 'private_list' && <>
                        <th className="text-left px-4 py-3 font-semibold">#</th>
                        <th className="text-left px-4 py-3 font-semibold">Name</th>
                        <th className="text-left px-4 py-3 font-semibold">AKAs</th>
                      </>}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {results.entries.map((entry, idx) => (
                      <tr key={entry.uid || idx} className="hover:bg-slate-50">

                        {/* Public Gaps rows */}
                        {activeSection === 'public_not_on_private' && <>
                          <td className="px-4 py-3 font-medium text-slate-800 max-w-xs">{entry.name}</td>
                          <td className="px-4 py-3 text-slate-500 max-w-xs">
                            {entry.akas?.length > 0
                              ? <span className="text-xs">{entry.akas.slice(0, 3).join(' · ')}{entry.akas.length > 3 ? ` +${entry.akas.length - 3}` : ''}</span>
                              : <span className="text-slate-300">—</span>}
                          </td>
                          <td className="px-4 py-3"><Badge cls={WL_COLORS[entry.watchlist] || 'bg-slate-100 text-slate-600'}>{entry.watchlist}</Badge></td>
                          <td className="px-4 py-3 text-slate-500 text-xs capitalize">{entry.entity_type || '—'}</td>
                          <td className="px-4 py-3 text-slate-500 text-xs max-w-xs truncate">{entry.sanctions_program || '—'}</td>
                          <td className="px-4 py-3 text-slate-500 text-xs whitespace-nowrap">{entry.date_listed || '—'}</td>
                        </>}

                        {/* Private Extras rows */}
                        {activeSection === 'private_not_on_public' && <>
                          <td className="px-4 py-3 font-medium text-slate-800 max-w-xs">{entry.name}</td>
                          <td className="px-4 py-3 text-slate-500 max-w-xs">
                            {entry.akas?.length > 0
                              ? <span className="text-xs">{entry.akas.slice(0, 3).join(' · ')}{entry.akas.length > 3 ? ` +${entry.akas.length - 3}` : ''}</span>
                              : <span className="text-slate-300">—</span>}
                          </td>
                        </>}

                        {/* Matches rows */}
                        {activeSection === 'matches' && <>
                          <td className="px-4 py-3 font-medium text-slate-800 max-w-[200px] truncate">{entry.name}</td>
                          <td className="px-4 py-3"><Badge cls={WL_COLORS[entry.watchlist] || 'bg-slate-100 text-slate-600'}>{entry.watchlist}</Badge></td>
                          <td className="px-4 py-3 text-slate-500 text-xs capitalize">{entry.entity_type || '—'}</td>
                          <td className="px-4 py-3">
                            {entry.match_tier && <Badge cls={TIER_LABELS[entry.match_tier]?.cls || 'bg-slate-100 text-slate-600'}>{TIER_LABELS[entry.match_tier]?.label}</Badge>}
                          </td>
                          <td className="px-4 py-3 font-mono text-xs text-slate-400">{entry.uid || '—'}</td>
                          <td className="px-4 py-3 text-slate-700 text-xs max-w-[180px] truncate">{entry.matched_to || '—'}</td>
                          <td className="px-4 py-3 font-mono text-xs text-slate-400">{entry.matched_key || '—'}</td>
                        </>}

                        {/* Public List inspect rows */}
                        {activeSection === 'public_list' && <>
                          <td className="px-4 py-3 font-mono text-xs text-slate-400">{(results.page_size * (results.page - 1)) + idx + 1}</td>
                          <td className="px-4 py-3 font-medium text-slate-800 max-w-xs">{entry.name}</td>
                          <td className="px-4 py-3 text-slate-500 max-w-xs">
                            {entry.akas?.length > 0
                              ? <span className="text-xs">{entry.akas.slice(0, 3).join(' · ')}{entry.akas.length > 3 ? ` +${entry.akas.length - 3}` : ''}</span>
                              : <span className="text-slate-300">—</span>}
                          </td>
                          <td className="px-4 py-3"><Badge cls={WL_COLORS[entry.watchlist] || 'bg-slate-100 text-slate-600'}>{entry.watchlist}</Badge></td>
                          <td className="px-4 py-3 text-slate-500 text-xs capitalize">{entry.entity_type || '—'}</td>
                          <td className="px-4 py-3 text-slate-500 text-xs max-w-xs truncate">{entry.sanctions_program || '—'}</td>
                          <td className="px-4 py-3 text-slate-500 text-xs whitespace-nowrap">{entry.date_listed || '—'}</td>
                        </>}

                        {/* Private List inspect rows */}
                        {activeSection === 'private_list' && <>
                          <td className="px-4 py-3 font-mono text-xs text-slate-400">{entry.key || (results.page_size * (results.page - 1)) + idx + 1}</td>
                          <td className="px-4 py-3 font-medium text-slate-800 max-w-xs">{entry.name}</td>
                          <td className="px-4 py-3 text-slate-500 max-w-xs">
                            {entry.akas?.length > 0
                              ? <span className="text-xs">{entry.akas.slice(0, 3).join(' · ')}{entry.akas.length > 3 ? ` +${entry.akas.length - 3}` : ''}</span>
                              : <span className="text-slate-300">—</span>}
                          </td>
                        </>}

                        {/* Full Public List rows */}
                        {activeSection === 'full_public' && <>
                          <td className="px-4 py-3 font-medium text-slate-800 max-w-[200px] truncate">{entry.name}</td>
                          <td className="px-4 py-3">
                            <Badge cls={entry.is_primary ? 'bg-slate-100 text-slate-700' : 'bg-sky-100 text-sky-700'}>
                              {entry.is_primary ? 'Primary' : 'AKA'}
                            </Badge>
                          </td>
                          <td className="px-4 py-3">
                            {entry.match_tier
                              ? <Badge cls="bg-emerald-100 text-emerald-700">Matched</Badge>
                              : <Badge cls="bg-red-100 text-red-600">Not Matched</Badge>}
                          </td>
                          <td className="px-4 py-3">
                            {entry.match_tier
                              ? <Badge cls={TIER_LABELS[entry.match_tier]?.cls || 'bg-slate-100 text-slate-600'}>{TIER_LABELS[entry.match_tier]?.label}</Badge>
                              : <span className="text-slate-300 text-xs">—</span>}
                          </td>
                          <td className="px-4 py-3"><Badge cls={WL_COLORS[entry.watchlist] || 'bg-slate-100 text-slate-600'}>{entry.watchlist}</Badge></td>
                          <td className="px-4 py-3 font-mono text-xs text-slate-400">{entry.uid || '—'}</td>
                          <td className="px-4 py-3 text-slate-700 text-xs max-w-[180px] truncate">{entry.matched_to || <span className="text-slate-300">—</span>}</td>
                          <td className="px-4 py-3 font-mono text-xs text-slate-400">{entry.matched_key || <span className="text-slate-300">—</span>}</td>
                        </>}

                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between px-4 py-3 border-t border-slate-100 text-xs text-slate-500">
                  <span>
                    Page {page} of {totalPages} · {results.total.toLocaleString()} entries
                  </span>
                  <div className="flex gap-2">
                    <button
                      disabled={page <= 1}
                      onClick={() => goPage(page - 1)}
                      className="px-3 py-1 rounded border border-slate-200 disabled:opacity-40 hover:bg-slate-50"
                    >
                      Previous
                    </button>
                    <button
                      disabled={page >= totalPages}
                      onClick={() => goPage(page + 1)}
                      className="px-3 py-1 rounded border border-slate-200 disabled:opacity-40 hover:bg-slate-50"
                    >
                      Next
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
