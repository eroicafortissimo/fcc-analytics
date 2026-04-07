import { useState, useEffect, useCallback, useRef } from 'react'
import { testcasesApi } from '../api/testcasesApi'
import { listsApi } from '../api/listsApi'

// ── Constants ──────────────────────────────────────────────────────────────────

const OUTCOMES = ['Must Hit', 'Should Hit', 'Testing Purposes', 'Should Not Hit']

const OUTCOME_COLOURS = {
  'Must Hit':         'bg-red-100 text-red-700',
  'Should Hit':       'bg-amber-100 text-amber-700',
  'Testing Purposes': 'bg-blue-100 text-blue-700',
  'Should Not Hit':   'bg-slate-100 text-slate-600',
}

const ENTITY_COLOURS = {
  individual: 'bg-violet-100 text-violet-700',
  entity:     'bg-sky-100 text-sky-700',
  vessel:     'bg-cyan-100 text-cyan-700',
  aircraft:   'bg-teal-100 text-teal-700',
  country:    'bg-orange-100 text-orange-700',
  unknown:    'bg-slate-100 text-slate-500',
}

const WATCHLIST_COLOURS = {
  OFAC_SDN:     'bg-red-500',
  OFAC_NON_SDN: 'bg-orange-400',
  EU:           'bg-blue-500',
  HMT:          'bg-violet-500',
  BIS:          'bg-amber-500',
  JAPAN:        'bg-rose-500',
}

const ALL_ENTITY_TYPES = ['individual', 'entity', 'vessel', 'aircraft', 'country', 'unknown']

const ENTITY_ABBREVS = {
  individual: 'Ind',
  entity:     'Org',
  vessel:     'Vsl',
  aircraft:   'Air',
  country:    'Cty',
  unknown:    'Unk',
}

const REASON_LABELS = {
  not_applicable:       'Entity type not applicable to this test case type',
  no_watchlist_data:    'No watchlist data available for this entity type',
  all_names_skipped:    'All candidate names failed to qualify',
  no_variation_function: 'No variation function implemented for this test case type',
}

const REASON_COLOURS = {
  not_applicable:       'text-slate-400',
  no_watchlist_data:    'text-amber-600',
  all_names_skipped:    'text-orange-600',
  no_variation_function: 'text-slate-500',
}

function nextOutcome(curr) {
  const idx = OUTCOMES.indexOf(curr)
  return OUTCOMES[idx === -1 ? 1 : (idx + 1) % OUTCOMES.length]
}

// ── Outcome matrix initializer ─────────────────────────────────────────────────

const LOCKED_MUST_HIT_CATEGORY = 'Exact Match'

function initOutcomeMatrix(types) {
  const matrix = {}
  types.forEach(t => {
    const applicable = t.applicable_entity_types || []
    const applyTo = applicable.length > 0 ? applicable : ALL_ENTITY_TYPES
    const typeOutcomes = {}
    const outcome = t.category === LOCKED_MUST_HIT_CATEGORY ? 'Must Hit' : (t.expected_outcome || 'Should Hit')
    applyTo.forEach(et => {
      typeOutcomes[et] = outcome
    })
    matrix[t.type_id] = typeOutcomes
  })
  return matrix
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
    slate:  'border-slate-200 text-slate-700',
    green:  'border-emerald-200 text-emerald-700',
    red:    'border-rose-200 text-rose-700',
    blue:   'border-blue-200 text-blue-700',
    amber:  'border-amber-200 text-amber-700',
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

function ThemeGroup({ theme, types, selected, onToggle, onToggleAll, outcomeMatrix, onOutcomeChange, isOpen, onToggleOpen }) {
  const allSelected = types.every(t => selected.has(t.type_id))
  const someSelected = types.some(t => selected.has(t.type_id))

  return (
    <div className="border border-slate-200 rounded-lg overflow-hidden mb-2">
      <div
        className="flex items-center gap-2 px-3 py-2 bg-slate-50 cursor-pointer hover:bg-slate-100 select-none"
        onClick={() => onToggleOpen(theme)}
      >
        <span className="text-slate-400 text-xs w-4">{isOpen ? '▼' : '▶'}</span>
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

      {isOpen && (
        <div className="divide-y divide-slate-100">
          {types.map(t => {
            const applicableSet = new Set(t.applicable_entity_types || [])
            const allApplicable = applicableSet.size === 0
            const typeOutcomes = outcomeMatrix[t.type_id] || {}

            return (
              <label
                key={t.type_id}
                className="flex items-center gap-2 px-3 py-1.5 hover:bg-slate-50 cursor-pointer"
              >
                <input
                  type="checkbox"
                  checked={selected.has(t.type_id)}
                  onChange={() => onToggle(t.type_id)}
                  className="h-3.5 w-3.5 rounded border-slate-300 text-blue-600 cursor-pointer flex-shrink-0"
                />
                <span className="text-[11px] font-mono text-slate-400 w-12 flex-shrink-0">{t.type_id}</span>
                <span
                  className="text-xs text-slate-700 flex-1 min-w-0 truncate"
                  title={t.description}
                >
                  {t.type_name}
                </span>
                {/* Entity-type outcome pills — only applicable types, click to cycle */}
                <div className="flex gap-0.5 flex-shrink-0" onClick={e => e.preventDefault()}>
                  {(allApplicable ? ALL_ENTITY_TYPES : ALL_ENTITY_TYPES.filter(et => applicableSet.has(et))).map(et => {
                    const outcome = typeOutcomes[et] || t.expected_outcome || 'Should Hit'
                    return (
                      <button
                        key={et}
                        type="button"
                        title={`${et}: ${outcome} — click to change`}
                        onClick={e => {
                          e.stopPropagation()
                          onOutcomeChange(t.type_id, et, nextOutcome(outcome))
                        }}
                        className={`text-[10px] px-1.5 py-0.5 rounded font-medium cursor-pointer transition-colors ${OUTCOME_COLOURS[outcome] || 'bg-slate-100 text-slate-500'}`}
                      >
                        {ENTITY_ABBREVS[et]}
                      </button>
                    )
                  })}
                </div>
              </label>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── Simple markdown renderer ───────────────────────────────────────────────────

function MdText({ text }) {
  const parts = text.split(/(\*\*[^*]+\*\*)/g)
  return (
    <span>
      {parts.map((p, i) =>
        p.startsWith('**') && p.endsWith('**')
          ? <strong key={i}>{p.slice(2, -2)}</strong>
          : <span key={i}>{p}</span>
      )}
    </span>
  )
}

// ── AI Type Assistant ──────────────────────────────────────────────────────────

function AiTypeAssistant({ onTypeSaved }) {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [sending, setSending] = useState(false)
  const [sessionId, setSessionId] = useState(null)
  const messagesEndRef = useRef(null)

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const send = async () => {
    const content = input.trim()
    if (!content || sending) return
    setInput('')

    const withGreeting = messages.length === 0
      ? [
          { role: 'assistant', content: "Hello! Describe a new name variation type and I'll help you define it. Once confirmed, it will appear in the list below." },
          { role: 'user', content },
        ]
      : [...messages, { role: 'user', content }]

    setMessages(withGreeting)
    setSending(true)
    try {
      const r = await testcasesApi.chatMessage(sessionId, content)
      const data = r.data
      setSessionId(data.session_id)
      setMessages(m => [...m, { role: 'assistant', content: data.reply }])
      if (data.saved_type_id) onTypeSaved(data.saved_type_id)
    } catch {
      setMessages(m => [...m, {
        role: 'assistant',
        content: 'An error occurred. Make sure the backend is running and ANTHROPIC_API_KEY is set.',
      }])
    } finally {
      setSending(false)
    }
  }

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() }
  }

  const reset = () => { setMessages([]); setSessionId(null); setInput('') }

  return (
    <div className="rounded-lg bg-indigo-50 border border-indigo-200 p-3">
      <div className="flex items-center justify-between mb-2">
        <p className="text-xs font-semibold text-indigo-700 flex items-center gap-1.5">
          <span>✦</span> New Type Assistant — describe a name variation to auto-generate a test type
        </p>
        {messages.length > 0 && (
          <button onClick={reset} className="text-[11px] text-indigo-400 hover:text-indigo-600 transition-colors">
            Reset
          </button>
        )}
      </div>

      <div className="flex gap-2">
        <input
          type="text"
          placeholder='e.g. "swap the first two letters of each name token"'
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKey}
          className="flex-1 px-3 py-2 text-sm border border-indigo-200 bg-white rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-400"
        />
        <button
          onClick={send}
          disabled={!input.trim() || sending}
          className="px-4 py-2 text-sm font-medium bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-colors"
        >
          {sending ? '…' : 'Send'}
        </button>
      </div>

      {messages.length > 0 && (
        <div className="mt-2 max-h-52 overflow-y-auto space-y-1.5">
          {messages.map((m, i) => (
            <div
              key={i}
              className={`text-xs rounded-lg px-3 py-2 leading-relaxed whitespace-pre-wrap ${
                m.role === 'assistant'
                  ? 'bg-white text-slate-700'
                  : 'bg-indigo-100 text-indigo-800 ml-8'
              }`}
            >
              {m.role === 'assistant'
                ? m.content.split('\n').map((line, j) => <div key={j}><MdText text={line} /></div>)
                : m.content
              }
            </div>
          ))}
          {sending && (
            <div className="flex items-center gap-1.5 text-xs text-indigo-500 px-3 py-1">
              <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24" fill="none">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
              </svg>
              Thinking…
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
      )}
    </div>
  )
}

// ── Resizable column hook ──────────────────────────────────────────────────────

const DEFAULT_COL_WIDTHS = {
  id:       80,
  type:     170,
  original: 200,
  test:     210,
  record:   110,
  culture:  130,
  list:     120,
  expected: 140,
}

function useResizableColumns(initial = DEFAULT_COL_WIDTHS) {
  const [widths, setWidths] = useState(initial)

  const startResize = useCallback((col, e) => {
    e.preventDefault()
    const startX = e.clientX
    const startW = widths[col]

    const onMove = (e) => {
      const newW = Math.max(50, startW + e.clientX - startX)
      setWidths(prev => ({ ...prev, [col]: newW }))
    }
    const onUp = () => {
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }, [widths])

  return [widths, startResize]
}

// ── Resizable TH ──────────────────────────────────────────────────────────────

function ResizerTh({ col, width, onResizeStart, children, className = '' }) {
  return (
    <th
      style={{ width, minWidth: 50, position: 'relative', userSelect: 'none' }}
      className={`px-3 py-2 text-left text-xs font-semibold text-slate-500 whitespace-nowrap ${className}`}
    >
      {children}
      <span
        onMouseDown={e => onResizeStart(col, e)}
        style={{
          position: 'absolute', right: 0, top: 0, bottom: 0,
          width: 5, cursor: 'col-resize',
          background: 'transparent',
        }}
        title="Drag to resize"
      />
    </th>
  )
}

// ── Main page ──────────────────────────────────────────────────────────────────

export default function TestCaseGenerator() {
  // Types & selection
  const [types, setTypes] = useState([])
  const [selected, setSelected] = useState(new Set())
  const [outcomeMatrix, setOutcomeMatrix] = useState({})

  // Tabs
  const [activeTab, setActiveTab] = useState('types')

  // Theme open/closed state (controlled from parent for expand/collapse all)
  const [openThemes, setOpenThemes] = useState({})

  // Generation settings
  const [countPerType, setCountPerType] = useState(250)
  const [cultureDist, setCultureDist] = useState('balanced')
  const [genWatchlists, setGenWatchlists] = useState(new Set()) // empty = all watchlists
  const [showWatchlistModal, setShowWatchlistModal] = useState(false)
  const [showCustomModal, setShowCustomModal] = useState(false)
  const [cultures, setCultures] = useState([])
  const [customDraft, setCustomDraft] = useState({})   // culture → string input value
  const [customDist, setCustomDist] = useState({})     // confirmed custom distribution
  const [customError, setCustomError] = useState('')

  // State
  const [generating, setGenerating] = useState(null) // null | 'replace' | 'add'
  const [genResult, setGenResult] = useState(null)
  const [genError, setGenError] = useState(null)

  // Stats
  const [stats, setStats] = useState(null)
  const [tableTypeIds, setTableTypeIds] = useState(new Set())

  // Results table
  const [cases, setCases] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const PAGE_SIZE = 100
  const [tableFilter, setTableFilter] = useState({ expectedResult: '', entityType: '', watchlist: '', typeId: '', search: '' })
  const [searchDraft, setSearchDraft] = useState('')
  const searchTimer = useRef(null)

  // Progress bar
  const [genProgress, setGenProgress] = useState(0)
  const progressVal = useRef(0)
  const progressTimer = useRef(null)

  // Resizable columns
  const [colWidths, startResize] = useResizableColumns()

  // ── Load types ───────────────────────────────────────────────────────────────
  const loadTypes = useCallback((autoSelectNew = null) => {
    testcasesApi.types().then(r => {
      setTypes(r.data)
      // Preserve existing user overrides, add defaults for any new types
      setOutcomeMatrix(prev => ({ ...initOutcomeMatrix(r.data), ...prev }))
      setSelected(prev => {
        const next = new Set(prev)
        if (autoSelectNew) next.add(autoSelectNew)
        return next
      })
    }).catch(() => {})
  }, [])

  useEffect(() => {
    testcasesApi.types().then(r => {
      setTypes(r.data)
      setSelected(new Set(r.data.map(t => t.type_id)))
      setOutcomeMatrix(initOutcomeMatrix(r.data))
      setOpenThemes(prev => {
        const next = { ...prev }
        r.data.forEach(t => { if (!(t.theme in next)) next[t.theme] = true })
        return next
      })
    }).catch(() => {})
  }, [])

  const onTypeSaved = useCallback((newTypeId) => {
    loadTypes(newTypeId)
  }, [loadTypes])

  // ── Outcome change ───────────────────────────────────────────────────────────
  const onOutcomeChange = useCallback((typeId, entityType, newOutcome) => {
    setOutcomeMatrix(prev => ({
      ...prev,
      [typeId]: {
        ...(prev[typeId] || {}),
        [entityType]: newOutcome,
      },
    }))
  }, [])

  // ── Load stats ───────────────────────────────────────────────────────────────
  const refreshStats = useCallback(() => {
    testcasesApi.stats().then(r => setStats(r.data)).catch(() => {})
    testcasesApi.tableTypes().then(r => setTableTypeIds(new Set(r.data))).catch(() => {})
  }, [])

  useEffect(() => { refreshStats() }, [refreshStats])

  // ── Load table ───────────────────────────────────────────────────────────────
  const loadTable = useCallback((p = 1, filter = tableFilter) => {
    testcasesApi.cases({
      page: p,
      pageSize: PAGE_SIZE,
      expectedResult: filter.expectedResult || undefined,
      entityType: filter.entityType || undefined,
      watchlist: filter.watchlist || undefined,
      typeId: filter.typeId || undefined,
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
  const generate = async (mode) => {
    if (selected.size === 0) return
    setGenerating(mode)
    setGenError(null)
    setGenResult(null)

    if (mode === 'replace') {
      await testcasesApi.clear().catch(() => {})
    }

    // Animate progress 0 → 85% while waiting, ease-out style
    progressVal.current = 0
    setGenProgress(0)
    progressTimer.current = setInterval(() => {
      progressVal.current = Math.min(85, progressVal.current + (85 - progressVal.current) * 0.04)
      setGenProgress(Math.round(progressVal.current))
    }, 150)

    try {
      const r = await testcasesApi.generate({
        type_ids: [...selected],
        count_per_type: countPerType,
        culture_distribution: cultureDist,
        custom_distribution: cultureDist === 'custom' ? customDist : undefined,
        export_format: 'names_only',
        outcome_overrides: outcomeMatrix,
        watchlists: genWatchlists.size > 0 ? [...genWatchlists] : [],
      })
      clearInterval(progressTimer.current)
      setGenProgress(100)
      setGenResult(r.data)
      refreshStats()
      loadTable(1)
      setActiveTab('cases')
    } catch (e) {
      clearInterval(progressTimer.current)
      setGenProgress(0)
      setGenError(e?.response?.data?.detail || 'Generation failed')
    } finally {
      setGenerating(null)
      setTimeout(() => setGenProgress(0), 800)
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

  // ── Export / Import type selection ────────────────────────────────────────────
  const importTypesRef = useRef(null)

  const handleExportTypes = () => {
    const payload = {
      version: 1,
      exported_at: new Date().toISOString(),
      types: {},
    }
    types.forEach(t => {
      payload.types[t.type_id] = {
        selected: selected.has(t.type_id),
        outcomes: outcomeMatrix[t.type_id] || {},
      }
    })
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `screeniq-types-${new Date().toISOString().slice(0, 10)}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleImportTypes = (e) => {
    const file = e.target.files[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = (ev) => {
      try {
        const data = JSON.parse(ev.target.result)
        if (!data.types || typeof data.types !== 'object') throw new Error('Missing "types" key')
        const newSelected = new Set()
        const newOutcomes = { ...outcomeMatrix }
        Object.entries(data.types).forEach(([typeId, typeData]) => {
          if (typeData.selected) newSelected.add(typeId)
          if (typeData.outcomes && typeof typeData.outcomes === 'object') {
            newOutcomes[typeId] = { ...(newOutcomes[typeId] || {}), ...typeData.outcomes }
          }
        })
        setSelected(newSelected)
        setOutcomeMatrix(newOutcomes)
      } catch (err) {
        alert(`Import failed: ${err.message}`)
      }
    }
    reader.readAsText(file)
    e.target.value = ''
  }

  // ── Export (test cases) ────────────────────────────────────────────────────────
  const [exportMenuOpen, setExportMenuOpen] = useState(false)
  const exportRef = useRef(null)

  useEffect(() => {
    const handler = (e) => { if (exportRef.current && !exportRef.current.contains(e.target)) setExportMenuOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const exportFormats = [
    { id: 'excel',  label: 'Excel (.xlsx)', desc: 'All columns + summary sheet' },
    { id: 'csv',    label: 'CSV',           desc: 'Plain text, all columns' },
    { id: 'pacs008', label: 'pacs.008 (ZIP)', desc: 'ISO 20022 Customer Credit Transfer' },
    { id: 'pacs009', label: 'pacs.009 (ZIP)', desc: 'ISO 20022 FI Credit Transfer' },
    { id: 'fuf',    label: 'FUF / MT103',   desc: 'SWIFT MT103 / Firco Universal Format' },
  ]

  const getExportUrl = (fmt) => testcasesApi.exportUrl(fmt, {
    expectedResult: tableFilter.expectedResult || undefined,
    entityType: tableFilter.entityType || undefined,
  })

  // ── Render ────────────────────────────────────────────────────────────────────
  return (
    <div className="max-w-[1600px] mx-auto">

      {/* Page header */}
      <div className="mb-4">
        <h1 className="text-2xl font-bold text-slate-900">Test Case Generator</h1>
        <p className="text-sm text-slate-500 mt-0.5">
          Generate name variation test cases from the sanctions watchlists
        </p>
      </div>

      {/* Stats row */}
      {stats && (
        <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-5">
          <StatCard label="Total Generated" value={stats.total} colour="slate" />
          <StatCard
            label="Must Hit"
            value={stats.by_result?.['Must Hit'] ?? 0}
            sub={stats.total ? `${Math.round(((stats.by_result?.['Must Hit'] ?? 0) / stats.total) * 100)}%` : undefined}
            colour="red"
          />
          <StatCard
            label="Should Hit"
            value={stats.by_result?.['Should Hit'] ?? 0}
            sub={stats.total ? `${Math.round(((stats.by_result?.['Should Hit'] ?? 0) / stats.total) * 100)}%` : undefined}
            colour="amber"
          />
          <StatCard
            label="Testing Purposes"
            value={stats.by_result?.['Testing Purposes'] ?? 0}
            sub={stats.total ? `${Math.round(((stats.by_result?.['Testing Purposes'] ?? 0) / stats.total) * 100)}%` : undefined}
            colour="blue"
          />
          <StatCard
            label="Should Not Hit"
            value={stats.by_result?.['Should Not Hit'] ?? 0}
            sub={stats.total ? `${Math.round(((stats.by_result?.['Should Not Hit'] ?? 0) / stats.total) * 100)}%` : undefined}
            colour="slate"
          />
        </div>
      )}

      {/* Generation result / error banners */}
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

      {/* Generation controls */}
      <div className="bg-white border border-slate-200 rounded-xl mb-4 overflow-hidden">

        {/* Row 1: selection + count + watchlists */}
        <div className="flex items-center gap-0 divide-x divide-slate-100">

          {/* Selection summary */}
          <div className="flex items-center gap-2.5 px-6 py-3 shrink-0">
            <span className="text-sm text-slate-700">
              <span className="font-semibold text-slate-900">{selected.size}</span>
              <span className="text-slate-400"> / {types.length}</span>
            </span>
            <span className="text-xs text-slate-400">types</span>
            <button onClick={selectAll} className="text-xs text-blue-600 hover:text-blue-800 hover:underline">All</button>
            <button onClick={clearAll} className="text-xs text-slate-400 hover:text-slate-600 hover:underline">None</button>
          </div>

          {/* Count per type */}
          <div className="flex items-center gap-2.5 px-6 py-3 shrink-0">
            <span className="text-xs text-slate-500 whitespace-nowrap">Cases / type / ET</span>
            <input
              type="range"
              min={10} max={1000} step={10}
              value={countPerType}
              onChange={e => setCountPerType(Number(e.target.value))}
              className="w-28 h-1.5 accent-blue-600"
            />
            <input
              type="number"
              min={1} max={5000}
              value={countPerType}
              onChange={e => setCountPerType(Math.max(1, Number(e.target.value)))}
              className="w-16 border border-slate-200 rounded px-2 py-0.5 text-xs text-right focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
          </div>

          {/* Source watchlists */}
          <div className="flex items-center gap-2.5 px-6 py-3 shrink-0">
            <span className="text-xs text-slate-500 shrink-0">Watchlists</span>
            <button
              onClick={() => setShowWatchlistModal(true)}
              className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded border border-slate-200 bg-white text-xs text-slate-700 hover:bg-slate-50 transition-colors"
            >
              {genWatchlists.size === 0
                ? <span className="font-medium">All</span>
                : [...genWatchlists].map(wl => (
                    <span key={wl} className="flex items-center gap-1">
                      <span className={`w-2 h-2 rounded-full ${WATCHLIST_COLOURS[wl] || 'bg-slate-400'}`} />
                      {wl}
                    </span>
                  ))
              }
              <span className="text-slate-400">▾</span>
            </button>
            {genWatchlists.size > 0 && (
              <button onClick={() => setGenWatchlists(new Set())} className="text-xs text-slate-400 hover:text-slate-600">
                Clear
              </button>
            )}
          </div>

        </div>

        {/* Row 2: culture + watchlists + generate buttons */}
        <div className="flex items-center gap-4 px-4 py-2 border-t border-slate-100 bg-slate-50/60">

          {/* Culture distribution */}
          <span className="text-[11px] text-slate-400 uppercase tracking-wide shrink-0">Culture</span>
          {[
            { id: 'balanced', label: 'Balanced', desc: 'Equal across cultures' },
            { id: 'weighted', label: 'Weighted', desc: 'Proportional to watchlist' },
          ].map(opt => (
            <label key={opt.id} className="flex items-center gap-1.5 cursor-pointer shrink-0">
              <input
                type="radio"
                name="cultureDist"
                checked={cultureDist === opt.id}
                onChange={() => setCultureDist(opt.id)}
                className="text-blue-600 border-slate-300 focus:ring-blue-500"
              />
              <span className="text-xs font-medium text-slate-700">{opt.label}</span>
              <span className="text-[11px] text-slate-400 hidden xl:inline">{opt.desc}</span>
            </label>
          ))}
          <label className="flex items-center gap-1.5 cursor-pointer shrink-0">
            <input
              type="radio"
              name="cultureDist"
              checked={cultureDist === 'custom'}
              onChange={async () => {
                setCultureDist('custom')
                if (cultures.length === 0) {
                  try {
                    const { data } = await listsApi.cultures()
                    setCultures(data)
                    const init = {}
                    data.forEach(c => { init[c] = '' })
                    setCustomDraft(init)
                  } catch (_) {}
                }
                setCustomError('')
                setShowCustomModal(true)
              }}
              className="text-blue-600 border-slate-300 focus:ring-blue-500"
            />
            <span className="text-xs font-medium text-slate-700">Custom</span>
            {cultureDist === 'custom' && Object.keys(customDist).length > 0 && (
              <button
                type="button"
                onClick={async () => {
                  if (cultures.length === 0) {
                    try {
                      const { data } = await listsApi.cultures()
                      setCultures(data)
                    } catch (_) {}
                  }
                  setCustomError('')
                  setShowCustomModal(true)
                }}
                className="text-[11px] text-blue-500 hover:underline ml-1"
              >Edit</button>
            )}
          </label>

          {/* Spacer */}
          <div className="flex-1" />

          {/* Est count + buttons */}
          <span className="text-[11px] text-slate-400 whitespace-nowrap shrink-0">
            Est. {types.filter(t => selected.has(t.type_id)).reduce((sum, t) => sum + countPerType * (t.applicable_entity_types?.length || ALL_ENTITY_TYPES.length), 0).toLocaleString()} cases
          </span>
          <button
            onClick={() => generate('replace')}
            disabled={generating !== null || selected.size === 0}
            className="px-4 py-1.5 rounded-lg bg-blue-600 text-white text-sm font-semibold
                       hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed
                       flex items-center gap-2 transition-colors whitespace-nowrap shrink-0"
          >
            {generating === 'replace' ? (
              <>
                <svg className="animate-spin h-3.5 w-3.5" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
                </svg>
                Generating…
              </>
            ) : 'Generate new'}
          </button>
          <button
            onClick={() => generate('add')}
            disabled={generating !== null || selected.size === 0}
            className="px-4 py-1.5 rounded-lg bg-slate-700 text-white text-sm font-semibold
                       hover:bg-slate-800 disabled:opacity-50 disabled:cursor-not-allowed
                       flex items-center gap-2 transition-colors whitespace-nowrap shrink-0"
          >
            {generating === 'add' ? (
              <>
                <svg className="animate-spin h-3.5 w-3.5" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
                </svg>
                Adding…
              </>
            ) : 'Add to existing'}
          </button>

        </div>

        {/* Custom distribution modal */}
        {showCustomModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
            onClick={() => { if (cultureDist !== 'custom' || Object.keys(customDist).length > 0) setShowCustomModal(false) }}>
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg mx-4 max-h-[85vh] flex flex-col"
              onClick={e => e.stopPropagation()}>
              <div className="px-6 py-4 border-b border-slate-100">
                <h2 className="font-bold text-slate-900">Custom Culture Distribution</h2>
                <p className="text-xs text-slate-500 mt-0.5">Enter the % weight for each name culture. Must total exactly 100.</p>
              </div>
              <div className="overflow-y-auto flex-1 px-6 py-4 space-y-2">
                {cultures.length === 0 ? (
                  <p className="text-sm text-slate-400">Loading cultures…</p>
                ) : cultures.map(culture => (
                  <div key={culture} className="flex items-center gap-3">
                    <span className="flex-1 text-sm text-slate-700 truncate">{culture}</span>
                    <div className="flex items-center gap-1">
                      <input
                        type="number"
                        min="0"
                        max="100"
                        step="0.1"
                        value={customDraft[culture] ?? ''}
                        onChange={e => {
                          setCustomDraft(d => ({ ...d, [culture]: e.target.value }))
                          setCustomError('')
                        }}
                        className="w-20 text-right border border-slate-200 rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                        placeholder="0"
                      />
                      <span className="text-sm text-slate-400">%</span>
                    </div>
                  </div>
                ))}
              </div>
              <div className="px-6 py-4 border-t border-slate-100 space-y-3">
                {(() => {
                  const total = cultures.reduce((sum, c) => sum + (parseFloat(customDraft[c]) || 0), 0)
                  const rounded = Math.round(total * 10) / 10
                  return (
                    <div className={`flex items-center justify-between text-sm font-medium rounded-lg px-3 py-2 ${
                      rounded === 100 ? 'bg-green-50 text-green-700' : 'bg-slate-50 text-slate-600'
                    }`}>
                      <span>Total</span>
                      <span>{rounded}%</span>
                    </div>
                  )
                })()}
                {customError && (
                  <p className="text-sm text-red-600">{customError}</p>
                )}
                <div className="flex gap-2 justify-end">
                  <button
                    onClick={() => {
                      if (Object.keys(customDist).length === 0) setCultureDist('balanced')
                      setShowCustomModal(false)
                    }}
                    className="px-4 py-2 text-sm rounded-lg border border-slate-200 text-slate-600 hover:bg-slate-50"
                  >Cancel</button>
                  <button
                    onClick={() => {
                      const total = cultures.reduce((sum, c) => sum + (parseFloat(customDraft[c]) || 0), 0)
                      const rounded = Math.round(total * 10) / 10
                      if (rounded !== 100) {
                        setCustomError(`Percentages must total 100% — current total is ${rounded}%.`)
                        return
                      }
                      const confirmed = {}
                      cultures.forEach(c => { confirmed[c] = parseFloat(customDraft[c]) || 0 })
                      setCustomDist(confirmed)
                      setShowCustomModal(false)
                      setCustomError('')
                    }}
                    className="px-4 py-2 text-sm font-medium rounded-lg bg-blue-600 text-white hover:bg-blue-700"
                  >Apply</button>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Watchlist picker modal */}
        {showWatchlistModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30" onClick={() => setShowWatchlistModal(false)}>
            <div className="bg-white rounded-xl shadow-xl border border-slate-200 w-72" onClick={e => e.stopPropagation()}>
              <div className="px-5 py-4 border-b border-slate-100">
                <p className="text-sm font-semibold text-slate-800">Source watchlists</p>
                <p className="text-xs text-slate-400 mt-0.5">Only records from selected lists will be used. Leave all unchecked to use all lists.</p>
              </div>
              <div className="px-5 py-3 space-y-2">
                {Object.keys(WATCHLIST_COLOURS).map(wl => (
                  <label key={wl} className="flex items-center gap-3 cursor-pointer group">
                    <input
                      type="checkbox"
                      checked={genWatchlists.has(wl)}
                      onChange={() => setGenWatchlists(prev => {
                        const next = new Set(prev)
                        next.has(wl) ? next.delete(wl) : next.add(wl)
                        return next
                      })}
                      className="h-4 w-4 rounded border-slate-300 text-blue-600 cursor-pointer"
                    />
                    <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${WATCHLIST_COLOURS[wl]}`} />
                    <span className="text-sm text-slate-700">{wl}</span>
                  </label>
                ))}
              </div>
              <div className="px-5 py-3 border-t border-slate-100 flex justify-between">
                <button onClick={() => setGenWatchlists(new Set())} className="text-xs text-slate-400 hover:text-slate-600">
                  Clear all
                </button>
                <button
                  onClick={() => setShowWatchlistModal(false)}
                  className="px-4 py-1.5 text-sm font-medium bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                >
                  Done
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Progress bar — full-width at bottom of card */}
        {genProgress > 0 && (
          <div className="h-0.5 bg-slate-100">
            <div
              className="h-full bg-blue-500 transition-all duration-300 ease-out"
              style={{ width: `${genProgress}%` }}
            />
          </div>
        )}
      </div>

      {/* Tabbed panel */}
      <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">

        {/* Tab bar */}
        <div className="flex border-b border-slate-200 bg-slate-50">
          <button
            onClick={() => setActiveTab('types')}
            className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
              activeTab === 'types'
                ? 'border-blue-600 text-blue-700 bg-white'
                : 'border-transparent text-slate-500 hover:text-slate-700 hover:bg-slate-100'
            }`}
          >
            Test Case Types
            <span className="ml-2 text-xs text-slate-400 font-normal">{types.length}</span>
          </button>
          <button
            onClick={() => setActiveTab('cases')}
            className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
              activeTab === 'cases'
                ? 'border-blue-600 text-blue-700 bg-white'
                : 'border-transparent text-slate-500 hover:text-slate-700 hover:bg-slate-100'
            }`}
          >
            Test Cases
            {stats?.total > 0 && (
              <span className="ml-2 text-xs text-slate-400 font-normal">{stats.total.toLocaleString()}</span>
            )}
          </button>
          <button
            onClick={() => setActiveTab('log')}
            className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
              activeTab === 'log'
                ? 'border-blue-600 text-blue-700 bg-white'
                : 'border-transparent text-slate-500 hover:text-slate-700 hover:bg-slate-100'
            }`}
          >
            Generation Log
            {genResult && (() => {
              const issues = Object.values(genResult.by_type || {}).reduce((n, tr) =>
                n + Object.values(tr.by_entity_type || {}).filter(r => r.generated === 0).length, 0)
              return issues > 0
                ? <span className="ml-2 text-xs bg-amber-100 text-amber-700 font-medium px-1.5 py-0.5 rounded-full">{issues}</span>
                : null
            })()}
          </button>
        </div>

        {/* ── Test Case Types tab ──────────────────────────────────────────────── */}
        {activeTab === 'types' && (
          <div className="p-5 space-y-4">

            {/* AI Type Assistant — top, indigo-styled */}
            <AiTypeAssistant onTypeSaved={onTypeSaved} />

            {/* Selection export / import / refresh toolbar */}
            <div className="flex items-center justify-between gap-2">
              {/* Expand / Collapse all */}
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setOpenThemes(prev => Object.fromEntries(Object.keys(prev).map(k => [k, true])))}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded border border-slate-200 bg-white text-xs text-slate-600 hover:bg-slate-50 transition-colors"
                >
                  ▼ Expand all
                </button>
                <button
                  onClick={() => setOpenThemes(prev => Object.fromEntries(Object.keys(prev).map(k => [k, false])))}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded border border-slate-200 bg-white text-xs text-slate-600 hover:bg-slate-50 transition-colors"
                >
                  ▶ Collapse all
                </button>
              </div>

              <div className="flex items-center gap-2">
              <input
                ref={importTypesRef}
                type="file"
                accept=".json"
                className="hidden"
                onChange={handleImportTypes}
              />
              <button
                onClick={() => importTypesRef.current?.click()}
                className="inline-flex items-center gap-1.5 px-3 py-1 rounded border border-slate-200 bg-white text-xs text-slate-600 hover:bg-slate-50 transition-colors"
              >
                ↑ Import selection
              </button>
              <button
                onClick={handleExportTypes}
                className="inline-flex items-center gap-1.5 px-3 py-1 rounded border border-slate-200 bg-white text-xs text-slate-600 hover:bg-slate-50 transition-colors"
              >
                ↓ Export selection
              </button>
              <button
                onClick={async () => {
                  await testcasesApi.clearCustomTypes().catch(() => {})
                  loadTypes()
                }}
                className="inline-flex items-center gap-1.5 px-3 py-1 rounded border border-slate-200 bg-white text-xs text-slate-600 hover:bg-slate-50 transition-colors"
              >
                ↻ Refresh types
              </button>
              </div>
            </div>

            {/* Theme groups */}
            <div className="columns-1 md:columns-2 xl:columns-3 gap-4">
              {Object.entries(themeGroups).map(([theme, themeTypes]) => (
                <div key={theme} className="break-inside-avoid mb-2">
                  <ThemeGroup
                    theme={theme}
                    types={themeTypes}
                    selected={selected}
                    onToggle={toggleType}
                    onToggleAll={toggleTheme}
                    outcomeMatrix={outcomeMatrix}
                    onOutcomeChange={onOutcomeChange}
                    isOpen={openThemes[theme] !== false}
                    onToggleOpen={t => setOpenThemes(prev => ({ ...prev, [t]: !prev[t] }))}
                  />
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ── Test Cases tab ───────────────────────────────────────────────────── */}
        {activeTab === 'cases' && (
          <div className="p-5 space-y-4">
            {/* Filter bar */}
            <div className="flex items-center gap-2">
              <input
                type="text"
                placeholder="Search names or type…"
                value={searchDraft}
                onChange={e => handleSearchChange(e.target.value)}
                className="border border-slate-200 rounded px-2 py-0.5 text-xs w-44 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <select
                value={tableFilter.expectedResult}
                onChange={e => setFilter('expectedResult', e.target.value)}
                className="border border-slate-200 rounded px-2 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="">All outcomes</option>
                <option value="Must Hit">Must Hit</option>
                <option value="Should Hit">Should Hit</option>
                <option value="Testing Purposes">Testing Purposes</option>
                <option value="Should Not Hit">Should Not Hit</option>
              </select>
              <select
                value={tableFilter.entityType}
                onChange={e => setFilter('entityType', e.target.value)}
                className="border border-slate-200 rounded px-2 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="">All entity types</option>
                {ALL_ENTITY_TYPES.map(et => (
                  <option key={et} value={et}>{et}</option>
                ))}
              </select>
              <select
                value={tableFilter.watchlist}
                onChange={e => setFilter('watchlist', e.target.value)}
                className="border border-slate-200 rounded px-2 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="">All watchlists</option>
                {Object.keys(WATCHLIST_COLOURS).map(wl => (
                  <option key={wl} value={wl}>{wl}</option>
                ))}
              </select>
              <select
                value={tableFilter.typeId}
                onChange={e => setFilter('typeId', e.target.value)}
                className="border border-slate-200 rounded px-2 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500 max-w-[180px]"
              >
                <option value="">All types</option>
                {types.filter(t => tableTypeIds.has(t.type_id)).map(t => (
                  <option key={t.type_id} value={t.type_id}>{t.type_id} — {t.type_name}</option>
                ))}
              </select>
              <span className="text-xs text-slate-400">
                {total.toLocaleString()} {total === 1 ? 'case' : 'cases'}
              </span>
              <div className="ml-auto flex gap-2">
                {stats?.total > 0 && (
                  <>
                    <div className="relative" ref={exportRef}>
                      <button
                        onClick={() => setExportMenuOpen(o => !o)}
                        className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded border border-slate-200 bg-white text-xs text-slate-600 hover:bg-slate-50"
                      >
                        ↓ Export <span className="text-slate-400">▾</span>
                      </button>
                      {exportMenuOpen && (
                        <div className="absolute right-0 mt-1 w-56 bg-white border border-slate-200 rounded-xl shadow-lg z-20 overflow-hidden">
                          {exportFormats.map(fmt => (
                            <a
                              key={fmt.id}
                              href={getExportUrl(fmt.id)}
                              onClick={() => setExportMenuOpen(false)}
                              className="flex flex-col px-4 py-2 hover:bg-slate-50 transition-colors"
                            >
                              <span className="text-sm font-medium text-slate-700">{fmt.label}</span>
                              <span className="text-xs text-slate-400">{fmt.desc}</span>
                            </a>
                          ))}
                        </div>
                      )}
                    </div>
                    <button
                      onClick={handleClear}
                      className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded border border-rose-200 bg-white text-xs text-rose-600 hover:bg-rose-50"
                    >
                      ✕ Clear
                    </button>
                  </>
                )}
              </div>
            </div>

            {/* Table */}
            {cases.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-24 text-slate-400">
                <p className="text-lg font-medium text-slate-500">No test cases yet</p>
                <p className="text-sm mt-1">
                  Select types in the <button onClick={() => setActiveTab('types')} className="text-blue-600 hover:underline">Test Case Types</button> tab and click Generate
                </p>
              </div>
            ) : (
              <>
                <div className="border border-slate-200 rounded-lg overflow-hidden">
                  <div className="overflow-x-auto">
                    <table className="text-sm" style={{ tableLayout: 'fixed', width: Object.values(colWidths).reduce((a, b) => a + b, 0) + 'px', minWidth: '100%' }}>
                      <thead>
                        <tr className="bg-slate-50 border-b border-slate-200">
                          <ResizerTh col="id"       width={colWidths.id}       onResizeStart={startResize}>ID</ResizerTh>
                          <ResizerTh col="type"     width={colWidths.type}     onResizeStart={startResize}>Type</ResizerTh>
                          <ResizerTh col="original" width={colWidths.original} onResizeStart={startResize}>Original Name</ResizerTh>
                          <ResizerTh col="test"     width={colWidths.test}     onResizeStart={startResize}>Test Name</ResizerTh>
                          <ResizerTh col="record"   width={colWidths.record}   onResizeStart={startResize}>Record Type</ResizerTh>
                          <ResizerTh col="culture"  width={colWidths.culture}  onResizeStart={startResize}>Culture</ResizerTh>
                          <ResizerTh col="list"     width={colWidths.list}     onResizeStart={startResize}>List</ResizerTh>
                          <ResizerTh col="expected" width={colWidths.expected} onResizeStart={startResize}>Expected</ResizerTh>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {cases.map(c => (
                          <tr key={c.test_case_id} className="hover:bg-slate-50">
                            <td className="px-3 py-2 font-mono text-[11px] text-slate-400 overflow-hidden text-ellipsis whitespace-nowrap">
                              {c.test_case_id}
                            </td>
                            <td className="px-3 py-2 text-xs text-slate-600 overflow-hidden text-ellipsis whitespace-nowrap" title={c.test_case_type}>
                              {c.test_case_type}
                            </td>
                            <td className="px-3 py-2 text-xs text-slate-700 overflow-hidden text-ellipsis whitespace-nowrap" title={c.cleaned_original_name}>
                              {c.cleaned_original_name}
                            </td>
                            <td className="px-3 py-2 text-xs font-medium text-slate-900 overflow-hidden text-ellipsis whitespace-nowrap" title={c.test_name}>
                              {c.test_name}
                            </td>
                            <td className="px-3 py-2 overflow-hidden">
                              <Badge
                                text={c.entity_type ? c.entity_type.charAt(0).toUpperCase() + c.entity_type.slice(1) : c.entity_type}
                                colourClass={ENTITY_COLOURS[c.entity_type] || 'bg-slate-100 text-slate-500'}
                              />
                            </td>
                            <td className="px-3 py-2 text-xs text-slate-600 overflow-hidden text-ellipsis whitespace-nowrap" title={c.culture_nationality}>
                              {c.culture_nationality || <span className="text-slate-300">—</span>}
                            </td>
                            <td className="px-3 py-2 overflow-hidden">
                              <span className="flex items-center gap-1.5">
                                <span className={`w-2 h-2 rounded-full flex-shrink-0 ${WATCHLIST_COLOURS[c.watchlist] || 'bg-slate-400'}`} />
                                <span className="text-xs text-slate-600 overflow-hidden text-ellipsis whitespace-nowrap">{c.watchlist}</span>
                              </span>
                            </td>
                            <td className="px-3 py-2 overflow-hidden">
                              <Badge
                                text={c.expected_result}
                                colourClass={OUTCOME_COLOURS[c.expected_result] || 'bg-slate-100 text-slate-500'}
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
                  <div className="flex items-center justify-between">
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
        )}
        {/* ── Generation Log tab ──────────────────────────────────────────────── */}
        {activeTab === 'log' && (
          <div className="p-5">
            {!genResult ? (
              <div className="flex flex-col items-center justify-center py-24 text-slate-400">
                <p className="text-lg font-medium text-slate-500">No generation run yet</p>
                <p className="text-sm mt-1">Click Generate to create test cases — the log will show results here.</p>
              </div>
            ) : (
              <div className="space-y-4">
                {Object.entries(genResult.by_type || {}).flatMap(([typeId, typeResult]) => {
                  const typeName = typeResult.type_name || typeId
                  const typeTarget = typeResult.target || genResult.count_per_type || 0
                  const typeComplete = typeResult.generated >= typeTarget
                  const etSections = Object.entries(typeResult.by_entity_type || {}).filter(([, etResult]) =>
                    etResult.reason !== 'not_applicable'
                  ).map(([et, etResult]) => {
                    const log = etResult.log || []
                    const ok = etResult.generated > 0
                    // Each entity-type section target is count_per_type (per-et)
                    const target = genResult.count_per_type || 0
                    const complete = etResult.generated >= target
                    return (
                      <div key={`${typeId}-${et}`} className="border border-slate-200 rounded-lg overflow-hidden">
                        {/* Section header */}
                        <div className={`flex items-center gap-3 px-4 py-2 text-xs font-semibold ${ok ? 'bg-emerald-50 text-emerald-800' : 'bg-slate-50 text-slate-500'}`}>
                          <span className={ok ? 'text-emerald-500' : 'text-slate-300'}>{ok ? '✓' : '○'}</span>
                          <span className="font-mono">{typeId}</span>
                          <span className="text-slate-400">·</span>
                          <span>{et}</span>
                          <span className="text-slate-400">·</span>
                          <span className="truncate">{typeName}</span>
                          <span className="ml-auto flex-shrink-0 flex items-center gap-3">
                            <span className={`font-bold px-2 py-0.5 rounded ${complete ? 'bg-emerald-100 text-emerald-700' : 'bg-red-100 text-red-600'}`}>
                              {etResult.generated}/{target}
                            </span>
                            {etResult.skipped > 0 && <span className="text-slate-400">{etResult.skipped} rejected</span>}
                          </span>
                        </div>
                        {/* Per-name log rows */}
                        {log.length > 0 && (
                          <div className="font-mono text-xs divide-y divide-slate-100 max-h-72 overflow-y-auto">
                            {log.map((entry, i) => (
                              <div key={i} className={`flex items-start gap-2 px-4 py-1.5 ${entry.accepted ? '' : 'bg-red-50/40'}`}>
                                {entry.accepted ? (
                                  <span className="flex-shrink-0 text-emerald-500 w-24 text-right">accepted-{entry.accepted_seq}</span>
                                ) : (
                                  <span className="flex-shrink-0 text-red-400 w-24 text-right">rejected-{entry.rejected_seq}</span>
                                )}
                                <span className="text-slate-600 flex-shrink-0 max-w-[220px] truncate" title={entry.source_name}>
                                  {entry.source_name}
                                </span>
                                {entry.accepted ? (
                                  <>
                                    <span className="text-slate-300 flex-shrink-0">→</span>
                                    <span className="text-slate-800 font-medium truncate" title={entry.variant}>{entry.variant}</span>
                                  </>
                                ) : (
                                  <span className="text-red-400 truncate" title={entry.reject_reason}>— {entry.reject_reason}</span>
                                )}
                              </div>
                            ))}
                          </div>
                        )}
                        {log.length === 0 && etResult.reason && (
                          <div className="px-4 py-2 text-xs text-slate-400">
                            {REASON_LABELS[etResult.reason] || etResult.reason}
                          </div>
                        )}
                      </div>
                    )
                  })
                  // Prepend a type-level summary header when there are multiple entity-type sections
                  const typeHeader = etSections.length > 1 ? (
                    <div key={`${typeId}-header`} className={`flex items-center gap-3 px-4 py-1.5 rounded text-xs font-bold border ${typeComplete ? 'border-emerald-200 bg-emerald-50 text-emerald-800' : 'border-red-200 bg-red-50 text-red-700'}`}>
                      <span className="font-mono">{typeId}</span>
                      <span className="text-slate-400">·</span>
                      <span>{typeName}</span>
                      <span className="ml-auto">
                        <span className={`px-2 py-0.5 rounded font-bold ${typeComplete ? 'bg-emerald-100 text-emerald-700' : 'bg-red-100 text-red-600'}`}>
                          {typeResult.generated}/{typeTarget} total
                        </span>
                      </span>
                    </div>
                  ) : null
                  return typeHeader ? [typeHeader, ...etSections] : etSections
                })}
              </div>
            )}
          </div>
        )}

      </div>
    </div>
  )
}
