import { useState, useCallback, useEffect, useRef } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, PieChart, Pie, Cell,
} from 'recharts'
import { transactiqApi } from '../api/transactiqApi'

const ENTITY_COLORS = ['#14b8a6', '#6366f1', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#3b82f6']
const TOKEN_COLOR   = '#6366f1'
const LENGTH_COLOR  = '#f59e0b'

const CULTURE_COLOR_MAP = {
  'Western / Other':                         '#94a3b8',
  'Middle Eastern / North African':          '#f59e0b',
  'Russian / Eastern European':              '#6366f1',
  'Chinese':                                 '#ef4444',
  'East Asian':                              '#ec4899',
  'Korean':                                  '#8b5cf6',
  'North Korean':                            '#64748b',
  'Japanese':                                '#3b82f6',
  'South Asian (Indian subcontinent)':       '#10b981',
  'Iranian':                                 '#f97316',
  'Turkish / Central Asian':                 '#84cc16',
  'Israeli / Middle Eastern':                '#eab308',
  'Thai':                                    '#06b6d4',
  'Georgian':                                '#a78bfa',
  'Armenian':                                '#fb7185',
  'East African (Ethiopian/Eritrean)':       '#4ade80',
  'Sri Lankan':                              '#22d3ee',
  'Burmese':                                 '#fb923c',
  'Cambodian':                               '#a3e635',
  'Laotian':                                 '#34d399',
  'Greek':                                   '#60a5fa',
  'South Asian (Tamil)':                     '#c084fc',
  'South Asian (Bengali/Bangladeshi)':       '#f472b6',
  'South Asian (Gujarati)':                  '#facc15',
  'South Asian (Punjabi/Sikh)':              '#2dd4bf',
}
const cultureColor = (label, idx) =>
  CULTURE_COLOR_MAP[label] || ENTITY_COLORS[idx % ENTITY_COLORS.length]

// ── Auto-detection helpers ────────────────────────────────────────────────────

const _BENE_CTY_PATS  = [/bene.*country/, /beneficiary.*country/, /creditor.*country/, /bene.*ctry/]
const _ORD_CTY_PATS   = [/ord.*country/, /ordering.*country/, /remitter.*country/, /ord.*ctry/, /debtor.*country/]
const _GENERIC_CTY    = [/^country$/, /^ctry$/, /nationality/, /country_code/]
const _ET_PATS        = [/^entity_type$/, /^entity type$/, /^type$/, /party_type/, /customer_type/]

function _matchCol(columns, pats) {
  const lc = columns.map(c => c.toLowerCase().replace(/\s+/g, '_'))
  for (const pat of pats) {
    const idx = lc.findIndex(c => pat.test(c))
    if (idx !== -1) return columns[idx]
  }
  return ''
}

function autoDetectFields(columns, suggestedBene, suggestedOrd) {
  const bene    = suggestedBene || _matchCol(columns, [/bene.*name/, /beneficiary/, /creditor/]) || columns[0] || ''
  const ord     = suggestedOrd  || _matchCol(columns, [/ord.*name/, /ordering/, /remitter/]) || ''
  // Only split bene/ord country if we can tell them apart; otherwise give bene the generic country
  const beneCty = _matchCol(columns, [..._BENE_CTY_PATS, ..._GENERIC_CTY])
  const ordCty  = beneCty
    ? _matchCol(columns, _ORD_CTY_PATS)
    : _matchCol(columns, [..._ORD_CTY_PATS, ..._GENERIC_CTY])
  const et      = _matchCol(columns, _ET_PATS)
  return { bene, ord, beneCty, ordCty, et }
}

// ── Merge two distribution arrays by key ────────────────────────────────────

function mergeDist(key, a, b) {
  if (!b || b.length === 0) return a || []
  const map = {}
  for (const d of (a || [])) map[d[key]] = (map[d[key]] || 0) + d.count
  for (const d of b)         map[d[key]] = (map[d[key]] || 0) + d.count
  return Object.entries(map)
    .map(([k, v]) => ({ [key]: k, count: v }))
    .sort((x, y) => y.count - x.count)
}

// ── Small UI components ───────────────────────────────────────────────────────

function StatCard({ label, value, sub }) {
  return (
    <div className="bg-white rounded-xl border border-slate-200 p-4">
      <p className="text-xs font-medium text-slate-500 mb-1">{label}</p>
      <p className="text-2xl font-bold text-slate-900">{value}</p>
      {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
    </div>
  )
}

function ChartCard({ title, sub, children }) {
  return (
    <div className="bg-white rounded-2xl border border-slate-200 p-5">
      <h3 className="text-sm font-semibold text-slate-700 mb-0.5">{title}</h3>
      {sub && <p className="text-xs text-slate-400 mb-4">{sub}</p>}
      {children}
    </div>
  )
}

function FieldPill({ label, value, color }) {
  if (!value) return null
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${color}`}>
      <span className="text-[10px] opacity-70 uppercase tracking-wide">{label}</span>
      <span className="font-semibold">{value}</span>
    </span>
  )
}

function ColSelect({ label, required, value, onChange, columns }) {
  return (
    <div>
      <label className="block text-xs font-medium text-slate-600 mb-1">
        {label}
        {required
          ? <span className="text-rose-500 ml-0.5">*</span>
          : <span className="text-slate-400 ml-1">(optional)</span>}
      </label>
      <select
        value={value}
        onChange={e => onChange(e.target.value)}
        className="w-full border border-slate-200 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-teal-400"
      >
        {!required && <option value="">— None —</option>}
        {required && value === '' && <option value="" disabled>Select…</option>}
        {columns.map(c => <option key={c} value={c}>{c}</option>)}
      </select>
    </div>
  )
}

function CultureChart({ data }) {
  if (!data || data.length === 0)
    return <p className="text-sm text-slate-400 py-10 text-center">No data</p>
  const total = data.reduce((s, d) => s + d.count, 0)
  return (
    <div className="flex items-start gap-5">
      <ResponsiveContainer width="50%" height={210}>
        <PieChart>
          <Pie data={data} dataKey="count" nameKey="culture" cx="50%" cy="50%" outerRadius={78} strokeWidth={1}>
            {data.map((d, i) => <Cell key={i} fill={cultureColor(d.culture, i)} />)}
          </Pie>
          <Tooltip formatter={v => [v.toLocaleString(), 'Names']} contentStyle={{ fontSize: 12 }} />
        </PieChart>
      </ResponsiveContainer>
      <div className="flex-1 space-y-1.5 pt-1 overflow-y-auto max-h-[210px]">
        {data.map((d, i) => (
          <div key={d.culture} className="flex items-center justify-between text-xs gap-2">
            <span className="flex items-center gap-1.5 min-w-0">
              <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ background: cultureColor(d.culture, i) }} />
              <span className="text-slate-700 truncate">{d.culture}</span>
            </span>
            <span className="text-slate-400 shrink-0">
              {d.count.toLocaleString()} ({((d.count / total) * 100).toFixed(1)}%)
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

function CountryChart({ data, hasCountry }) {
  if (!hasCountry)
    return <p className="text-sm text-slate-400 py-10 text-center">No country column mapped</p>
  if (!data || data.length === 0)
    return <p className="text-sm text-slate-400 py-10 text-center">No country data</p>
  const rows = data.slice(0, 15)
  return (
    <ResponsiveContainer width="100%" height={Math.max(180, Math.min(rows.length * 24 + 10, 320))}>
      <BarChart data={rows} layout="vertical" margin={{ top: 0, right: 30, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" horizontal={false} />
        <XAxis type="number" tick={{ fontSize: 10 }} />
        <YAxis type="category" dataKey="country" tick={{ fontSize: 10 }} width={72} />
        <Tooltip formatter={v => [v.toLocaleString(), 'Names']} contentStyle={{ fontSize: 12 }} />
        <Bar dataKey="count" fill="#14b8a6" radius={[0, 3, 3, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── AI Chat panel ─────────────────────────────────────────────────────────────

function AiChat({ analysisId }) {
  const [messages, setMessages] = useState([
    { role: 'assistant', content: 'Hi! Ask me anything about this transaction data — e.g. "What\'s the avg token count for Chinese names?" or "How many entities are there?"' }
  ])
  const [input, setInput]     = useState('')
  const [loading, setLoading] = useState(false)
  const bottomRef             = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const send = useCallback(async () => {
    const text = input.trim()
    if (!text || loading) return
    setInput('')
    const history = messages.filter(m => m.role !== 'assistant' || messages.indexOf(m) > 0)
    const updated = [...messages, { role: 'user', content: text }]
    setMessages(updated)
    setLoading(true)
    try {
      const { data } = await transactiqApi.chat(
        analysisId,
        text,
        updated.slice(-11, -1).map(m => ({ role: m.role, content: m.content }))
      )
      setMessages(prev => [...prev, { role: 'assistant', content: data.reply }])
    } catch (e) {
      setMessages(prev => [...prev, { role: 'assistant', content: '⚠ Error: ' + (e.response?.data?.detail || 'Request failed') }])
    } finally {
      setLoading(false)
    }
  }, [input, loading, messages, analysisId])

  const onKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() }
  }

  return (
    <div className="bg-white rounded-2xl border border-slate-200 flex flex-col" style={{ height: 480 }}>
      <div className="px-5 py-3 border-b border-slate-100">
        <h3 className="text-sm font-semibold text-slate-800">Ask AI about this data</h3>
        <p className="text-xs text-slate-400 mt-0.5">Powered by Claude · answers use the computed analytics</p>
      </div>

      <div className="flex-1 overflow-y-auto px-5 py-4 space-y-3">
        {messages.map((m, i) => (
          <div key={i} className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div className={`max-w-[80%] px-3.5 py-2.5 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap
              ${m.role === 'user'
                ? 'bg-teal-600 text-white rounded-br-sm'
                : 'bg-slate-100 text-slate-800 rounded-bl-sm'}`}
            >
              {m.content}
            </div>
          </div>
        ))}
        {loading && (
          <div className="flex justify-start">
            <div className="bg-slate-100 text-slate-500 px-3.5 py-2.5 rounded-2xl rounded-bl-sm text-sm">
              Thinking…
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <div className="px-4 py-3 border-t border-slate-100 flex gap-2">
        <input
          className="flex-1 border border-slate-200 rounded-xl px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-teal-400"
          placeholder="Ask a question about the data…"
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={onKey}
          disabled={loading}
        />
        <button
          onClick={send}
          disabled={loading || !input.trim()}
          className="bg-teal-600 hover:bg-teal-700 disabled:opacity-50 text-white text-sm font-medium px-4 rounded-xl transition-colors"
        >
          Send
        </button>
      </div>
    </div>
  )
}

// ── Paginated data table ──────────────────────────────────────────────────────

function DataTable({ analysisId, beneNameCol, ordNameCol, beneCountryCol, ordCountryCol, etCol }) {
  const [tableData, setTableData] = useState(null)
  const [page, setPage]           = useState(1)
  const [loadingRows, setLoadingRows] = useState(false)
  const PAGE_SIZE = 25

  const fetchPage = useCallback(async (p) => {
    setLoadingRows(true)
    try {
      const { data } = await transactiqApi.rows(analysisId, p, PAGE_SIZE)
      setTableData(data)
      setPage(p)
    } catch (e) {
      console.error(e)
    } finally {
      setLoadingRows(false)
    }
  }, [analysisId])

  useEffect(() => { fetchPage(1) }, [fetchPage])

  if (!tableData) return <div className="py-20 text-center text-sm text-slate-400">Loading…</div>

  const totalPages = Math.ceil(tableData.total / PAGE_SIZE)

  // Column config: which columns to show and how
  // Show original columns (excluding our _*_culture internal cols), then add inferred culture col(s)
  const displayCols = tableData.columns.filter(c => !c.startsWith('_'))
  const hasBeneCulture = tableData.columns.includes('_bene_culture')
  const hasOrdCulture  = tableData.columns.includes('_ord_culture')

  const colTag = (c) => {
    if (c === beneNameCol)    return 'bg-teal-50 text-teal-700'
    if (c === ordNameCol)     return 'bg-indigo-50 text-indigo-700'
    if (c === beneCountryCol) return 'bg-sky-50 text-sky-600'
    if (c === ordCountryCol)  return 'bg-violet-50 text-violet-600'
    if (c === etCol)          return 'bg-amber-50 text-amber-600'
    if (c === 'Bene Region')  return 'bg-teal-50 text-teal-700 font-semibold'
    if (c === 'Ord Region')   return 'bg-indigo-50 text-indigo-700 font-semibold'
    return null
  }

  return (
    <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
      {/* Table header */}
      <div className="px-5 py-3 border-b border-slate-100 flex items-center justify-between">
        <p className="text-sm font-medium text-slate-700">
          {tableData.total.toLocaleString()} rows
          {loadingRows && <span className="ml-2 text-slate-400">Loading…</span>}
        </p>
        <div className="flex gap-2 text-xs flex-wrap justify-end">
          <span className="px-2 py-0.5 rounded-full bg-teal-50 text-teal-700 font-medium">bene name + region</span>
          {ordNameCol  && <span className="px-2 py-0.5 rounded-full bg-indigo-50 text-indigo-700 font-medium">ord name + region</span>}
          {beneCountryCol && <span className="px-2 py-0.5 rounded-full bg-sky-50 text-sky-600 font-medium">bene country</span>}
          {ordCountryCol  && <span className="px-2 py-0.5 rounded-full bg-violet-50 text-violet-600 font-medium">ord country</span>}
          {etCol          && <span className="px-2 py-0.5 rounded-full bg-amber-50 text-amber-600 font-medium">entity type</span>}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="text-xs w-full">
          <thead className="bg-slate-50 sticky top-0">
            <tr>
              <th className="px-3 py-2.5 text-left text-slate-400 font-medium">#</th>
              {displayCols.map(c => (
                <th key={c} className={`px-3 py-2.5 text-left font-medium whitespace-nowrap ${colTag(c) || 'text-slate-500'}`}>
                  {c}
                </th>
              ))}
              {hasBeneCulture && (
                <th className="px-3 py-2.5 text-left font-semibold whitespace-nowrap bg-teal-50 text-teal-700">
                  Bene Region
                </th>
              )}
              {hasOrdCulture && (
                <th className="px-3 py-2.5 text-left font-semibold whitespace-nowrap bg-indigo-50 text-indigo-700">
                  Ord Region
                </th>
              )}
            </tr>
          </thead>
          <tbody>
            {tableData.rows.map((row, i) => {
              const rowNum = (page - 1) * PAGE_SIZE + i + 1
              const beneCulture = row['_bene_culture'] || ''
              const ordCulture  = row['_ord_culture']  || ''
              return (
                <tr key={i} className="border-t border-slate-100 hover:bg-slate-50">
                  <td className="px-3 py-2 text-slate-300">{rowNum}</td>
                  {displayCols.map(c => (
                    <td key={c} className={`px-3 py-2 whitespace-nowrap max-w-[200px] overflow-hidden text-ellipsis ${colTag(c) || 'text-slate-600'}`}>
                      {String(row[c] ?? '')}
                    </td>
                  ))}
                  {hasBeneCulture && (
                    <td className="px-3 py-2 whitespace-nowrap">
                      {beneCulture ? (
                        <span className="inline-flex items-center gap-1.5">
                          <span className="w-2 h-2 rounded-full shrink-0" style={{ background: cultureColor(beneCulture, 0) }} />
                          <span className="text-slate-600">{beneCulture}</span>
                        </span>
                      ) : <span className="text-slate-300">—</span>}
                    </td>
                  )}
                  {hasOrdCulture && (
                    <td className="px-3 py-2 whitespace-nowrap">
                      {ordCulture ? (
                        <span className="inline-flex items-center gap-1.5">
                          <span className="w-2 h-2 rounded-full shrink-0" style={{ background: cultureColor(ordCulture, 0) }} />
                          <span className="text-slate-600">{ordCulture}</span>
                        </span>
                      ) : <span className="text-slate-300">—</span>}
                    </td>
                  )}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="px-5 py-3 border-t border-slate-100 flex items-center justify-between">
        <p className="text-xs text-slate-400">
          Rows {((page - 1) * PAGE_SIZE + 1).toLocaleString()}–{Math.min(page * PAGE_SIZE, tableData.total).toLocaleString()} of {tableData.total.toLocaleString()}
        </p>
        <div className="flex items-center gap-1">
          <button
            onClick={() => fetchPage(1)}
            disabled={page === 1 || loadingRows}
            className="px-2 py-1 text-xs rounded border border-slate-200 text-slate-500 hover:bg-slate-50 disabled:opacity-40"
          >
            «
          </button>
          <button
            onClick={() => fetchPage(page - 1)}
            disabled={page === 1 || loadingRows}
            className="px-3 py-1 text-xs rounded border border-slate-200 text-slate-500 hover:bg-slate-50 disabled:opacity-40"
          >
            ‹ Prev
          </button>
          <span className="px-3 py-1 text-xs text-slate-600 font-medium">
            {page} / {totalPages}
          </span>
          <button
            onClick={() => fetchPage(page + 1)}
            disabled={page >= totalPages || loadingRows}
            className="px-3 py-1 text-xs rounded border border-slate-200 text-slate-500 hover:bg-slate-50 disabled:opacity-40"
          >
            Next ›
          </button>
          <button
            onClick={() => fetchPage(totalPages)}
            disabled={page >= totalPages || loadingRows}
            className="px-2 py-1 text-xs rounded border border-slate-200 text-slate-500 hover:bg-slate-50 disabled:opacity-40"
          >
            »
          </button>
        </div>
      </div>
    </div>
  )
}

// ── Module-level persistence (survives navigation) ────────────────────────────

const _saved = {
  file: null, preview: null, analysis: null, analysisId: null,
  beneNameCol: '', ordNameCol: '', beneCountryCol: '', ordCountryCol: '', etCol: '',
  tab: 'analytics',
}

// ── Main component ────────────────────────────────────────────────────────────

export default function TransactIQ() {
  const [file, setFile]         = useState(_saved.file)
  const [preview, setPreview]   = useState(_saved.preview)
  const [analysis, setAnalysis] = useState(_saved.analysis)
  const [analysisId, setAnalysisId] = useState(_saved.analysisId)
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState(null)
  const [dragging, setDragging] = useState(false)

  const [beneNameCol,    setBeneNameCol]    = useState(_saved.beneNameCol)
  const [ordNameCol,     setOrdNameCol]     = useState(_saved.ordNameCol)
  const [beneCountryCol, setBeneCountryCol] = useState(_saved.beneCountryCol)
  const [ordCountryCol,  setOrdCountryCol]  = useState(_saved.ordCountryCol)
  const [etCol,          setEtCol]          = useState(_saved.etCol)
  const [showAdjust,     setShowAdjust]     = useState(false)

  const [tab, setTab] = useState(_saved.tab)

  // Sync state → _saved on every relevant change
  useEffect(() => { _saved.file       = file       }, [file])
  useEffect(() => { _saved.preview    = preview    }, [preview])
  useEffect(() => { _saved.analysis   = analysis   }, [analysis])
  useEffect(() => { _saved.analysisId = analysisId }, [analysisId])
  useEffect(() => { _saved.beneNameCol    = beneNameCol    }, [beneNameCol])
  useEffect(() => { _saved.ordNameCol     = ordNameCol     }, [ordNameCol])
  useEffect(() => { _saved.beneCountryCol = beneCountryCol }, [beneCountryCol])
  useEffect(() => { _saved.ordCountryCol  = ordCountryCol  }, [ordCountryCol])
  useEffect(() => { _saved.etCol          = etCol          }, [etCol])
  useEffect(() => { _saved.tab            = tab            }, [tab])

  const handleFile = useCallback(async (f) => {
    if (!f) return
    setFile(f)
    setPreview(null)
    setAnalysis(null)
    setAnalysisId(null)
    setError(null)
    setShowAdjust(false)
    setLoading(true)
    try {
      const { data } = await transactiqApi.preview(f)
      setPreview(data)
      const det = autoDetectFields(data.columns, data.suggested_bene_col, data.suggested_ord_col)
      setBeneNameCol(det.bene)
      setOrdNameCol(det.ord)
      setBeneCountryCol(det.beneCty)
      setOrdCountryCol(det.ordCty)
      setEtCol(det.et)
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to parse file')
    } finally {
      setLoading(false)
    }
  }, [])

  const handleDrop = useCallback((e) => {
    e.preventDefault()
    setDragging(false)
    const f = e.dataTransfer.files[0]
    if (f) handleFile(f)
  }, [handleFile])

  const handleAnalyze = useCallback(async () => {
    if (!file || !beneNameCol) return
    setLoading(true)
    setError(null)
    try {
      const { data } = await transactiqApi.analyze(
        file, beneNameCol, ordNameCol || null,
        beneCountryCol || null, ordCountryCol || null, etCol || null,
      )
      const { analysis_id, ...rest } = data
      setAnalysis(rest)
      setAnalysisId(analysis_id)
      setTab('analytics')
    } catch (e) {
      setError(e.response?.data?.detail || 'Analysis failed')
    } finally {
      setLoading(false)
    }
  }, [file, beneNameCol, ordNameCol, beneCountryCol, ordCountryCol, etCol])

  const reset = useCallback(() => {
    _saved.file = null; _saved.preview = null; _saved.analysis = null
    _saved.analysisId = null; _saved.beneNameCol = ''; _saved.ordNameCol = ''
    _saved.beneCountryCol = ''; _saved.ordCountryCol = ''; _saved.etCol = ''
    _saved.tab = 'analytics'
    setFile(null); setPreview(null); setAnalysis(null); setAnalysisId(null); setError(null)
    setBeneNameCol(''); setOrdNameCol(''); setBeneCountryCol('')
    setOrdCountryCol(''); setEtCol(''); setShowAdjust(false); setTab('analytics')
  }, [])

  // Merged analytics
  const merged = analysis ? (() => {
    const b = analysis.bene
    const o = analysis.ord
    const totalNames = b.total + (o?.total ?? 0)
    const avgTokens  = o
      ? ((b.stats.avg_tokens * b.total + o.stats.avg_tokens * o.total) / totalNames).toFixed(2)
      : String(b.stats.avg_tokens)
    const avgChars   = o
      ? ((b.stats.avg_chars * b.total + o.stats.avg_chars * o.total) / totalNames).toFixed(1)
      : String(b.stats.avg_chars)
    return {
      totalNames,
      avgTokens,
      avgChars,
      maxTokens: o ? Math.max(b.stats.max_tokens, o.stats.max_tokens) : b.stats.max_tokens,
      maxChars:  o ? Math.max(b.stats.max_chars,  o.stats.max_chars)  : b.stats.max_chars,
      cultureDist: mergeDist('culture', b.culture_dist, o?.culture_dist),
      countryDist: mergeDist('country', b.country_dist, o?.country_dist),
      tokenDist:   mergeDist('tokens',  b.token_dist,   o?.token_dist),
      lengthDist:  mergeDist('bucket',  b.length_dist,  o?.length_dist),
      hasCountry:  b.has_country || (o?.has_country ?? false),
    }
  })() : null

  return (
    <div className="max-w-6xl mx-auto py-8 space-y-6">

      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Transaction Analytics</h1>
        <p className="text-sm text-slate-500 mt-1">
          Upload a CSV or Excel file of transactions to explore name culture/region distributions,
          countries, entity types, and name characteristics.
        </p>
      </div>

      {/* ── Upload ── */}
      {!preview && !analysis && (
        <div
          className={`border-2 border-dashed rounded-2xl p-14 text-center transition-colors cursor-pointer select-none
            ${dragging ? 'border-teal-400 bg-teal-50' : 'border-slate-300 hover:border-teal-300 bg-white'}`}
          onDragOver={e => { e.preventDefault(); setDragging(true) }}
          onDragLeave={() => setDragging(false)}
          onDrop={handleDrop}
          onClick={() => document.getElementById('tiq-file-input').click()}
        >
          <input id="tiq-file-input" type="file" className="hidden" accept=".csv,.xlsx,.xls"
            onChange={e => handleFile(e.target.files[0])} />
          {loading ? (
            <p className="text-slate-500 text-sm">Parsing file…</p>
          ) : (
            <>
              <div className="text-5xl mb-4">📊</div>
              <p className="text-slate-700 font-medium">Drop a CSV or Excel file here</p>
              <p className="text-slate-400 text-sm mt-1">or click to browse · .csv, .xlsx, .xls</p>
            </>
          )}
        </div>
      )}

      {error && (
        <div className="bg-rose-50 border border-rose-200 rounded-xl px-4 py-3 text-sm text-rose-700">{error}</div>
      )}

      {/* ── Column mapping ── */}
      {preview && !analysis && (
        <div className="bg-white rounded-2xl border border-slate-200 p-6 space-y-5">
          <div className="flex items-start justify-between">
            <div>
              <h2 className="text-base font-semibold text-slate-800">{file.name}</h2>
              <p className="text-xs text-slate-500 mt-0.5">
                {preview.row_count.toLocaleString()} rows · {preview.columns.length} columns
              </p>
            </div>
            <button onClick={reset} className="text-xs text-slate-400 hover:text-slate-600 transition-colors">✕ Change file</button>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <p className="text-xs font-semibold text-slate-500 uppercase tracking-widest">Auto-detected fields</p>
              <button onClick={() => setShowAdjust(v => !v)} className="text-xs text-teal-600 hover:text-teal-800 transition-colors">
                {showAdjust ? 'Hide' : 'Adjust mappings'}
              </button>
            </div>
            <div className="flex flex-wrap gap-2">
              <FieldPill label="Bene name"    value={beneNameCol}    color="bg-teal-50 text-teal-700" />
              <FieldPill label="Ord name"     value={ordNameCol}     color="bg-indigo-50 text-indigo-700" />
              <FieldPill label="Bene country" value={beneCountryCol} color="bg-sky-50 text-sky-600" />
              <FieldPill label="Ord country"  value={ordCountryCol}  color="bg-violet-50 text-violet-600" />
              <FieldPill label="Entity type"  value={etCol}          color="bg-amber-50 text-amber-600" />
              {!ordNameCol && !beneCountryCol && !ordCountryCol && !etCol && (
                <span className="text-xs text-slate-400 italic">Only beneficiary name detected — adjust if needed</span>
              )}
            </div>
          </div>

          {showAdjust && (
            <div className="border-t border-slate-100 pt-4 space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <ColSelect label="Beneficiary Name" required value={beneNameCol} onChange={setBeneNameCol} columns={preview.columns} />
                <ColSelect label="Ordering / By-Order Name"  value={ordNameCol}     onChange={setOrdNameCol}     columns={preview.columns} />
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <ColSelect label="Beneficiary Country" value={beneCountryCol} onChange={setBeneCountryCol} columns={preview.columns} />
                <ColSelect label="Ordering Country"    value={ordCountryCol}  onChange={setOrdCountryCol}  columns={preview.columns} />
                <ColSelect label="Entity Type"         value={etCol}          onChange={setEtCol}          columns={preview.columns} />
              </div>
            </div>
          )}

          <div>
            <p className="text-xs text-slate-400 mb-2">First {preview.sample.length} rows:</p>
            <div className="overflow-x-auto rounded-lg border border-slate-100">
              <table className="text-xs w-full">
                <thead className="bg-slate-50">
                  <tr>
                    {preview.columns.map(c => {
                      const cls = c === beneNameCol    ? 'text-teal-700 bg-teal-50'
                               : c === ordNameCol     ? 'text-indigo-700 bg-indigo-50'
                               : c === beneCountryCol ? 'text-sky-600 bg-sky-50'
                               : c === ordCountryCol  ? 'text-violet-600 bg-violet-50'
                               : c === etCol          ? 'text-amber-600 bg-amber-50'
                               : 'text-slate-500'
                      return <th key={c} className={`px-3 py-2 text-left font-medium whitespace-nowrap ${cls}`}>{c}</th>
                    })}
                  </tr>
                </thead>
                <tbody>
                  {preview.sample.map((row, i) => (
                    <tr key={i} className="border-t border-slate-100 hover:bg-slate-50">
                      {preview.columns.map(c => {
                        const cls = c === beneNameCol    ? 'text-teal-700 bg-teal-50'
                                 : c === ordNameCol     ? 'text-indigo-700 bg-indigo-50'
                                 : c === beneCountryCol ? 'text-sky-600 bg-sky-50'
                                 : c === ordCountryCol  ? 'text-violet-600 bg-violet-50'
                                 : c === etCol          ? 'text-amber-600 bg-amber-50'
                                 : 'text-slate-600'
                        return (
                          <td key={c} className={`px-3 py-1.5 whitespace-nowrap max-w-[200px] overflow-hidden text-ellipsis ${cls}`}>
                            {String(row[c] ?? '')}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <button
            onClick={handleAnalyze}
            disabled={loading || !beneNameCol}
            className="w-full bg-teal-600 hover:bg-teal-700 disabled:opacity-50 text-white font-semibold py-2.5 rounded-xl transition-colors text-sm"
          >
            {loading ? 'Analyzing…' : `Analyze ${preview.row_count.toLocaleString()} rows →`}
          </button>
        </div>
      )}

      {/* ── Results ── */}
      {analysis && merged && analysisId && (
        <>
          <div className="flex items-center justify-between">
            <p className="text-sm text-slate-500">
              Analyzed{' '}
              <span className="font-semibold text-slate-800">{merged.totalNames.toLocaleString()}</span>{' '}
              names from <span className="font-medium">{file?.name}</span>
            </p>
            <button onClick={reset}
              className="text-xs font-medium text-slate-500 hover:text-rose-600 border border-slate-200 hover:border-rose-200 px-3 py-1.5 rounded-lg transition-colors">
              Reset
            </button>
          </div>

          {/* Tabs */}
          <div className="flex gap-1 bg-slate-100 p-1 rounded-xl w-fit">
            {[['analytics', 'Analytics'], ['table', 'Data Table'], ['chat', 'Ask AI']].map(([key, label]) => (
              <button key={key} onClick={() => setTab(key)}
                className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors
                  ${tab === key ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}>
                {label}
              </button>
            ))}
          </div>

          {/* Analytics tab */}
          {tab === 'analytics' && (
            <div className="space-y-6">
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                <StatCard label="Total names"  value={merged.totalNames.toLocaleString()} />
                <StatCard label="Avg tokens"   value={merged.avgTokens} sub="words per name" />
                <StatCard label="Avg length"   value={`${merged.avgChars}`} sub="chars per name" />
                <StatCard label="Longest name" value={`${merged.maxChars} chars`} sub={`${merged.maxTokens} tokens`} />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <ChartCard title="Name Region / Culture" sub="Inferred from script and phonetic patterns">
                  <CultureChart data={merged.cultureDist} />
                </ChartCard>
                <ChartCard title="Country Distribution">
                  <CountryChart data={merged.countryDist} hasCountry={merged.hasCountry} />
                </ChartCard>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <ChartCard title="Token Count Distribution" sub="Words per name">
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={merged.tokenDist} margin={{ top: 4, right: 10, left: -10, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                      <XAxis dataKey="tokens" tick={{ fontSize: 12 }} />
                      <YAxis tick={{ fontSize: 11 }} />
                      <Tooltip formatter={v => [v.toLocaleString(), 'Names']} contentStyle={{ fontSize: 12 }} />
                      <Bar dataKey="count" fill={TOKEN_COLOR} radius={[3, 3, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </ChartCard>
                <ChartCard title="Name Length Distribution" sub="Character count (bucketed)">
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={merged.lengthDist} margin={{ top: 4, right: 10, left: -10, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                      <XAxis dataKey="bucket" tick={{ fontSize: 11 }} />
                      <YAxis tick={{ fontSize: 11 }} />
                      <Tooltip formatter={v => [v.toLocaleString(), 'Names']} contentStyle={{ fontSize: 12 }} />
                      <Bar dataKey="count" fill={LENGTH_COLOR} radius={[3, 3, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </ChartCard>
              </div>

              <ChartCard title="Entity Type Distribution">
                {analysis.entity_type_inferred && (
                  <p className="text-xs text-amber-600 mb-3">Inferred from name patterns — no entity type column was mapped.</p>
                )}
                {analysis.entity_type_dist.length === 0 ? (
                  <p className="text-sm text-slate-400 py-8 text-center">No data</p>
                ) : (
                  <div className="flex items-center gap-6">
                    <ResponsiveContainer width="40%" height={180}>
                      <PieChart>
                        <Pie data={analysis.entity_type_dist} dataKey="count" nameKey="type" cx="50%" cy="50%" outerRadius={68} strokeWidth={1}>
                          {analysis.entity_type_dist.map((_, i) => <Cell key={i} fill={ENTITY_COLORS[i % ENTITY_COLORS.length]} />)}
                        </Pie>
                        <Tooltip formatter={v => [v.toLocaleString(), 'Names']} contentStyle={{ fontSize: 12 }} />
                      </PieChart>
                    </ResponsiveContainer>
                    <div className="flex-1 space-y-2.5">
                      {analysis.entity_type_dist.map((d, i) => (
                        <div key={d.type} className="flex items-center justify-between text-sm">
                          <span className="flex items-center gap-2">
                            <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ background: ENTITY_COLORS[i % ENTITY_COLORS.length] }} />
                            <span className="text-slate-700 capitalize">{d.type}</span>
                          </span>
                          <span className="text-slate-400 text-xs ml-2">
                            {d.count.toLocaleString()}
                            <span className="ml-1">({((d.count / merged.totalNames) * 100).toFixed(1)}%)</span>
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </ChartCard>
            </div>
          )}

          {/* Data Table tab */}
          {tab === 'table' && (
            <DataTable
              analysisId={analysisId}
              beneNameCol={beneNameCol}
              ordNameCol={ordNameCol}
              beneCountryCol={beneCountryCol}
              ordCountryCol={ordCountryCol}
              etCol={etCol}
            />
          )}

          {/* AI Chat tab */}
          {tab === 'chat' && <AiChat analysisId={analysisId} />}
        </>
      )}
    </div>
  )
}
