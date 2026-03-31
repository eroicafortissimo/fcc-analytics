import { useState, useEffect, useCallback, useRef } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'
import { resultsApi } from '../api/resultsApi'

// ── Colour helpers ─────────────────────────────────────────────────────────────

const WATCHLIST_COLOURS = {
  OFAC_SDN: '#EF4444', OFAC_NON_SDN: '#F97316', EU: '#3B82F6',
  HMT: '#8B5CF6', BIS: '#F59E0B', JAPAN: '#EC4899',
}

function pct(v) {
  if (v == null) return '—'
  return `${(v * 100).toFixed(1)}%`
}

// ── Small components ────────────────────────────────────────────────────────────

function MetricCard({ label, value, sub, colour = 'slate', size = 'normal' }) {
  const colours = {
    green: 'border-emerald-200 bg-emerald-50 text-emerald-700',
    red: 'border-rose-200 bg-rose-50 text-rose-700',
    blue: 'border-blue-200 bg-blue-50 text-blue-700',
    amber: 'border-amber-200 bg-amber-50 text-amber-700',
    slate: 'border-slate-200 bg-white text-slate-700',
  }
  return (
    <div className={`rounded-xl border p-4 ${colours[colour]}`}>
      <p className="text-xs font-medium opacity-70 mb-1">{label}</p>
      <p className={`font-bold ${size === 'large' ? 'text-3xl' : 'text-2xl'}`}>{value}</p>
      {sub && <p className="text-xs opacity-60 mt-0.5">{sub}</p>}
    </div>
  )
}

function ConfusionMatrix({ tp, fp, tn, fn }) {
  const total = tp + fp + tn + fn || 1
  return (
    <div className="bg-white border border-slate-200 rounded-xl p-5">
      <h3 className="text-sm font-semibold text-slate-700 mb-4">Confusion Matrix</h3>
      <div className="grid grid-cols-3 gap-1 text-center text-sm">
        <div />
        <div className="text-xs font-bold text-slate-500 py-1.5 border-b border-slate-200">Actual HIT</div>
        <div className="text-xs font-bold text-slate-500 py-1.5 border-b border-slate-200">Actual MISS</div>
        <div className="text-xs font-bold text-slate-500 flex items-center justify-end pr-2">Expected HIT</div>
        <div className="bg-emerald-50 border-2 border-emerald-300 rounded-lg p-3">
          <div className="text-2xl font-bold text-emerald-700">{tp.toLocaleString()}</div>
          <div className="text-xs font-semibold text-emerald-600 mt-0.5">TP</div>
          <div className="text-[11px] text-emerald-500">{pct(tp / total)}</div>
        </div>
        <div className="bg-rose-50 border-2 border-rose-300 rounded-lg p-3">
          <div className="text-2xl font-bold text-rose-700">{fn.toLocaleString()}</div>
          <div className="text-xs font-semibold text-rose-600 mt-0.5">FN</div>
          <div className="text-[11px] text-rose-500">{pct(fn / total)}</div>
        </div>
        <div className="text-xs font-bold text-slate-500 flex items-center justify-end pr-2">Expected MISS</div>
        <div className="bg-amber-50 border-2 border-amber-300 rounded-lg p-3">
          <div className="text-2xl font-bold text-amber-700">{fp.toLocaleString()}</div>
          <div className="text-xs font-semibold text-amber-600 mt-0.5">FP</div>
          <div className="text-[11px] text-amber-500">{pct(fp / total)}</div>
        </div>
        <div className="bg-blue-50 border-2 border-blue-300 rounded-lg p-3">
          <div className="text-2xl font-bold text-blue-700">{tn.toLocaleString()}</div>
          <div className="text-xs font-semibold text-blue-600 mt-0.5">TN</div>
          <div className="text-[11px] text-blue-500">{pct(tn / total)}</div>
        </div>
      </div>
      <div className="mt-3 flex flex-wrap gap-2 text-[11px] text-slate-500 justify-center">
        {[
          ['bg-emerald-200', 'TP = Correct hit'],
          ['bg-rose-200', 'FN = Missed hit'],
          ['bg-amber-200', 'FP = False alarm'],
          ['bg-blue-200', 'TN = Correct clear'],
        ].map(([bg, label]) => (
          <span key={label} className="flex items-center gap-1">
            <span className={`w-2.5 h-2.5 rounded-sm ${bg} inline-block`} />{label}
          </span>
        ))}
      </div>
    </div>
  )
}

// ── Upload zone ────────────────────────────────────────────────────────────────

function UploadZone({ onUpload }) {
  const [dragging, setDragging] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState(null)
  const inputRef = useRef(null)

  const handleFile = async (file) => {
    if (!file) return
    const ext = file.name.split('.').pop().toLowerCase()
    if (!['csv', 'xlsx', 'xls'].includes(ext)) {
      setError('Please upload a CSV or Excel (.xlsx) file.')
      return
    }
    setUploading(true)
    setError(null)
    try {
      const r = await resultsApi.upload(file)
      onUpload(r.data)
    } catch (e) {
      setError(e?.response?.data?.detail || 'Upload failed. Check the file format.')
    } finally {
      setUploading(false)
    }
  }

  return (
    <div className="bg-white border border-slate-200 rounded-xl p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-base font-semibold text-slate-700">Upload Screening Results</h2>
          <p className="text-sm text-slate-500 mt-0.5">CSV or Excel file with test_case_id + actual_result columns</p>
        </div>
        <a href={resultsApi.templateUrl()}
          className="text-xs text-blue-600 hover:underline flex items-center gap-1">
          ↓ Download template
        </a>
      </div>

      <div
        className={`border-2 border-dashed rounded-xl p-10 flex flex-col items-center gap-3 transition-colors cursor-pointer
          ${dragging ? 'border-blue-400 bg-blue-50' : 'border-slate-200 hover:border-slate-300 hover:bg-slate-50'}`}
        onDragOver={e => { e.preventDefault(); setDragging(true) }}
        onDragLeave={() => setDragging(false)}
        onDrop={e => { e.preventDefault(); setDragging(false); handleFile(e.dataTransfer.files[0]) }}
        onClick={() => inputRef.current?.click()}
      >
        <input ref={inputRef} type="file" accept=".csv,.xlsx,.xls" className="hidden"
          onChange={e => handleFile(e.target.files[0])} />
        {uploading ? (
          <>
            <svg className="animate-spin h-8 w-8 text-blue-500" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
            </svg>
            <span className="text-sm text-slate-500">Uploading and matching results…</span>
          </>
        ) : (
          <>
            <div className="text-slate-300 text-4xl font-light">↑</div>
            <p className="text-sm font-medium text-slate-600">Drop CSV or Excel file here</p>
            <p className="text-xs text-slate-400">or click to browse</p>
          </>
        )}
      </div>

      {error && (
        <p className="mt-3 text-sm text-rose-600 bg-rose-50 border border-rose-200 rounded-lg px-3 py-2">{error}</p>
      )}

      <div className="mt-4 border border-slate-100 rounded-lg p-3 bg-slate-50">
        <p className="text-xs font-semibold text-slate-500 mb-1.5">Required columns:</p>
        <div className="flex flex-wrap gap-1.5">
          {['test_case_id', 'actual_result (HIT or MISS)'].map(c => (
            <span key={c} className="font-mono text-xs bg-white border border-slate-200 rounded px-2 py-0.5 text-slate-600">{c}</span>
          ))}
        </div>
        <p className="text-xs font-semibold text-slate-500 mt-2 mb-1.5">Optional:</p>
        <div className="flex flex-wrap gap-1.5">
          {['match_score', 'matched_list_entry', 'alert_details'].map(c => (
            <span key={c} className="font-mono text-xs bg-white border border-slate-200 rounded px-2 py-0.5 text-slate-400">{c}</span>
          ))}
        </div>
      </div>
    </div>
  )
}

// ── Breakdown chart ────────────────────────────────────────────────────────────

const BREAKDOWN_DIMS = [
  { id: 'entity_type', label: 'Entity Type' },
  { id: 'watchlist', label: 'Watchlist' },
  { id: 'num_tokens', label: 'Token Count' },
  { id: 'name_length_bucket', label: 'Name Length' },
  { id: 'culture_nationality', label: 'Nationality' },
  { id: 'test_case_type', label: 'Test Case Type' },
]

function BreakdownChart({ data }) {
  if (!data || data.length === 0) return (
    <div className="text-center text-slate-400 py-8 text-sm">No breakdown data available</div>
  )
  const chartData = data
    .filter(d => d.tp + d.fn > 0)
    .slice(0, 20)
    .map(d => ({
      name: String(d.dimension || 'Unknown').length > 28
        ? String(d.dimension).slice(0, 26) + '…'
        : String(d.dimension || 'Unknown'),
      fullName: d.dimension,
      'TP (caught)': d.tp,
      'FN (missed)': d.fn,
      detection_rate: d.detection_rate,
    }))
    .sort((a, b) => (b['TP (caught)'] + b['FN (missed)']) - (a['TP (caught)'] + a['FN (missed)']))

  const CustomTooltip = ({ active, payload }) => {
    if (!active || !payload?.length) return null
    const d = payload[0]?.payload
    return (
      <div className="bg-white border border-slate-200 rounded-lg shadow-lg px-3 py-2 text-xs">
        <p className="font-semibold text-slate-700 mb-1">{d?.fullName}</p>
        <p className="text-emerald-600">TP (caught): {d?.['TP (caught)']}</p>
        <p className="text-rose-600">FN (missed): {d?.['FN (missed)']}</p>
        <p className="text-slate-600 font-medium mt-0.5">Detection rate: {d?.detection_rate != null ? pct(d.detection_rate) : '—'}</p>
      </div>
    )
  }

  const height = Math.max(200, chartData.length * 28 + 60)
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={chartData} layout="vertical" margin={{ left: 8, right: 40, top: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#E2E8F0" />
        <XAxis type="number" tick={{ fontSize: 11 }} />
        <YAxis type="category" dataKey="name" width={180} tick={{ fontSize: 11 }} />
        <Tooltip content={<CustomTooltip />} />
        <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
        <Bar dataKey="TP (caught)" stackId="a" fill="#10B981" />
        <Bar dataKey="FN (missed)" stackId="a" fill="#FCA5A5" radius={[0, 3, 3, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── Outcome badge ──────────────────────────────────────────────────────────────

function OutcomeBadge({ expected, actual }) {
  if (expected === 'HIT' && actual === 'HIT') return <span className="text-xs font-bold px-2 py-0.5 rounded bg-emerald-100 text-emerald-700">TP</span>
  if (expected === 'MISS' && actual === 'MISS') return <span className="text-xs font-bold px-2 py-0.5 rounded bg-blue-100 text-blue-700">TN</span>
  if (expected === 'HIT' && actual === 'MISS') return <span className="text-xs font-bold px-2 py-0.5 rounded bg-rose-100 text-rose-700">FN</span>
  return <span className="text-xs font-bold px-2 py-0.5 rounded bg-amber-100 text-amber-700">FP</span>
}

// ── Miss Analysis Panel ────────────────────────────────────────────────────────

const CATEGORY_COLOURS = {
  'Exact match noise': '#F97316',
  'Transliteration variant': '#8B5CF6',
  'Abbreviation / initials': '#3B82F6',
  'Token omission': '#EF4444',
  'Token insertion': '#10B981',
  'Token reorder / permutation': '#F59E0B',
  'Legal form variant': '#6366F1',
  'Special characters / spacing': '#EC4899',
  'Script / encoding issue': '#0EA5E9',
  'Deliberate obfuscation': '#DC2626',
  'Threshold gap': '#84CC16',
  'Other': '#94A3B8',
}

const CONF_COLOURS = { high: 'bg-emerald-100 text-emerald-700', medium: 'bg-amber-100 text-amber-700', low: 'bg-slate-100 text-slate-500' }

function MissAnalysisPanel({ fnCount }) {
  const [status, setStatus] = useState('idle') // idle | running | done | error
  const [result, setResult] = useState(null)
  const [savedLoading, setSavedLoading] = useState(false)
  const [error, setError] = useState(null)

  const loadSaved = () => {
    setSavedLoading(true)
    resultsApi.getMissAnalyses()
      .then(r => {
        if (r.data.length > 0) {
          // Rebuild a summary-like structure from saved analyses
          const cats = {}
          r.data.forEach(a => { cats[a.miss_category] = (cats[a.miss_category] || 0) + 1 })
          setResult({
            total_fns: fnCount,
            analyzed: r.data.length,
            categories: Object.fromEntries(Object.entries(cats).sort((a, b) => b[1] - a[1])),
            top_recommendations: [...new Set(r.data.map(a => a.recommendation).filter(Boolean))].slice(0, 10),
            cases: r.data,
          })
          setStatus('done')
        }
      })
      .catch(() => {})
      .finally(() => setSavedLoading(false))
  }

  const runAnalysis = async () => {
    setStatus('running')
    setError(null)
    try {
      const r = await resultsApi.analyzeMisses()
      setResult(r.data)
      setStatus('done')
    } catch (e) {
      setError(e?.response?.data?.detail || 'Analysis failed. Check that the API key is configured.')
      setStatus('error')
    }
  }

  const categoryEntries = result ? Object.entries(result.categories || {}) : []
  const maxCount = categoryEntries.length ? Math.max(...categoryEntries.map(([, v]) => v)) : 1

  return (
    <div className="bg-white border border-slate-200 rounded-xl overflow-hidden mb-5">
      <div className="p-4 border-b border-slate-100 flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold text-slate-800">Miss Analysis <span className="ml-1 text-rose-600">(AI-powered)</span></h3>
          <p className="text-xs text-slate-400 mt-0.5">
            Claude Haiku analyzes each false negative and explains why the screening system missed it
          </p>
        </div>
        <div className="flex gap-2">
          {status === 'idle' && (
            <button onClick={loadSaved} disabled={savedLoading}
              className="px-3 py-1.5 text-xs border border-slate-200 rounded-lg text-slate-600 hover:bg-slate-50 disabled:opacity-50">
              {savedLoading ? 'Loading…' : 'Load saved'}
            </button>
          )}
          <button onClick={runAnalysis} disabled={status === 'running' || fnCount === 0}
            className={`px-4 py-1.5 text-xs rounded-lg font-medium disabled:opacity-50 transition-colors
              ${status === 'running'
                ? 'bg-slate-100 text-slate-500 cursor-not-allowed'
                : 'bg-rose-600 text-white hover:bg-rose-700'}`}>
            {status === 'running' ? '⏳ Analyzing…' : `▶ Analyze ${fnCount} FN${fnCount !== 1 ? 's' : ''}`}
          </button>
        </div>
      </div>

      {status === 'idle' && fnCount === 0 && (
        <div className="p-6 text-center text-sm text-slate-400">No false negatives to analyze</div>
      )}

      {error && (
        <div className="m-4 p-3 rounded-lg border border-rose-200 bg-rose-50 text-sm text-rose-700">{error}</div>
      )}

      {status === 'running' && (
        <div className="p-8 flex flex-col items-center gap-3 text-sm text-slate-500">
          <svg className="animate-spin h-7 w-7 text-rose-400" viewBox="0 0 24 24" fill="none">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
          </svg>
          <span>Calling Claude Haiku on {fnCount} false negative{fnCount !== 1 ? 's' : ''}…</span>
          <span className="text-xs text-slate-400">This may take 15–60 seconds depending on volume</span>
        </div>
      )}

      {status === 'done' && result && (
        <div className="p-4 space-y-5">
          {/* Summary row */}
          <div className="flex flex-wrap gap-3 text-sm">
            <div className="px-3 py-2 rounded-lg bg-slate-50 border border-slate-200">
              <span className="text-slate-500 text-xs">FNs analyzed</span>
              <div className="font-bold text-slate-800">{result.analyzed} / {result.total_fns}</div>
            </div>
            <div className="px-3 py-2 rounded-lg bg-slate-50 border border-slate-200">
              <span className="text-slate-500 text-xs">Miss categories</span>
              <div className="font-bold text-slate-800">{categoryEntries.length}</div>
            </div>
          </div>

          {/* Category distribution */}
          {categoryEntries.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-slate-500 mb-2">Miss categories</p>
              <div className="space-y-1.5">
                {categoryEntries.map(([cat, count]) => (
                  <div key={cat} className="flex items-center gap-2 text-xs">
                    <div className="w-36 text-slate-600 flex-shrink-0 truncate" title={cat}>{cat}</div>
                    <div className="flex-1 h-3 bg-slate-100 rounded-full overflow-hidden">
                      <div
                        className="h-full rounded-full transition-all"
                        style={{
                          width: `${Math.round(count / maxCount * 100)}%`,
                          backgroundColor: CATEGORY_COLOURS[cat] || '#94A3B8',
                        }}
                      />
                    </div>
                    <div className="w-6 text-right font-mono font-medium text-slate-700">{count}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Top recommendations */}
          {result.top_recommendations?.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-slate-500 mb-2">Top recommendations</p>
              <ol className="space-y-1.5 list-decimal list-inside">
                {result.top_recommendations.map((r, i) => (
                  <li key={i} className="text-xs text-slate-700 leading-relaxed">{r}</li>
                ))}
              </ol>
            </div>
          )}

          {/* Case list */}
          {result.cases?.length > 0 && (
            <details>
              <summary className="cursor-pointer text-xs text-blue-600 hover:underline select-none">
                Show per-case analysis ({result.cases.length} cases)
              </summary>
              <div className="mt-3 space-y-2 max-h-96 overflow-y-auto pr-1">
                {result.cases.map(c => (
                  <div key={c.test_case_id} className="rounded-lg border border-slate-100 p-3 bg-slate-50 text-xs">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="font-mono text-[11px] text-slate-400">{c.test_case_id}</span>
                      <span className="px-1.5 py-0.5 rounded text-[10px] font-medium"
                        style={{ backgroundColor: (CATEGORY_COLOURS[c.miss_category] || '#94A3B8') + '22', color: CATEGORY_COLOURS[c.miss_category] || '#64748B' }}>
                        {c.miss_category}
                      </span>
                      {c.confidence && (
                        <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${CONF_COLOURS[c.confidence] || ''}`}>
                          {c.confidence}
                        </span>
                      )}
                    </div>
                    <div className="flex items-baseline gap-1.5 text-slate-600 mb-1">
                      <span className="font-medium text-slate-800">{c.test_name}</span>
                      <span className="text-slate-400">→</span>
                      <span>{c.original_name}</span>
                    </div>
                    <p className="text-slate-600 mb-0.5">{c.explanation}</p>
                    {c.recommendation && (
                      <p className="text-blue-700 italic">{c.recommendation}</p>
                    )}
                  </div>
                ))}
              </div>
            </details>
          )}
        </div>
      )}
    </div>
  )
}


// ── Main page ──────────────────────────────────────────────────────────────────

export default function ResultsInterpreter() {
  const [summary, setSummary] = useState(null)
  const [summaryLoading, setSummaryLoading] = useState(true)
  const [activeDim, setActiveDim] = useState('entity_type')
  const [breakdownData, setBreakdownData] = useState({})
  const [breakdownLoading, setBreakdownLoading] = useState(false)
  const [cases, setCases] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const PAGE_SIZE = 100
  const [outcomeFilter, setOutcomeFilter] = useState('')
  const [entityFilter, setEntityFilter] = useState('')
  const [search, setSearch] = useState('')
  const [searchDraft, setSearchDraft] = useState('')
  const searchTimer = useRef(null)
  const [uploadResult, setUploadResult] = useState(null)
  const [exportMenuOpen, setExportMenuOpen] = useState(false)
  const exportRef = useRef(null)

  const loadSummary = useCallback(() => {
    setSummaryLoading(true)
    resultsApi.summary().then(r => setSummary(r.data)).catch(() => {}).finally(() => setSummaryLoading(false))
  }, [])

  useEffect(() => { loadSummary() }, [loadSummary])

  const loadBreakdown = useCallback((dim) => {
    if (breakdownData[dim]) return
    setBreakdownLoading(true)
    resultsApi.breakdown(dim)
      .then(r => setBreakdownData(prev => ({ ...prev, [dim]: r.data })))
      .catch(() => {})
      .finally(() => setBreakdownLoading(false))
  }, [breakdownData])

  useEffect(() => {
    if (summary?.total > 0) loadBreakdown(activeDim)
  }, [activeDim, summary, loadBreakdown])

  const loadTable = useCallback((p = 1) => {
    resultsApi.cases({ page: p, pageSize: PAGE_SIZE, outcome: outcomeFilter || undefined, entityType: entityFilter || undefined, search: search || undefined })
      .then(r => { setCases(r.data.items); setTotal(r.data.total); setPage(p) })
      .catch(() => {})
  }, [outcomeFilter, entityFilter, search])

  useEffect(() => { loadTable(1) }, [outcomeFilter, entityFilter, search])

  const handleUpload = (data) => {
    setUploadResult(data)
    loadSummary()
    setBreakdownData({})
    loadTable(1)
  }

  const handleClear = async () => {
    if (!window.confirm('Delete all uploaded screening results?')) return
    await resultsApi.clear()
    setSummary(null)
    setBreakdownData({})
    setCases([])
    setTotal(0)
    setUploadResult(null)
    loadSummary()
  }

  const handleSearch = (val) => {
    setSearchDraft(val)
    clearTimeout(searchTimer.current)
    searchTimer.current = setTimeout(() => setSearch(val), 300)
  }

  useEffect(() => {
    const handler = (e) => { if (exportRef.current && !exportRef.current.contains(e.target)) setExportMenuOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const hasResults = summary && summary.total > 0

  return (
    <div className="max-w-[1600px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Results Interpreter</h1>
          <p className="text-sm text-slate-500 mt-0.5">Upload screening results to compute detection rates and analyze gaps</p>
        </div>
        {hasResults && (
          <div className="flex gap-2">
            <div className="relative" ref={exportRef}>
              <button onClick={() => setExportMenuOpen(o => !o)}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 bg-white text-sm text-slate-600 hover:bg-slate-50">
                ↓ Export <span className="text-slate-400 text-xs">▾</span>
              </button>
              {exportMenuOpen && (
                <div className="absolute right-0 mt-1 w-52 bg-white border border-slate-200 rounded-xl shadow-lg z-20">
                  <a href={resultsApi.exportUrl()} onClick={() => setExportMenuOpen(false)}
                    className="flex items-center gap-2 px-4 py-2.5 hover:bg-slate-50 text-sm text-slate-700">
                    Excel workbook
                  </a>
                </div>
              )}
            </div>
            <button onClick={handleClear}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-rose-200 bg-white text-sm text-rose-600 hover:bg-rose-50">
              ✕ Clear Results
            </button>
          </div>
        )}
      </div>

      {/* Upload result banner */}
      {uploadResult && (
        <div className={`mb-4 p-3 rounded-lg border text-sm flex justify-between items-start gap-3
          ${uploadResult.matched === 0
            ? 'border-amber-200 bg-amber-50 text-amber-800'
            : 'border-emerald-200 bg-emerald-50 text-emerald-800'}`}>
          <span className="flex-1">
            Uploaded <strong>{uploadResult.total_rows}</strong> row{uploadResult.total_rows !== 1 ? 's' : ''} —{' '}
            <strong>{uploadResult.matched}</strong> matched to test cases
            {uploadResult.matched_by_name > 0 && <span className="text-xs ml-1">({uploadResult.matched_by_name} by name)</span>}
            {uploadResult.unmatched > 0 && `, ${uploadResult.unmatched} unmatched (ID not found)`}
            {uploadResult.skipped_bad_result > 0 && `, ${uploadResult.skipped_bad_result} skipped (missing or invalid actual_result)`}
            .
            {uploadResult.matched === 0 && uploadResult.total_rows > 0 && (
              <span className="block mt-1 text-xs font-medium">
                {uploadResult.skipped_bad_result > 0
                  ? 'Ensure your file has an actual_result column with HIT or MISS values.'
                  : 'No test case IDs matched. Export your test cases first, add an actual_result column (HIT/MISS), then re-upload. Or include a test_name column matching the generated test names.'}
              </span>
            )}
          </span>
          <button onClick={() => setUploadResult(null)} className={`text-lg leading-none flex-shrink-0 ${uploadResult.matched === 0 ? 'text-amber-400' : 'text-emerald-500'}`}>×</button>
        </div>
      )}

      {/* Upload zone */}
      {!hasResults && !summaryLoading && <UploadZone onUpload={handleUpload} />}
      {hasResults && (
        <details className="mb-4">
          <summary className="cursor-pointer text-sm text-blue-600 hover:underline select-none mb-2">↑ Upload new / replace results</summary>
          <UploadZone onUpload={handleUpload} />
        </details>
      )}

      {/* Dashboard */}
      {hasResults && (
        <>
          {/* Stats row */}
          <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-3 mb-5">
            <MetricCard label="Total" value={summary.total.toLocaleString()} colour="slate" />
            <MetricCard label="TP" value={summary.tp.toLocaleString()} colour="green" sub="Correct hits" />
            <MetricCard label="FN" value={summary.fn.toLocaleString()} colour="red" sub="Missed hits" />
            <MetricCard label="FP" value={summary.fp.toLocaleString()} colour="amber" sub="False alarms" />
            <MetricCard label="TN" value={summary.tn.toLocaleString()} colour="blue" sub="Correct clears" />
            <MetricCard label="Detection Rate" value={pct(summary.detection_rate)} colour="green" sub="TP / (TP+FN)" size="large" />
            <MetricCard label="Precision" value={pct(summary.precision)} colour="slate" sub="TP / (TP+FP)" />
            <MetricCard label="F1 Score" value={pct(summary.f1)} colour="slate" sub="Harmonic mean" />
          </div>

          {/* Confusion matrix + extra metrics */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-5">
            <div className="lg:col-span-2">
              <ConfusionMatrix tp={summary.tp} fp={summary.fp} tn={summary.tn} fn={summary.fn} />
            </div>
            <div className="flex flex-col gap-3">
              <MetricCard label="False Positive Rate" value={pct(summary.false_positive_rate)} sub="FP / (FP+TN)" colour="amber" size="large" />
              <MetricCard label="Recall" value={pct(summary.recall)} sub="= Detection Rate" colour="green" size="large" />
              <div className="bg-white border border-slate-200 rounded-xl p-4 flex-1">
                <p className="text-xs font-semibold text-slate-500 mb-2">Coverage</p>
                <div className="space-y-2">
                  {[
                    { label: 'Expected HITs', n: summary.tp + summary.fn },
                    { label: 'Expected MISSes', n: summary.fp + summary.tn },
                  ].map(({ label, n }) => (
                    <div key={label}>
                      <div className="flex justify-between text-xs text-slate-600 mb-0.5">
                        <span>{label}</span><span className="font-medium">{n.toLocaleString()}</span>
                      </div>
                      <div className="w-full h-1.5 bg-slate-100 rounded-full">
                        <div className="h-1.5 rounded-full bg-slate-300" style={{ width: `${Math.round(n / summary.total * 100)}%` }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>

          {/* Breakdown charts */}
          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden mb-5">
            <div className="flex border-b border-slate-200 overflow-x-auto">
              {BREAKDOWN_DIMS.map(d => (
                <button key={d.id}
                  onClick={() => { setActiveDim(d.id); loadBreakdown(d.id) }}
                  className={`px-4 py-2.5 text-xs font-medium whitespace-nowrap transition-colors
                    ${activeDim === d.id ? 'border-b-2 border-blue-600 text-blue-600 bg-blue-50' : 'text-slate-500 hover:text-slate-700 hover:bg-slate-50'}`}>
                  {d.label}
                </button>
              ))}
            </div>
            <div className="p-5">
              <p className="text-xs text-slate-400 mb-3">
                Stacked bar — expected HITs per group (green = caught, red = missed). Hover for detection rate.
              </p>
              {breakdownLoading ? (
                <div className="flex justify-center py-12">
                  <svg className="animate-spin h-6 w-6 text-slate-400" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
                  </svg>
                </div>
              ) : (
                <BreakdownChart data={breakdownData[activeDim]} />
              )}
            </div>
          </div>

          {/* Miss Analysis */}
          <MissAnalysisPanel fnCount={summary.fn} />

          {/* Results table */}
          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div className="p-3 border-b border-slate-100 flex flex-wrap gap-2 items-center">
              <input type="text" placeholder="Search test name or original…"
                value={searchDraft} onChange={e => handleSearch(e.target.value)}
                className="border border-slate-200 rounded px-3 py-1.5 text-sm flex-1 min-w-40" />
              <select value={outcomeFilter} onChange={e => setOutcomeFilter(e.target.value)}
                className="border border-slate-200 rounded px-2 py-1.5 text-sm">
                <option value="">All outcomes</option>
                <option value="TP">TP — Correct hits</option>
                <option value="FN">FN — Missed hits</option>
                <option value="FP">FP — False alarms</option>
                <option value="TN">TN — Correct clears</option>
              </select>
              <select value={entityFilter} onChange={e => setEntityFilter(e.target.value)}
                className="border border-slate-200 rounded px-2 py-1.5 text-sm">
                <option value="">All entity types</option>
                {['individual', 'entity', 'vessel', 'aircraft', 'country', 'unknown'].map(et => (
                  <option key={et} value={et}>{et}</option>
                ))}
              </select>
              <span className="text-xs text-slate-400 ml-auto">{total.toLocaleString()} results</span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-slate-50 border-b border-slate-200">
                    {['Outcome', 'Test Case ID', 'Test Name', 'Original Name', 'Type', 'Entity', 'List', 'Score'].map(h => (
                      <th key={h} className="px-3 py-2 text-left text-xs font-semibold text-slate-500 whitespace-nowrap">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {cases.map(c => (
                    <tr key={c.test_case_id} className="hover:bg-slate-50">
                      <td className="px-3 py-2 whitespace-nowrap">
                        <OutcomeBadge expected={c.expected_result} actual={c.actual_result} />
                      </td>
                      <td className="px-3 py-2 font-mono text-[11px] text-slate-400 whitespace-nowrap">{c.test_case_id}</td>
                      <td className="px-3 py-2 text-xs font-medium text-slate-900 max-w-[180px]">
                        <span className="line-clamp-1" title={c.test_name}>{c.test_name}</span>
                      </td>
                      <td className="px-3 py-2 text-xs text-slate-600 max-w-[180px]">
                        <span className="line-clamp-1" title={c.cleaned_original_name}>{c.cleaned_original_name}</span>
                      </td>
                      <td className="px-3 py-2 text-xs text-slate-500 max-w-[160px]">
                        <span className="line-clamp-1" title={c.test_case_type}>{c.test_case_type}</span>
                      </td>
                      <td className="px-3 py-2 text-xs text-slate-500 whitespace-nowrap">{c.entity_type}</td>
                      <td className="px-3 py-2 text-xs whitespace-nowrap">
                        <span className="inline-block w-2 h-2 rounded-full mr-1.5 flex-shrink-0"
                          style={{ backgroundColor: WATCHLIST_COLOURS[c.watchlist] || '#94A3B8' }} />
                        {c.watchlist}
                      </td>
                      <td className="px-3 py-2 text-xs text-slate-500 whitespace-nowrap">
                        {c.match_score != null ? Number(c.match_score).toFixed(2) : '—'}
                      </td>
                    </tr>
                  ))}
                  {cases.length === 0 && (
                    <tr><td colSpan={8} className="px-3 py-8 text-center text-sm text-slate-400">No results match the current filters</td></tr>
                  )}
                </tbody>
              </table>
            </div>

            {total > PAGE_SIZE && (
              <div className="flex items-center justify-between px-4 py-3 border-t border-slate-100">
                <span className="text-xs text-slate-500">Page {page} of {Math.ceil(total / PAGE_SIZE)}</span>
                <div className="flex gap-2">
                  <button onClick={() => loadTable(page - 1)} disabled={page <= 1}
                    className="px-3 py-1.5 rounded border border-slate-200 text-xs disabled:opacity-40 hover:bg-slate-50">← Prev</button>
                  <button onClick={() => loadTable(page + 1)} disabled={page >= Math.ceil(total / PAGE_SIZE)}
                    className="px-3 py-1.5 rounded border border-slate-200 text-xs disabled:opacity-40 hover:bg-slate-50">Next →</button>
                </div>
              </div>
            )}
          </div>
        </>
      )}

      {summaryLoading && (
        <div className="flex justify-center py-20">
          <svg className="animate-spin h-8 w-8 text-slate-300" viewBox="0 0 24 24" fill="none">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
          </svg>
        </div>
      )}
    </div>
  )
}
