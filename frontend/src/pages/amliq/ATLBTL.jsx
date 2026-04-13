import { useState, useRef, useEffect } from 'react'
import { useLocation, useNavigate, Link } from 'react-router-dom'
import axios from 'axios'
import {
  ComposedChart, Line, ReferenceLine,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'

// ── Formatters ───────────────────────────────────────────────────────────────
const fmtCurrency = (v) => {
  if (v == null) return '—'
  const n = Number(v)
  if (isNaN(n)) return '—'
  if (Math.abs(n) >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`
  if (Math.abs(n) >= 1_000) return `$${(n / 1_000).toFixed(1)}K`
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(n)
}
const fmtFull = (v) => {
  if (v == null) return '—'
  const n = Number(v)
  if (isNaN(n)) return '—'
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n)
}

// Deduplicate events by composite key (key + date_start + date_end + sum)
const dedupEvents = (events) => {
  const seen = new Set()
  return events.filter(ev => {
    const k = `${ev.key}|${ev.date_start ?? ''}|${ev.date_end ?? ''}|${ev.sum}`
    if (seen.has(k)) return false
    seen.add(k)
    return true
  })
}

// ── Source mode pill tabs ────────────────────────────────────────────────────
function ModeTabs({ mode, onChange }) {
  return (
    <div className="flex gap-1 bg-slate-100 rounded-xl p-1 w-fit mb-6">
      {[['upload', 'Upload Data'], ['threshold', 'From Threshold Setting']].map(([val, label]) => (
        <button
          key={val}
          onClick={() => onChange(val)}
          className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
            mode === val
              ? 'bg-white shadow-sm text-teal-700 border border-slate-200'
              : 'text-slate-500 hover:text-slate-700'
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  )
}

// ── Events table (shared between ATL and BTL tabs) ───────────────────────────
function EventsTable({ events, arAggKey, onRowClick, selectedEvent, accentColor = 'teal' }) {
  const colors = {
    teal:  { row: 'bg-teal-50', amount: 'text-teal-700' },
    amber: { row: 'bg-amber-50', amount: 'text-amber-700' },
  }
  const c = colors[accentColor] || colors.teal
  return (
    <div className="overflow-x-auto max-h-[480px] overflow-y-auto">
      <table className="w-full text-xs">
        <thead className="bg-slate-50 text-slate-500 uppercase tracking-wide sticky top-0 z-10 whitespace-nowrap">
          <tr>
            <th className="px-4 py-2 text-right w-10">#</th>
            <th className="px-4 py-2 text-left">{arAggKey || 'Key'}</th>
            <th className="px-4 py-2 text-right whitespace-nowrap">Start date</th>
            <th className="px-4 py-2 text-right whitespace-nowrap">End date</th>
            <th className="px-4 py-2 text-right">Days</th>
            <th className="px-4 py-2 text-right">Sum</th>
            <th className="px-4 py-2 text-right">Count</th>
            <th className="px-4 py-2 text-left">Transaction IDs</th>
          </tr>
        </thead>
        <tbody>
          {events.map((ev, i) => {
            const isSel = selectedEvent?.key === ev.key && selectedEvent?.date_start === ev.date_start
            const tidDisplay = ev.transaction_ids?.slice(0, 3).join(', ') +
              (ev.transaction_ids?.length > 3 ? ` +${ev.transaction_ids.length - 3} more` : '')
            return (
              <tr key={i} onClick={() => onRowClick(isSel ? null : ev)}
                className={`cursor-pointer transition-colors ${isSel ? c.row : i % 2 === 0 ? 'bg-white hover:bg-slate-50' : 'bg-slate-50/40 hover:bg-slate-100'}`}>
                <td className="px-4 py-2 text-right text-slate-400 tabular-nums">{i + 1}</td>
                <td className="px-4 py-2 font-medium text-slate-800">{ev.key}</td>
                <td className="px-4 py-2 text-right text-slate-600 tabular-nums whitespace-nowrap">{ev.date_start || '—'}</td>
                <td className="px-4 py-2 text-right text-slate-600 tabular-nums whitespace-nowrap">{ev.date_end || '—'}</td>
                <td className="px-4 py-2 text-right text-slate-600 tabular-nums">{ev.days ?? '—'}</td>
                <td className={`px-4 py-2 text-right font-semibold tabular-nums ${c.amount}`}>{fmtCurrency(ev.sum)}</td>
                <td className="px-4 py-2 text-right text-slate-600 tabular-nums">{ev.count?.toLocaleString()}</td>
                <td className="px-4 py-2 text-slate-400 max-w-xs truncate" title={ev.transaction_ids?.join(', ')}>{tidDisplay}</td>
              </tr>
            )
          })}
          {events.length === 0 && (
            <tr><td colSpan={8} className="px-4 py-8 text-center text-slate-400">No events in this zone.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

// ── Main component ───────────────────────────────────────────────────────────
export default function ATLBTL() {
  const location = useLocation()
  const navigate = useNavigate()
  const passed = location.state  // { analysisContext, candidateThreshold, analysisResult, simResult, thresholdInputs }

  // Source mode
  const [mode, setMode] = useState(passed ? 'threshold' : 'upload')

  // ── Upload-mode state ───────────────────────────────────────────────────
  const fileRef = useRef()
  const [dragging, setDragging] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [uploadPreview, setUploadPreview] = useState(null)
  const [valueColumn, setValueColumn] = useState('')
  const [thresholdInput, setThresholdInput] = useState('')

  // ── Shared result state ─────────────────────────────────────────────────
  const [btlResult, setBtlResult] = useState(null)
  const [btlLoading, setBtlLoading] = useState(false)
  const [btlError, setBtlError] = useState(null)
  const [candidateThreshold, setCandidateThreshold] = useState(passed?.candidateThreshold ?? null)
  const [analysisResult, setAnalysisResult] = useState(passed?.analysisResult ?? null)

  // ── Result tab + table state ────────────────────────────────────────────
  const [tab, setTab] = useState(0)
  const [selectedEvent, setSelectedEvent] = useState(null)
  const [txSort, setTxSort] = useState({ col: null, dir: 'asc' })

  // Auto-run when arriving from Threshold Setting
  useEffect(() => {
    if (passed?.analysisContext && passed?.candidateThreshold) {
      runFromThreshold(passed.analysisContext, passed.candidateThreshold)
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Handlers ───────────────────────────────────────────────────────────

  const runFromThreshold = async (ctx, threshold) => {
    setBtlLoading(true)
    setBtlError(null)
    setBtlResult(null)
    setTab(0)
    setSelectedEvent(null)
    try {
      const r = await axios.post('/api/threshold/analysis/atl-btl', {
        ...ctx,
        candidate_threshold: threshold,
      })
      setBtlResult(r.data)
      setCandidateThreshold(threshold)
      setAnalysisResult(passed?.analysisResult ?? null)
    } catch (err) {
      setBtlError(err?.response?.data?.detail || 'BTL computation failed')
    } finally {
      setBtlLoading(false)
    }
  }

  const handleFileDrop = (e) => {
    e.preventDefault()
    setDragging(false)
    const file = e.dataTransfer?.files?.[0]
    if (file) doUpload(file)
  }

  const handleFileChange = (e) => {
    const file = e.target.files?.[0]
    if (file) doUpload(file)
    e.target.value = ''
  }

  const doUpload = async (file) => {
    setUploading(true)
    setBtlResult(null)
    setBtlError(null)
    setUploadPreview(null)
    setValueColumn('')
    setThresholdInput('')
    try {
      const fd = new FormData()
      fd.append('file', file)
      const r = await axios.post('/api/btl/upload-preview', fd)
      setUploadPreview(r.data)
      if (r.data.numeric_columns?.length) setValueColumn(r.data.numeric_columns[0])
    } catch (err) {
      setBtlError(err?.response?.data?.detail || 'Upload failed')
    } finally {
      setUploading(false)
    }
  }

  const runUploadAnalysis = async () => {
    if (!uploadPreview?.upload_id || !valueColumn || !thresholdInput) return
    setBtlLoading(true)
    setBtlError(null)
    setBtlResult(null)
    setTab(0)
    setSelectedEvent(null)
    const threshold = Number(thresholdInput)
    try {
      const r = await axios.post('/api/btl/analyze', {
        upload_id: uploadPreview.upload_id,
        value_column: valueColumn,
        candidate_threshold: threshold,
      })
      setBtlResult(r.data)
      setCandidateThreshold(threshold)
      setAnalysisResult(null)
    } catch (err) {
      setBtlError(err?.response?.data?.detail || 'BTL computation failed')
    } finally {
      setBtlLoading(false)
    }
  }

  const handleModeChange = (newMode) => {
    setMode(newMode)
    setBtlResult(null)
    setBtlError(null)
    setSelectedEvent(null)
    setTab(0)
    if (newMode === 'threshold') {
      setCandidateThreshold(passed?.candidateThreshold ?? null)
      setAnalysisResult(passed?.analysisResult ?? null)
    }
  }

  const handleReturnToSimulate = () => {
    navigate('/amliq/thresholds', {
      state: {
        returnFromBTL: true,
        analysisContext:  passed?.analysisContext,
        analysisResult:   passed?.analysisResult,
        simResult:        passed?.simResult,
        thresholdInputs:  passed?.thresholdInputs,
      },
    })
  }

  // ── Derived values ──────────────────────────────────────────────────────

  const rawEvents  = analysisResult?.events || []
  const arEvents   = dedupEvents(rawEvents)   // deduplicated
  const arRawTx    = analysisResult?.raw_transactions || []
  const arCols     = analysisResult?.raw_columns || []
  const arAggKey   = analysisResult?.agg_key_col || null
  const arTidCol   = analysisResult?.tid_col || null
  const hasEvents  = arEvents.length > 0

  const p95 = btlResult?.p95 ?? null
  const anchorCluster = btlResult?.tranches?.find(t => t.contains_candidate)

  // ATL ceiling = lesser of the anchor cluster's top bound and P95
  const atlUpperBound = (p95 != null || anchorCluster?.hi != null)
    ? Math.min(anchorCluster?.hi ?? Infinity, p95 ?? Infinity)
    : null

  const atlEvents = (btlResult && hasEvents && atlUpperBound != null && atlUpperBound > candidateThreshold)
    ? arEvents.filter(ev => ev.sum >= candidateThreshold && ev.sum < atlUpperBound)
    : []

  const btlEvents = btlResult
    ? arEvents.filter(ev => ev.sum >= btlResult.btl_threshold && ev.sum < candidateThreshold)
    : []

  // Full-transactions panel: filtered by selected event
  const visibleTx = (() => {
    if (!selectedEvent) return arRawTx
    if (selectedEvent.transaction_ids?.length && arTidCol) {
      const idSet = new Set(selectedEvent.transaction_ids)
      return arRawTx.filter(r => idSet.has(r[arTidCol]))
    }
    if (arAggKey) return arRawTx.filter(r => r[arAggKey] === selectedEvent.key)
    return arRawTx
  })()

  const toggleTx = col => setTxSort(p =>
    p.col === col ? { col, dir: p.dir === 'asc' ? 'desc' : 'asc' } : { col, dir: 'asc' })
  const SortIcon = ({ col }) => txSort.col !== col
    ? <span className="opacity-30 ml-1 text-[10px]">⇅</span>
    : <span className="text-teal-600 ml-1 text-[10px]">{txSort.dir === 'asc' ? '▲' : '▼'}</span>
  const sortedTx = txSort.col ? [...visibleTx].sort((a, b) => {
    const av = a[txSort.col] ?? '', bv = b[txSort.col] ?? ''
    const an = Number(av), bn = Number(bv)
    const cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : String(av).localeCompare(String(bv))
    return txSort.dir === 'asc' ? cmp : -cmp
  }) : visibleTx

  // Tab configuration
  const tabs = [
    ['Cluster analysis', 0],
    ...(hasEvents ? [
      [`ATL events (${atlEvents.length})`, 1],
      [`BTL events (${btlEvents.length})`, 2],
      ['Full transactions', 3],
    ] : []),
  ]

  // ── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="w-full py-10 px-6 max-w-6xl mx-auto">

      {/* Page header */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-slate-900 mb-1">ATL / BTL Analysis</h1>
        <p className="text-slate-500 text-sm">
          Identify Above-the-Line and Below-the-Line thresholds using k-means clustering.
          Upload transaction data directly, or receive it from the Threshold Setting workflow.
        </p>
      </div>

      {/* Source mode selector */}
      <ModeTabs mode={mode} onChange={handleModeChange} />

      {/* ── Upload mode panel ─────────────────────────────────────────── */}
      {mode === 'upload' && (
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-8 mb-6 space-y-6">
          <div>
            <h2 className="text-sm font-semibold text-slate-700 mb-1">Upload transaction data</h2>
            <p className="text-xs text-slate-400">CSV or Excel (.xlsx). The file should contain a numeric column representing transaction amounts or aggregated values.</p>
          </div>

          {/* Drop zone */}
          <div
            onDragOver={e => { e.preventDefault(); setDragging(true) }}
            onDragLeave={() => setDragging(false)}
            onDrop={handleFileDrop}
            onClick={() => fileRef.current?.click()}
            className={`border-2 border-dashed rounded-xl p-10 text-center cursor-pointer transition-colors
              ${dragging ? 'border-teal-400 bg-teal-50' : 'border-slate-300 hover:border-teal-400 hover:bg-slate-50'}`}
          >
            <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" className="hidden" onChange={handleFileChange} />
            {uploading ? (
              <p className="text-sm text-slate-400 animate-pulse">Uploading…</p>
            ) : uploadPreview ? (
              <div className="space-y-1">
                <p className="text-sm font-medium text-teal-700">File loaded — {uploadPreview.row_count.toLocaleString()} rows</p>
                <p className="text-xs text-slate-400">{uploadPreview.columns.length} columns detected · click to replace</p>
              </div>
            ) : (
              <div className="space-y-1">
                <p className="text-sm font-medium text-slate-500">Drop a file here or click to browse</p>
                <p className="text-xs text-slate-400">CSV or Excel</p>
              </div>
            )}
          </div>

          {/* Column + threshold config */}
          {uploadPreview && (
            <div className="grid grid-cols-2 gap-5">
              <div>
                <label className="block text-xs font-semibold text-slate-600 mb-1.5">Value column</label>
                <select
                  value={valueColumn}
                  onChange={e => setValueColumn(e.target.value)}
                  className="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-teal-500"
                >
                  <option value="">— select column —</option>
                  {uploadPreview.numeric_columns.map(c => (
                    <option key={c} value={c}>{c}</option>
                  ))}
                  {uploadPreview.columns.filter(c => !uploadPreview.numeric_columns.includes(c)).map(c => (
                    <option key={c} value={c}>{c} (non-numeric)</option>
                  ))}
                </select>
                <p className="text-xs text-slate-400 mt-1">{uploadPreview.numeric_columns.length} numeric columns detected</p>
              </div>
              <div>
                <label className="block text-xs font-semibold text-slate-600 mb-1.5">Candidate (ATL) threshold</label>
                <input
                  type="number"
                  value={thresholdInput}
                  onChange={e => setThresholdInput(e.target.value)}
                  placeholder="e.g. 10000"
                  className="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-teal-500"
                />
                <p className="text-xs text-slate-400 mt-1">The threshold above which alerts are generated (ATL)</p>
              </div>
            </div>
          )}

          {uploadPreview && (
            <button
              onClick={runUploadAnalysis}
              disabled={!valueColumn || !thresholdInput || btlLoading}
              className="px-6 py-2.5 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              {btlLoading ? 'Running k-means…' : 'Run BTL Analysis'}
            </button>
          )}
        </div>
      )}

      {/* ── From Threshold Setting panel ──────────────────────────────── */}
      {mode === 'threshold' && (
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-8 mb-6">
          {passed?.analysisContext ? (
            <div className="space-y-5">
              <div className="flex items-start justify-between">
                <div>
                  <h2 className="text-sm font-semibold text-slate-700 mb-1">Data received from Threshold Setting</h2>
                  <p className="text-xs text-slate-400">Analysis context and candidate threshold were passed from the Threshold Setting workflow.</p>
                </div>
                <button
                  onClick={handleReturnToSimulate}
                  className="shrink-0 ml-6 px-4 py-2 text-xs font-medium text-teal-700 border border-teal-300 rounded-lg hover:bg-teal-50 transition-colors whitespace-nowrap"
                >
                  ← Return to choose candidate threshold
                </button>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div className="bg-slate-50 border border-slate-200 rounded-xl px-4 py-3">
                  <p className="text-xs text-slate-400 uppercase tracking-wide font-semibold mb-0.5">Analysis type</p>
                  <p className="text-sm font-semibold text-slate-700 capitalize">{passed.analysisContext.analysis_type}</p>
                </div>
                {passed.analysisContext.aggregation_key && (
                  <div className="bg-slate-50 border border-slate-200 rounded-xl px-4 py-3">
                    <p className="text-xs text-slate-400 uppercase tracking-wide font-semibold mb-0.5">Group by</p>
                    <p className="text-sm font-semibold text-slate-700">{passed.analysisContext.aggregation_key}</p>
                  </div>
                )}
                <div className="bg-amber-50 border border-amber-200 rounded-xl px-4 py-3">
                  <p className="text-xs text-amber-600 uppercase tracking-wide font-semibold mb-0.5">Candidate threshold</p>
                  <p className="text-sm font-bold text-amber-800">{fmtFull(passed.candidateThreshold)}</p>
                </div>
              </div>
              {!btlResult && !btlLoading && (
                <button
                  onClick={() => runFromThreshold(passed.analysisContext, passed.candidateThreshold)}
                  className="px-6 py-2.5 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700 transition-colors"
                >
                  Re-run BTL Analysis
                </button>
              )}
            </div>
          ) : (
            <div className="text-center py-10">
              <p className="text-sm font-medium text-slate-500 mb-2">No data received from Threshold Setting</p>
              <p className="text-xs text-slate-400 mb-5">
                Navigate to Threshold Setting, run a simulation, and click <strong>Choose →</strong> on a threshold to send data here.
              </p>
              <Link
                to="/amliq/thresholds"
                className="inline-block px-5 py-2 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700"
              >
                Go to Threshold Setting →
              </Link>
            </div>
          )}
        </div>
      )}

      {/* ── Error banner ──────────────────────────────────────────────── */}
      {btlError && (
        <div className="bg-red-50 border border-red-200 rounded-xl px-5 py-3 mb-5">
          <p className="text-sm text-red-700">{btlError}</p>
        </div>
      )}

      {/* ── Results ───────────────────────────────────────────────────── */}
      {(btlLoading || btlResult) && (
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-8 space-y-5">

          {/* Header cards */}
          <div className="grid grid-cols-4 gap-4">
            <div className="bg-slate-50 border border-slate-200 rounded-xl px-4 py-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-400 mb-1">Candidate (ATL)</p>
              <p className="text-xl font-bold tabular-nums text-slate-700">
                {candidateThreshold != null ? fmtFull(candidateThreshold) : '—'}
              </p>
              <p className="text-xs text-slate-400 mt-0.5">
                {mode === 'threshold' ? 'Chosen from simulation' : 'Entered manually'}
              </p>
            </div>
            <div className={`rounded-xl px-4 py-3 border-2 ${btlResult ? 'bg-amber-50 border-amber-300' : 'bg-slate-50 border-slate-200'}`}>
              <p className="text-xs font-semibold uppercase tracking-wide text-amber-600 mb-1">BTL threshold</p>
              <p className={`text-xl font-bold tabular-nums ${btlResult ? 'text-amber-700' : 'text-slate-300'}`}>
                {btlLoading
                  ? <span className="text-base animate-pulse">Computing…</span>
                  : btlResult ? fmtFull(btlResult.btl_threshold) : '—'}
              </p>
              <p className="text-xs text-amber-600 mt-0.5">
                {btlResult && anchorCluster
                  ? `Lower bound · cluster ${anchorCluster.rank + 1} of ${btlResult.optimal_k}`
                  : 'Awaiting k-means'}
              </p>
            </div>
            <div className={`rounded-xl px-4 py-3 border ${atlUpperBound != null ? 'bg-teal-50 border-teal-200' : 'bg-slate-50 border-slate-200'}`}>
              <p className="text-xs font-semibold uppercase tracking-wide text-teal-600 mb-1">ATL upper bound</p>
              <p className={`text-xl font-bold tabular-nums ${atlUpperBound != null ? 'text-teal-700' : 'text-slate-300'}`}>
                {atlUpperBound != null ? fmtFull(atlUpperBound) : '—'}
              </p>
              <p className="text-xs text-teal-500 mt-0.5">
                {atlUpperBound != null
                  ? (anchorCluster?.hi != null && p95 != null
                      ? (anchorCluster.hi <= p95 ? 'Cluster top (< P95)' : 'P95 (< cluster top)')
                      : 'min(cluster top, P95)')
                  : 'Awaiting result'}
              </p>
            </div>
            <div className="bg-slate-50 border border-slate-200 rounded-xl px-4 py-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-400 mb-1">k-means k</p>
              <p className="text-xl font-bold tabular-nums text-slate-700">
                {btlResult ? `k=${btlResult.optimal_k}` : '—'}
              </p>
              <p className="text-xs text-slate-400 mt-0.5">Optimal clusters (elbow)</p>
            </div>
          </div>

          {/* Sub-tabs */}
          {btlResult && (
            <>
              <div className="flex items-center gap-0 border-b border-slate-200">
                {tabs.map(([label, idx]) => (
                  <button
                    key={idx}
                    onClick={() => setTab(idx)}
                    className={`px-5 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px
                      ${tab === idx ? 'border-teal-600 text-teal-700' : 'border-transparent text-slate-500 hover:text-slate-700'}`}
                  >
                    {label}
                  </button>
                ))}
              </div>

              {/* ── Tab 0: Cluster Analysis ── */}
              {tab === 0 && (
                <div className="space-y-4">
                  {/* Cluster table */}
                  <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
                    <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
                      <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">
                        K-means clusters — k={btlResult.optimal_k}
                      </p>
                      <p className="text-xs text-slate-400">Candidate threshold falls in highlighted cluster → BTL = its lower bound</p>
                    </div>
                    <table className="w-full text-sm">
                      <thead className="bg-slate-50 text-xs text-slate-500 uppercase tracking-wide">
                        <tr>
                          <th className="px-5 py-2 text-left">Cluster</th>
                          <th className="px-5 py-2 text-right">Low</th>
                          <th className="px-5 py-2 text-right">High</th>
                          <th className="px-5 py-2 text-right">Center</th>
                          <th className="px-5 py-2 text-right">Count</th>
                          <th className="px-5 py-2 text-right">% of total</th>
                          <th className="px-5 py-2 text-right">Role</th>
                        </tr>
                      </thead>
                      <tbody>
                        {btlResult.tranches.map((t, i) => {
                          const isAnchor = t.contains_candidate
                          const role = isAnchor
                            ? 'BTL anchor'
                            : t.lo >= btlResult.candidate_threshold
                              ? 'Above candidate'
                              : 'Below BTL'
                          const roleColor = isAnchor
                            ? 'text-amber-700 bg-amber-50 border border-amber-200'
                            : t.lo >= btlResult.candidate_threshold
                              ? 'text-teal-700 bg-teal-50 border border-teal-200'
                              : 'text-slate-400 bg-white border border-slate-200'
                          return (
                            <tr key={i} className={isAnchor ? 'bg-amber-50/50' : i % 2 === 0 ? 'bg-white' : 'bg-slate-50/30'}>
                              <td className="px-5 py-2.5 font-medium text-slate-700">Cluster {t.rank + 1}</td>
                              <td className="px-5 py-2.5 text-right tabular-nums text-slate-600">{fmtFull(t.lo)}</td>
                              <td className="px-5 py-2.5 text-right tabular-nums text-slate-600">{fmtFull(t.hi)}</td>
                              <td className="px-5 py-2.5 text-right tabular-nums font-medium text-slate-700">{fmtFull(t.center)}</td>
                              <td className="px-5 py-2.5 text-right tabular-nums">{t.count.toLocaleString()}</td>
                              <td className="px-5 py-2.5 text-right tabular-nums text-slate-500">{t.pct}%</td>
                              <td className="px-5 py-2.5 text-right">
                                <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${roleColor}`}>{role}</span>
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>

                  {/* Elbow chart + rationale */}
                  <div className="flex gap-4">
                    {btlResult.elbow_data.length > 1 && (
                      <div className="flex-1 bg-white border border-slate-200 rounded-xl p-5">
                        <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide mb-4">
                          Elbow curve — optimal k={btlResult.optimal_k}
                        </p>
                        <ResponsiveContainer width="100%" height={200}>
                          <ComposedChart data={btlResult.elbow_data} margin={{ left: 10, right: 20, bottom: 10 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                            <XAxis dataKey="k" tick={{ fontSize: 11 }} label={{ value: 'k (clusters)', position: 'insideBottom', offset: -4, fontSize: 10 }} />
                            <YAxis tick={{ fontSize: 11 }} width={55}
                              tickFormatter={v => v >= 1e9 ? `${(v/1e9).toFixed(1)}B` : v >= 1e6 ? `${(v/1e6).toFixed(1)}M` : v >= 1e3 ? `${(v/1e3).toFixed(0)}K` : v} />
                            <Tooltip
                              formatter={(v, name) => name === 'inertia' ? [v.toLocaleString(), 'Inertia'] : [`${v}%`, '% reduction']}
                              labelFormatter={v => `k = ${v}`} />
                            <Line type="monotone" dataKey="inertia" stroke="#0d9488" strokeWidth={2} dot={{ fill: '#0d9488', r: 4 }} name="inertia" />
                            <ReferenceLine x={btlResult.optimal_k} stroke="#f59e0b" strokeDasharray="4 4"
                              label={{ value: `k=${btlResult.optimal_k}`, fill: '#d97706', fontSize: 11, position: 'top' }} />
                          </ComposedChart>
                        </ResponsiveContainer>
                      </div>
                    )}
                    <div className="flex-1 bg-slate-50 border border-slate-200 rounded-xl p-5">
                      <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide mb-3">Rationale</p>
                      <pre className="text-xs text-slate-600 whitespace-pre-wrap font-mono leading-relaxed overflow-auto max-h-[220px]">
                        {btlResult.rationale}
                      </pre>
                    </div>
                  </div>

                  {/* Zone definition — 3 zones */}
                  <div className="bg-white border border-slate-200 rounded-xl px-5 py-4">
                    <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Zone definition</p>
                    <div className="flex gap-3">
                      {[
                        ['Below BTL', `< ${fmtFull(btlResult.btl_threshold)}`, 'bg-slate-50 border-slate-200 text-slate-600', 'No enhanced review'],
                        ['BTL zone', `${fmtFull(btlResult.btl_threshold)} – ${fmtFull(candidateThreshold)}`, 'bg-amber-50 border-amber-300 text-amber-700', 'Enhanced monitoring'],
                        ['ATL zone', `${fmtFull(candidateThreshold)} – ${atlUpperBound != null ? fmtFull(atlUpperBound) : '—'}`, 'bg-teal-50 border-teal-300 text-teal-700', 'Full alert · min(cluster top, P95)'],
                      ].map(([zone, range, cls, action]) => (
                        <div key={zone} className={`flex-1 border rounded-xl p-4 ${cls}`}>
                          <p className="font-semibold text-sm">{zone}</p>
                          <p className="text-xs mt-1 font-mono">{range}</p>
                          <p className="text-xs mt-2 opacity-70">{action}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              {/* ── Tab 1: ATL Events ── */}
              {tab === 1 && hasEvents && (
                <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
                  <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
                    <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">
                      ATL events — {fmtFull(candidateThreshold)} to {atlUpperBound != null ? fmtFull(atlUpperBound) : '—'}
                      <span className="ml-2 text-teal-400 font-normal">(above line, up to P95)</span>
                    </p>
                    <p className="text-xs text-slate-400">Click a row to view transactions</p>
                  </div>
                  <EventsTable
                    events={atlEvents}
                    arAggKey={arAggKey}
                    selectedEvent={selectedEvent}
                    onRowClick={ev => { setSelectedEvent(ev); setTab(3) }}
                    accentColor="teal"
                  />
                </div>
              )}

              {/* ── Tab 2: BTL Events ── */}
              {tab === 2 && hasEvents && (
                <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
                  <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
                    <p className="text-xs font-semibold text-amber-700 uppercase tracking-wide">
                      BTL events — {fmtFull(btlResult.btl_threshold)} to {fmtFull(candidateThreshold)}
                      <span className="ml-2 text-amber-400 font-normal">(enhanced monitoring zone)</span>
                    </p>
                    <p className="text-xs text-slate-400">Click a row to view transactions</p>
                  </div>
                  <EventsTable
                    events={btlEvents}
                    arAggKey={arAggKey}
                    selectedEvent={selectedEvent}
                    onRowClick={ev => { setSelectedEvent(ev); setTab(3) }}
                    accentColor="amber"
                  />
                </div>
              )}

              {/* ── Tab 3: Full Transactions ── */}
              {tab === 3 && hasEvents && (
                <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
                  <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
                    <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">Full transaction data</p>
                    {selectedEvent ? (
                      <div className="flex items-center gap-3">
                        <span className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1">
                          Filtered: {visibleTx.length} txns for <strong>{selectedEvent.key}</strong>
                          {selectedEvent.date_start && <span className="ml-1">({selectedEvent.date_start} → {selectedEvent.date_end})</span>}
                        </span>
                        <button onClick={() => setSelectedEvent(null)} className="text-xs text-slate-400 hover:text-red-500">✕ Clear</button>
                      </div>
                    ) : (
                      <p className="text-xs text-slate-400">{arRawTx.length.toLocaleString()} rows (select an ATL or BTL event to filter)</p>
                    )}
                  </div>
                  <div className="overflow-x-auto max-h-[480px] overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead className="bg-slate-50 text-slate-500 uppercase tracking-wide sticky top-0 z-10 whitespace-nowrap">
                        <tr>
                          {arCols.map(c => (
                            <th key={c} onClick={() => c !== '_row' && toggleTx(c)}
                              className={`px-3 py-2 text-left select-none ${c !== '_row' ? 'cursor-pointer hover:text-slate-700' : ''}`}>
                              {c === '_row' ? '#' : c}{c !== '_row' && <SortIcon col={c} />}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sortedTx.map((row, i) => (
                          <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-slate-50/40'}>
                            {arCols.map(c => (
                              <td key={c} className="px-3 py-2 text-slate-700 whitespace-nowrap">{row[c] ?? '—'}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}

          {/* Loading state */}
          {btlLoading && !btlResult && (
            <div className="bg-white border border-slate-200 rounded-xl p-10 text-center">
              <p className="text-sm text-slate-400 animate-pulse">Running k-means clustering with elbow method…</p>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
