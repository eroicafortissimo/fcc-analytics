import { useState, useRef, useCallback, useEffect } from 'react'
import {
  LineChart, Line, BarChart, Bar, ComposedChart,
  ScatterChart, Scatter, ReferenceLine,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ZAxis
} from 'recharts'
import { thresholdApi } from '../../api/thresholdApi'

// ── Step indicator ──────────────────────────────────────────────────────────

const STEPS = ['Upload', 'Scenario', 'Configure', 'Analysis', 'Simulate', 'Report']

function StepIndicator({ current }) {
  return (
    <div className="flex items-center gap-0 mb-8">
      {STEPS.map((s, i) => (
        <div key={i} className="flex items-center">
          <div className="flex flex-col items-center">
            <div className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold border-2 transition-colors
              ${i < current ? 'bg-teal-600 border-teal-600 text-white'
                : i === current ? 'bg-white border-teal-600 text-teal-700'
                : 'bg-white border-slate-300 text-slate-400'}`}>
              {i < current ? '✓' : i + 1}
            </div>
            <span className={`text-xs mt-1 font-medium ${i === current ? 'text-teal-700' : i < current ? 'text-teal-600' : 'text-slate-400'}`}>
              {s}
            </span>
          </div>
          {i < STEPS.length - 1 && (
            <div className={`h-0.5 w-12 mx-1 mb-4 ${i < current ? 'bg-teal-600' : 'bg-slate-200'}`} />
          )}
        </div>
      ))}
    </div>
  )
}

// ── Predefined scenario library ─────────────────────────────────────────────

const SCENARIO_LIBRARY = [
  {
    id: 'high_value',
    name: 'High Value Transactions',
    description: 'Analyze the distribution of transaction amounts — typically used to calibrate wire transfer thresholds.',
    hint: 'Select your amount column in Configure, then set a threshold in Simulate.',
  },
  {
    id: 'high_risk_jurisdiction',
    name: 'High-Risk Jurisdiction',
    description: 'Filter to transactions involving high-risk or sanctioned countries, then analyze volume distribution.',
    hint: 'Use Manual filters to add: country_code in [IR, KP, SY, ...] — matching your column names.',
  },
  {
    id: 'structuring',
    name: 'Structuring Detection',
    description: 'Aggregate transactions per customer over a rolling window to detect structuring patterns.',
    hint: 'Use Aggregate mode in Configure — group by customer/account ID, sum amounts over 7 days.',
  },
]

// ── Column name helper ──────────────────────────────────────────────────────
// dataset.columns may be [{name, kind}] objects or plain strings
const colName = (c) => (typeof c === 'string' ? c : c?.name)

// ── Main component ──────────────────────────────────────────────────────────

export default function ThresholdSetting() {
  const [step, setStep] = useState(0)

  // Step 0 — Upload
  const [datasets, setDatasets] = useState([])
  const [selectedDataset, setSelectedDataset] = useState(null)
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState(null)
  const [reloading, setReloading] = useState(false)
  const [dragging, setDragging] = useState(false)
  const fileRef = useRef()

  // Step 1 — Scenario
  const [scenarioMode, setScenarioMode] = useState('library')
  const [selectedLibId, setSelectedLibId] = useState(null)
  const [filterRules, setFilterRules] = useState({ group_operator: 'AND', groups: [] })
  const [simpleFilters, setSimpleFilters] = useState([{ column: '', operator: '=', value: '', logic: 'AND' }])
  const [aiPrompt, setAiPrompt] = useState('')
  const [aiLoading, setAiLoading] = useState(false)
  const [savedScenarios, setSavedScenarios] = useState([])
  const [activeScenario, setActiveScenario] = useState(null)
  const [scenarioPreview, setScenarioPreview] = useState(null)

  // Step 2 — Configure
  const [analysisType, setAnalysisType] = useState('single')
  const [aggKey, setAggKey] = useState('')
  const [aggAmount, setAggAmount] = useState('')
  const [aggDate, setAggDate] = useState('')
  const [aggPeriod, setAggPeriod] = useState('none')
  const [aggFunction, setAggFunction] = useState('SUM')
  const [paramColumn, setParamColumn] = useState('')

  // Analysis context (persisted for simulate / auto-thresholds)
  const [analysisContext, setAnalysisContext] = useState(null)

  // Step 3 — Analysis results
  const [analysisResult, setAnalysisResult] = useState(null)
  const [analysisLoading, setAnalysisLoading] = useState(false)
  const [analysisId, setAnalysisId] = useState(null)

  // Step 4 — Simulate
  const [thresholdInputs, setThresholdInputs] = useState(['', '', ''])
  const [simResult, setSimResult] = useState(null)
  const [simLoading, setSimLoading] = useState(false)
  const [autoLoading, setAutoLoading] = useState(false)

  // Step 5 — Report
  const [reportText, setReportText] = useState('')
  const [reportLoading, setReportLoading] = useState(false)

  useEffect(() => {
    thresholdApi.listDatasets().then(r => setDatasets(r.data)).catch(() => {})
  }, [])

  // ── Column list helpers ────────────────────────────────────────────────────

  const columns = (selectedDataset?.columns || []).map(colName)

  // ── Step 0 handlers ────────────────────────────────────────────────────────

  const handleFileDrop = useCallback(async (e) => {
    e.preventDefault()
    setDragging(false)
    const file = e.dataTransfer?.files?.[0] || e.target?.files?.[0]
    if (!file) return
    setUploading(true)
    setUploadError(null)
    try {
      const r = await thresholdApi.uploadDataset(file, file.name)
      const ds = r.data
      setDatasets(prev => [ds, ...prev])
      setSelectedDataset(ds)
    } catch (err) {
      setUploadError(err?.response?.data?.detail || 'Upload failed')
    } finally {
      setUploading(false)
    }
  }, [])

  const handleDeleteDataset = async (id, e) => {
    e.stopPropagation()
    await thresholdApi.deleteDataset(id)
    setDatasets(prev => prev.filter(d => d.id !== id))
    if (selectedDataset?.id === id) setSelectedDataset(null)
  }

  // ── Step 1 handlers ────────────────────────────────────────────────────────

  const applyLibrary = (sc) => {
    setSelectedLibId(sc.id)
    // Library scenarios are conceptual templates — don't impose filters.
    // User adds actual filters in Manual mode if needed.
  }

  const addFilter = () => setSimpleFilters(f => [...f, { column: '', operator: '=', value: '', logic: 'AND' }])
  const removeFilter = (i) => setSimpleFilters(f => f.filter((_, j) => j !== i))
  const updateFilter = (i, key, val) => setSimpleFilters(f => f.map((x, j) => j === i ? { ...x, [key]: val } : x))

  // Convert simple filters UI to filter_rules format
  const simpleFiltersToRules = () => {
    const conditions = simpleFilters.filter(f => f.column && f.value !== '')
    if (!conditions.length) return {}
    return {
      group_operator: 'AND',
      groups: [{ operator: 'AND', conditions: conditions.map(f => ({ column: f.column, operator: f.operator, value: f.value })) }],
    }
  }

  const handleAiScenario = async () => {
    if (!aiPrompt.trim() || !selectedDataset) return
    setAiLoading(true)
    try {
      const r = await thresholdApi.aiScenario(selectedDataset.id, aiPrompt)
      const sc = r.data
      setSavedScenarios(prev => [{ id: Date.now(), name: sc.name || 'AI Scenario', ...sc }, ...prev])
      setFilterRules(sc)
      setScenarioMode('manual')
    } catch (err) {
      alert(err?.response?.data?.detail || 'AI scenario failed')
    } finally {
      setAiLoading(false)
    }
  }

  const handleSaveScenario = async () => {
    if (!selectedDataset) return
    const rules = scenarioMode === 'manual' ? simpleFiltersToRules() : filterRules
    try {
      const r = await thresholdApi.createScenario({
        dataset_id: selectedDataset.id,
        name: `Scenario ${savedScenarios.length + 1}`,
        filter_rules: rules,
        analysis_type: analysisType,
      })
      const sc = r.data
      setSavedScenarios(prev => [sc, ...prev])
      setActiveScenario(sc)
    } catch (err) {
      alert(err?.response?.data?.detail || 'Save failed')
    }
  }

  const handlePreviewScenario = async () => {
    if (!activeScenario) return
    try {
      const r = await thresholdApi.previewScenario(activeScenario.id)
      // backend returns { total, original_total, preview }
      const d = r.data
      setScenarioPreview({
        match_count: d.total,
        total_rows: d.original_total,
        match_pct: d.original_total ? (d.total / d.original_total * 100) : 0,
      })
    } catch {}
  }

  // ── Step 2 handlers ────────────────────────────────────────────────────────

  const buildAnalysisBody = () => {
    const rules = scenarioMode === 'manual' ? simpleFiltersToRules() : filterRules
    return {
      dataset_id: selectedDataset.id,
      scenario_id: activeScenario?.id || null,
      filter_rules: rules,
      analysis_type: analysisType,
      parameter_column: paramColumn,
      aggregation_key: aggKey,
      aggregation_amount: aggAmount,
      aggregation_date: aggDate,
      aggregation_period: aggPeriod,
      aggregation_function: aggFunction,
    }
  }

  const handleRunAnalysis = async () => {
    if (!selectedDataset || !paramColumn) return
    setAnalysisLoading(true)
    try {
      const body = buildAnalysisBody()
      setAnalysisContext(body)
      const r = await thresholdApi.runAnalysis(body)
      const d = r.data
      // Normalize response field names
      const tranches = d.tranches || []
      const trimTranches = d.trim_tranches || []
      // Merge into single CDF dataset keyed by tranche upper bound
      const cdf = tranches.map((t, i) => ({
        value: t.hi,
        label: t.label,
        all_pct: t.cumulative_pct,
        trim_pct: trimTranches[i]?.cumulative_pct ?? null,
      }))
      setAnalysisResult({
        stats: d.statistics || {},
        tranches,
        cdf,
        categorical: d.categorical_dist?.rows || [],
        matched_rows: d.matched_rows,
        original_rows: d.original_rows,
        column: d.column,
        is_categorical: d.is_categorical,
        sample_values: d.sample_values || [],
      })
      setAnalysisId(d.analysis_id)
      setStep(3)
    } catch (err) {
      alert(err?.response?.data?.detail || 'Analysis failed')
    } finally {
      setAnalysisLoading(false)
    }
  }

  // ── Step 4 handlers ────────────────────────────────────────────────────────

  const handleAutoThresholds = async () => {
    if (!analysisContext) return
    setAutoLoading(true)
    try {
      const r = await thresholdApi.autoThresholds(analysisContext)
      const byPct = {}
      ;(r.data.percentiles || []).forEach((p, i) => { byPct[p] = r.data.thresholds[i] })
      const empty = thresholdInputs.filter(v => v === '').length
      const slots = empty <= 1 ? [85] : empty === 2 ? [85, 95] : [75, 85, 95]
      setThresholdInputs(slots.map(p => byPct[p] != null ? String(byPct[p]) : '').concat(['', '', '']).slice(0, 3))
    } catch {}
    setAutoLoading(false)
  }

  const handleSimulate = async () => {
    if (!analysisContext) return
    const vals = thresholdInputs.map(v => parseFloat(v)).filter(v => !isNaN(v))
    if (!vals.length) return
    setSimLoading(true)
    try {
      const body = { ...analysisContext, analysis_id: analysisId, thresholds: vals }
      const r = await thresholdApi.simulate(body)
      const d = r.data
      // Normalize field names for display
      const results = (d.results || []).map(row => ({
        ...row,
        volume_pct: row.pct_volume_captured,
        events_pct: row.pct_events_captured,
        monthly_alerts: row.est_monthly_alerts,
      }))
      const recThreshold = d.recommendation?.threshold
      const recRow = results.find(r => r.threshold === recThreshold) || null
      setSimResult({ results, recommendation: recRow })
      setStep(4)
    } catch (err) {
      alert(err?.response?.data?.detail || 'Simulation failed')
    } finally {
      setSimLoading(false)
    }
  }

  // ── Step 5 handlers ────────────────────────────────────────────────────────

  const handleGenerateReport = async () => {
    if (!analysisId) return
    setReportLoading(true)
    try {
      const r = await thresholdApi.generateReport(analysisId)
      setReportText(r.data.report_text || '')
    } catch {}
    setReportLoading(false)
  }

  const handleDownloadReport = () => {
    const blob = new Blob([reportText], { type: 'text/plain' })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = 'threshold-report.txt'
    a.click()
  }

  // ── Reset ──────────────────────────────────────────────────────────────────

  const handleReset = () => {
    setStep(0)
    setSelectedDataset(null)
    setAnalysisResult(null)
    setSimResult(null)
    setReportText('')
    setAnalysisId(null)
    setAnalysisContext(null)
  }

  // ── Step renders ───────────────────────────────────────────────────────────

  const renderStep0 = () => (
    <div className="space-y-6">
      <div
        onDragOver={e => { e.preventDefault(); setDragging(true) }}
        onDragLeave={() => setDragging(false)}
        onDrop={handleFileDrop}
        onClick={() => fileRef.current?.click()}
        className={`border-2 border-dashed rounded-xl p-10 text-center cursor-pointer transition-colors
          ${dragging ? 'border-teal-500 bg-teal-50' : 'border-slate-300 bg-slate-50 hover:border-teal-400 hover:bg-teal-50/40'}`}
      >
        <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" className="hidden" onChange={handleFileDrop} />
        {uploading
          ? <p className="text-teal-600 text-sm font-medium animate-pulse">Uploading…</p>
          : <>
              <p className="text-slate-500 text-sm">Drop a CSV or Excel file here, or <span className="text-teal-600 font-medium">click to browse</span></p>
              <p className="text-slate-400 text-xs mt-1">Supports .csv, .xlsx, .xls</p>
            </>
        }
      </div>
      {uploadError && <p className="text-red-600 text-xs">{uploadError}</p>}

      {datasets.length > 0 && (
        <div>
          <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Previous datasets</h3>
          <div className="space-y-2">
            {datasets.map(ds => {
              const reuploadRef = { current: null }
              const needsReupload = !ds.has_file
              return (
                <div key={ds.id}>
                  <div
                    onClick={async () => {
                      if (needsReupload) return // handled by button below
                      setSelectedDataset(ds)
                      if (!ds.in_memory) {
                        setReloading(true)
                        try {
                          const r = await thresholdApi.reloadDataset(ds.id)
                          setSelectedDataset(prev => ({ ...prev, in_memory: true, has_file: true, columns: r.data.columns || prev.columns }))
                          setDatasets(prev => prev.map(d => d.id === ds.id ? { ...d, in_memory: true, has_file: true } : d))
                        } catch { }
                        setReloading(false)
                      }
                    }}
                    className={`flex items-center justify-between px-4 py-3 rounded-lg border transition-colors
                      ${needsReupload ? 'border-amber-200 bg-amber-50/60 cursor-default' :
                        selectedDataset?.id === ds.id ? 'border-teal-500 bg-teal-50 cursor-pointer' :
                        'border-slate-200 hover:border-teal-300 hover:bg-slate-50 cursor-pointer'}`}>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <p className="text-sm font-medium text-slate-800 truncate">{ds.name}</p>
                        {needsReupload && (
                          <span className="text-xs font-medium text-amber-700 bg-amber-100 border border-amber-200 rounded px-1.5 py-0.5 whitespace-nowrap">
                            ⚠ Re-upload needed
                          </span>
                        )}
                      </div>
                      <p className="text-xs text-slate-400">
                        {ds.row_count?.toLocaleString()} rows · {(ds.columns || []).length} columns
                        {ds.date_range_start && ` · ${ds.date_range_start} – ${ds.date_range_end}`}
                      </p>
                    </div>
                    <div className="flex items-center gap-2 shrink-0 ml-3">
                      {needsReupload && (
                        <>
                          <input type="file" accept=".csv,.xlsx,.xls" className="hidden"
                            ref={el => reuploadRef.current = el}
                            onChange={async e => {
                              const f = e.target.files?.[0]
                              if (!f) return
                              setReloading(true)
                              try {
                                const r = await thresholdApi.reuploadDataset(ds.id, f)
                                const updated = { ...ds, has_file: true, in_memory: true, columns: r.data.columns || ds.columns, row_count: r.data.row_count || ds.row_count }
                                setDatasets(prev => prev.map(d => d.id === ds.id ? updated : d))
                                setSelectedDataset(updated)
                              } catch { }
                              setReloading(false)
                            }}
                          />
                          <button
                            onClick={e => { e.stopPropagation(); reuploadRef.current?.click() }}
                            className="text-xs font-medium text-amber-700 border border-amber-300 rounded px-2 py-1 hover:bg-amber-100">
                            Upload file
                          </button>
                        </>
                      )}
                      <button onClick={e => handleDeleteDataset(ds.id, e)}
                        className="text-slate-300 hover:text-red-500 text-xs px-2">✕</button>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {selectedDataset && (
        <div className={`border rounded-lg px-4 py-3 ${reloading ? 'border-amber-200 bg-amber-50' : 'border-teal-200 bg-teal-50'}`}>
          <p className={`text-sm font-medium ${reloading ? 'text-amber-800' : 'text-teal-800'}`}>
            {reloading ? 'Loading dataset into memory…' : `Selected: ${selectedDataset.name}`}
          </p>
          {!reloading && (
            <p className="text-teal-600 text-xs mt-0.5">
              {selectedDataset.row_count?.toLocaleString()} rows · Columns: {columns.join(', ')}
            </p>
          )}
        </div>
      )}

      <div className="flex justify-end">
        <button disabled={!selectedDataset} onClick={() => setStep(1)}
          className="px-5 py-2 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700 disabled:opacity-40">
          Next: Scenario →
        </button>
      </div>
    </div>
  )

  const renderStep1 = () => (
    <div className="space-y-6">
      <div className="flex gap-1 bg-slate-100 rounded-lg p-1 w-fit">
        {[['library', 'Library'], ['manual', 'Manual filters'], ['ai', 'AI Prompt']].map(([k, label]) => (
          <button key={k} onClick={() => setScenarioMode(k)}
            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors
              ${scenarioMode === k ? 'bg-white text-teal-700 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}>
            {label}
          </button>
        ))}
      </div>

      {scenarioMode === 'library' && (
        <div className="space-y-3">
          {SCENARIO_LIBRARY.map(sc => (
            <div key={sc.id} onClick={() => applyLibrary(sc)}
              className={`border rounded-xl p-4 cursor-pointer transition-colors
                ${selectedLibId === sc.id ? 'border-teal-500 bg-teal-50' : 'border-slate-200 hover:border-teal-300 hover:bg-slate-50'}`}>
              <p className="font-semibold text-slate-800 text-sm">{sc.name}</p>
              <p className="text-slate-500 text-xs mt-1">{sc.description}</p>
              {selectedLibId === sc.id && sc.hint && (
                <p className="text-teal-700 text-xs mt-2 bg-teal-50 rounded px-2 py-1">{sc.hint}</p>
              )}
            </div>
          ))}
          <p className="text-xs text-slate-400">Library scenarios are templates — no filters applied. Add filters in Manual mode if needed.</p>
        </div>
      )}

      {scenarioMode === 'manual' && (
        <div className="space-y-3">
          <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Filter conditions</h3>
          {simpleFilters.map((f, i) => (
            <div key={i} className="flex items-center gap-2">
              <div className="w-14 text-xs text-center text-slate-400 shrink-0">
                {i === 0 ? 'WHERE' : (
                  <select value={f.logic} onChange={e => updateFilter(i, 'logic', e.target.value)}
                    className="text-xs border border-slate-200 rounded px-1 py-1 bg-white w-14">
                    <option>AND</option>
                    <option>OR</option>
                  </select>
                )}
              </div>
              <select value={f.column} onChange={e => updateFilter(i, 'column', e.target.value)}
                className="text-xs border border-slate-200 rounded px-2 py-1.5 bg-white flex-1 min-w-0">
                <option value="">-- column --</option>
                {columns.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
              <select value={f.operator} onChange={e => updateFilter(i, 'operator', e.target.value)}
                className="text-xs border border-slate-200 rounded px-2 py-1.5 bg-white w-24">
                {['=','!=','>','>=','<','<=','contains','in'].map(op => <option key={op} value={op}>{op}</option>)}
              </select>
              <input value={f.value} onChange={e => updateFilter(i, 'value', e.target.value)}
                placeholder="value"
                className="text-xs border border-slate-200 rounded px-2 py-1.5 flex-1 min-w-0" />
              <button onClick={() => removeFilter(i)} className="text-slate-300 hover:text-red-400 text-sm shrink-0">✕</button>
            </div>
          ))}
          <div className="flex gap-3 pt-1">
            <button onClick={addFilter} className="text-xs text-teal-600 hover:text-teal-700 font-medium">+ Add condition</button>
            <button onClick={handleSaveScenario} className="text-xs text-slate-600 font-medium border border-slate-200 rounded px-3 py-1 hover:bg-slate-50">Save scenario</button>
          </div>
        </div>
      )}

      {scenarioMode === 'ai' && (
        <div className="space-y-3">
          <textarea value={aiPrompt} onChange={e => setAiPrompt(e.target.value)} rows={4}
            placeholder="Describe in plain English, e.g. 'Wire transfers over $5,000 to Russia or Iran'"
            className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2.5 resize-none focus:outline-none focus:border-teal-400" />
          <button onClick={handleAiScenario} disabled={aiLoading || !aiPrompt.trim()}
            className="px-4 py-2 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700 disabled:opacity-40">
            {aiLoading ? 'Generating…' : 'Generate filters with AI'}
          </button>
        </div>
      )}

      {activeScenario && (
        <div className="border border-slate-200 rounded-lg p-4 bg-slate-50">
          <div className="flex items-center justify-between mb-2">
            <p className="text-xs font-semibold text-slate-500">Active: {activeScenario.name}</p>
            <button onClick={handlePreviewScenario} className="text-xs text-teal-600 hover:text-teal-700">Preview match count</button>
          </div>
          {scenarioPreview && (
            <p className="text-sm text-slate-700">
              Matches <span className="font-bold text-teal-700">{scenarioPreview.match_count?.toLocaleString()}</span> of{' '}
              {scenarioPreview.total_rows?.toLocaleString()} rows ({scenarioPreview.match_pct?.toFixed(1)}%)
            </p>
          )}
        </div>
      )}

      <div className="flex justify-between">
        <button onClick={() => setStep(0)} className="px-4 py-2 text-sm text-slate-500 hover:text-slate-700">← Back</button>
        <button onClick={() => setStep(2)}
          className="px-5 py-2 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700">
          Next: Configure →
        </button>
      </div>
    </div>
  )

  const renderStep2 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Analysis mode</h3>
        <div className="flex gap-3">
          {[
            ['single', 'Per-transaction', 'Flag individual transactions above the threshold'],
            ['aggregate', 'Aggregated', 'Sum transactions per entity/account, then flag entities whose total exceeds the threshold'],
          ].map(([k, label, desc]) => (
            <div key={k} onClick={() => setAnalysisType(k)}
              className={`flex-1 border rounded-xl p-4 cursor-pointer transition-colors
                ${analysisType === k ? 'border-teal-500 bg-teal-50' : 'border-slate-200 hover:border-teal-300'}`}>
              <p className="font-semibold text-sm text-slate-800">{label}</p>
              <p className="text-xs text-slate-500 mt-1">{desc}</p>
            </div>
          ))}
        </div>
      </div>

      {analysisType === 'aggregate' && (
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Entity column <span className="text-slate-400 font-normal">(group by)</span></label>
            <select value={aggKey} onChange={e => setAggKey(e.target.value)}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white">
              <option value="">-- select --</option>
              {columns.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Amount column <span className="text-slate-400 font-normal">(to aggregate)</span></label>
            <select value={aggAmount} onChange={e => { setAggAmount(e.target.value); setParamColumn(e.target.value) }}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white">
              <option value="">-- select --</option>
              {columns.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Date column <span className="text-slate-400 font-normal">(for rolling window)</span></label>
            <select value={aggDate} onChange={e => setAggDate(e.target.value)}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white">
              <option value="">-- select --</option>
              {columns.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Rolling window</label>
            <select value={aggPeriod} onChange={e => setAggPeriod(e.target.value)}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white">
              <option value="none">No window (lifetime total)</option>
              <option value="daily">Daily</option>
              <option value="rolling_7">7-day rolling</option>
              <option value="rolling_30">30-day rolling</option>
              <option value="rolling_90">90-day rolling</option>
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Aggregation function</label>
            <select value={aggFunction} onChange={e => setAggFunction(e.target.value)}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white">
              {['SUM','COUNT','AVG','MAX','MIN'].map(fn => <option key={fn} value={fn}>{fn}</option>)}
            </select>
          </div>
        </div>
      )}

      {analysisType === 'single' && (
        <div>
          <label className="block text-xs font-medium text-slate-600 mb-1">Column to analyze</label>
          <select value={paramColumn} onChange={e => setParamColumn(e.target.value)}
            className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white">
            <option value="">-- select --</option>
            {columns.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      )}

      <div className="flex justify-between">
        <button onClick={() => setStep(1)} className="px-4 py-2 text-sm text-slate-500 hover:text-slate-700">← Back</button>
        <button onClick={handleRunAnalysis}
          disabled={analysisLoading || (analysisType === 'aggregate' ? !aggKey || !aggAmount : !paramColumn)}
          className="px-5 py-2 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700 disabled:opacity-40">
          {analysisLoading ? 'Analyzing…' : 'Run Analysis →'}
        </button>
      </div>
    </div>
  )

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

  const renderStep3 = () => {
    if (!analysisResult) return null
    const { stats, tranches, cdf, categorical } = analysisResult

    return (
      <div className="space-y-5">
        <div className="flex items-center gap-4 text-xs text-slate-500">
          <span>Column: <strong className="text-slate-700">{analysisResult.column}</strong></span>
          <span>Matched: <strong className="text-teal-700">{analysisResult.matched_rows?.toLocaleString()}</strong> of {analysisResult.original_rows?.toLocaleString()} rows</span>
        </div>

        {/* Row 1: General statistics cards */}
        <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
          <div className="px-5 py-3 border-b border-slate-100 bg-slate-50">
            <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">General statistics</p>
          </div>
          <div className="grid grid-cols-8 divide-x divide-slate-100">
            {[
              ['Transaction count', stats.count?.toLocaleString(),          'text-slate-800',  ''],
              ['Mean',              fmtFull(stats.mean),                    'text-slate-800',  ''],
              ['Median',            fmtFull(stats.median),                  'text-slate-800',  ''],
              ['Std deviation',     fmtFull(stats.std),                     'text-slate-800',  ''],
              ['Min',               fmtFull(stats.min),                     'text-slate-800',  ''],
              ['Max',               fmtFull(stats.max),                     'text-slate-800',  ''],
              ['Mild outlier ↑',    fmtFull(stats.outlier_mild_upper),      'text-yellow-700', ''],
              ['Extreme outlier ↑', fmtFull(stats.outlier_extreme_upper),   'text-red-600',    ''],
            ].map(([label, val, color, bg]) => (
              <div key={label} className={`px-4 py-4 ${bg}`}>
                <p className="text-xs text-slate-400 mb-1 whitespace-nowrap">{label}</p>
                <p className={`text-sm font-semibold ${color}`}>{val}</p>
              </div>
            ))}
          </div>
        </div>

        {/* Row 2: Scatter | Percentiles | Tranche distribution */}
        <div className="flex gap-4 items-stretch">

          {/* 1D scatter plot */}
          <div className="flex-[1.3] bg-white border border-slate-200 rounded-xl overflow-hidden flex flex-col">
            <div className="px-4 py-3 border-b border-slate-100 bg-slate-50">
              <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">Distribution</p>
            </div>
            <div className="flex-1 px-3 py-3 flex flex-col gap-2">
              <div className="flex items-center gap-2 text-xs text-slate-400 flex-wrap">
                <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-yellow-400 inline-block"/>Mild</span>
                <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-red-500 inline-block"/>Extreme</span>
              </div>
              <ResponsiveContainer width="100%" height={340}>
                <ScatterChart margin={{ top: 10, right: 10, bottom: 10, left: 10 }}>
                  <XAxis type="number" dataKey="x" domain={[-1, 1]} hide />
                  <YAxis type="number" dataKey="y" domain={['auto', 'auto']}
                    tick={{ fontSize: 10 }} tickFormatter={v => fmtCurrency(v)} width={56} />
                  <ZAxis range={[12, 12]} />
                  <Tooltip
                    cursor={false}
                    content={({ payload }) => payload?.length
                      ? <div className="bg-white border border-slate-200 rounded px-2 py-1 text-xs shadow">{fmtCurrency(payload[0]?.value)}</div>
                      : null}
                  />
                  <Scatter
                    data={(analysisResult.sample_values || []).map(v => ({
                      x: (Math.sin(v * 1000) % 1) * 0.7,
                      y: v,
                    }))}
                    fill="#0d9488" fillOpacity={0.35} stroke="none"
                  />
                  {stats.outlier_mild_lower != null && <ReferenceLine y={stats.outlier_mild_lower} stroke="#eab308" strokeWidth={1.5} strokeDasharray="4 2" />}
                  {stats.outlier_mild_upper != null && <ReferenceLine y={stats.outlier_mild_upper} stroke="#eab308" strokeWidth={1.5} strokeDasharray="4 2" label={{ value: `Mild ${fmtCurrency(stats.outlier_mild_upper)}`, position: 'insideTopRight', fontSize: 9, fill: '#a16207' }} />}
                  {stats.outlier_extreme_lower != null && <ReferenceLine y={stats.outlier_extreme_lower} stroke="#ef4444" strokeWidth={1.5} strokeDasharray="4 2" />}
                  {stats.outlier_extreme_upper != null && <ReferenceLine y={stats.outlier_extreme_upper} stroke="#ef4444" strokeWidth={1.5} strokeDasharray="4 2" label={{ value: `Ext. ${fmtCurrency(stats.outlier_extreme_upper)}`, position: 'insideTopRight', fontSize: 9, fill: '#b91c1c' }} />}
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Percentiles */}
          <div className="w-96 shrink-0 bg-white border border-slate-200 rounded-xl overflow-hidden flex flex-col">
            <div className="px-4 py-3 border-b border-slate-100 bg-slate-50">
              <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">Percentiles</p>
            </div>
            <table className="w-full text-sm flex-1">
              <thead className="bg-slate-50 text-xs text-slate-500 uppercase tracking-wide">
                <tr>
                  <th className="px-3 py-2 text-left">Pct.</th>
                  <th className="px-2 py-2 text-right">All txns</th>
                  <th className="px-2 py-2 text-right whitespace-nowrap">Excl. mild</th>
                  <th className="px-2 py-2 text-right whitespace-nowrap">Excl. extreme</th>
                </tr>
              </thead>
              <tbody>
                {[
                  ['Min', stats.min, stats.trim_min,  stats.xtrim_min],
                  ['P25', stats.p25, stats.trim_p25,  stats.xtrim_p25],
                  ['P50', stats.p50, stats.trim_p50,  stats.xtrim_p50],
                  ['P75', stats.p75, stats.trim_p75,  stats.xtrim_p75],
                  ['P85', stats.p85, stats.trim_p85,  stats.xtrim_p85],
                  ['P90', stats.p90, stats.trim_p90,  stats.xtrim_p90],
                  ['P95', stats.p95, stats.trim_p95,  stats.xtrim_p95],
                  ['P99', stats.p99, stats.trim_p99,  stats.xtrim_p99],
                  ['Max', stats.max, stats.trim_max,  stats.xtrim_max],
                ].map(([label, all, mild, extreme], i) => (
                  <tr key={label} className={i % 2 === 0 ? 'bg-white' : 'bg-slate-50/40'}>
                    <td className="px-3 py-2 text-slate-500 text-xs font-medium">{label}</td>
                    <td className="px-2 py-2 text-right text-slate-800 tabular-nums text-xs">{fmtCurrency(all)}</td>
                    <td className="px-2 py-2 text-right text-yellow-700 tabular-nums text-xs">{fmtCurrency(mild)}</td>
                    <td className="px-2 py-2 text-right text-red-600 tabular-nums text-xs">{fmtCurrency(extreme)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Tranche distribution */}
          {tranches?.length > 0 ? (
            <div className="flex-[1.2] bg-white border border-slate-200 rounded-xl overflow-hidden flex flex-col min-w-0">
              <div className="px-4 py-3 border-b border-slate-100 bg-slate-50">
                <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">Tranche distribution</p>
              </div>
              <table className="w-full text-xs flex-1">
                <thead className="bg-slate-50 text-slate-500 uppercase tracking-wide whitespace-nowrap">
                  <tr>
                    <th className="px-4 py-2 text-left">Range</th>
                    <th className="px-3 py-2 text-right">Count</th>
                    <th className="px-3 py-2 text-right">% of rows</th>
                    <th className="px-3 py-2 text-right">Cumul. %</th>
                    <th className="px-3 py-2 w-20">Share</th>
                  </tr>
                </thead>
                <tbody>
                  {tranches.map((t, i) => (
                    <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-slate-50/40'}>
                      <td className="px-4 py-2 text-slate-700 whitespace-nowrap">{t.label}</td>
                      <td className="px-3 py-2 text-right text-slate-700 tabular-nums">{t.count?.toLocaleString()}</td>
                      <td className="px-3 py-2 text-right text-slate-700 tabular-nums">{t.pct?.toFixed(1)}%</td>
                      <td className="px-3 py-2 text-right text-slate-400 tabular-nums">{t.cumulative_pct?.toFixed(1)}%</td>
                      <td className="px-3 py-2">
                        <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
                          <div className="h-full bg-teal-500 rounded-full" style={{ width: `${t.pct ?? 0}%` }} />
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="flex-1 bg-white border border-slate-200 rounded-xl p-8 text-center text-slate-400 text-sm">
              No tranche data available.
            </div>
          )}
        </div>

        {/* CDF chart — with and without outliers */}
        {cdf?.length > 1 && (
          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
              <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">Cumulative distribution</p>
              <div className="flex items-center gap-4 text-xs text-slate-500">
                <span className="flex items-center gap-1.5"><span className="w-5 h-0.5 bg-teal-500 inline-block rounded" />All transactions</span>
                <span className="flex items-center gap-1.5"><span className="w-5 h-0.5 bg-amber-400 inline-block rounded border-dashed border-t-2 border-amber-400" />Excl. outliers</span>
              </div>
            </div>
            <div className="p-5">
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={cdf} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                  <XAxis dataKey="value" tick={{ fontSize: 11 }} tickFormatter={v => fmtCurrency(v)} />
                  <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `${v}%`} domain={[0, 100]} width={38} />
                  <Tooltip
                    formatter={(v, name) => [`${Number(v).toFixed(1)}%`, name === 'all_pct' ? 'All transactions' : 'Excl. outliers']}
                    labelFormatter={v => fmtCurrency(v)}
                  />
                  <Line type="monotone" dataKey="all_pct" stroke="#0d9488" strokeWidth={2} dot={false} name="all_pct" />
                  <Line type="monotone" dataKey="trim_pct" stroke="#f59e0b" strokeWidth={2} dot={false} strokeDasharray="5 3" name="trim_pct" connectNulls />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}

        {/* Outlier methodology footnote */}
        <div className="text-xs text-slate-400 leading-relaxed space-y-0.5">
          <p><span className="text-yellow-600 font-medium">Mild outliers</span>: values more than 2 standard deviations from the mean (outside μ ± 2σ).</p>
          <p><span className="text-red-500 font-medium">Extreme outliers</span>: values more than 3 standard deviations from the mean (outside μ ± 3σ).</p>
        </div>

        {/* Categorical distribution */}
        {categorical?.length > 0 && (
          <div className="bg-white border border-slate-200 rounded-xl p-5">
            <p className="text-xs font-semibold text-slate-600 uppercase tracking-wide mb-4">Category breakdown</p>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={categorical.slice(0, 15)} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={v => `${v}%`} />
                <YAxis type="category" dataKey="value" tick={{ fontSize: 11 }} width={100} />
                <Tooltip formatter={v => `${Number(v).toFixed(1)}%`} />
                <Bar dataKey="pct" fill="#0d9488" radius={[0, 3, 3, 0]} name="%" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        <div className="flex justify-between">
          <button onClick={() => setStep(2)} className="px-4 py-2 text-sm text-slate-500 hover:text-slate-700">← Back</button>
          <button onClick={() => setStep(4)}
            className="px-5 py-2 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700">
            Next: Simulate →
          </button>
        </div>
      </div>
    )
  }

  const renderStep4 = () => (
    <div className="space-y-6">
      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Test thresholds (up to 3)</h3>
          <button onClick={handleAutoThresholds} disabled={autoLoading}
            className="text-xs text-teal-600 font-medium border border-teal-200 rounded px-3 py-1 hover:bg-teal-50 disabled:opacity-40">
            {autoLoading ? 'Suggesting…' : 'Auto-suggest from percentiles'}
          </button>
        </div>
        <div className="flex gap-3 mb-3">
          {thresholdInputs.map((v, i) => (
            <input key={i} value={v}
              onChange={e => setThresholdInputs(prev => prev.map((x, j) => j === i ? e.target.value : x))}
              placeholder={`Threshold ${i + 1}`}
              className="flex-1 text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono"
            />
          ))}
        </div>
        <button onClick={handleSimulate} disabled={simLoading}
          className="px-5 py-2 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700 disabled:opacity-40">
          {simLoading ? 'Running simulation…' : 'Run simulation'}
        </button>
      </div>

      {simResult && (
        <>
          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div className="px-5 py-3 border-b border-slate-100 bg-slate-50">
              <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">Threshold comparison</p>
            </div>
            <table className="w-full text-sm">
              <thead className="bg-slate-50 text-xs text-slate-500 uppercase tracking-wide">
                <tr>
                  <th className="px-5 py-2 text-left">Threshold</th>
                  <th className="px-5 py-2 text-right whitespace-nowrap">Pctile.</th>
                  <th className="px-5 py-2 text-right">Alerts</th>
                  <th className="px-5 py-2 text-right whitespace-nowrap">% of txns</th>
                  <th className="px-5 py-2 text-right whitespace-nowrap">% of total $</th>
                  <th className="px-5 py-2 text-right whitespace-nowrap">Est. monthly</th>
                </tr>
              </thead>
              <tbody>
                {simResult.results.map((r, i) => {
                  // Percentile = % of txns BELOW threshold = 100 - % alerted
                  const pctile = r.events_pct != null ? 100 - r.events_pct : null
                  return (
                  <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-slate-50/50'}>
                    <td className="px-5 py-2 font-mono font-semibold text-slate-700">{fmtCurrency(r.threshold)}</td>
                    <td className="px-5 py-2 text-right text-slate-400 tabular-nums">{pctile != null ? `P${pctile.toFixed(0)}` : '—'}</td>
                    <td className="px-5 py-2 text-right">{r.alert_count?.toLocaleString()}</td>
                    <td className="px-5 py-2 text-right text-slate-500">{r.events_pct != null ? `${r.events_pct.toFixed(1)}%` : '—'}</td>
                    <td className="px-5 py-2 text-right">{r.volume_pct != null ? `${r.volume_pct.toFixed(1)}%` : '—'}</td>
                    <td className="px-5 py-2 text-right">{r.monthly_alerts?.toLocaleString() ?? '—'}</td>
                  </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {simResult.results.length > 1 && (
            <div className="flex gap-4">
              {/* Left: Alert counts */}
              <div className="flex-1 bg-white border border-slate-200 rounded-xl p-5">
                <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide mb-4">Alert counts by threshold</p>
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={simResult.results} barCategoryGap="35%">
                    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                    <XAxis dataKey="threshold" tick={{ fontSize: 11 }} tickFormatter={v => fmtCurrency(v)} />
                    <YAxis tick={{ fontSize: 11 }} width={40} />
                    <Tooltip
                      formatter={(v, name) => [v?.toLocaleString(), name]}
                      labelFormatter={v => `Threshold: ${fmtCurrency(v)}`}
                    />
                    <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 11 }} />
                    <Bar dataKey="alert_count" fill="#0d9488" name="Total alerts" radius={[3, 3, 0, 0]} />
                    <Bar dataKey="monthly_alerts" fill="#7c3aed" name="Est. monthly" radius={[3, 3, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              {/* Right: Coverage % */}
              <div className="flex-1 bg-white border border-slate-200 rounded-xl p-5">
                <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide mb-4">Coverage by threshold</p>
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={simResult.results} barCategoryGap="35%">
                    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                    <XAxis dataKey="threshold" tick={{ fontSize: 11 }} tickFormatter={v => fmtCurrency(v)} />
                    <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `${v}%`} domain={[0, 100]} width={40} />
                    <Tooltip
                      formatter={(v, name) => [`${Number(v).toFixed(1)}%`, name]}
                      labelFormatter={v => `Threshold: ${fmtCurrency(v)}`}
                    />
                    <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 11 }} />
                    <Bar dataKey="events_pct" fill="#f59e0b" name="% of txns" radius={[3, 3, 0, 0]} />
                    <Bar dataKey="volume_pct" fill="#0ea5e9" name="% of total $" radius={[3, 3, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {simResult.recommendation && (() => {
            const rec = simResult.recommendation
            return (
              <div className="bg-amber-50 border border-amber-200 rounded-xl px-5 py-4">
                <p className="text-xs font-semibold text-amber-700 uppercase tracking-wide mb-1">Recommendation</p>
                <p className="text-sm text-amber-900">
                  Set threshold at <span className="font-semibold">{fmtCurrency(rec.threshold)}</span>, capturing{' '}
                  <span className="font-semibold">{rec.volume_pct?.toFixed(1)}%</span> of total dollar volume and{' '}
                  <span className="font-semibold">{rec.events_pct?.toFixed(1)}%</span> of transactions,{' '}
                  generating an estimated <span className="font-semibold">{rec.monthly_alerts?.toLocaleString()}</span> monthly alerts.
                </p>
              </div>
            )
          })()}
        </>
      )}

      <div className="flex justify-between">
        <button onClick={() => setStep(3)} className="px-4 py-2 text-sm text-slate-500 hover:text-slate-700">← Back</button>
        <button onClick={() => { setStep(5); handleGenerateReport() }}
          className="px-5 py-2 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700">
          Next: Report →
        </button>
      </div>
    </div>
  )

  const renderStep5 = () => (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide">AI-generated report</h3>
        <div className="flex gap-2">
          <button onClick={handleGenerateReport} disabled={reportLoading}
            className="text-xs text-teal-600 font-medium border border-teal-200 rounded px-3 py-1 hover:bg-teal-50 disabled:opacity-40">
            {reportLoading ? 'Generating…' : 'Regenerate'}
          </button>
          {reportText && (
            <button onClick={handleDownloadReport}
              className="text-xs text-slate-600 font-medium border border-slate-200 rounded px-3 py-1 hover:bg-slate-50">
              Download
            </button>
          )}
        </div>
      </div>

      {reportLoading
        ? <div className="bg-white border border-slate-200 rounded-xl p-8 text-center">
            <p className="text-sm text-slate-400 animate-pulse">Generating report with AI…</p>
          </div>
        : reportText
          ? <textarea value={reportText} onChange={e => setReportText(e.target.value)} rows={24}
              className="w-full text-sm font-mono border border-slate-200 rounded-xl px-4 py-3 resize-y focus:outline-none focus:border-teal-400" />
          : <div className="bg-white border border-slate-200 rounded-xl p-8 text-center">
              <p className="text-sm text-slate-400">No report yet — click Regenerate to create one.</p>
            </div>
      }

      <div className="flex justify-between">
        <button onClick={() => setStep(4)} className="px-4 py-2 text-sm text-slate-500 hover:text-slate-700">← Back</button>
        <button onClick={handleReset}
          className="px-5 py-2 bg-slate-100 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-200">
          Start over
        </button>
      </div>
    </div>
  )

  const stepRenders = [renderStep0, renderStep1, renderStep2, renderStep3, renderStep4, renderStep5]

  return (
    <div className="w-full py-10 px-6">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-slate-900 mb-1">Threshold Setting</h1>
        <p className="text-slate-500 text-sm">Calibrate AML monitoring thresholds using historical transaction data and statistical analysis.</p>
      </div>

      <StepIndicator current={step} />

      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-8">
        {stepRenders[step]?.()}
      </div>
    </div>
  )
}
