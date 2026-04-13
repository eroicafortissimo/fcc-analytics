import { useState, useRef } from 'react'

const TABS = ['Before Rule', 'During Rule', 'After Rule']

function TabBar({ active, onChange }) {
  return (
    <div className="flex items-center gap-0 border-b border-slate-200 mb-6">
      {TABS.map((label, i) => (
        <button
          key={i}
          onClick={() => onChange(i)}
          className={`px-5 py-2.5 text-sm font-medium border-b-2 transition-colors ${
            active === i
              ? 'border-blue-600 text-blue-700'
              : 'border-transparent text-slate-500 hover:text-slate-700'
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  )
}

function downloadText(filename, content) {
  const a = document.createElement('a')
  a.href = URL.createObjectURL(new Blob([content], { type: 'text/plain' }))
  a.download = filename
  a.click()
  URL.revokeObjectURL(a.href)
}

function downloadCSV(filename, rows, headers) {
  const escape = v => `"${String(v ?? '').replace(/"/g, '""')}"`
  const lines = [headers.map(escape).join(','), ...rows.map(r => headers.map(h => escape(r[h])).join(','))]
  const a = document.createElement('a')
  a.href = URL.createObjectURL(new Blob([lines.join('\n')], { type: 'text/csv' }))
  a.download = filename
  a.click()
  URL.revokeObjectURL(a.href)
}

// ── Before Rule ───────────────────────────────────────────────────────────────
function BeforeRule() {
  const [description, setDescription] = useState('')
  const [ruleType, setRuleType] = useState('suppression')
  const [generating, setGenerating] = useState(false)
  const [result, setResult] = useState(null)
  const [copied, setCopied] = useState(false)

  const handleGenerate = async () => {
    if (!description.trim()) return
    setGenerating(true)
    setResult(null)
    await new Promise(r => setTimeout(r, 900))
    setResult({
      rule: `// Generated ${ruleType} rule\n// Description: ${description}\n\n// Rule logic will be generated here once rule format is configured`,
      notes: 'Rule format configuration pending. Connect the backend AI endpoint to generate real rules.',
    })
    setGenerating(false)
  }

  const handleCopy = () => {
    if (!result) return
    navigator.clipboard.writeText(result.rule)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className="space-y-5">
      <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4">
        <div>
          <label className="block text-xs font-semibold text-slate-700 mb-1.5">Rule type</label>
          <div className="flex gap-3">
            {[
              { value: 'suppression', label: 'Suppression rule' },
              { value: 'alert', label: 'Alert rule' },
              { value: 'filter', label: 'Filter rule' },
            ].map(opt => (
              <label key={opt.value} className="flex items-center gap-2 text-sm text-slate-600 cursor-pointer">
                <input
                  type="radio"
                  name="ruleType"
                  value={opt.value}
                  checked={ruleType === opt.value}
                  onChange={() => setRuleType(opt.value)}
                  className="accent-blue-600"
                />
                {opt.label}
              </label>
            ))}
          </div>
        </div>
        <div>
          <label className="block text-xs font-semibold text-slate-700 mb-1.5">
            Describe the rule you want to create
          </label>
          <textarea
            value={description}
            onChange={e => setDescription(e.target.value)}
            placeholder="e.g. Suppress alerts where the counterparty name matches a known internal entity list and the transaction amount is below $10,000..."
            rows={5}
            className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2.5 resize-none focus:outline-none focus:ring-1 focus:ring-blue-400"
          />
        </div>
        <div className="flex justify-end">
          <button
            onClick={handleGenerate}
            disabled={!description.trim() || generating}
            className="px-5 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-40 transition-colors"
          >
            {generating ? 'Generating…' : 'Generate rule'}
          </button>
        </div>
      </div>

      {result && (
        <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
          <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
            <p className="text-xs font-semibold text-blue-700 uppercase tracking-wide">Generated rule</p>
            <div className="flex gap-2">
              <button
                onClick={() => downloadText('generated-rule.txt', result.rule)}
                className="text-xs text-slate-500 hover:text-slate-700 border border-slate-200 rounded px-2.5 py-1"
              >
                Export
              </button>
              <button
                onClick={handleCopy}
                className="text-xs text-slate-500 hover:text-slate-700 border border-slate-200 rounded px-2.5 py-1"
              >
                {copied ? '✓ Copied' : 'Copy'}
              </button>
            </div>
          </div>
          <pre className="px-5 py-4 text-xs font-mono text-slate-700 bg-slate-50 overflow-x-auto whitespace-pre-wrap">
            {result.rule}
          </pre>
          {result.notes && (
            <div className="px-5 py-3 border-t border-slate-100 bg-amber-50">
              <p className="text-xs text-amber-700">{result.notes}</p>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ── During Rule ───────────────────────────────────────────────────────────────
function DuringRule() {
  const [inputMode, setInputMode] = useState('paste')
  const [pasteText, setPasteText] = useState('')
  const [fileName, setFileName] = useState(null)
  const [interpreting, setInterpreting] = useState(false)
  const [result, setResult] = useState(null)
  const fileRef = useRef(null)

  const handleFile = (e) => {
    const f = e.target.files?.[0]
    if (!f) return
    setFileName(f.name)
    const reader = new FileReader()
    reader.onload = ev => setPasteText(ev.target.result)
    reader.readAsText(f)
  }

  const handleClear = () => {
    setPasteText('')
    setFileName(null)
    setResult(null)
    if (fileRef.current) fileRef.current.value = ''
  }

  const handleInterpret = async () => {
    const text = pasteText.trim()
    if (!text) return
    setInterpreting(true)
    setResult(null)
    await new Promise(r => setTimeout(r, 900))
    setResult({
      summary: `Parsed rule content (${text.split('\n').length} lines). Detailed interpretation will be available once rule format is configured.`,
      rules: [
        { id: 'RULE-001', name: 'Example rule', trigger: 'Pending configuration', fields: ['field1', 'field2'], action: 'suppress' },
      ],
    })
    setInterpreting(false)
  }

  const handleExportSummary = () => {
    if (!result) return
    const lines = [
      'RULE INTERPRETATION SUMMARY',
      '===========================',
      `Generated: ${new Date().toLocaleString()}`,
      '',
      'SUMMARY',
      '-------',
      result.summary,
      '',
      'RULE INVENTORY',
      '--------------',
      ...result.rules.map(r =>
        `ID: ${r.id} | Name: ${r.name} | Trigger: ${r.trigger} | Fields: ${r.fields.join(', ')} | Action: ${r.action}`
      ),
    ]
    downloadText('rule-interpretation-summary.txt', lines.join('\n'))
  }

  return (
    <div className="space-y-5">
      <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4">
        <div className="flex items-center justify-between border-b border-slate-100 pb-4">
          <div className="flex gap-3">
            {[['paste', 'Paste rule text'], ['upload', 'Upload file']].map(([val, label]) => (
              <button
                key={val}
                onClick={() => setInputMode(val)}
                className={`text-sm px-4 py-1.5 rounded-lg font-medium transition-colors ${
                  inputMode === val ? 'bg-blue-600 text-white' : 'text-slate-600 hover:bg-slate-100'
                }`}
              >
                {label}
              </button>
            ))}
          </div>
          {pasteText && (
            <button
              onClick={handleClear}
              className="text-xs text-slate-400 hover:text-red-500 border border-slate-200 rounded px-3 py-1 transition-colors"
            >
              Clear
            </button>
          )}
        </div>

        {inputMode === 'paste' ? (
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1.5">Rule content</label>
            <textarea
              value={pasteText}
              onChange={e => setPasteText(e.target.value)}
              placeholder="Paste one or more rules here…"
              rows={8}
              className="w-full text-sm font-mono border border-slate-200 rounded-lg px-3 py-2.5 resize-none focus:outline-none focus:ring-1 focus:ring-blue-400"
            />
          </div>
        ) : (
          <div className="space-y-3">
            <div
              onClick={() => fileRef.current?.click()}
              className="border-2 border-dashed border-slate-200 rounded-xl p-8 text-center cursor-pointer hover:border-blue-300 hover:bg-blue-50/30 transition-colors"
            >
              <input ref={fileRef} type="file" className="hidden" accept=".txt,.xml,.json,.csv,.rule,.rules" onChange={handleFile} />
              {fileName ? (
                <p className="text-sm font-medium text-slate-700">{fileName}</p>
              ) : (
                <>
                  <p className="text-sm font-medium text-slate-600">Click to upload rule file</p>
                  <p className="text-xs text-slate-400 mt-1">.txt, .xml, .json, .csv, .rule, .rules</p>
                </>
              )}
            </div>
            {pasteText && (
              <div className="bg-slate-50 rounded-lg px-3 py-2 text-xs text-slate-500 font-mono truncate">
                {pasteText.split('\n').length} lines loaded{fileName ? ` from ${fileName}` : ''}
              </div>
            )}
          </div>
        )}

        <div className="flex justify-end">
          <button
            onClick={handleInterpret}
            disabled={!pasteText.trim() || interpreting}
            className="px-5 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-40 transition-colors"
          >
            {interpreting ? 'Interpreting…' : 'Interpret rules'}
          </button>
        </div>
      </div>

      {result && (
        <div className="space-y-4">
          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
              <p className="text-xs font-semibold text-blue-700 uppercase tracking-wide">Summary</p>
              <button
                onClick={handleExportSummary}
                className="text-xs text-slate-500 hover:text-slate-700 border border-slate-200 rounded px-2.5 py-1"
              >
                Export summary
              </button>
            </div>
            <div className="px-5 py-4">
              <p className="text-sm text-slate-600">{result.summary}</p>
            </div>
          </div>

          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div className="px-5 py-3 border-b border-slate-100 bg-slate-50">
              <p className="text-xs font-semibold text-blue-700 uppercase tracking-wide">Rule inventory</p>
            </div>
            <div className="divide-y divide-slate-100">
              {result.rules.map((rule, i) => (
                <div key={i} className="px-5 py-4 grid grid-cols-4 gap-4 text-sm">
                  <div>
                    <p className="text-xs text-slate-400 mb-0.5">Rule ID</p>
                    <p className="font-mono font-semibold text-slate-700">{rule.id}</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400 mb-0.5">Name</p>
                    <p className="text-slate-700">{rule.name}</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400 mb-0.5">Trigger</p>
                    <p className="text-slate-600">{rule.trigger}</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400 mb-0.5">Fields impacted</p>
                    <div className="flex flex-wrap gap-1 mt-0.5">
                      {rule.fields.map(f => (
                        <span key={f} className="text-xs bg-slate-100 text-slate-600 px-2 py-0.5 rounded font-mono">{f}</span>
                      ))}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ── After Rule ────────────────────────────────────────────────────────────────
function AfterRule() {
  const [fileName, setFileName] = useState(null)
  const [pasteText, setPasteText] = useState('')
  const [inputMode, setInputMode] = useState('upload')
  const [generating, setGenerating] = useState(false)
  const [log, setLog] = useState(null)
  const fileRef = useRef(null)

  const handleFile = (e) => {
    const f = e.target.files?.[0]
    if (!f) return
    setFileName(f.name)
    const reader = new FileReader()
    reader.onload = ev => setPasteText(ev.target.result)
    reader.readAsText(f)
  }

  const handleClear = () => {
    setPasteText('')
    setFileName(null)
    setLog(null)
    if (fileRef.current) fileRef.current.value = ''
  }

  const handleGenerate = async () => {
    const text = pasteText.trim()
    if (!text) return
    setGenerating(true)
    setLog(null)
    await new Promise(r => setTimeout(r, 900))
    setLog({
      generatedAt: new Date().toLocaleString(),
      source: fileName || 'Pasted content',
      totalRules: 1,
      entries: [
        {
          id: 'RULE-001',
          name: 'Example rule',
          type: 'Suppression',
          trigger: 'Pending configuration',
          fieldsRead: ['field1', 'field2'],
          fieldsWritten: ['outcome'],
          action: 'suppress',
          notes: 'Rule format configuration required for accurate analysis.',
        },
      ],
    })
    setGenerating(false)
  }

  const handleExportLog = () => {
    if (!log) return
    downloadCSV(
      `rule-log-${new Date().toISOString().slice(0, 10)}.csv`,
      log.entries.map(r => ({
        'Rule ID': r.id,
        'Name': r.name,
        'Type': r.type,
        'Trigger': r.trigger,
        'Fields Read': r.fieldsRead.join('; '),
        'Fields Written': r.fieldsWritten.join('; '),
        'Action': r.action,
        'Notes': r.notes || '',
      })),
      ['Rule ID', 'Name', 'Type', 'Trigger', 'Fields Read', 'Fields Written', 'Action', 'Notes']
    )
  }

  const canGenerate = pasteText.trim().length > 0

  return (
    <div className="space-y-5">
      <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4">
        <div className="flex items-center justify-between border-b border-slate-100 pb-4">
          <div className="flex gap-3">
            {[['upload', 'Upload file'], ['paste', 'Paste rule text']].map(([val, label]) => (
              <button
                key={val}
                onClick={() => setInputMode(val)}
                className={`text-sm px-4 py-1.5 rounded-lg font-medium transition-colors ${
                  inputMode === val ? 'bg-blue-600 text-white' : 'text-slate-600 hover:bg-slate-100'
                }`}
              >
                {label}
              </button>
            ))}
          </div>
          {pasteText && (
            <button
              onClick={handleClear}
              className="text-xs text-slate-400 hover:text-red-500 border border-slate-200 rounded px-3 py-1 transition-colors"
            >
              Clear
            </button>
          )}
        </div>

        {inputMode === 'upload' ? (
          <div className="space-y-3">
            <div
              onClick={() => fileRef.current?.click()}
              className="border-2 border-dashed border-slate-200 rounded-xl p-8 text-center cursor-pointer hover:border-blue-300 hover:bg-blue-50/30 transition-colors"
            >
              <input ref={fileRef} type="file" className="hidden" accept=".txt,.xml,.json,.csv,.rule,.rules" onChange={handleFile} />
              {fileName ? (
                <p className="text-sm font-medium text-slate-700">{fileName}</p>
              ) : (
                <>
                  <p className="text-sm font-medium text-slate-600">Click to upload rule file</p>
                  <p className="text-xs text-slate-400 mt-1">.txt, .xml, .json, .csv, .rule, .rules</p>
                </>
              )}
            </div>
            {pasteText && (
              <div className="bg-slate-50 rounded-lg px-3 py-2 text-xs text-slate-500 font-mono truncate">
                {pasteText.split('\n').length} lines loaded{fileName ? ` from ${fileName}` : ''}
              </div>
            )}
          </div>
        ) : (
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1.5">Rule content</label>
            <textarea
              value={pasteText}
              onChange={e => setPasteText(e.target.value)}
              placeholder="Paste rule file content here…"
              rows={8}
              className="w-full text-sm font-mono border border-slate-200 rounded-lg px-3 py-2.5 resize-none focus:outline-none focus:ring-1 focus:ring-blue-400"
            />
          </div>
        )}

        <div className="flex justify-end">
          <button
            onClick={handleGenerate}
            disabled={!canGenerate || generating}
            className="px-5 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-40 transition-colors"
          >
            {generating ? 'Generating log…' : 'Generate log'}
          </button>
        </div>
      </div>

      {log && (
        <div className="space-y-4">
          <div className="bg-white border border-slate-200 rounded-xl p-5 flex flex-wrap gap-6">
            {[
              ['Source', log.source],
              ['Generated', log.generatedAt],
              ['Total rules', log.totalRules],
            ].map(([label, val]) => (
              <div key={label}>
                <p className="text-xs text-slate-400">{label}</p>
                <p className="text-sm font-semibold text-slate-700 mt-0.5">{val}</p>
              </div>
            ))}
          </div>

          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
              <p className="text-xs font-semibold text-blue-700 uppercase tracking-wide">Rule inventory log</p>
              <button
                onClick={handleExportLog}
                className="text-xs text-slate-500 hover:text-slate-700 border border-slate-200 rounded px-2.5 py-1"
              >
                Export CSV
              </button>
            </div>
            <table className="w-full text-sm">
              <thead className="bg-slate-50 text-xs text-slate-500 uppercase tracking-wide">
                <tr>
                  <th className="px-4 py-2 text-left">Rule ID</th>
                  <th className="px-4 py-2 text-left">Name</th>
                  <th className="px-4 py-2 text-left">Type</th>
                  <th className="px-4 py-2 text-left">Trigger</th>
                  <th className="px-4 py-2 text-left">Fields read</th>
                  <th className="px-4 py-2 text-left">Fields written</th>
                  <th className="px-4 py-2 text-left">Action</th>
                </tr>
              </thead>
              <tbody>
                {log.entries.map((row, i) => (
                  <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-slate-50/50'}>
                    <td className="px-4 py-3 font-mono text-xs text-slate-600">{row.id}</td>
                    <td className="px-4 py-3 font-medium text-slate-700">{row.name}</td>
                    <td className="px-4 py-3 text-slate-600">{row.type}</td>
                    <td className="px-4 py-3 text-slate-500">{row.trigger}</td>
                    <td className="px-4 py-3">
                      <div className="flex flex-wrap gap-1">
                        {row.fieldsRead.map(f => (
                          <span key={f} className="text-xs bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded font-mono">{f}</span>
                        ))}
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex flex-wrap gap-1">
                        {row.fieldsWritten.map(f => (
                          <span key={f} className="text-xs bg-violet-50 text-violet-700 px-1.5 py-0.5 rounded font-mono">{f}</span>
                        ))}
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-xs bg-slate-100 text-slate-600 px-2 py-0.5 rounded">{row.action}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {log.entries[0]?.notes && (
              <div className="px-5 py-3 border-t border-slate-100 bg-amber-50">
                <p className="text-xs text-amber-700">{log.entries[0].notes}</p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

export default function RuleAutomation() {
  const [tab, setTab] = useState(0)

  return (
    <div className="max-w-5xl mx-auto space-y-5">
      <div>
        <p className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-1">False Positive Suppression</p>
        <h1 className="text-2xl font-bold text-slate-900">Rules Manager</h1>
        <p className="text-sm text-slate-500 mt-1">
          Create, interpret, and document suppression rules using AI-assisted tooling.
        </p>
      </div>

      <div className="bg-white border border-slate-200 rounded-xl p-6">
        <TabBar active={tab} onChange={setTab} />
        {tab === 0 && <BeforeRule />}
        {tab === 1 && <DuringRule />}
        {tab === 2 && <AfterRule />}
      </div>
    </div>
  )
}
