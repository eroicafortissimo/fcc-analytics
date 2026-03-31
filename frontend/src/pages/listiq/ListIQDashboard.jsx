import { useState, useEffect, useCallback } from 'react'
import { Link } from 'react-router-dom'
import { listiqApi } from '../../api/listiqApi'

const TYPE_COLORS = {
  ADDITION: { bg: 'bg-green-50', border: 'border-l-green-500', badge: 'bg-green-100 text-green-700' },
  DELETION: { bg: 'bg-red-50', border: 'border-l-red-500', badge: 'bg-red-100 text-red-700' },
  MODIFICATION: { bg: 'bg-amber-50', border: 'border-l-amber-500', badge: 'bg-amber-100 text-amber-700' },
}

function Badge({ type }) {
  const c = TYPE_COLORS[type] || { badge: 'bg-slate-100 text-slate-600' }
  return <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${c.badge}`}>{type}</span>
}

function DiffPanel({ label, data, color, changedFields }) {
  if (!data) return <div className={`flex-1 rounded-lg p-4 bg-slate-50 text-slate-400 text-sm`}>{label}: no data</div>
  const fields = [
    ['Primary Name', 'primary_name'],
    ['Record Type', 'record_type'],
    ['AKAs', 'akas'],
    ['IDs', 'ids'],
    ['Addresses', 'addresses'],
    ['Programs', 'programs'],
  ]
  return (
    <div className={`flex-1 rounded-lg border ${color} p-4 space-y-2`}>
      <div className="text-xs font-semibold text-slate-500 uppercase mb-3">{label}</div>
      {fields.map(([title, key]) => {
        const val = data[key]
        const isChanged = changedFields?.includes(key)
        const display = Array.isArray(val)
          ? val.length ? val.join(', ') : '—'
          : val || '—'
        return (
          <div key={key} className={`text-sm rounded px-2 py-1 ${isChanged ? 'bg-amber-100 font-medium' : 'text-slate-600'}`}>
            <span className="text-xs text-slate-400 mr-2">{title}</span>
            {display}
          </div>
        )
      })}
    </div>
  )
}

function ChangeDetail({ change, onClose }) {
  if (!change) return null
  const c = TYPE_COLORS[change.change_type] || {}
  const headerColors = {
    ADDITION: 'bg-green-50 text-green-800 border-green-200',
    DELETION: 'bg-red-50 text-red-800 border-red-200',
    MODIFICATION: 'bg-amber-50 text-amber-800 border-amber-200',
  }
  const headerLabels = {
    ADDITION: 'New Record Added',
    DELETION: 'Record Removed',
    MODIFICATION: 'Record Modified',
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={onClose}>
      <div
        className="bg-white rounded-2xl shadow-2xl w-full max-w-3xl mx-4 max-h-[90vh] overflow-y-auto"
        onClick={e => e.stopPropagation()}
      >
        <div className={`rounded-t-2xl border-b px-6 py-4 ${headerColors[change.change_type] || 'bg-slate-50'}`}>
          <div className="flex items-center justify-between">
            <div>
              <h2 className="font-bold text-lg">{headerLabels[change.change_type]}</h2>
              <p className="text-sm opacity-70 font-mono mt-0.5">UID: {change.record_uid}</p>
            </div>
            <button onClick={onClose} className="text-slate-500 hover:text-slate-800 text-2xl leading-none">×</button>
          </div>
        </div>

        <div className="p-6">
          {change.change_type === 'MODIFICATION' ? (
            <div className="flex gap-4">
              <DiffPanel label="Yesterday" data={change.before_data} color="border-slate-200" changedFields={change.modification_fields} />
              <DiffPanel label="Today" data={change.after_data} color="border-amber-300" changedFields={change.modification_fields} />
            </div>
          ) : change.change_type === 'ADDITION' ? (
            <DiffPanel label="New Record" data={change.after_data} color="border-green-300" />
          ) : (
            <DiffPanel label="Removed Record" data={change.before_data} color="border-red-300" />
          )}
        </div>
      </div>
    </div>
  )
}

export default function ListIQDashboard() {
  const [status, setStatus] = useState(null)
  const [summary, setSummary] = useState(null)
  const [changes, setChanges] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [selectedDate, setSelectedDate] = useState('')
  const [availableDates, setAvailableDates] = useState([])
  const [filterType, setFilterType] = useState('')
  const [selectedChange, setSelectedChange] = useState(null)
  const [syncing, setSyncing] = useState(false)
  const [loading, setLoading] = useState(false)

  const PAGE_SIZE = 25

  const fetchStatus = useCallback(async () => {
    const { data } = await listiqApi.syncStatus()
    setStatus(data)
    return data
  }, [])

  const fetchDates = useCallback(async () => {
    const { data } = await listiqApi.availableDates()
    setAvailableDates(data)
    return data
  }, [])

  const fetchSummary = useCallback(async (date) => {
    if (!date) return
    const { data } = await listiqApi.changesSummary(date)
    setSummary(data)
  }, [])

  const fetchChanges = useCallback(async (date, type, pg) => {
    if (!date) { setChanges([]); setTotal(0); return }
    setLoading(true)
    const params = { change_date: date, page: pg, page_size: PAGE_SIZE }
    if (type) params.change_type = type
    const { data } = await listiqApi.changes(params)
    setChanges(data.items)
    setTotal(data.total)
    setLoading(false)
  }, [])

  useEffect(() => {
    const init = async () => {
      const s = await fetchStatus()
      const dates = await fetchDates()
      const initDate = dates[0] || s.snapshot_date || ''
      if (initDate) {
        setSelectedDate(initDate)
        await fetchSummary(initDate)
        await fetchChanges(initDate, '', 1)
      }
    }
    init()
  }, [])

  const handleSync = async () => {
    setSyncing(true)
    try {
      await listiqApi.triggerSync()
      const s = await fetchStatus()
      const dates = await fetchDates()
      const date = dates[0] || s.snapshot_date || selectedDate
      setSelectedDate(date)
      await fetchSummary(date)
      await fetchChanges(date, filterType, 1)
      setPage(1)
    } catch { }
    setSyncing(false)
  }

  const handleDateChange = async (date) => {
    setSelectedDate(date)
    setPage(1)
    setFilterType('')
    await fetchSummary(date)
    await fetchChanges(date, '', 1)
  }

  const handleTypeFilter = async (type) => {
    const newType = type === filterType ? '' : type
    setFilterType(newType)
    setPage(1)
    await fetchChanges(selectedDate, newType, 1)
  }

  const handlePage = async (pg) => {
    setPage(pg)
    await fetchChanges(selectedDate, filterType, pg)
  }

  const fmtDate = (iso) => {
    if (!iso) return '—'
    // Append T12:00:00 so date-only strings aren't parsed as UTC midnight (which shifts the day in US timezones)
    const d = new Date(iso.length === 10 ? iso + 'T12:00:00' : iso)
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
  }

  const totalPages = Math.ceil(total / PAGE_SIZE)

  return (
    <div className="max-w-screen-xl mx-auto space-y-5">
      {selectedChange && <ChangeDetail change={selectedChange} onClose={() => setSelectedChange(null)} />}

      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-xs text-slate-400 mb-1">
            <Link to="/screeniq" className="hover:text-slate-600">← ScreenIQ</Link>
          </div>
          <h1 className="text-2xl font-bold text-slate-900">List Update Manager</h1>
          <p className="text-sm text-slate-500 mt-0.5">Daily sanctions watchlist change intelligence</p>
        </div>
        <div className="flex items-center gap-3">
          <Link to="/screeniq/list-update-manager/settings" className="text-sm text-slate-500 hover:text-slate-700 px-3 py-2 rounded-lg border border-slate-200 hover:border-slate-300 transition-colors">
            ⚙ Settings
          </Link>
          <button
            onClick={handleSync}
            disabled={syncing}
            className="px-4 py-2 text-sm font-medium rounded-lg bg-[#1a2744] hover:bg-[#243560] disabled:bg-slate-400 text-white transition-colors"
          >
            {syncing ? 'Syncing…' : 'Sync Now'}
          </button>
        </div>
      </div>

      {/* Status bar */}
      {status && (
        <div className="text-xs text-slate-500 bg-white rounded-lg border border-slate-200 px-4 py-2 flex items-center gap-4">
          <span className={`w-2 h-2 rounded-full ${status.synced ? 'bg-green-400' : 'bg-amber-400'}`} />
          {status.synced
            ? `Last synced: ${fmtDate(status.snapshot_date)} · ${status.record_count?.toLocaleString()} records`
            : 'No sync data yet — click Sync Now to download'}
        </div>
      )}

      {/* Date picker + summary cards */}
      <div className="flex flex-wrap items-start gap-4">
        <div className="flex items-center gap-2">
          <label className="text-sm text-slate-500 font-medium">Date</label>
          <input
            type="date"
            value={selectedDate}
            onChange={e => handleDateChange(e.target.value)}
            className="border border-slate-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        {availableDates.length > 0 && (
          <div className="flex gap-2 flex-wrap">
            {availableDates.slice(0, 5).map(d => (
              <button
                key={d}
                onClick={() => handleDateChange(d)}
                className={`text-xs px-3 py-1.5 rounded-lg border transition-colors ${selectedDate === d ? 'bg-[#1a2744] text-white border-[#1a2744]' : 'border-slate-200 text-slate-600 hover:border-slate-400'}`}
              >
                {fmtDate(d)}
              </button>
            ))}
          </div>
        )}
      </div>

      {summary && (
        <div className="grid grid-cols-3 gap-4">
          {[
            { type: 'ADDITION', label: 'Additions', count: summary.additions, color: 'border-t-green-500 text-green-700' },
            { type: 'DELETION', label: 'Deletions', count: summary.deletions, color: 'border-t-red-500 text-red-700' },
            { type: 'MODIFICATION', label: 'Modifications', count: summary.modifications, color: 'border-t-amber-500 text-amber-700' },
          ].map(({ type, label, count, color }) => (
            <button
              key={type}
              onClick={() => handleTypeFilter(type)}
              className={`bg-white rounded-xl border border-slate-200 border-t-4 ${color} shadow-sm p-4 text-left transition-all ${filterType === type ? 'ring-2 ring-blue-500' : 'hover:shadow-md'}`}
            >
              <div className={`text-3xl font-bold ${color.split(' ')[1]}`}>{count.toLocaleString()}</div>
              <div className="text-sm text-slate-500 mt-1">{label}</div>
            </button>
          ))}
        </div>
      )}

      {/* Change log table */}
      {loading ? (
        <div className="text-center py-12 text-slate-400">Loading changes…</div>
      ) : changes.length === 0 ? (
        <div className="text-center py-12 text-slate-400 bg-white rounded-xl border border-slate-200">
          {selectedDate ? 'No changes recorded for this date.' : 'Select a date to view changes.'}
        </div>
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-slate-500 text-xs uppercase tracking-wide">
              <tr>
                <th className="px-4 py-3 text-left">Type</th>
                <th className="px-4 py-3 text-left">UID</th>
                <th className="px-4 py-3 text-left">Primary Name</th>
                <th className="px-4 py-3 text-left">Record Type</th>
                <th className="px-4 py-3 text-left">Modified Fields</th>
                <th className="px-4 py-3 text-left">Programs</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {changes.map(c => {
                const col = TYPE_COLORS[c.change_type] || {}
                const data = c.after_data || c.before_data || {}
                return (
                  <tr
                    key={c.id}
                    onClick={() => setSelectedChange(c)}
                    className={`cursor-pointer border-l-4 ${col.border || ''} ${col.bg || ''} hover:brightness-95 transition-all`}
                  >
                    <td className="px-4 py-3"><Badge type={c.change_type} /></td>
                    <td className="px-4 py-3 font-mono text-xs text-slate-500">{c.record_uid}</td>
                    <td className="px-4 py-3 font-medium text-slate-800">{data.primary_name || '—'}</td>
                    <td className="px-4 py-3 text-slate-500">{data.record_type || '—'}</td>
                    <td className="px-4 py-3 text-slate-500 text-xs">{c.modification_fields?.join(', ') || '—'}</td>
                    <td className="px-4 py-3 text-slate-500 text-xs">{Array.isArray(data.programs) ? data.programs.slice(0, 2).join(', ') : '—'}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="px-4 py-3 border-t border-slate-100 flex items-center justify-between text-sm text-slate-500">
              <span>{total.toLocaleString()} total changes</span>
              <div className="flex gap-2">
                <button disabled={page <= 1} onClick={() => handlePage(page - 1)}
                  className="px-3 py-1 rounded border border-slate-200 disabled:opacity-40 hover:bg-slate-50">←</button>
                <span className="px-3 py-1">Page {page} of {totalPages}</span>
                <button disabled={page >= totalPages} onClick={() => handlePage(page + 1)}
                  className="px-3 py-1 rounded border border-slate-200 disabled:opacity-40 hover:bg-slate-50">→</button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
