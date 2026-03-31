import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { listiqApi } from '../../api/listiqApi'

export default function ListIQSettings() {
  const [schedule, setSchedule] = useState({ sync_hour: 6, sync_minute: 0, sync_enabled: true })
  const [history, setHistory] = useState([])
  const [saving, setSaving] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [saved, setSaved] = useState(false)
  const [syncResult, setSyncResult] = useState(null)

  useEffect(() => {
    listiqApi.syncSchedule().then(r => setSchedule(r.data)).catch(() => {})
    listiqApi.syncHistory().then(r => setHistory(r.data)).catch(() => {})
  }, [])

  const handleSave = async () => {
    setSaving(true)
    setSaved(false)
    await listiqApi.updateSchedule(schedule)
    setSaving(false)
    setSaved(true)
    setTimeout(() => setSaved(false), 3000)
  }

  const handleSync = async () => {
    setSyncing(true)
    setSyncResult(null)
    try {
      const { data } = await listiqApi.triggerSync()
      setSyncResult(data)
      const { data: hist } = await listiqApi.syncHistory()
      setHistory(hist)
    } catch (e) {
      setSyncResult({ status: 'error', error: String(e) })
    }
    setSyncing(false)
  }

  const fmtDateTime = (iso) => {
    if (!iso) return '—'
    const d = new Date(iso.length === 10 ? iso + 'T12:00:00' : iso)
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  }

  const hourOptions = Array.from({ length: 24 }, (_, i) => i)
  const minuteOptions = [0, 15, 30, 45]

  return (
    <div className="max-w-3xl mx-auto space-y-6">
      <div>
        <div className="text-xs text-slate-400 mb-1">
          <Link to="/screeniq/list-update-manager" className="hover:text-slate-600">← List Update Manager</Link>
          {' · '}
          <Link to="/" className="hover:text-slate-600">FCC Modelling and Analytics</Link>
        </div>
        <h1 className="text-2xl font-bold text-slate-900">List Update Manager Settings</h1>
      </div>

      {/* Sync Schedule */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 space-y-4">
        <h2 className="font-semibold text-slate-800">Sync Schedule</h2>
        <p className="text-sm text-slate-500">
          Current: Daily at{' '}
          <strong>{String(schedule.sync_hour).padStart(2, '0')}:{String(schedule.sync_minute).padStart(2, '0')}</strong>
          {' '}· Auto-sync is{' '}
          <strong className={schedule.sync_enabled ? 'text-green-600' : 'text-red-500'}>
            {schedule.sync_enabled ? 'enabled' : 'disabled'}
          </strong>
        </p>

        <div className="flex flex-wrap items-end gap-4">
          <div>
            <label className="block text-xs text-slate-500 mb-1">Hour</label>
            <select
              value={schedule.sync_hour}
              onChange={e => setSchedule(s => ({ ...s, sync_hour: Number(e.target.value) }))}
              className="border border-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {hourOptions.map(h => (
                <option key={h} value={h}>{String(h).padStart(2, '0')}:00</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Minute</label>
            <select
              value={schedule.sync_minute}
              onChange={e => setSchedule(s => ({ ...s, sync_minute: Number(e.target.value) }))}
              className="border border-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {minuteOptions.map(m => (
                <option key={m} value={m}>{String(m).padStart(2, '0')}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Auto-sync</label>
            <button
              onClick={() => setSchedule(s => ({ ...s, sync_enabled: !s.sync_enabled }))}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${schedule.sync_enabled ? 'bg-green-100 text-green-700 hover:bg-green-200' : 'bg-red-100 text-red-700 hover:bg-red-200'}`}
            >
              {schedule.sync_enabled ? 'Enabled' : 'Disabled'}
            </button>
          </div>
          <button
            onClick={handleSave}
            disabled={saving}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-[#1a2744] hover:bg-[#243560] text-white transition-colors disabled:opacity-50"
          >
            {saving ? 'Saving…' : saved ? 'Saved ✓' : 'Save'}
          </button>
        </div>
      </div>

      {/* Manual sync */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 space-y-4">
        <h2 className="font-semibold text-slate-800">Manual Sync</h2>
        <p className="text-sm text-slate-500">Trigger a sync immediately regardless of schedule.</p>
        <button
          onClick={handleSync}
          disabled={syncing}
          className="px-4 py-2 rounded-lg text-sm font-medium bg-[#1a2744] hover:bg-[#243560] text-white transition-colors disabled:opacity-50"
        >
          {syncing ? 'Syncing…' : 'Sync Now'}
        </button>
        {syncResult && (
          <div className={`text-sm rounded-lg p-3 ${syncResult.status === 'error' ? 'bg-red-50 text-red-700' : 'bg-green-50 text-green-700'}`}>
            {syncResult.status === 'error'
              ? `Error: ${syncResult.error}`
              : syncResult.status === 'skipped' || syncResult.status === 'no_changes'
                ? 'No changes detected — already up to date.'
                : `Sync complete: ${syncResult.record_count?.toLocaleString()} records · +${syncResult.additions} −${syncResult.deletions} ~${syncResult.modifications}`
            }
          </div>
        )}
      </div>

      {/* Sync history */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
        <h2 className="font-semibold text-slate-800 mb-4">Sync History</h2>
        {history.length === 0 ? (
          <p className="text-sm text-slate-400">No sync history yet.</p>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-xs text-slate-400 uppercase">
              <tr>
                <th className="text-left pb-2">Date</th>
                <th className="text-left pb-2">Synced At</th>
                <th className="text-right pb-2">Records</th>
                <th className="text-right pb-2">+Added</th>
                <th className="text-right pb-2">−Deleted</th>
                <th className="text-right pb-2">~Modified</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {history.map(h => (
                <tr key={h.snapshot_date}>
                  <td className="py-2 font-medium">{h.snapshot_date}</td>
                  <td className="py-2 text-slate-400">{fmtDateTime(h.created_at)}</td>
                  <td className="py-2 text-right text-slate-500">{h.record_count?.toLocaleString()}</td>
                  <td className="py-2 text-right text-green-600">+{h.additions}</td>
                  <td className="py-2 text-right text-red-500">−{h.deletions}</td>
                  <td className="py-2 text-right text-amber-600">~{h.modifications}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
