import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import {
  BarChart, Bar, PieChart, Pie, Cell, Tooltip, XAxis, YAxis,
  CartesianGrid, ResponsiveContainer, Legend
} from 'recharts'

const API = '/api/lists'

const WATCHLIST_COLORS = {
  OFAC_SDN: '#ef4444',
  OFAC_NON_SDN: '#f97316',
  EU: '#3b82f6',
  HMT: '#8b5cf6',
  BIS: '#10b981',
  JAPAN: '#f59e0b',
}

const ENTITY_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#6b7280']

export default function ListExplorer() {
  const [downloadStatus, setDownloadStatus] = useState([])
  const [downloading, setDownloading] = useState(false)
  const [summary, setSummary] = useState(null)
  const [entries, setEntries] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [filters, setFilters] = useState({
    watchlists: [],
    entity_types: [],
    nationalities: [],
    search: '',
    recently_modified_only: false,
  })
  const [filterOptions, setFilterOptions] = useState({
    watchlists: [],
    entity_types: [],
    nationalities: [],
  })
  const [loading, setLoading] = useState(false)

  const fetchSummary = useCallback(async () => {
    try {
      const { data } = await axios.get(`${API}/summary`)
      setSummary(data)
    } catch (_) {}
  }, [])

  const fetchEntries = useCallback(async (pg = 1) => {
    setLoading(true)
    try {
      const params = {
        page: pg,
        page_size: 100,
        ...(filters.search && { search: filters.search }),
        ...(filters.recently_modified_only && { recently_modified_only: true }),
      }
      filters.watchlists.forEach(w => params['watchlists'] = [...(params.watchlists || []), w])
      filters.entity_types.forEach(e => params['entity_types'] = [...(params.entity_types || []), e])

      const { data } = await axios.get(`${API}/entries`, { params })
      setEntries(data.items)
      setTotal(data.total)
      setPage(pg)
    } catch (_) {}
    setLoading(false)
  }, [filters])

  const fetchFilterOptions = useCallback(async () => {
    try {
      const { data } = await axios.get(`${API}/filters`)
      setFilterOptions(data)
    } catch (_) {}
  }, [])

  useEffect(() => {
    fetchSummary()
    fetchEntries()
    fetchFilterOptions()
  }, [])

  const handleDownload = async () => {
    setDownloading(true)
    try {
      const { data } = await axios.post(`${API}/download`)
      setDownloadStatus(data)
      await fetchSummary()
      await fetchEntries()
      await fetchFilterOptions()
    } catch (err) {
      setDownloadStatus([{ watchlist: 'all', status: 'failed', error: String(err) }])
    }
    setDownloading(false)
  }

  // Chart data
  const watchlistChartData = summary
    ? Object.entries(summary.by_watchlist).map(([k, v]) => ({ name: k, count: v }))
    : []

  const entityChartData = summary
    ? Object.entries(summary.by_entity_type).map(([k, v]) => ({ name: k, value: v }))
    : []

  return (
    <div className="max-w-7xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">List Explorer</h1>
          <p className="text-slate-500 text-sm mt-1">
            {summary ? `${summary.total.toLocaleString()} entries across ${Object.keys(summary.by_watchlist).length} lists` : 'No data loaded yet'}
          </p>
        </div>
        <button
          onClick={handleDownload}
          disabled={downloading}
          className="bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white px-5 py-2 rounded-lg font-medium text-sm transition-colors"
        >
          {downloading ? 'Downloading...' : 'Download / Refresh Lists'}
        </button>
      </div>

      {/* Download status */}
      {downloadStatus.length > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {downloadStatus.map(s => (
            <div
              key={s.watchlist}
              className={`rounded-lg p-3 text-xs border ${
                s.status === 'success' ? 'bg-green-50 border-green-200' :
                s.status === 'cached' ? 'bg-blue-50 border-blue-200' :
                'bg-red-50 border-red-200'
              }`}
            >
              <div className="font-semibold">{s.watchlist}</div>
              <div className="capitalize">{s.status}</div>
              {s.count > 0 && <div>{s.count.toLocaleString()} entries</div>}
              {s.error && <div className="text-red-600 truncate" title={s.error}>{s.error}</div>}
            </div>
          ))}
        </div>
      )}

      {/* Charts */}
      {summary && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="bg-white rounded-xl shadow-sm p-4">
            <h3 className="font-semibold text-slate-700 mb-3 text-sm">Distribution by Watchlist</h3>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={watchlistChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip />
                <Bar dataKey="count" fill="#3b82f6" radius={[4, 4, 0, 0]}>
                  {watchlistChartData.map((entry) => (
                    <Cell key={entry.name} fill={WATCHLIST_COLORS[entry.name] || '#3b82f6'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-white rounded-xl shadow-sm p-4">
            <h3 className="font-semibold text-slate-700 mb-3 text-sm">Distribution by Entity Type</h3>
            <ResponsiveContainer width="100%" height={220}>
              <PieChart>
                <Pie
                  data={entityChartData}
                  cx="50%"
                  cy="50%"
                  outerRadius={80}
                  dataKey="value"
                  label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                  labelLine={false}
                >
                  {entityChartData.map((_, i) => (
                    <Cell key={i} fill={ENTITY_COLORS[i % ENTITY_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="bg-white rounded-xl shadow-sm p-4">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <input
            type="text"
            placeholder="Search names..."
            className="border border-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            value={filters.search}
            onChange={e => setFilters(f => ({ ...f, search: e.target.value }))}
            onKeyDown={e => e.key === 'Enter' && fetchEntries(1)}
          />
          <select
            multiple
            className="border border-slate-200 rounded-lg px-3 py-2 text-sm h-10"
            value={filters.watchlists}
            onChange={e => setFilters(f => ({
              ...f,
              watchlists: Array.from(e.target.selectedOptions, o => o.value),
            }))}
          >
            {filterOptions.watchlists?.map(w => <option key={w} value={w}>{w}</option>)}
          </select>
          <select
            multiple
            className="border border-slate-200 rounded-lg px-3 py-2 text-sm h-10"
            value={filters.entity_types}
            onChange={e => setFilters(f => ({
              ...f,
              entity_types: Array.from(e.target.selectedOptions, o => o.value),
            }))}
          >
            {filterOptions.entity_types?.map(et => <option key={et} value={et}>{et}</option>)}
          </select>
          <button
            onClick={() => fetchEntries(1)}
            className="bg-slate-800 hover:bg-slate-900 text-white px-4 py-2 rounded-lg text-sm"
          >
            Apply Filters
          </button>
        </div>
        <label className="flex items-center gap-2 mt-3 text-sm text-slate-600 cursor-pointer">
          <input
            type="checkbox"
            checked={filters.recently_modified_only}
            onChange={e => setFilters(f => ({ ...f, recently_modified_only: e.target.checked }))}
            className="rounded"
          />
          Recently modified only (last 90 days)
        </label>
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl shadow-sm overflow-hidden">
        {loading ? (
          <div className="p-8 text-center text-slate-400">Loading entries...</div>
        ) : entries.length === 0 ? (
          <div className="p-8 text-center text-slate-400">
            No entries found. Click "Download / Refresh Lists" to load data.
          </div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead className="bg-slate-50 border-b border-slate-200">
                  <tr>
                    {['Watchlist', 'Program', 'Cleaned Name', 'Original Name', 'P/AKA', 'Type',
                      'Tokens', 'Length', 'Nationality', 'Date Listed', 'Recent'].map(h => (
                      <th key={h} className="px-3 py-2 text-left font-semibold text-slate-600">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {entries.map((e, i) => (
                    <tr key={e.uid || i} className="hover:bg-slate-50">
                      <td className="px-3 py-1.5">
                        <span className="px-1.5 py-0.5 rounded text-white text-xs"
                          style={{ background: WATCHLIST_COLORS[e.watchlist] || '#6b7280' }}>
                          {e.watchlist}
                        </span>
                      </td>
                      <td className="px-3 py-1.5 text-slate-500">{e.sub_watchlist_1 || '-'}</td>
                      <td className="px-3 py-1.5 font-medium text-slate-800 max-w-xs truncate">{e.cleaned_name}</td>
                      <td className="px-3 py-1.5 text-slate-500 max-w-xs truncate">{e.original_name}</td>
                      <td className="px-3 py-1.5">
                        <span className={`px-1.5 py-0.5 rounded text-xs ${
                          e.primary_aka === 'primary'
                            ? 'bg-blue-100 text-blue-700'
                            : 'bg-slate-100 text-slate-500'
                        }`}>{e.primary_aka}</span>
                      </td>
                      <td className="px-3 py-1.5 text-slate-600">{e.entity_type}</td>
                      <td className="px-3 py-1.5 text-center text-slate-500">{e.num_tokens}</td>
                      <td className="px-3 py-1.5 text-center text-slate-500">{e.name_length}</td>
                      <td className="px-3 py-1.5 text-slate-500">{e.nationality || '-'}</td>
                      <td className="px-3 py-1.5 text-slate-500">{e.date_listed || '-'}</td>
                      <td className="px-3 py-1.5 text-center">
                        {e.recently_modified ? '✓' : ''}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="px-4 py-3 border-t border-slate-100 flex items-center justify-between text-xs text-slate-500">
              <span>{total.toLocaleString()} total entries</span>
              <div className="flex gap-2">
                <button
                  onClick={() => fetchEntries(Math.max(1, page - 1))}
                  disabled={page === 1}
                  className="px-3 py-1 rounded border border-slate-200 disabled:opacity-40"
                >Prev</button>
                <span className="px-2 py-1">Page {page}</span>
                <button
                  onClick={() => fetchEntries(page + 1)}
                  disabled={page * 100 >= total}
                  className="px-3 py-1 rounded border border-slate-200 disabled:opacity-40"
                >Next</button>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
