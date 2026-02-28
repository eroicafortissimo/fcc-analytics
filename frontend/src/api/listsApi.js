import axios from 'axios'

const BASE = '/api/lists'

function filterParams(filters) {
  const p = new URLSearchParams()
  ;(filters.watchlists || []).forEach(w => p.append('watchlists', w))
  ;(filters.entity_types || []).forEach(e => p.append('entity_types', e))
  ;(filters.nationalities || []).forEach(n => p.append('nationalities', n))
  if (filters.search) p.set('search', filters.search)
  if (filters.recently_modified_only) p.set('recently_modified_only', 'true')
  return p
}

export const listsApi = {
  download: (watchlists = []) => {
    const p = new URLSearchParams()
    watchlists.forEach(w => p.append('watchlists', w))
    return axios.post(`${BASE}/download?${p}`)
  },

  entries: (filters, page = 1, pageSize = 100) => {
    const p = filterParams(filters)
    p.set('page', page)
    p.set('page_size', pageSize)
    return axios.get(`${BASE}/entries?${p}`)
  },

  chartData: (filters) =>
    axios.get(`${BASE}/chart-data?${filterParams(filters)}`),

  filterOptions: () => axios.get(`${BASE}/filters`),

  inferNationalities: (watchlists = [], batchSize = 1000, llmEnabled = true) => {
    const p = new URLSearchParams()
    watchlists.forEach(w => p.append('watchlists', w))
    p.set('batch_size', batchSize)
    p.set('llm_enabled', llmEnabled)
    return axios.post(`${BASE}/infer-nationalities?${p}`)
  },

  inferStatus: () => axios.get(`${BASE}/infer-nationalities/status`),
}
