import axios from 'axios'

const BASE = '/api/lists'

function filterParams(filters) {
  const p = new URLSearchParams()
  ;(filters.watchlists || []).forEach(w => p.append('watchlists', w))
  ;(filters.entity_types || []).forEach(e => p.append('entity_types', e))
  if (filters.search) p.set('search', filters.search)
  if (filters.recently_modified_only) p.set('recently_modified_only', 'true')
  if (filters.min_tokens != null) p.set('min_tokens', filters.min_tokens)
  if (filters.max_tokens != null) p.set('max_tokens', filters.max_tokens)
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

  nlFilter: (query) => axios.post(`${BASE}/nl-filter`, { query }),

  overlap: () => axios.get(`${BASE}/overlap`),

  inferCultures: (batchSize = 500) => {
    const p = new URLSearchParams()
    p.set('batch_size', batchSize)
    return axios.post(`${BASE}/infer-cultures?${p}`)
  },

  inferCulturesStatus: () => axios.get(`${BASE}/infer-cultures/status`),

  clearDatabase: () => axios.delete(`${BASE}/clear`),

  cultures: () => axios.get(`${BASE}/cultures`),
}
