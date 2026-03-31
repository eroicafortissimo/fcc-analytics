import axios from 'axios'

const BASE = '/api/reconciliation'

export const reconciliationApi = {
  start: (files, watchlists, useAI) => {
    const fd = new FormData()
    files.forEach(f => fd.append('files', f))
    watchlists.forEach(w => fd.append('watchlists', w))
    fd.append('use_ai', String(useAI))
    return axios.post(`${BASE}/run`, fd)
  },

  status: (jobId) => axios.get(`${BASE}/status/${jobId}`),

  results: (jobId, section, page = 1, pageSize = 50, filters = {}) =>
    axios.get(`${BASE}/results/${jobId}`, {
      params: {
        section,
        page,
        page_size: pageSize,
        ...(filters.watchlist   ? { watchlist: filters.watchlist }     : {}),
        ...(filters.entity_type ? { entity_type: filters.entity_type } : {}),
        ...(filters.search      ? { search: filters.search }           : {}),
      },
    }),
}
