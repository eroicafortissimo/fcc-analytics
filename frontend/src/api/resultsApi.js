import axios from 'axios'

const BASE = '/api/results'

export const resultsApi = {
  upload: (file) => {
    const fd = new FormData()
    fd.append('file', file)
    return axios.post(`${BASE}/upload`, fd, {
      headers: { 'Content-Type': 'multipart/form-data' },
    })
  },

  summary: () => axios.get(`${BASE}/summary`),

  breakdown: (by = 'entity_type') => axios.get(`${BASE}/breakdown?by=${by}`),

  cases: ({ page = 1, pageSize = 100, outcome, entityType, search } = {}) => {
    const p = new URLSearchParams()
    p.set('page', page)
    p.set('page_size', pageSize)
    if (outcome) p.set('outcome', outcome)
    if (entityType) p.set('entity_type', entityType)
    if (search) p.set('search', search)
    return axios.get(`${BASE}/?${p}`)
  },

  clear: () => axios.delete(`${BASE}/clear`),

  exportUrl: () => `${BASE}/export/excel`,

  templateUrl: () => `${BASE}/template`,

  analyzeMisses: () => axios.post(`${BASE}/analyze-misses`),

  getMissAnalyses: () => axios.get(`${BASE}/miss-analyses`),
}
