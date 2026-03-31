import axios from 'axios'

const BASE = '/api/testcases'

export const testcasesApi = {
  types: () => axios.get(`${BASE}/types`),

  generate: (payload) => axios.post(`${BASE}/generate`, payload),

  stats: () => axios.get(`${BASE}/stats`),
  tableTypes: () => axios.get(`${BASE}/table-types`),

  clear: () => axios.delete(`${BASE}/clear`),
  clearCustomTypes: () => axios.delete(`${BASE}/types/custom`),

  cases: ({ page = 1, pageSize = 100, expectedResult, entityType, watchlist, typeId, search } = {}) => {
    const p = new URLSearchParams()
    p.set('page', page)
    p.set('page_size', pageSize)
    if (expectedResult) p.set('expected_result', expectedResult)
    if (entityType) p.set('entity_type', entityType)
    if (watchlist) p.set('watchlist', watchlist)
    if (typeId) p.set('type_id', typeId)
    if (search) p.set('search', search)
    return axios.get(`${BASE}/?${p}`)
  },

  exportUrl: (format, { expectedResult, entityType } = {}) => {
    const p = new URLSearchParams()
    if (expectedResult) p.set('expected_result', expectedResult)
    if (entityType) p.set('entity_type', entityType)
    const formatMap = { csv: 'csv', excel: 'excel', pacs008: 'pacs008', pacs009: 'pacs009', fuf: 'fuf' }
    return `${BASE}/export/${formatMap[format] || 'csv'}?${p}`
  },

  chatMessage: (sessionId, content) =>
    axios.post(`${BASE}/chatbot/message`, { session_id: sessionId || null, content }),
}
