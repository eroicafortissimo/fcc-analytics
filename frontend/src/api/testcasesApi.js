import axios from 'axios'

const BASE = '/api/testcases'

export const testcasesApi = {
  types: () => axios.get(`${BASE}/types`),

  generate: (payload) => axios.post(`${BASE}/generate`, payload),

  stats: () => axios.get(`${BASE}/stats`),

  clear: () => axios.delete(`${BASE}/clear`),

  cases: ({ page = 1, pageSize = 100, expectedResult, entityType, search } = {}) => {
    const p = new URLSearchParams()
    p.set('page', page)
    p.set('page_size', pageSize)
    if (expectedResult) p.set('expected_result', expectedResult)
    if (entityType) p.set('entity_type', entityType)
    if (search) p.set('search', search)
    return axios.get(`${BASE}/?${p}`)
  },

  exportCsv: ({ expectedResult, entityType } = {}) => {
    const p = new URLSearchParams()
    if (expectedResult) p.set('expected_result', expectedResult)
    if (entityType) p.set('entity_type', entityType)
    return `${BASE}/export/csv?${p}`
  },
}
