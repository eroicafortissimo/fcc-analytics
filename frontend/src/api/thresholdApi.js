import axios from 'axios'

const BASE = '/api/threshold'

export const thresholdApi = {
  // Datasets
  uploadDataset: (file, name) => {
    const fd = new FormData()
    fd.append('file', file)
    if (name) fd.append('name', name)
    return axios.post(`${BASE}/datasets/upload`, fd)
  },
  listDatasets: () => axios.get(`${BASE}/datasets`),
  getDataset: (id) => axios.get(`${BASE}/datasets/${id}`),
  previewDataset: (id, rows = 100) => axios.get(`${BASE}/datasets/${id}/preview?rows=${rows}`),
  deleteDataset: (id) => axios.delete(`${BASE}/datasets/${id}`),
  reloadDataset: (id) => axios.post(`${BASE}/datasets/${id}/reload`),
  reuploadDataset: (id, file) => {
    const fd = new FormData()
    fd.append('file', file)
    return axios.post(`${BASE}/datasets/${id}/reupload`, fd)
  },

  // Scenarios
  createScenario: (body) => axios.post(`${BASE}/scenarios`, body),
  aiScenario: (dataset_id, prompt) => axios.post(`${BASE}/scenarios/ai`, { dataset_id, prompt }),
  listScenarios: (dataset_id) => axios.get(`${BASE}/scenarios${dataset_id ? `?dataset_id=${dataset_id}` : ''}`),
  deleteScenario: (id) => axios.delete(`${BASE}/scenarios/${id}`),
  previewScenario: (id) => axios.post(`${BASE}/scenarios/${id}/preview`),

  // Analysis
  runAnalysis: (body) => axios.post(`${BASE}/analysis`, body),
  simulate: (body) => axios.post(`${BASE}/analysis/simulate`, body),
  autoThresholds: (body) => axios.post(`${BASE}/analysis/auto-thresholds`, body),

  // Percentile curve (P50–P100 alert counts for charting)
  percentileCurve: (body) => axios.post(`${BASE}/analysis/percentile-curve`, body),

  // ATL/BTL
  computeAtlBtl: (body) => axios.post(`${BASE}/analysis/atl-btl`, body),

  // Report
  generateReport: (analysis_id) => axios.post(`${BASE}/report/generate`, { analysis_id }),
}
