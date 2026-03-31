import axios from 'axios'

const BASE = '/api/listiq'

export const listiqApi = {
  // Sync
  triggerSync: () => axios.post(`${BASE}/sync/trigger`),
  syncStatus: () => axios.get(`${BASE}/sync/status`),
  syncSchedule: () => axios.get(`${BASE}/sync/schedule`),
  updateSchedule: (body) => axios.put(`${BASE}/sync/schedule`, body),
  syncHistory: () => axios.get(`${BASE}/sync/history`),

  // Changes
  changes: (params = {}) => axios.get(`${BASE}/changes`, { params }),
  changesSummary: (date) => axios.get(`${BASE}/changes/summary/${date}`),
  availableDates: () => axios.get(`${BASE}/changes/dates`),
  getChange: (id) => axios.get(`${BASE}/changes/${id}`),

  // Records
  snapshots: () => axios.get(`${BASE}/records/snapshots`),
  recordHistory: (uid) => axios.get(`${BASE}/records/${uid}/history`),
}
