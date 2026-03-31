import axios from 'axios'

const BASE = '/api/transactiq'

export const transactiqApi = {
  preview: (file) => {
    const form = new FormData()
    form.append('file', file)
    return axios.post(`${BASE}/preview`, form)
  },

  analyze: (file, beneNameCol, ordNameCol, beneCountryCol, ordCountryCol, entityTypeCol) => {
    const form = new FormData()
    form.append('file', file)
    form.append('bene_name_col', beneNameCol)
    if (ordNameCol)     form.append('ord_name_col',     ordNameCol)
    if (beneCountryCol) form.append('bene_country_col', beneCountryCol)
    if (ordCountryCol)  form.append('ord_country_col',  ordCountryCol)
    if (entityTypeCol)  form.append('entity_type_col',  entityTypeCol)
    return axios.post(`${BASE}/analyze`, form)
  },

  rows: (analysisId, page = 1, pageSize = 25) =>
    axios.get(`${BASE}/rows/${analysisId}`, { params: { page, page_size: pageSize } }),

  chat: (analysisId, message, history = []) =>
    axios.post(`${BASE}/chat/${analysisId}`, { message, history }),
}
