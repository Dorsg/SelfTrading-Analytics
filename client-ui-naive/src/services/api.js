import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 60_000
})

// ---- Simulation endpoints ----
// (Adjust the paths if your backend differs, the UI is isolated here)
export const SimulationAPI = {
  start:  (payload = {}) => api.post('/simulation/start', payload).then(r => r.data),
  stop:   () => api.post('/simulation/stop').then(r => r.data),
  reset:  () => api.post('/simulation/reset').then(r => r.data),
  status: () => api.get('/simulation/status').then(r => r.data),
  importStatus: () => api.get('/simulation/import/status').then(r => r.data),
  logs:   (params) => api.get('/simulation/logs', { params }).then(r => r.data)
}

// ---- Results endpoints ----
export const ResultsAPI = {
  summary: (params = {}) => api.get('/results/summary', { params }).then(r => r.data),
  topStocks: (params = {}) => api.get('/results/top-stocks', { params }).then(r => r.data)
}

export default api
