import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 60_000,
  headers: { 'Cache-Control': 'no-cache, no-store, must-revalidate' }
})

// ---- Simulation endpoints ----
// (Adjust the paths if your backend differs, the UI is isolated here)
export const SimulationAPI = {
  // Use the analytics router prefix so calls reach the analytics API endpoints
  start:  (payload = {}) => api.post('/analytics/simulation/start', payload).then(r => r.data),
  stop:   () => api.post('/analytics/simulation/stop').then(r => r.data),
  reset:  async () => {
    // Try primary endpoint; fall back to legacy /sim/reset with a full payload
    try {
      const r = await api.post('/analytics/simulation/reset')
      return r.data
    } catch (e) {
      const status = e?.response?.status
      if (status === 404 || status === 405) {
        // Fallback body mirrors server ResetRequest defaults
        const body = {
          hard: true,
          reset_account: true,
          truncate_logs: false,
          clear_runner_executions: true,
          clear_executed_trades: true,
          clear_orders: true,
          clear_open_positions: true,
          clear_analytics_results: true
        }
        const r2 = await api.post('/sim/reset', body)
        return r2.data
      }
      throw e
    }
  },
  // status endpoint exposed as /analytics/simulation/state
  status: () => api.get('/analytics/simulation/state').then(r => r.data),
  // progress endpoint and helper to force a tick (useful for dev/testing)
  // Add a cache-busting timestamp param to avoid stale cached responses in browsers or intermediaries.
  progress: () => api.get('/analytics/progress', { params: { _t: Date.now() } }).then(r => r.data),
  forceTick: () => api.post('/analytics/simulation/force-tick').then(r => r.data),
  // import status (if supported) under analytics; keep as-is but namespaced
  importStatus: () => api.get('/analytics/simulation/import/status').then(r => r.data),
  // database readiness/counters for deriving import readiness
  dbStatus: () => api.get('/analytics/database/status').then(r => r.data),
  // surface errors/warnings via the analytics 'errors' or 'warns' endpoints
  logs:   (params) => api.get('/analytics/errors', { params }).then(r => r.data)
}

// ---- Results endpoints ----
export const ResultsAPI = {
  summary: (params = {}) => api.get('/analytics/results/summary', { params }).then(r => r.data),
  topStocks: (params = {}) => api.get('/analytics/results/top-stocks', { params }).then(r => r.data)
}

export default api
