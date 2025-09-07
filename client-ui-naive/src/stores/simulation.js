import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { SimulationAPI } from '@/services/api'

export const useSimulationStore = defineStore('simulation', () => {
  const isStarting = ref(false)
  const isStopping = ref(false)
  const isResetting = ref(false)

  const status = ref({
    state: 'idle',            // 'idle' | 'running' | 'stopped' | 'completed'
    progress_percent: 0,      // 0..100
    eta_seconds: null,
    total_buys: 0,
    total_sells: 0
  })

  const importStatus = ref({
    state: 'idle',            // 'idle' | 'importing' | 'ready' | 'error'
    progress_percent: 0,      // 0..100
    processed: 0,
    total: 0
  })

  const logs = ref({
    warnings: [],
    errors: []
  })

  let pollTimer = null
  let importTimer = null
  let logsTimer = null

  const isRunning = computed(() => status.value.state === 'running')

  function _startPolling () {
    _stopPolling()
    pollTimer = setInterval(async () => {
      try {
        const s = await SimulationAPI.status()
        status.value = {
          state: s.state ?? s.status ?? 'idle',
          progress_percent: Math.round(s.progress_percent ?? s.progress ?? 0),
          eta_seconds: s.eta_seconds ?? s.eta ?? null,
          total_buys: s.total_buys ?? 0,
          total_sells: s.total_sells ?? 0
        }
      } catch { /* silent */ }
    }, 1500)
  }

  function _stopPolling () {
    if (pollTimer) {
      clearInterval(pollTimer)
      pollTimer = null
    }
  }

  function _startImportPolling () {
    _stopImportPolling()
    importTimer = setInterval(async () => {
      try {
        const s = await SimulationAPI.importStatus()
        importStatus.value = {
          state: s.state ?? 'idle',
          progress_percent: Math.round(s.progress_percent ?? s.progress ?? 0),
          processed: s.processed ?? s.done ?? 0,
          total: s.total ?? s.count ?? 0
        }
      } catch { /* silent */ }
    }, 2000)
  }

  function _stopImportPolling () {
    if (importTimer) {
      clearInterval(importTimer)
      importTimer = null
    }
  }

  function _startLogsPolling () {
    _stopLogsPolling()
    // Fetch warnings & errors every 3s
    logsTimer = setInterval(async () => {
      try {
        const [w, e] = await Promise.all([
          SimulationAPI.logs({ level: 'warning', limit: 500 }),
          SimulationAPI.logs({ level: 'error', limit: 500 })
        ])
        logs.value = {
          warnings: w?.lines ?? [],
          errors: e?.lines ?? []
        }
      } catch { /* silent */ }
    }, 3000)
  }

  function _stopLogsPolling () {
    if (logsTimer) {
      clearInterval(logsTimer)
      logsTimer = null
    }
  }

  async function warmUp () {
    // Initial pulls to show current state without waiting for the first interval
    try {
      const [s, i] = await Promise.all([
        SimulationAPI.status(),
        SimulationAPI.importStatus()
      ])
      status.value = {
        state: s.state ?? s.status ?? 'idle',
        progress_percent: Math.round(s.progress_percent ?? s.progress ?? 0),
        eta_seconds: s.eta_seconds ?? s.eta ?? null,
        total_buys: s.total_buys ?? 0,
        total_sells: s.total_sells ?? 0
      }
      importStatus.value = {
        state: i.state ?? 'idle',
        progress_percent: Math.round(i.progress_percent ?? i.progress ?? 0),
        processed: i.processed ?? i.done ?? 0,
        total: i.total ?? i.count ?? 0
      }
    } finally {
      _startPolling()
      _startImportPolling()
      _startLogsPolling()
    }
  }

  async function start () {
    if (isStarting.value) return
    isStarting.value = true
    try {
      const res = await SimulationAPI.start()
      // on success the backend updates status; polling will reflect it
      return res
    } finally {
      isStarting.value = false
    }
  }

  async function stop () {
    if (isStopping.value) return
    isStopping.value = true
    try {
      const res = await SimulationAPI.stop()
      return res
    } finally {
      isStopping.value = false
    }
  }

  async function reset () {
    if (isResetting.value) return
    isResetting.value = true
    try {
      const res = await SimulationAPI.reset()
      return res
    } finally {
      isResetting.value = false
    }
  }

  function destroy () {
    _stopPolling()
    _stopImportPolling()
    _stopLogsPolling()
  }

  return {
    status, importStatus, logs,
    isStarting, isStopping, isResetting, isRunning,
    warmUp, start, stop, reset, destroy
  }
})
