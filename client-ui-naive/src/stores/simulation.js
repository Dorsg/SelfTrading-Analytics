import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { SimulationAPI } from '@/services/api'

export const useSimulationStore = defineStore('simulation', () => {
  const isStarting = ref(false)
  const isStopping = ref(false)
  const isResetting = ref(false)

  // For client-side ETA calculation
  let lastProgressData = null
  let lastPollTime = null
  const etaRateSmoother = 0.1 // EMA alpha for smoothing rate calculations
  // Publish gating for ETA to avoid second-by-second flapping
  let lastEtaPublishAtMs = null
  let lastEtaPublishedSeconds = null
  const ETA_PUBLISH_MIN_INTERVAL_MS = 30000 // update visible ETA at most every 30s
  const ETA_PUBLISH_MIN_DELTA_SECONDS = 60   // or if ETA changes by >= 1 minute
  
  function sleep (ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  // ───────── Simulation status cache (prevents blank/0% flash on refresh) ─────────
  const SIM_STATUS_CACHE_KEY = 'analytics_sim_status_cache'
  const ETA_CACHE_KEY = 'analytics_eta_cache_v1'
  const ETA_TTL_MS = 60 * 1000 // 1 minute TTL for cached ETA
  function readSimCache () {
    try {
      const raw = sessionStorage.getItem(SIM_STATUS_CACHE_KEY)
      if (!raw) return null
      return JSON.parse(raw)
    } catch { return null }
  }
  function writeSimCache (val) {
    try { sessionStorage.setItem(SIM_STATUS_CACHE_KEY, JSON.stringify(val)) } catch {}
  }
  function readEtaCache () {
    try {
      const raw = sessionStorage.getItem(ETA_CACHE_KEY)
      if (!raw) return null
      const obj = JSON.parse(raw)
      if (!obj || !obj.when || (Date.now() - obj.when) > ETA_TTL_MS) return null
      return obj
    } catch { return null }
  }
  function writeEtaCache (etaSeconds, finishIso, rate) {
    try { sessionStorage.setItem(ETA_CACHE_KEY, JSON.stringify({ when: Date.now(), etaSeconds, finishIso, rate })) } catch {}
  }

  const status = ref(readSimCache() || {
    state: 'idle',            // 'idle' | 'running' | 'stopped' | 'completed'
    progress_percent: 0,      // 0..100
    eta_seconds: null,
    eta_label: null,
    estimated_finish_iso: null,
    total_buys: 0,
    total_sells: 0,
    last_ts: null,
    current: null
  })
  // snapshot_age_seconds: how old the server snapshot is (if provided)
  status.value.snapshot_age_seconds = null

  // ───────── Import status cache (prevents 0% flash on refresh) ─────────
  const IMPORT_STATUS_CACHE_KEY = 'analytics_import_status_cache'
  const IMPORT_STATUS_ONCE_KEY = 'analytics_import_status_cache_once'
  function readImportCache () {
    try {
      const raw = sessionStorage.getItem(IMPORT_STATUS_CACHE_KEY)
      if (!raw) return null
      return JSON.parse(raw)
    } catch { return null }
  }
  function writeImportCache (val) {
    try { sessionStorage.setItem(IMPORT_STATUS_CACHE_KEY, JSON.stringify(val)) } catch {}
  }
  function readImportOnceCache () {
    try {
      const raw = localStorage.getItem(IMPORT_STATUS_ONCE_KEY)
      if (!raw) return null
      return JSON.parse(raw)
    } catch { return null }
  }
  function writeImportOnceCache (val) {
    try { localStorage.setItem(IMPORT_STATUS_ONCE_KEY, JSON.stringify(val)) } catch {}
  }

  const importDefault = {
    state: 'idle',            // 'idle' | 'importing' | 'ready' | 'error' | 'pending'
    progress_percent: 0,      // 0..100
    processed: 0,
    total: 0,
    details: { daily_bars: 0, minute_bars: 0, users: 0, runners: 0, date_range: { start: null, end: null }, checks_done: 0, checks_total: 0 }
  }
  const importStatus = ref(readImportCache() || importDefault)
  const importLoading = ref(false)

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
    const intervalMs = 1200
    pollTimer = setInterval(async () => {
      try {
        // Prefer the detailed /progress snapshot for percentages and ETA
        // but fall back to /simulation/state when needed.
        let progressData = null
        let stateData = null
        try {
          progressData = await SimulationAPI.progress()
        } catch (e) {
          // ignore
        }
        try {
          stateData = await SimulationAPI.status()
        } catch (e) {
          // ignore
        }

        // ---- Client-side ETA Calculation ----
        if (progressData && progressData.sim_time_epoch && progressData.max_epoch && progressData.state === 'running') {
          const now = Date.now()
          if (lastProgressData && lastPollTime) {
            const simSecondsPerTick = progressData.sim_time_epoch - lastProgressData.sim_time_epoch
            const wallSecondsPerTick = (now - lastPollTime) / 1000

            if (simSecondsPerTick > 0 && wallSecondsPerTick > 0) {
              const currentRate = simSecondsPerTick / wallSecondsPerTick // sim seconds per wall second
              // Use an exponential moving average to smooth the rate
              const smoothedRate = status.value.rate ? (etaRateSmoother * currentRate) + (1 - etaRateSmoother) * status.value.rate : currentRate
              status.value.rate = smoothedRate

              const remainingSimSeconds = progressData.max_epoch - progressData.sim_time_epoch
              if (remainingSimSeconds > 0 && smoothedRate > 0) {
                const newEtaSeconds = Math.round(remainingSimSeconds / smoothedRate)
                const newFinishIso = new Date(now + newEtaSeconds * 1000).toISOString()
                // Gate visible ETA updates to avoid jitter
                const shouldPublishByTime = !lastEtaPublishAtMs || (now - lastEtaPublishAtMs) >= ETA_PUBLISH_MIN_INTERVAL_MS
                const shouldPublishByDelta = (lastEtaPublishedSeconds == null) || (Math.abs(newEtaSeconds - lastEtaPublishedSeconds) >= ETA_PUBLISH_MIN_DELTA_SECONDS)
                if (shouldPublishByTime || shouldPublishByDelta) {
                  status.value.eta_seconds = newEtaSeconds
                  status.value.estimated_finish_iso = newFinishIso
                  lastEtaPublishAtMs = now
                  lastEtaPublishedSeconds = newEtaSeconds
                  writeEtaCache(status.value.eta_seconds, status.value.estimated_finish_iso, status.value.rate)
                } else {
                  // Keep internal cache fresh without updating UI-facing fields
                  writeEtaCache(newEtaSeconds, newFinishIso, status.value.rate)
                }
              } else {
                status.value.eta_seconds = 0
                status.value.estimated_finish_iso = new Date().toISOString()
              }
            }
          }
          lastProgressData = progressData
          lastPollTime = now
        } else {
          // Reset if not running
          lastProgressData = null
          lastPollTime = null
          lastEtaPublishAtMs = null
          lastEtaPublishedSeconds = null
          // keep last cached ETA while idle to avoid flicker
          const cached = readEtaCache()
          if (cached) {
            status.value.eta_seconds = cached.etaSeconds
            status.value.estimated_finish_iso = cached.finishIso
            status.value.rate = cached.rate
          } else {
            status.value.eta_seconds = null
            status.value.estimated_finish_iso = null
          }
        }
        
        const runningFlag = stateData && (typeof stateData.running !== 'undefined' ? stateData.running : stateData.is_running)

        // derive values, preferring progress snapshot
        const percent = progressData?.progress_percent ?? progressData?.progress ?? stateData?.progress_percent ?? stateData?.progress ?? 0
        const totalBuys = progressData?.total_buys ?? stateData?.total_buys ?? status.value.total_buys ?? 0
        const totalSells = progressData?.total_sells ?? stateData?.total_sells ?? status.value.total_sells ?? 0
        const lastTs = progressData?.sim_time_iso ?? stateData?.last_ts ?? status.value.last_ts ?? null
        const currentRunnerInfo = progressData?.current_runner_info ?? null

        status.value.state = (stateData?.state ?? stateData?.status) ?? (runningFlag ? 'running' : 'idle')
        status.value.progress_percent = Math.round(percent ?? 0)
        status.value.total_buys = totalBuys ?? 0
        status.value.total_sells = totalSells ?? 0
        status.value.last_ts = lastTs
        status.value.current = currentRunnerInfo
        status.value.snapshot_age_seconds = stateData?.snapshot_age_seconds ?? null
        // Prefer server-provided ETA when available to populate Finish immediately
        if (progressData?.estimated_finish_iso) {
          status.value.estimated_finish_iso = progressData.estimated_finish_iso
        }
        if (typeof progressData?.estimated_finish_seconds !== 'undefined' && progressData.estimated_finish_seconds !== null) {
          status.value.eta_seconds = progressData.estimated_finish_seconds
        }
        // Pass-through per-timeframe progress if provided by snapshot
        if (progressData?.timeframes && typeof progressData.timeframes === 'object') {
          status.value.timeframes = progressData.timeframes
        }
        
        writeSimCache(status.value)
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('Simulation status poll error:', err)
      }
    }, intervalMs)
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
        const [s, db] = await Promise.all([
          SimulationAPI.importStatus(),
          SimulationAPI.dbStatus().catch(() => null)
        ])

        // Map server states to UI states
        const mappedState = (function mapState () {
          const st = s?.state
          if (st === 'completed') return 'ready'
          if (st === 'pending' && db && db.ready) return 'ready'
          return st ?? 'idle'
        })()

        // Derive percent: prefer server, else DB readiness gates
        let percent = Math.round(s?.progress_percent ?? s?.progress ?? 0)
        if ((!percent || percent <= 0) && db) {
          const gates = [ (db?.data?.daily_bars || 0) > 0, (db?.data?.minute_bars || 0) > 0, (db?.setup?.users || 0) > 0 && (db?.setup?.runners || 0) > 0 ]
          const done = gates.filter(Boolean).length
          percent = Math.round((done / 3) * 100)
          if (db.ready) percent = 100
        }

        const details = s?.details || {
          daily_bars: db?.data?.daily_bars || 0,
          minute_bars: db?.data?.minute_bars || 0,
          users: db?.setup?.users || 0,
          runners: db?.setup?.runners || 0,
          date_range: db?.data?.date_range || { start: null, end: null },
          checks_done: undefined,
          checks_total: undefined
        }

        importStatus.value = {
          state: mappedState,
          progress_percent: percent,
          processed: s?.processed ?? s?.done ?? (db?.ready ? 3 : 0),
          total: s?.total ?? s?.count ?? 3,
          details
        }
        // Persist to cache to avoid 0% flash on next refresh
        writeImportCache(importStatus.value)

        // Stop polling once import is fully ready to reduce noise
        if (importStatus.value.progress_percent >= 100 || importStatus.value.state === 'ready') {
          _stopImportPolling()
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

  function _startLogsPolling () { /* removed logs panel from SimulationView; keep no-op for compatibility */ }

  function _stopLogsPolling () {
    if (logsTimer) {
      clearInterval(logsTimer)
      logsTimer = null
    }
  }

  async function warmUp () {
    // Initial pulls to show current state without waiting for the first interval
    try {
      // Reset ETA calculation on warmup
      lastProgressData = null
      lastPollTime = null
      // Use cached ETA immediately if fresh
      const cachedEta = readEtaCache()
      if (cachedEta) {
        status.value.eta_seconds = cachedEta.etaSeconds
        status.value.estimated_finish_iso = cachedEta.finishIso
        status.value.rate = cachedEta.rate
      } else {
        status.value.eta_seconds = null
        status.value.estimated_finish_iso = null
      }

      // Import status: perform only once. If not cached yet, show spinner and retry until backend becomes available (up to ~15s)
      const cachedOnce = readImportOnceCache()
      if (!cachedOnce) importLoading.value = true

      const [s, p] = await Promise.all([
        SimulationAPI.status().catch(() => null),
        SimulationAPI.progress().catch(() => null)
      ])
      // Normalize running indicator here too (avoid throwing if s is null)
      const runningFlag = s ? ((typeof s.running !== 'undefined') ? s.running : s.is_running) : false
      status.value = {
        state: (s?.state ?? s?.status) ?? (runningFlag ? 'running' : 'idle'),
        progress_percent: Math.round(s?.progress_percent ?? s?.progress ?? 0),
        eta_seconds: s?.eta_seconds ?? s?.eta ?? null,
        eta_label: p?.estimated_finish ?? null,
        estimated_finish_iso: p?.estimated_finish_iso ?? null,
        total_buys: s?.total_buys ?? 0,
        total_sells: s?.total_sells ?? 0,
        last_ts: p?.sim_time_iso ?? s?.last_ts ?? null,
        current: p?.current_runner_info ?? null,
        snapshot_age_seconds: typeof s?.snapshot_age_seconds !== 'undefined' ? s.snapshot_age_seconds : null
      }
      writeSimCache(status.value)
      if (status.value.eta_seconds != null || status.value.estimated_finish_iso) {
        writeEtaCache(status.value.eta_seconds, status.value.estimated_finish_iso, status.value.rate)
      }
      // Resolve import status with short retries if needed
      let finalImport = cachedOnce || null
      if (!finalImport) {
        const deadline = Date.now() + 15000
        while (!finalImport && Date.now() < deadline) {
          const [iTry, dbTry] = await Promise.all([
            SimulationAPI.importStatus().catch(() => null),
            SimulationAPI.dbStatus().catch(() => null)
          ])

          // Build resilient snapshot when either endpoint is reachable
          if (iTry || dbTry) {
            const stRaw = iTry?.state
            const mappedState = (function mapState () {
              if (stRaw === 'completed') return 'ready'
              if (stRaw === 'pending' && dbTry && dbTry.ready) return 'ready'
              return stRaw ?? (dbTry ? (dbTry.ready ? 'ready' : (dbTry.data?.daily_bars || dbTry.data?.minute_bars ? 'importing' : 'pending')) : 'pending')
            })()
            let percent = Math.round(iTry?.progress_percent ?? iTry?.progress ?? 0)
            if ((!percent || percent <= 0) && dbTry) {
              const gates = [ (dbTry?.data?.daily_bars || 0) > 0, (dbTry?.data?.minute_bars || 0) > 0, (dbTry?.setup?.users || 0) > 0 && (dbTry?.setup?.runners || 0) > 0 ]
              const done = gates.filter(Boolean).length
              percent = Math.round((done / 3) * 100)
              if (dbTry?.ready) percent = 100
            }
            finalImport = {
              state: mappedState,
              progress_percent: percent,
              processed: iTry?.processed ?? iTry?.done ?? (dbTry?.ready ? 3 : (dbTry ? (Math.max(0, Math.min(3, ((dbTry?.data?.daily_bars>0) + (dbTry?.data?.minute_bars>0) + ((dbTry?.setup?.users>0 && dbTry?.setup?.runners>0))))) ) : 0)),
              total: iTry?.total ?? iTry?.count ?? 3,
              details: iTry?.details || {
                daily_bars: dbTry?.data?.daily_bars || 0,
                minute_bars: dbTry?.data?.minute_bars || 0,
                users: dbTry?.setup?.users || 0,
                runners: dbTry?.setup?.runners || 0,
                date_range: dbTry?.data?.date_range || { start: null, end: null },
                checks_done: dbTry ? (((dbTry?.data?.daily_bars || 0) > 0) + ((dbTry?.data?.minute_bars || 0) > 0) + (((dbTry?.setup?.users || 0) > 0 && (dbTry?.setup?.runners || 0) > 0))) : undefined,
                checks_total: 3
              }
            }
            // keep session cache fresh while we retry
            writeImportCache(finalImport)
            // Persist permanent cache when we have authoritative server response or DB is ready
            if (iTry || (dbTry && dbTry.ready) || finalImport.progress_percent >= 100 || finalImport.state === 'ready') {
              writeImportOnceCache(finalImport)
            } else {
              finalImport = null
            }
          }

          if (!finalImport) await sleep(1000)
        }
      }

      if (finalImport) {
        importStatus.value = finalImport
        writeImportCache(importStatus.value)
      } else {
        // Final fallback: persist a minimal pending snapshot so refreshes don't re-trigger spinner
        const fallback = {
          state: 'pending',
          progress_percent: 0,
          processed: 0,
          total: 3,
          details: { daily_bars: 0, minute_bars: 0, users: 0, runners: 0, date_range: { start: null, end: null }, checks_done: 0, checks_total: 3 }
        }
        importStatus.value = fallback
        writeImportCache(fallback)
        writeImportOnceCache(fallback)
      }
    } finally {
      _startPolling()
      // We intentionally DO NOT poll import status; it's a one-time check
      importLoading.value = false
      // Logs panel removed from SimulationView; skip logs polling
    }
  }

  // Manual one-shot refresh for the Simulation card
  async function refreshOnce () {
    try {
      // Reset ETA calculation on manual refresh
      lastProgressData = null
      lastPollTime = null

      const [p, s] = await Promise.all([
        SimulationAPI.progress().catch(() => null),
        SimulationAPI.status().catch(() => null)
      ])
      if (p || s) {
        const runningFlag = s && (typeof s.running !== 'undefined' ? s.running : s?.is_running)
        const percent = p?.progress_percent ?? p?.progress ?? s?.progress_percent ?? s?.progress ?? status.value.progress_percent
        const eta = p?.estimated_finish_seconds ?? p?.eta_seconds ?? s?.eta_seconds ?? s?.eta ?? status.value.eta_seconds
        
        status.value.state = (s?.state ?? s?.status) ?? (runningFlag ? 'running' : status.value.state)
        status.value.progress_percent = Math.round(percent ?? 0)
        status.value.eta_seconds = eta ?? null
        status.value.eta_label = p?.estimated_finish ?? status.value.eta_label ?? null
        status.value.estimated_finish_iso = p?.estimated_finish_iso ?? status.value.estimated_finish_iso ?? null
        status.value.total_buys = p?.total_buys ?? s?.total_buys ?? status.value.total_buys
        status.value.total_sells = p?.total_sells ?? s?.total_sells ?? status.value.total_sells
        status.value.last_ts = p?.sim_time_iso ?? s?.last_ts ?? status.value.last_ts
        status.value.current = p?.current_runner_info ?? status.value.current
        status.value.snapshot_age_seconds = s?.snapshot_age_seconds ?? status.value.snapshot_age_seconds ?? null
        writeSimCache(status.value)
      }
    } catch { /* ignore */ }
  }

  async function start () {
    if (isStarting.value) return
    isStarting.value = true
    try {
      const res = await SimulationAPI.start()
      // Update store immediately so UI reflects start without waiting for next poll
      try {
        status.value.state = res.state ?? (res.running ? 'running' : status.value.state)
        status.value.progress_percent = Math.round(res.progress_percent ?? res.progress ?? status.value.progress_percent)
        status.value.eta_seconds = res.eta_seconds ?? res.eta ?? status.value.eta_seconds
        // Mirror counters if present
        if (typeof res.total_buys !== 'undefined') status.value.total_buys = res.total_buys
        if (typeof res.total_sells !== 'undefined') status.value.total_sells = res.total_sells
        // last_ts may be present
        if (res.last_ts) {
          status.value.last_ts = res.last_ts
        }
      } catch (e) {
        /* ignore */
      }
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
      try {
        status.value.state = res.state ?? (res.running ? 'running' : 'stopped')
        if (res.last_ts) status.value.last_ts = res.last_ts
      } catch (e) { /* ignore */ }
      return res
    } finally {
      isStopping.value = false
    }
  }

  async function reset () {
    if (isResetting.value) return
    isResetting.value = true
    try {
      // Schedule server-side async reset
      const res = await SimulationAPI.reset().catch(err => ({ ok: false, error: err?.message }))

      // Poll status until completed/failed or timeout
      const timeoutMs = 120000
      const pollIntervalMs = 750
      const deadline = Date.now() + timeoutMs
      let statusRes = null
      // brief initial delay to let the job start
      await sleep(250)
      while (Date.now() < deadline) {
        statusRes = await SimulationAPI.resetStatus().catch(() => null)
        const st = statusRes?.status
        if (st === 'completed' || st === 'failed') break
        await sleep(pollIntervalMs)
      }

      if (!statusRes || (statusRes.status !== 'completed')) {
        const msg = statusRes?.error ? (`Reset failed: ${statusRes.error}`) : 'Reset timed out before completion'
        return { ok: false, message: msg, status: statusRes?.status || 'unknown' }
      }

      // Clear execution-related caches and store values AFTER server confirms completion
      try {
        sessionStorage.removeItem(SIM_STATUS_CACHE_KEY)
        sessionStorage.removeItem(ETA_CACHE_KEY)
        sessionStorage.removeItem(IMPORT_STATUS_CACHE_KEY)
      } catch {}

      lastProgressData = null
      lastPollTime = null

      status.value = {
        state: 'idle',
        progress_percent: 0,
        eta_seconds: null,
        eta_label: null,
        estimated_finish_iso: null,
        total_buys: 0,
        total_sells: 0,
        last_ts: null,
        current: null,
        snapshot_age_seconds: null
      }
      importStatus.value = { ...importDefault, state: 'ready', progress_percent: 100, processed: 3, total: 3 }
      logs.value = { warnings: [], errors: [] }
      writeSimCache(status.value)
      return { ok: true, message: 'Reset completed', deleted: statusRes?.deleted || null }
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
    status, importStatus, logs, importLoading,
    isStarting, isStopping, isResetting, isRunning,
    warmUp, start, stop, reset, destroy, refreshOnce
  }
})
