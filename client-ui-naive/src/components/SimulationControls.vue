<template>
    <n-card title="Simulation Controls" size="large">
      <n-space vertical size="large">
        <n-space justify="space-between" wrap>
          <n-space>
            <n-button type="primary" ghost size="large"
                      :loading="isStarting" :disabled="isRunning"
                      @click="onStart">
              Start
            </n-button>
            <n-button type="warning" ghost size="large"
                      :loading="isStopping" :disabled="!isRunning"
                      @click="onStop">
              Stop
            </n-button>
            <n-popconfirm @positive-click="onReset" :positive-text="'Reset'" :negative-text="'Cancel'">
              <template #trigger>
                <n-button type="error" ghost size="large"
                          :loading="isResetting" :disabled="isRunning">
                  Reset
                </n-button>
              </template>
              This will clear the current run state and counters.
            </n-popconfirm>
          </n-space>
  
          <n-space align="center">
            <n-button quaternary size="small" @click="onRefresh" :disabled="refreshing" :loading="refreshing">
              Refresh
            </n-button>
            <n-tag :type="stateTag" round>{{ status.state.toUpperCase() }}</n-tag>
          </n-space>
        </n-space>
  
        <n-space vertical size="large">
          <n-card size="small" :bordered="false" :style="'border-left:4px solid #3b82f6;background:rgba(59,130,246,0.06);'">
            <template #header>
              <n-space align="center">
                <n-text>5 minutes</n-text>
                <n-tag size="small" type="info" round>{{ (Number(tf5m.percent) || 0).toFixed(2) }}%</n-tag>
              </n-space>
            </template>
            <n-grid :cols="5" x-gap="16" y-gap="16">
              <n-gi>
                <n-statistic label="Progress" :value="`${(Number(tf5m.percent) || 0).toFixed(2)}%`" />
              </n-gi>
              <n-gi>
                <n-statistic label="ETA" :value="tf5mEta" />
              </n-gi>
              <n-gi>
                <n-statistic label="Total Buys" :value="tf5m.totalBuys ?? 0" />
              </n-gi>
              <n-gi>
                <n-statistic label="Total Sells" :value="tf5m.totalSells ?? 0" />
              </n-gi>
              <n-gi>
                <n-statistic label="Current Tick" :value="formattedTick" />
              </n-gi>
            </n-grid>
            <n-progress type="line" :percentage="tf5m.percent" :show-indicator="false" processing style="width:100%" />
            <n-text depth="3">{{ tf5m.ticksDone }} / {{ tf5m.ticksTotal }} ticks</n-text>
          </n-card>

          <n-card size="small" :bordered="false" :style="'border-left:4px solid #8b5cf6;background:rgba(139,92,246,0.06);'">
            <template #header>
              <n-space align="center">
                <n-text>1 day</n-text>
                <n-tag size="small" type="success" round>{{ (Number(tf1d.percent) || 0).toFixed(2) }}%</n-tag>
              </n-space>
            </template>
            <n-grid :cols="5" x-gap="16" y-gap="16">
              <n-gi>
                <n-statistic label="Progress" :value="`${(Number(tf1d.percent) || 0).toFixed(2)}%`" />
              </n-gi>
              <n-gi>
                <n-statistic label="ETA" :value="tf1dEta" />
              </n-gi>
              <n-gi>
                <n-statistic label="Total Buys" :value="tf1d.totalBuys ?? 0" />
              </n-gi>
                <n-gi>
                <n-statistic label="Total Sells" :value="tf1d.totalSells ?? 0" />
              </n-gi>
              <n-gi>
                <n-statistic label="Current Tick" :value="currentDay" />
              </n-gi>
            </n-grid>
            <n-progress type="line" :percentage="tf1d.percent" :show-indicator="false" processing style="width:100%" />
            <n-text depth="3">{{ tf1d.ticksDone }} / {{ tf1d.ticksTotal }} days</n-text>
          </n-card>
          <n-card size="small" :bordered="false" title="Live Metrics">
            <n-grid :cols="4" x-gap="16" y-gap="16">
              <n-gi>
                <n-statistic label="Tick Rate (ticks/s)" :value="ticksPerSec" />
              </n-gi>
              <n-gi>
                <n-statistic label="Finish">
                  <template #value>
                    <n-tooltip trigger="hover">
                      <template #trigger>
                        <span>{{ finishHuman }}</span>
                      </template>
                      {{ status.estimated_finish_iso || '—' }}
                    </n-tooltip>
                  </template>
                </n-statistic>
              </n-gi>
            </n-grid>
          </n-card>
        </n-space>
      </n-space>
    </n-card>
  </template>
  
  <script setup>
  import { computed, ref } from 'vue'
  import {
    NCard, NSpace, NButton, NProgress, NStatistic, NTag, NGrid, NGi, NPopconfirm, NText, NTooltip, useMessage
  } from 'naive-ui'
  import { useSimulationStore } from '@/stores/simulation'
  
  const props = defineProps({
    isRunning: { type: Boolean, default: false },
    isStarting: { type: Boolean, default: false },
    isStopping: { type: Boolean, default: false },
    isResetting: { type: Boolean, default: false }
  })

  // Prefer reading the central store directly to avoid stale prop snapshots
  const sim = useSimulationStore()
  const status = computed(() => sim.status)
  
  const emits = defineEmits(['start', 'stop', 'reset'])
  const message = useMessage()
  
  const stateTag = computed(() => {
    if (status.value.state === 'running') return 'info'
    if (status.value.state === 'completed') return 'success'
    if (status.value.state === 'stopped') return 'warning'
    return 'default'
  })
  
  const etaHuman = computed(() => {
    if (status.value.state !== 'running') return 'not running'

    const finishISO = status.value.estimated_finish_iso
    if (finishISO) {
      try {
        const finishDate = new Date(finishISO)
        const now = new Date()

        if (finishDate < now) return 'finishing…'

        const diffSeconds = Math.round((finishDate.getTime() - now.getTime()) / 1000)
        
        if (diffSeconds < 60) return 'in less than a minute'
        if (diffSeconds < 3600) return `in ${Math.round(diffSeconds/60)} minutes`
        
        const diffDays = Math.floor(diffSeconds / 86400)
        const finishTime = finishDate.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true })
        const finishDateStr = finishDate.toLocaleDateString('en-GB') // DD/MM/YYYY

        if (diffDays > 1) {
          return `${finishDateStr} ${finishTime}`
        }
        
        const isToday = now.toDateString() === finishDate.toDateString()
        if (isToday) {
          return `today at ${finishTime}`
        }
        
        // If not today, and not > 1 day away, it must be tomorrow-ish
        return `tomorrow at ${finishTime}`

      } catch (e) { /* fall through to calculating */ }
    }

    // Default or on error
    return 'calculating…';
  })
  
  // Pull timeframe splits from the ProgressTab snapshot via store if available
  const tf5m = computed(() => {
    // The API writes 5m % into progress_percent as a global; fallback to that
    const percent = Number.isFinite(status.value?.timeframes?.['5m']?.percent)
      ? Number(status.value.timeframes['5m'].percent)
      : (Number.isFinite(status.value.progress_percent) ? Number(status.value.progress_percent) : 0)
    const ticksDone = Number.isFinite(status.value?.timeframes?.['5m']?.ticks_done) ? Number(status.value.timeframes['5m'].ticks_done) : undefined
    const ticksTotal = Number.isFinite(status.value?.timeframes?.['5m']?.ticks_total) ? Number(status.value.timeframes['5m'].ticks_total) : undefined
    const totalBuys = Number.isFinite(status.value?.timeframes?.['5m']?.total_buys) ? Number(status.value.timeframes['5m'].total_buys) : undefined
    const totalSells = Number.isFinite(status.value?.timeframes?.['5m']?.total_sells) ? Number(status.value.timeframes['5m'].total_sells) : undefined
    return { percent, ticksDone, ticksTotal, totalBuys, totalSells }
  })
  const tf1d = computed(() => {
    // If daily aggregation is not available now, show 0 for progress
    const percent = Number.isFinite(status.value?.timeframes?.['1d']?.percent)
      ? Number(status.value.timeframes['1d'].percent)
      : 0
    const ticksDone = Number.isFinite(status.value?.timeframes?.['1d']?.ticks_done) ? Number(status.value.timeframes['1d'].ticks_done) : undefined
    const ticksTotal = Number.isFinite(status.value?.timeframes?.['1d']?.ticks_total) ? Number(status.value.timeframes['1d'].ticks_total) : undefined
    const totalBuys = Number.isFinite(status.value?.timeframes?.['1d']?.total_buys) ? Number(status.value.timeframes['1d'].total_buys) : undefined
    const totalSells = Number.isFinite(status.value?.timeframes?.['1d']?.total_sells) ? Number(status.value.timeframes['1d'].total_sells) : undefined
    return { percent, ticksDone, ticksTotal, totalBuys, totalSells }
  })

  const tf5mEta = computed(() => finishHuman.value)
  const tf1dEta = computed(() => {
    // Prefer date-only for daily; fallback to finishHuman when ISO missing
    const iso = status.value.estimated_finish_iso
    if (!iso) return finishHuman.value
    try {
      const d = new Date(iso)
      return d.toLocaleDateString()
    } catch { return finishHuman.value }
  })

  const finishHuman = computed(() => {
    let iso = status.value.estimated_finish_iso
    // Fallback: synthesize ISO from eta_seconds if available
    if (!iso && Number.isFinite(status.value.eta_seconds)) {
      try {
        const dt = new Date(Date.now() + Number(status.value.eta_seconds) * 1000)
        iso = dt.toISOString()
      } catch {}
    }
    if (!iso) return '—'
    try {
      let finishDate = new Date(iso)
      const now = new Date()
      if (finishDate < now) {
        // If stale past ISO, try to recompute from eta_seconds
        if (Number.isFinite(status.value.eta_seconds)) {
          try {
            finishDate = new Date(Date.now() + Number(status.value.eta_seconds) * 1000)
          } catch {}
        }
        if (finishDate < now) return 'calculating…'
      }
      const timeStr = finishDate.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false })
      const isToday = now.toDateString() === finishDate.toDateString()
      if (isToday) return `today at ${timeStr}`
      const tomorrow = new Date(now)
      tomorrow.setDate(now.getDate() + 1)
      if (tomorrow.toDateString() === finishDate.toDateString()) return `tomorrow at ${timeStr}`
      return `${finishDate.toLocaleDateString('en-GB')} ${timeStr}`
    } catch { return '—' }
  })

  const formattedTick = computed(() => {
    try {
      if (!status.value.last_ts) return '—'
      return new Date(status.value.last_ts).toLocaleString()
    } catch { return String(status.value.last_ts || '—') }
  })

  const currentDay = computed(() => {
    try {
      if (!status.value.last_ts) return '—'
      return new Date(status.value.last_ts).toLocaleDateString()
    } catch { return '—' }
  })

  // Live runtime metrics (not in statistics tab)
  const ticksPerSec = computed(() => {
    // approximate from smoothed rate: sim seconds per wall second divided by step seconds
    const rate = Number(status.value.rate || 0)
    const stepSec = 300 // default 5m; backend-configured, but fine for display
    if (rate <= 0) return '—'
    const tps = rate / stepSec
    return tps.toFixed(2)
  })
  const snapshotAge = computed(() => {
    const age = Number(status.value.snapshot_age_seconds)
    if (!Number.isFinite(age) || age < 0) return '—'
    return `${Math.round(age)}s`
  })
  
  function onStart () {
    // Emit start request to parent; do not await return value (emits do not carry return values)
    emits('start')
    message.info('Start requested')
  }
  function onStop () {
    emits('stop')
    message.info('Stop requested')
  }
  function onReset () {
    emits('reset')
    message.info('Reset requested')
  }

  // Manual refresh button handler
  const refreshing = ref(false)
  async function onRefresh () {
    if (refreshing.value) return
    refreshing.value = true
    try {
      await sim.refreshOnce()
      message.success('Simulation status refreshed')
    } catch (e) {
      message.error('Failed to refresh')
    } finally {
      refreshing.value = false
    }
  }
  </script>
  