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
  
        <n-grid :cols="7" x-gap="16" y-gap="16">
          <n-gi>
            <n-statistic label="Progress (%)" :value="status.progress_percent ?? 0" />
          </n-gi>
          <n-gi>
            <n-statistic label="Time to finish (ETA)" :value="etaHuman" />
          </n-gi>
          <n-gi>
            <n-statistic label="Total Buys" :value="status.total_buys ?? 0" />
          </n-gi>
          <n-gi>
            <n-statistic label="Total Sells" :value="status.total_sells ?? 0" />
          </n-gi>
          <n-gi>
            <n-statistic label="Current Tick" :value="formattedTick" />
          </n-gi>
          <n-gi>
            <n-statistic label="Now Running" :value="nowRunning" />
          </n-gi>
        </n-grid>
  
        <n-progress
          type="line"
          :percentage="status.progress_percent ?? 0"
          :indicator-placement="'inside'"
          processing
          style="width:100%"
        />
      </n-space>
    </n-card>
  </template>
  
  <script setup>
  import { computed, ref } from 'vue'
  import {
    NCard, NSpace, NButton, NProgress, NStatistic, NTag, NGrid, NGi, NPopconfirm, useMessage
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
  
  const formattedTick = computed(() => {
    try {
      if (!status.value.last_ts) return '—'
      return new Date(status.value.last_ts).toLocaleString()
    } catch { return String(status.value.last_ts || '—') }
  })

  const nowRunning = computed(() => {
    const cur = status.value.current
    if (!cur) return '—'
    const tf = cur.timeframe || '—'
    const sym = cur.symbol || ''
    return `${tf}${sym ? ' · ' + sym : ''}`
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
  