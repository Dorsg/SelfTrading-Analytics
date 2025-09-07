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
  
          <n-tag :type="stateTag" round>{{ status.state.toUpperCase() }}</n-tag>
        </n-space>
  
        <n-grid :cols="4" x-gap="16" y-gap="16">
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
  import { computed } from 'vue'
  import {
    NCard, NSpace, NButton, NProgress, NStatistic, NTag, NGrid, NGi, NPopconfirm, useMessage
  } from 'naive-ui'
  
  const props = defineProps({
    status: { type: Object, required: true },
    isRunning: { type: Boolean, default: false },
    isStarting: { type: Boolean, default: false },
    isStopping: { type: Boolean, default: false },
    isResetting: { type: Boolean, default: false }
  })
  
  const emits = defineEmits(['start', 'stop', 'reset'])
  const message = useMessage()
  
  const stateTag = computed(() => {
    if (props.status.state === 'running') return 'info'
    if (props.status.state === 'completed') return 'success'
    if (props.status.state === 'stopped') return 'warning'
    return 'default'
  })
  
  const etaHuman = computed(() => {
    const s = Number(props.status.eta_seconds ?? 0)
    if (!s || s <= 0) return '—'
    const mm = Math.floor(s / 60)
    const ss = s % 60
    return `${mm}m ${ss}s`
  })
  
  async function onStart () {
    const res = await emits('start')
    message.success(res?.message ?? 'Simulation started')
  }
  async function onStop () {
    const res = await emits('stop')
    message.success(res?.message ?? 'Simulation stopped')
  }
  async function onReset () {
    const res = await emits('reset')
    message.success(res?.message ?? 'Simulation reset')
  }
  </script>
  