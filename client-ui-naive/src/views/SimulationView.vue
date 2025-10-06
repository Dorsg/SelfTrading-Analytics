<template>
    <n-space vertical size="large">
      <n-grid :cols="12" x-gap="16" y-gap="16">
        <n-gi :span="12">
          <ImportStatus
            :state="sim.importStatus.state"
            :progress="sim.importStatus.progress_percent"
            :processed="sim.importStatus.processed"
            :total="sim.importStatus.total"
            :details="sim.importStatus.details"
            :loading="sim.importLoading"
          />
        </n-gi>
  
        <n-gi :span="12">
          <SimulationControls
            :status="sim.status"
            :is-running="sim.isRunning"
            :is-starting="sim.isStarting"
            :is-stopping="sim.isStopping"
            :is-resetting="sim.isResetting"
            @start="handleStart"
            @stop="handleStop"
            @reset="handleReset"
          />
        </n-gi>
  
        
      </n-grid>
    </n-space>
  </template>
  
  <script setup>
  import { onMounted, onUnmounted } from 'vue'
  import { NGrid, NGi, NSpace, useMessage } from 'naive-ui'
  import ImportStatus from '@/components/ImportStatus.vue'
  import SimulationControls from '@/components/SimulationControls.vue'
  
  import { useSimulationStore } from '@/stores/simulation'
  
  const sim = useSimulationStore()
  const message = useMessage()
  
  onMounted(async () => {
    await sim.warmUp()
  })
  
  onUnmounted(() => {
    sim.destroy()
  })
  
  async function handleStart () {
    const res = await sim.start()
    // toast shown in child as well; double-safety:
    message.success(res?.message ?? 'Simulation started')
    return res
  }
  async function handleStop () {
    const res = await sim.stop()
    message.success(res?.message ?? 'Simulation stopped')
    return res
  }
  async function handleReset () {
    const res = await sim.reset()
    if (res?.ok) {
      message.success(res?.message ?? 'Reset completed')
    } else {
      message.error(res?.message ?? 'Reset failed')
    }
    return res
  }
  </script>
  