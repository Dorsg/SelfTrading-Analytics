<template>
    <n-space vertical size="large">
      <n-space justify="space-between" align="center">
        <n-h3 style="margin:0">Results</n-h3>
        <n-space>
          <n-button quaternary type="warning" :loading="sim.isResetting" @click="resetAll">
            Reset
          </n-button>
          <n-button type="primary" :loading="store.loading" @click="refresh">
            Generate Results
          </n-button>
        </n-space>
      </n-space>
  
      <ResultsBreakdown
        title="% P&L by Strategy"
        :rows="store.summary.pnl_by_strategy"
        :sortable="true"
        :default-sort="{ keys: ['win_rate_pct','avg_pct'], order: 'desc' }"
      />

      <YearStrategyTimeSummary :items="store.summary.pnl_by_year_strategy_time" />

      <BestStocksTable :items="store.topStocks" />
    </n-space>
  </template>
  
  <script setup>
  import { onMounted } from 'vue'
  import { NButton, NSpace, NGrid, NGi, NH3, useMessage } from 'naive-ui'
  import ResultsBreakdown from '@/components/ResultsBreakdown.vue'
  import BestStocksTable from '@/components/BestStocksTable.vue'
  import YearStrategyTimeSummary from '@/components/YearStrategyTimeSummary.vue'
  import { useResultsStore } from '@/stores/results'
  import { useSimulationStore } from '@/stores/simulation'
  
  const store = useResultsStore()
  const sim = useSimulationStore()
  const message = useMessage()
  
  onMounted(async () => {
    await refresh()
  })
  
  async function refresh () {
    const res = await store.fetchAll()
    if (res?.ok) message.success('Results updated')
  }

  async function resetAll () {
    const res = await sim.reset()
    if (res?.ok) {
      store.summary = { pnl_by_year: [], pnl_by_timeframe: [], pnl_by_strategy: [] }
      store.topStocks = []
      message.success(res?.message ?? 'Reset completed')
    } else {
      message.error(res?.message ?? 'Reset failed')
    }
  }
  </script>
  