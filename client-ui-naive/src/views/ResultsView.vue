<template>
    <n-space vertical size="large">
      <n-space justify="space-between" align="center">
        <n-h3 style="margin:0">Results</n-h3>
        <n-button type="primary" :loading="store.loading" @click="refresh">
          Refresh
        </n-button>
      </n-space>
  
      <n-grid :cols="3" x-gap="16" y-gap="16">
        <n-gi>
          <ResultsBreakdown
            title="% P&L by Year/Time"
            :rows="store.summary.pnl_by_year"
          />
        </n-gi>
        <n-gi>
          <ResultsBreakdown
            title="% P&L by Timeframe"
            :rows="store.summary.pnl_by_timeframe"
          />
        </n-gi>
        <n-gi>
          <ResultsBreakdown
            title="% P&L by Strategy"
            :rows="store.summary.pnl_by_strategy"
          />
        </n-gi>
      </n-grid>
  
      <BestStocksTable :items="store.topStocks" />
    </n-space>
  </template>
  
  <script setup>
  import { onMounted } from 'vue'
  import { NButton, NSpace, NGrid, NGi, NH3, useMessage } from 'naive-ui'
  import ResultsBreakdown from '@/components/ResultsBreakdown.vue'
  import BestStocksTable from '@/components/BestStocksTable.vue'
  import { useResultsStore } from '@/stores/results'
  
  const store = useResultsStore()
  const message = useMessage()
  
  onMounted(async () => {
    await refresh()
  })
  
  async function refresh () {
    const res = await store.fetchAll()
    if (res?.ok) message.success('Results updated')
  }
  </script>
  