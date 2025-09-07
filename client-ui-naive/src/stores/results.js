import { defineStore } from 'pinia'
import { ref } from 'vue'
import { ResultsAPI } from '@/services/api'

export const useResultsStore = defineStore('results', () => {
  const loading = ref(false)
  const summary = ref({
    pnl_by_year: [],       // [{ bucket: '2021', pct: 12.3 }]
    pnl_by_timeframe: [],  // [{ bucket: '1h', pct: 8.2 }]
    pnl_by_strategy: []    // [{ bucket: 'my_strategy', pct: 5.1 }]
  })
  const topStocks = ref([]) // [{ stock, time, timeframe, strategy, pct }]

  async function fetchAll () {
    loading.value = true
    try {
      const [s, t] = await Promise.all([
        ResultsAPI.summary(),
        ResultsAPI.topStocks()
      ])
      summary.value = normalizeSummary(s)
      topStocks.value = normalizeTopStocks(t)
      return { ok: true }
    } finally {
      loading.value = false
    }
  }

  function normalizeSummary (s) {
    // Flexible mapping to support different backend keys
    const map = (arr, labelKey, valueKey) =>
      (arr ?? []).map(x => ({
        bucket: x.bucket ?? x[labelKey] ?? x[labelKey?.toUpperCase?.()] ?? x[labelKey?.toLowerCase?.()],
        pct: Number(x.pct ?? x[valueKey] ?? x.percent ?? x.percentage ?? 0)
      }))

    return {
      pnl_by_year: map(s?.pnl_by_year ?? s?.pnl_by_time ?? s?.by_year, 'year', 'pct'),
      pnl_by_timeframe: map(s?.pnl_by_timeframe ?? s?.by_timeframe, 'timeframe', 'pct'),
      pnl_by_strategy: map(s?.pnl_by_strategy ?? s?.by_strategy, 'strategy', 'pct')
    }
  }

  function normalizeTopStocks (t) {
    return (t?.items ?? t ?? []).map(x => ({
      stock: x.stock ?? x.ticker ?? x.symbol,
      time: x.time ?? x.date ?? x.period ?? '-',
      timeframe: x.timeframe ?? x.tf ?? '-',
      strategy: x.strategy ?? '-',
      pct: Number(x.pct ?? x.pnl_pct ?? x.performance_pct ?? 0)
    }))
  }

  return { loading, summary, topStocks, fetchAll }
})
