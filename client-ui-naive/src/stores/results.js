import { defineStore } from 'pinia'
import { ref } from 'vue'
import { ResultsAPI } from '@/services/api'

export const useResultsStore = defineStore('results', () => {
  const loading = ref(false)
  const summary = ref({
    pnl_by_year: [],       // [{ bucket: '2021', pct: 12.3 }]
    pnl_by_timeframe: [],  // [{ bucket: '1h', pct: 8.2 }]
    pnl_by_strategy: [],   // [{ bucket: 'my_strategy', pct: 5.1 }]
    pnl_by_year_strategy_time: [] // [{ year, strategy, timeframe, weighted_pct, avg_pct, trades, avg_trade_days }]
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
    const map = (arr, labelKey = 'bucket', valueKey1 = 'weighted_pct', valueKey2 = 'avg_pct', tradesKey = 'trades') =>
      (arr ?? []).map(x => ({
        bucket: x[labelKey],
        weighted_pct: safeNumber(x[valueKey1]),
        avg_pct: safeNumber(x[valueKey2]),
        trades: safeInt(x[tradesKey])
      }))

    return {
      pnl_by_year: map(s?.pnl_by_year),
      // timeframe breakdown removed from UI usage; keep for compatibility but unused
      pnl_by_timeframe: map(s?.pnl_by_timeframe),
      pnl_by_strategy: (s?.pnl_by_strategy ?? []).map(x => ({
        bucket: x.bucket ?? x.strategy ?? '-',
        weighted_pct: safeNumber(x.weighted_pct),
        avg_pct: safeNumber(x.avg_pct),
        trades: safeInt(x.trades),
        win_rate_pct: safeNumber(x.win_rate_pct),
        avg_trade_days: safeNumber(x.avg_trade_days)
      })),
      pnl_by_year_strategy_time: (s?.pnl_by_year_strategy_time ?? []).map(x => ({
        year: safeInt(x.year),
        strategy: x.strategy ?? '-',
        timeframe: x.timeframe ?? x.tf ?? '-',
        timeframe_label: x.timeframe_label ?? x.timeframe ?? '-',
        weighted_pct: safeNumber(x.weighted_pct),
        avg_pct: safeNumber(x.avg_pct),
        trades: safeInt(x.trades),
        avg_trade_days: safeNumber(x.avg_trade_days)
      }))
    }
  }

  function normalizeTopStocks (t) {
    return (t?.items ?? t ?? []).map(x => ({
      stock: x.stock ?? x.ticker ?? x.symbol,
      time: x.time ?? x.date ?? x.period ?? '-',
      timeframe: x.timeframe ?? x.tf ?? '-',
      strategy: x.strategy ?? '-',
      weighted_pct: safeNumber(x.weighted_pct),
      avg_pct: safeNumber(x.avg_pct),
      trades: safeInt(x.trades)
    }))
  }

  function safeNumber (val) {
    const n = typeof val === 'string' ? parseFloat(val) : Number(val)
    return Number.isFinite(n) ? n : 0
  }

  function safeInt (val) {
    const n = typeof val === 'string' ? parseInt(val, 10) : Number(val)
    return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0
  }

  return { loading, summary, topStocks, fetchAll }
})
