<template>
  <n-card title="Top Stocks by % Performance">
    <n-data-table
      :columns="columns"
      :data="items"
      :pagination="{ pageSize: 10 }"
      :bordered="false"
      :single-line="false"
    />
  </n-card>
</template>

<script setup>
import { h } from 'vue'
import { NCard, NDataTable } from 'naive-ui'

defineProps({
  items: { type: Array, default: () => [] }
})

const columns = [
  { title: 'Stock', key: 'stock' },
  { title: 'Timeframe', key: 'timeframe' },
  { title: 'Strategy', key: 'strategy' },
  {
    title: 'Weighted P&L (%)',
    key: 'weighted_pct',
    render (row) {
      return h('span', (row.weighted_pct || 0).toFixed(2) + '%')
    },
    sorter: (a, b) => a.weighted_pct - b.weighted_pct
  },
  {
    title: 'Avg. P&L (%) per Trade',
    key: 'avg_pct',
    render (row) {
      const val = (row.avg_pct || 0).toFixed(2) + '%'
      const cls = row.avg_pct > 0 ? 'pos' : (row.avg_pct < 0 ? 'neg' : '')
      return h('span', { class: cls }, val)
    },
    sorter: (a, b) => a.avg_pct - b.avg_pct
  },
  {
    title: 'Trades',
    key: 'trades',
    sorter: (a, b) => (a.trades || 0) - (b.trades || 0)
  }
]
</script>

<style scoped>
/* Light green/red for Avg P&L column */
.pos { background-color: rgba(6, 170, 85, 0.08); padding: 2px 4px; border-radius: 4px; }
.neg { background-color: rgba(220, 38, 38, 0.08); padding: 2px 4px; border-radius: 4px; }
</style>
  