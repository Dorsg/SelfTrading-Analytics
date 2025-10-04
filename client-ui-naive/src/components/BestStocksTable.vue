<template>
  <n-card title="Top Stocks by % Performance" size="small">
    <n-data-table
      :columns="columns"
      :data="items"
      :pagination="{ pageSize: 10 }"
      :bordered="false"
      :single-line="false"
      size="small"
      class="compact-table"
      :default-sort="{ columnKey: 'avg_pct', order: 'descend' }"
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
  { title: 'Stock', key: 'stock', width: 80 },
  { title: 'Timeframe', key: 'timeframe', width: 80 },
  { title: 'Strategy', key: 'strategy', ellipsis: { tooltip: true }, width: 220 },
  {
    title: 'Weighted P&L (%)',
    key: 'weighted_pct',
    render (row) { return h('span', (row.weighted_pct || 0).toFixed(3) + '%') },
    sorter: (a, b) => a.weighted_pct - b.weighted_pct,
    width: 140,
  },
  {
    title: 'Avg. P&L (%) per Trade',
    key: 'avg_pct',
    render (row) {
      const val = (row.avg_pct || 0).toFixed(3) + '%'
      const cls = row.avg_pct > 0 ? 'pos' : (row.avg_pct < 0 ? 'neg' : '')
      return h('span', { class: cls }, val)
    },
    sorter: (a, b) => a.avg_pct - b.avg_pct,
    width: 160,
  },
    {
      title: 'Win Rate (%)', key: 'win_rate_pct', width: 120,
      render (row) { return h('span', (row.win_rate_pct || 0).toFixed(2) + '%') },
      sorter: (a, b) => (a.win_rate_pct || 0) - (b.win_rate_pct || 0)
    },
    {
      title: 'Avg Trade Time', key: 'avg_trade_days', width: 130,
      render (row) { return h('span', ((row.avg_trade_days || 0).toFixed(1)) + ' days') },
      sorter: (a, b) => (a.avg_trade_days || 0) - (b.avg_trade_days || 0)
    },
  {
    title: 'Trades',
    key: 'trades', width: 90,
    sorter: (a, b) => (a.trades || 0) - (b.trades || 0),
    align: 'right'
  }
]
</script>

<style scoped>
/* Light green/red for Avg P&L column */
.pos { background-color: rgba(6, 170, 85, 0.08); padding: 2px 4px; border-radius: 4px; }
.neg { background-color: rgba(220, 38, 38, 0.08); padding: 2px 4px; border-radius: 4px; }

/* Compact table styling */
.compact-table :deep(.n-data-table-th),
.compact-table :deep(.n-data-table-td) {
  padding: 6px 8px;
  font-size: 12px;
}
</style>
  