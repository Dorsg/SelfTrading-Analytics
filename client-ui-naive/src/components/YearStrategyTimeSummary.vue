<template>
  <n-card title="P&L by Year / Strategy / Time" size="small">
    <n-data-table
      :columns="columns"
      :data="items"
      :bordered="false"
      :single-line="false"
      size="small"
      class="compact-table"
      :default-sort="{ columnKey: 'avg_pct', order: 'descend' }"
    />
  </n-card>
</template>

<script setup>
import { NCard, NDataTable } from 'naive-ui'

defineProps({
  items: { type: Array, default: () => [] }
})

const columns = [
  { title: 'Year', key: 'year', width: 80, sorter: (a, b) => (a.year || 0) - (b.year || 0) },
  { title: 'Strategy', key: 'strategy', ellipsis: { tooltip: true }, width: 220 },
  { title: 'Timeframe', key: 'timeframe_label', width: 110 },
  { title: 'Trades', key: 'trades', width: 90, align: 'right', sorter: (a, b) => (a.trades || 0) - (b.trades || 0) },
  { title: 'Total P&L (%)', key: 'compounded_pnl_pct', width: 140, sorter: (a, b) => (a.compounded_pnl_pct || 0) - (b.compounded_pnl_pct || 0),
    render (row) { return ((row.compounded_pnl_pct || 0).toFixed(3) + '%') } },
  { title: 'Avg. P&L (%) per Trade', key: 'avg_pct', width: 170, sorter: (a, b) => (a.avg_pct || 0) - (b.avg_pct || 0),
    render (row) { return ((row.avg_pct || 0).toFixed(3) + '%') } },
  { title: 'Avg Trade Time', key: 'avg_trade_days', width: 130, sorter: (a, b) => (a.avg_trade_days || 0) - (b.avg_trade_days || 0),
    render (row) { return ((row.avg_trade_days || 0).toFixed(1) + ' days') } }
]
</script>

<style scoped>
.compact-table :deep(.n-data-table-th),
.compact-table :deep(.n-data-table-td) {
  padding: 6px 8px;
  font-size: 12px;
}
</style>


