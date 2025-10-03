<template>
  <n-card :title="title">
    <n-table :single-line="false">
      <thead>
        <tr>
          <th>Bucket</th>
          <th>Weighted P&L (%)</th>
          <th>Avg. P&L (%) per Trade</th>
          <th>Trades</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="row.bucket" :class="row.avg_pct > 0 ? 'pos' : (row.avg_pct < 0 ? 'neg' : '')">
          <td>{{ row.bucket }}</td>
          <td>{{ formatPercent(row.weighted_pct) }}</td>
          <td>{{ formatPercent(row.avg_pct) }}</td>
          <td>{{ formatInt(row.trades) }}</td>
        </tr>
      </tbody>
    </n-table>
    <div v-if="!rows || rows.length === 0" class="no-data">
      <n-icon-wrapper :size="24" :border-radius="12">
        <n-icon :component="BanIcon" />
      </n-icon-wrapper>
      <n-text depth="3">No Data</n-text>
    </div>
  </n-card>
</template>

<script setup>
import { NCard, NTable, NIconWrapper, NIcon, NText } from 'naive-ui'
import { Ban as BanIcon } from '@vicons/fa'

defineProps({
  title: { type: String, required: true },
  rows: { type: Array, default: () => [] }
})

function formatPercent (val) {
  if (typeof val !== 'number') return '—'
  return `${val.toFixed(2)}%`
}

function formatInt (val) {
  const n = Number(val)
  if (!Number.isFinite(n)) return '0'
  return String(Math.max(0, Math.floor(n)))
}
</script>

<style scoped>
.no-data {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 24px;
}

/* Light green/red backgrounds for positive/negative Avg P&L per Trade */
tr.pos td { background-color: rgba(6, 170, 85, 0.08); }
tr.neg td { background-color: rgba(220, 38, 38, 0.08); }
</style>
  