<template>
  <n-card :title="title" size="small">
    <n-table :single-line="false" class="compact-table">
      <thead>
        <tr>
          <th class="col-bucket">Bucket</th>
          <th class="col-num">Wgt P&L (%)</th>
          <th class="col-num">Avg P&L/trade</th>
          <th v-if="hasWinRate" class="col-num">Win Rate (%)</th>
          <th v-if="hasAvgTradeTime" class="col-num">Avg Trade Time</th>
          <th class="col-num">Trades</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="row.bucket" :class="row.avg_pct > 0 ? 'pos' : (row.avg_pct < 0 ? 'neg' : '')">
          <td class="col-bucket" :title="row.bucket">{{ row.bucket }}</td>
          <td>{{ formatPercent(row.weighted_pct) }}</td>
          <td>{{ formatPercent(row.avg_pct) }}</td>
          <td v-if="hasWinRate">{{ formatPercent(row.win_rate_pct) }}</td>
          <td v-if="hasAvgTradeTime">{{ formatDays(row.avg_trade_days) }}</td>
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
import { computed } from 'vue'
import { NCard, NTable, NIconWrapper, NIcon, NText } from 'naive-ui'
import { Ban as BanIcon } from '@vicons/fa'

const props = defineProps({
  title: { type: String, required: true },
  rows: { type: Array, default: () => [] }
})

const hasWinRate = computed(() => Array.isArray(props.rows) && props.rows.some(x => typeof x?.win_rate_pct === 'number'))
const hasAvgTradeTime = computed(() => Array.isArray(props.rows) && props.rows.some(x => typeof x?.avg_trade_days === 'number'))

function formatPercent (val) {
  if (typeof val !== 'number') return '—'
  return `${val.toFixed(2)}%`
}

function formatInt (val) {
  const n = Number(val)
  if (!Number.isFinite(n)) return '0'
  return String(Math.max(0, Math.floor(n)))
}

function formatDays (val) {
  const n = Number(val)
  if (!Number.isFinite(n)) return '—'
  return `${n.toFixed(1)} days`
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

/* Compact table styling for statistics cards */
.compact-table :deep(th),
.compact-table :deep(td) {
  padding: 4px 8px;
  font-size: 12px;
  line-height: 1.2;
  white-space: nowrap;
}
.compact-table :deep(th.col-bucket),
.compact-table :deep(td.col-bucket) {
  max-width: 180px;
  overflow: hidden;
  text-overflow: ellipsis;
}
.compact-table :deep(th.col-num),
.compact-table :deep(td.col-num) {
  width: 120px;
  text-align: right;
}
</style>
  