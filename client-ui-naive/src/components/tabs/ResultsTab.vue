<template>
  <div class="wrap">
    <n-space vertical size="large">
      <!-- Monthly Summary -->
      <n-card title="Monthly P&L Summary by Year" :bordered="false" size="small">
        <n-space vertical>
          <n-button @click="loadMonthlySummary" :loading="loadingMonthly">Refresh Monthly Summary</n-button>
          <div v-if="monthlySummary">
            <div v-for="(yearData, year) in monthlySummary" :key="year" class="year-section">
              <n-text strong>{{ year }}</n-text>
              <n-space vertical size="small">
                <div v-for="(monthData, month) in yearData" :key="`${year}-${month}`" class="month-row">
                  <n-text>{{ getMonthName(month) }}:</n-text>
                  <n-text>Results: {{ monthData.result_count }}</n-text>
                  <n-text>Avg P&L: ${{ monthData.avg_pnl_amount.toFixed(2) }}</n-text>
                  <n-text>Avg %: {{ monthData.avg_pnl_percent.toFixed(2) }}%</n-text>
                  <n-text>Total P&L: ${{ monthData.total_pnl_amount.toFixed(2) }}</n-text>
                  <n-text>Trades: {{ monthData.total_trades }}</n-text>
                </div>
              </n-space>
            </div>
          </div>
        </n-space>
      </n-card>

      <!-- Development Results -->
      <n-card title="Development Results (Recent)" :bordered="false" size="small">
        <n-space vertical>
          <n-button @click="loadPartialResults" :loading="loadingPartial">Refresh Partial Results</n-button>
          <div v-if="partialResults">
            <n-text strong>Execution Stats ({{ partialResults.period_days }} days):</n-text>
            <n-space vertical size="small">
              <n-text>Total Executions: {{ partialResults.execution_stats.total_executions }}</n-text>
              <n-text>Completed: {{ partialResults.execution_stats.completed }}</n-text>
              <n-text>Errors: {{ partialResults.execution_stats.errors }}</n-text>
              <n-text>Skipped: {{ partialResults.execution_stats.skipped }}</n-text>
              <n-text>Avg Execution Time: {{ (partialResults.execution_stats.avg_execution_time_seconds || 0).toFixed(2) }}s</n-text>
            </n-space>
            
            <n-text strong>Recent Results ({{ partialResults.results_count }}):</n-text>
            <n-data-table :columns="partialColumns" :data="partialResults.recent_results" :bordered="false" size="small" />
          </div>
        </n-space>
      </n-card>

      <!-- Detailed Results -->
      <n-card title="Detailed Results" :bordered="false" size="small">
        <n-space vertical>
          <n-space>
            <n-input v-model:value="filters.symbol" placeholder="Symbol" style="width: 160px" clearable />
            <n-input v-model:value="filters.strategy" placeholder="Strategy" style="width: 200px" clearable />
            <n-select v-model:value="filters.timeframe" :options="tfOptions" placeholder="Timeframe" clearable style="width: 140px" />
            <n-button type="primary" @click="load">Apply</n-button>
          </n-space>
          <n-data-table :columns="columns" :data="rows" :bordered="false" size="small" :single-line="false" />
        </n-space>
      </n-card>
    </n-space>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import axios from 'axios';
import { useMessage } from 'naive-ui';

const message = useMessage();
const filters = ref({ symbol: '', strategy: '', timeframe: null });
const tfOptions = [ { label: '5m', value: '5m' }, { label: '1d', value: '1d' } ];
const rows = ref([]);
const monthlySummary = ref(null);
const partialResults = ref(null);
const loadingMonthly = ref(false);
const loadingPartial = ref(false);

const columns = [
  { title: 'Symbol', key: 'symbol', sorter: 'default' },
  { title: 'Strategy', key: 'strategy' },
  { title: 'Timeframe', key: 'timeframe' },
  { title: 'Trades', key: 'trades_count', sorter: 'default' },
  { title: 'Final P&L', key: 'final_pnl_amount', sorter: 'default' },
  { title: 'Final %', key: 'final_pnl_percent', sorter: 'default' },
  { title: 'Start', key: 'start_ts' },
  { title: 'End', key: 'end_ts' },
];

const partialColumns = [
  { title: 'Symbol', key: 'symbol' },
  { title: 'Strategy', key: 'strategy' },
  { title: 'Timeframe', key: 'timeframe' },
  { title: 'P&L', key: 'final_pnl_amount' },
  { title: '%', key: 'final_pnl_percent' },
  { title: 'Trades', key: 'trades_count' },
  { title: 'Duration (days)', key: 'days_duration' },
  { title: 'End Date', key: 'end_ts' },
];

function getMonthName(month) {
  const months = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];
  return months[parseInt(month) - 1] || month;
}

async function loadMonthlySummary() {
  loadingMonthly.value = true;
  try {
    const res = await axios.get('/api/analytics/results/monthly-summary');
    monthlySummary.value = res.data.monthly_summary;
  } catch (err) {
    message.error('Failed to load monthly summary');
  } finally {
    loadingMonthly.value = false;
  }
}

async function loadPartialResults() {
  loadingPartial.value = true;
  try {
    const res = await axios.get('/api/analytics/results/partial?days_back=7&limit=50');
    partialResults.value = res.data;
  } catch (err) {
    message.error('Failed to load partial results');
  } finally {
    loadingPartial.value = false;
  }
}

async function load() {
  try {
    const params = new URLSearchParams();
    if (filters.value.symbol) params.append('symbol', filters.value.symbol);
    if (filters.value.strategy) params.append('strategy', filters.value.strategy);
    if (filters.value.timeframe) params.append('timeframe', filters.value.timeframe);
    const res = await axios.get(`/api/analytics/results?${params.toString()}`);
    rows.value = res.data || [];
  } catch (err) {
    message.error('Failed to load results');
  }
}

onMounted(() => {
  load();
  loadMonthlySummary();
  loadPartialResults();
});
</script>

<style scoped>
.wrap { padding: 8px; }

.year-section {
  margin-bottom: 16px;
  padding: 8px;
  border: 1px solid #444;
  border-radius: 4px;
}

.month-row {
  display: flex;
  gap: 16px;
  padding: 4px 0;
  border-bottom: 1px solid #333;
}

.month-row:last-child {
  border-bottom: none;
}
</style>


