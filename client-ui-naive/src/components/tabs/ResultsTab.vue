<template>
  <div class="wrap">
    <n-space vertical size="large">
      <n-card title="Filters" :bordered="false" size="small">
        <n-space>
          <n-input v-model:value="filters.symbol" placeholder="Symbol" style="width: 160px" clearable />
          <n-input v-model:value="filters.strategy" placeholder="Strategy" style="width: 200px" clearable />
          <n-select v-model:value="filters.timeframe" :options="tfOptions" placeholder="Timeframe" clearable style="width: 140px" />
          <n-button type="primary" @click="load">Apply</n-button>
        </n-space>
      </n-card>
      <n-card title="Results" :bordered="false" size="small">
        <n-data-table :columns="columns" :data="rows" :bordered="false" size="small" :single-line="false" />
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

onMounted(load);
</script>

<style scoped>
.wrap { padding: 8px; }
</style>


