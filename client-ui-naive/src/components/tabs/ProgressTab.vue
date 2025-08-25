<template>
  <div class="wrap">
    <n-space vertical size="large">
      <n-card title="Simulation Controls" size="small" :bordered="false">
        <n-space align="center">
          <n-tag type="success" v-if="state.running">Running</n-tag>
          <n-tag type="warning" v-else>Stopped</n-tag>
          <n-button type="primary" @click="toggleRun" ghost>
            {{ state.running ? 'Stop Simulation' : 'Start Simulation' }}
          </n-button>
          <n-text v-if="state.last_ts">Last TS: {{ new Date(state.last_ts).toLocaleString() }}</n-text>
        </n-space>
      </n-card>

      <n-card title="Progress" size="small" :bordered="false">
        <n-space vertical>
          <div>
            <n-text strong>5m</n-text>
            <n-progress type="line" :percentage="progress['5m']?.percent || 0" :indicator-placement="'inside'" processing />
            <n-text depth="3">{{ progress['5m']?.ticks_done || 0 }} / {{ progress['5m']?.ticks_total || 0 }}</n-text>
          </div>
          <div>
            <n-text strong>1d</n-text>
            <n-progress type="line" :percentage="progress['1d']?.percent || 0" :indicator-placement="'inside'" />
            <n-text depth="3">{{ progress['1d']?.ticks_done || 0 }} / {{ progress['1d']?.ticks_total || 0 }}</n-text>
          </div>
        </n-space>
      </n-card>

      <n-card title="Recent Errors" size="small" :bordered="false">
        <n-data-table :columns="errColumns" :data="errors" :bordered="false" size="small" :single-line="false" />
      </n-card>
    </n-space>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useMessage } from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const state = ref({ running: false, last_ts: null });
const progress = ref({ '5m': { percent: 0, ticks_done: 0, ticks_total: 0 }, '1d': { percent: 0, ticks_done: 0, ticks_total: 0 } });
const errors = ref([]);

const errColumns = [
  { title: 'Time', key: 'time' },
  { title: 'Symbol', key: 'symbol' },
  { title: 'Status', key: 'status' },
  { title: 'Reason', key: 'reason' },
  { title: 'Strategy', key: 'strategy' },
];

async function load() {
  try {
    const [p, s, e] = await Promise.all([
      axios.get('/api/analytics/progress'),
      axios.get('/api/analytics/simulation/state'),
      axios.get('/api/analytics/errors?limit=50'),
    ]);
    progress.value = p.data.timeframes || progress.value;
    state.value = { running: !!s.data.running, last_ts: s.data.last_ts };
    errors.value = (e.data || []).map(r => ({
      time: r.time,
      symbol: r.symbol,
      status: r.status,
      reason: r.reason,
      strategy: r.strategy,
    }));
  } catch (err) {
    message.error('Failed to load progress');
  }
}

async function toggleRun() {
  try {
    if (state.value.running) {
      await axios.post('/api/analytics/simulation/stop');
      state.value.running = false;
    } else {
      await axios.post('/api/analytics/simulation/start');
      state.value.running = true;
    }
    await load();
  } catch (err) {
    message.error('Failed to toggle simulation');
  }
}

onMounted(() => {
  load();
  const t = setInterval(load, 3000);
  window.addEventListener('beforeunload', () => clearInterval(t));
});
</script>

<style scoped>
.wrap { padding: 8px; }
</style>


