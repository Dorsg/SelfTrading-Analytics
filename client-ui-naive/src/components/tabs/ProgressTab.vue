<template>
  <div class="wrap">
    <n-space vertical size="large">
      <!-- Database Readiness Status -->
      <n-card title="Database Status" size="small" :bordered="false">
        <n-space vertical>
          <!-- Status indicator -->
          <n-space align="center">
            <n-tag :type="dbStatus.ready ? 'success' : 'warning'">
              {{ dbStatus.ready ? 'Ready' : dbStatus.status }}
            </n-tag>
            <n-text v-if="dbStatus.data?.daily_bars">
              {{ formatNumber(dbStatus.data.daily_bars) }} daily bars
            </n-text>
            <n-text v-if="dbStatus.data?.minute_bars">
              {{ formatNumber(dbStatus.data.minute_bars) }} minute bars
            </n-text>
            <n-text v-if="dbStatus.setup?.runners">
              {{ dbStatus.setup.runners }} runners configured
            </n-text>
            <n-text v-if="dbStatus.data?.date_range?.start && dbStatus.data?.date_range?.end">
              Data: {{ new Date(dbStatus.data.date_range.start).toLocaleDateString() }} - 
              {{ new Date(dbStatus.data.date_range.end).toLocaleDateString() }}
            </n-text>
          </n-space>
          
          <!-- Import progress bars (show when import in progress) -->
          <div v-if="dbStatus.import_progress && (dbStatus.status === 'importing' || !dbStatus.ready)">
            <n-space vertical size="small">
              <div>
                <n-text strong>Daily Bars Import ({{ formatNumber(dbStatus.data?.daily_bars || 0) }}/{{ formatNumber(dbStatus.import_progress?.targets?.daily_target || 0) }})</n-text>
                <n-progress 
                  type="line" 
                  :percentage="dbStatus.import_progress.daily_progress" 
                  :indicator-placement="'inside'" 
                  processing 
                />
              </div>
              <div>
                <n-text strong>Minute Bars Import ({{ formatNumber(dbStatus.data?.minute_bars || 0) }}/{{ formatNumber(dbStatus.import_progress?.targets?.minute_target || 0) }})</n-text>
                <n-progress 
                  type="line" 
                  :percentage="dbStatus.import_progress.minute_progress" 
                  :indicator-placement="'inside'" 
                  processing 
                />
              </div>
              <div>
                <n-text strong>Overall Progress</n-text>
                <n-progress 
                  type="line" 
                  :percentage="dbStatus.import_progress.overall_progress" 
                  :indicator-placement="'inside'" 
                  :status="dbStatus.import_progress.overall_progress >= 100 ? 'success' : 'default'"
                />
              </div>
            </n-space>
          </div>
          

        </n-space>
      </n-card>

      <n-card title="Simulation Controls" size="small" :bordered="false">
        <n-space align="center">
          <n-tag type="success" v-if="state.running">Running</n-tag>
          <n-tag type="warning" v-else>Stopped</n-tag>
          <n-button 
            type="primary" 
            @click="toggleRun" 
            :disabled="!dbStatus.ready" 
            ghost
          >
            {{ state.running ? 'Stop Simulation' : 'Start Simulation' }}
          </n-button>
          <n-text v-if="!dbStatus.ready" depth="3">
            Waiting for database to be ready...
          </n-text>
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
import { ref, onMounted, watch } from 'vue';
import { useMessage } from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const state = ref({ running: false, last_ts: null });
const progress = ref({ '5m': { percent: 0, ticks_done: 0, ticks_total: 0 }, '1d': { percent: 0, ticks_done: 0, ticks_total: 0 } });
const errors = ref([]);
const dbStatus = ref({ ready: false, status: 'checking...', data: {}, setup: {} });

const errColumns = [
  { title: 'Time', key: 'time' },
  { title: 'Symbol', key: 'symbol' },
  { title: 'Status', key: 'status' },
  { title: 'Reason', key: 'reason' },
  { title: 'Strategy', key: 'strategy' },
];

async function loadDatabaseStatus() {
  try {
    const res = await axios.get('/api/analytics/database/status');
    dbStatus.value = res.data;
  } catch (err) {
    dbStatus.value = { ready: false, status: 'error', data: {}, setup: {} };
    console.error('Failed to load database status:', err);
  }
}

function formatNumber(num) {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  } else if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num?.toString() || '0';
}

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
  loadDatabaseStatus();
  
  // Refresh progress every 3 seconds
  const progressTimer = setInterval(load, 3000);
  
  // Refresh database status more frequently during import
  const getDbRefreshInterval = () => {
    return dbStatus.value.status === 'importing' ? 2000 : 10000; // 2s when importing, 10s when stable
  };
  
  let dbTimer = setInterval(loadDatabaseStatus, getDbRefreshInterval());
  
  // Update timer interval based on status
  const updateDbTimer = () => {
    clearInterval(dbTimer);
    dbTimer = setInterval(loadDatabaseStatus, getDbRefreshInterval());
  };
  
  // Watch for status changes to adjust refresh rate
  watch(() => dbStatus.value.status, updateDbTimer);
  
  window.addEventListener('beforeunload', () => {
    clearInterval(progressTimer);
    clearInterval(dbTimer);
  });
});
</script>

<style scoped>
.wrap { padding: 8px; }
</style>


