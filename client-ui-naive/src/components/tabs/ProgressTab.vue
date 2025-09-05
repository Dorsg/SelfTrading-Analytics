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
            <n-text
              v-if="dbStatus.data?.date_range?.start && dbStatus.data?.date_range?.end"
            >
              Data:
              {{ new Date(dbStatus.data.date_range.start).toLocaleDateString() }}
              -
              {{ new Date(dbStatus.data.date_range.end).toLocaleDateString() }}
            </n-text>
          </n-space>

          <!-- Import progress bars (show when import in progress) -->
          <div
            v-if="dbStatus.import_progress && (dbStatus.status === 'importing' || !dbStatus.ready)"
          >
            <n-space vertical size="small">
              <div>
                <n-text strong>
                  Daily Bars Import
                  ({{ formatNumber(dbStatus.data?.daily_bars || 0) }}/{{ formatNumber(dbStatus.import_progress?.targets?.daily_target || 0) }})
                </n-text>
                <n-progress
                  type="line"
                  :percentage="dbStatus.import_progress.daily_progress"
                  :indicator-placement="'inside'"
                  processing
                />
              </div>

              <div>
                <n-text strong>
                  Minute Bars Import
                  ({{ formatNumber(dbStatus.data?.minute_bars || 0) }}/{{ formatNumber(dbStatus.import_progress?.targets?.minute_target || 0) }})
                </n-text>
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
            size="large"
          >
            {{ state.running ? 'Stop Simulation' : 'Start Simulation' }}
          </n-button>

          <n-text v-if="!dbStatus.ready" depth="3">
            Waiting for database to be ready...
          </n-text>
          <n-text v-if="state.last_ts">Last TS: {{ formatDateTime(state.last_ts) }}</n-text>
        </n-space>
      </n-card>

      <!-- Debug Information -->
      <n-card
        title="Debug Information"
        size="small"
        :bordered="false"
        v-if="progress.debug_info || progress.simulation_status"
      >
        <n-space vertical size="small">
          <div v-if="progress.data_range">
            <n-text strong>Data Range:</n-text>
            <n-text>{{ progress.data_range.start_readable }} to {{ progress.data_range.end_readable }}</n-text>
            <n-text>({{ progress.data_range.total_days }} days)</n-text>
          </div>

          <div v-if="progress.simulation_status">
            <n-text strong>Time Position:</n-text>
            <n-tag :type="progress.simulation_status.time_position === 'within_range' ? 'success' : 'error'">
              {{ progress.simulation_status.time_position }}
            </n-tag>
            <n-text v-if="progress.simulation_status.days_simulated">
              Simulated: {{ progress.simulation_status.days_simulated }} days
            </n-text>
            <n-text v-if="progress.simulation_status.days_remaining">
              Remaining: {{ progress.simulation_status.days_remaining }} days
            </n-text>
          </div>

          <div v-if="progress.debug_info">
            <n-text strong>Debug:</n-text>
            <n-text>Sim TS: {{ progress.debug_info.sim_timestamp }}</n-text>
            <n-text>State: {{ progress.debug_info.has_simulation_state ? 'Yes' : 'No' }}</n-text>
            <n-text>Running: {{ progress.debug_info.simulation_state_running }}</n-text>
          </div>
        </n-space>
      </n-card>

      <n-card title="Simulation Progress" size="small" :bordered="false">
        <n-space vertical>
          <!-- Current Context Above Bars -->
          <div v-if="progress.current || progress.sim_time_readable">
            <n-text strong>Current:</n-text>
            <n-text>
              <template v-if="progress.current">
                {{ progress.current.symbol }} · {{ progress.current.strategy }} · {{ progress.current.timeframe }} ·
              </template>
              {{ progress.sim_time_readable || '' }}
            </n-text>
          </div>

          <!-- Estimated Finish Time -->
          <div v-if="progress.estimated_finish && state.running">
            <n-text strong>Estimated Finish:</n-text>
            <n-text>{{ progress.estimated_finish }}</n-text>
          </div>

          <!-- Progress Bars -->
          <div>
            <n-text strong>5m Timeframe</n-text>
            <n-progress
              type="line"
              :percentage="(progress['5m']?.percent || 0)"
              :indicator-placement="'inside'"
              processing
            />
            <n-text depth="3">
              {{ progress['5m']?.ticks_done || 0 }} / {{ progress['5m']?.ticks_total || 0 }} ticks
            </n-text>
          </div>

          <div>
            <n-text strong>1d Timeframe</n-text>
            <n-progress
              type="line"
              :percentage="(progress['1d']?.percent || 0)"
              :indicator-placement="'inside'"
            />
            <n-text depth="3">
              {{ progress['1d']?.ticks_done || 0 }} / {{ progress['1d']?.ticks_total || 0 }} ticks
            </n-text>
          </div>

          <!-- Execution Stats -->
          <div v-if="progress.execution_stats">
            <n-text strong>Execution Statistics (24h):</n-text>
            <n-space vertical size="small">
              <n-text depth="3">Total: {{ progress.execution_stats.total_executions }}</n-text>
              <n-text depth="3">Completed: {{ progress.execution_stats.completed_executions }}</n-text>
              <n-text depth="3">Errors: {{ progress.execution_stats.error_executions }}</n-text>
              <n-text depth="3">Skipped: {{ progress.execution_stats.skipped_executions }}</n-text>
            </n-space>
          </div>

          <!-- Global Counters -->
          <div v-if="progress.counters">
            <n-text strong>Counters (All time):</n-text>
            <n-space vertical size="small">
              <n-text depth="3">Executions: {{ progress.counters.executions_all_time }}</n-text>
              <n-text depth="3">Trades: {{ progress.counters.trades_all_time }}</n-text>
            </n-space>
          </div>
        </n-space>
      </n-card>

      <n-card title="Recent Warnings & Errors" size="small" :bordered="false">
        <n-space vertical>
          <n-space align="center">
            <n-select v-model:value="logHours" :options="logHoursOptions" size="small" style="width: 120px;" />
            <n-button @click="loadLogs" size="small">Refresh</n-button>
          </n-space>

          <div class="log-container">
            <pre v-if="logs.length > 0" class="log-text">{{ logsText }}</pre>
            <n-empty v-else description="No logs found" />
          </div>
        </n-space>
      </n-card>
    </n-space>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue';
import { useMessage } from 'naive-ui';
import axios from 'axios';

const message = useMessage();

const state = ref({ running: false, last_ts: null });

const progress = ref({
  '5m': { percent: 0, ticks_done: 0, ticks_total: 0 },
  '1d': { percent: 0, ticks_done: 0, ticks_total: 0 }
});

const logs = ref([]);
const dbStatus = ref({ ready: false, status: 'checking...', data: {}, setup: {} });

// Log controls (warnings + errors only)
const logHours = ref(24);
const logHoursOptions = [
  { label: '1 hour', value: 1 },
  { label: '6 hours', value: 6 },
  { label: '12 hours', value: 12 },
  { label: '24 hours', value: 24 },
  { label: '48 hours', value: 48 }
];

// ───────── Caching (persist across refresh via sessionStorage) ─────────
const DB_STATUS_CACHE_KEY = 'analytics_db_status_cache';
function readDbCache() {
  try {
    const raw = sessionStorage.getItem(DB_STATUS_CACHE_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}
function writeDbCache(val) {
  try {
    sessionStorage.setItem(DB_STATUS_CACHE_KEY, JSON.stringify(val));
  } catch {}
}

async function loadDatabaseStatus() {
  // Use sessionStorage cache first
  const cached = readDbCache();
  if (cached) {
    dbStatus.value = cached;
    return;
  }
  try {
    const res = await axios.get('/api/analytics/database/status');
    dbStatus.value = res.data;
    writeDbCache(res.data);
  } catch (err) {
    dbStatus.value = { ready: false, status: 'error', data: {}, setup: {} };
    console.error('Failed to load database status:', err);
  }
}

function formatNumber(num) {
  if (!Number.isFinite(num)) return '0';
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M';
  if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K';
  return num.toString();
}

function formatDateTime(isoOrEpoch) {
  try {
    if (typeof isoOrEpoch === 'number') return new Date(isoOrEpoch * 1000).toLocaleString();
    return new Date(isoOrEpoch).toLocaleString();
  } catch {
    return '';
  }
}

async function loadLogs() {
  try {
    const [warningsRes, errorsRes] = await Promise.all([
      axios.get(`/api/analytics/logs/plain?hours_back=${logHours.value}&log_level=WARNING`),
      axios.get(`/api/analytics/logs/plain?hours_back=${logHours.value}&log_level=ERROR`)
    ]);
    const warnings = warningsRes.data?.log_entries || [];
    const errors = errorsRes.data?.log_entries || [];
    logs.value = [...warnings, ...errors].sort(
      (a, b) => new Date(b.timestamp) - new Date(a.timestamp)
    );
  } catch (err) {
    console.error('Failed to load logs:', err);
    logs.value = [];
  }
}

async function load() {
  try {
    const [p, s] = await Promise.all([
      axios.get('/api/analytics/progress'),
      axios.get('/api/analytics/simulation/state')
    ]);

    // normalize timeframes
    if (p.data?.timeframes) {
      progress.value['5m'] = p.data.timeframes['5m'] || progress.value['5m'];
      progress.value['1d'] = p.data.timeframes['1d'] || progress.value['1d'];
    }

    if (p.data?.sim_time_readable) {
      progress.value.sim_time_readable = p.data.sim_time_readable;
    }
    if (p.data?.execution_stats) {
      progress.value.execution_stats = p.data.execution_stats;
    }
    if (p.data?.current_runner_info) {
      progress.value.current_runner_info = p.data.current_runner_info;
    }
    if (p.data?.estimated_finish) {
      progress.value.estimated_finish = p.data.estimated_finish;
    }

    state.value = {
      running: !!s.data?.running,
      last_ts: s.data?.last_ts || null
    };
  } catch (err) {
    message.error('Failed to load progress');
  }
}

async function toggleRun() {
  try {
    if (state.value.running) {
      await axios.post('/api/analytics/simulation/stop');
      state.value.running = false;
      stopAutoAdvance();
    } else {
      await axios.post('/api/analytics/simulation/start');
      state.value.running = true;
      startAutoAdvance();
    }
    await load();
  } catch (err) {
    message.error('Failed to toggle simulation');
  }
}

let autoAdvanceTimer = null;
function startAutoAdvance() {
  if (autoAdvanceTimer) return; // Already running
  autoAdvanceTimer = setInterval(async () => {
    if (!state.value.running) return;
    try {
      // Regular mode (not "fast") so results are written
      await axios.post('/api/analytics/simulation/force-tick');
      // refresh progress ~every 5 ticks
      if (Math.random() < 0.2) await load();
    } catch (err) {
      console.error('Auto-advance error:', err);
    }
  }, 500); // 2 ticks/sec
  // eslint-disable-next-line no-console
  console.log('Auto-advance started (2 ticks/second)');
}
function stopAutoAdvance() {
  if (autoAdvanceTimer) {
    clearInterval(autoAdvanceTimer);
    autoAdvanceTimer = null;
    // eslint-disable-next-line no-console
    console.log('Auto-advance stopped');
  }
}

const logsText = computed(() =>
  logs.value
    .map(
      (entry) =>
        `[${entry.timestamp}] ${entry.level} (${entry.file}): ${entry.message}`
    )
    .join('\n')
);

onMounted(async () => {
  await load();
  await loadDatabaseStatus();
  await loadLogs();

  // If simulation is already running, resume auto-advance
  if (state.value.running) startAutoAdvance();

  const progressTimer = setInterval(load, 3000);
  const logsTimer = setInterval(loadLogs, 10000);

  window.addEventListener('beforeunload', () => {
    clearInterval(progressTimer);
    clearInterval(logsTimer);
    stopAutoAdvance();
  });
});
</script>

<style scoped>
.wrap {
  padding: 8px;
}
.log-container {
  max-height: 400px;
  overflow-y: auto;
  border: 1px solid #444;
  border-radius: 4px;
  padding: 8px;
  background: #1a1a1a;
}
.log-text {
  font-family: 'Courier New', monospace;
  font-size: 12px;
  line-height: 1.4;
  color: #ccc;
  margin: 0;
  white-space: pre-wrap;
  word-wrap: break-word;
}
</style>
