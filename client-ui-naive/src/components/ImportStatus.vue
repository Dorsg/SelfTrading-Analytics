  <template>
    <n-card title="Import Status" size="large">
      <n-space v-if="isReady" vertical>
        <n-tag :type="tagType" round>Ready</n-tag>
        <n-text v-if="details?.date_range?.start && details?.date_range?.end" depth="3">
          Range: {{ new Date(details.date_range.start).toLocaleDateString() }} - {{ new Date(details.date_range.end).toLocaleDateString() }}
        </n-text>
      </n-space>
      <n-space v-else vertical size="large">
        <n-space align="center" justify="space-between">
          <n-tag :type="tagType" round>{{ statusText }}</n-tag>
          <n-text depth="3">
            {{ processed }}/{{ total }} checks
          </n-text>
        </n-space>

        <n-progress
          v-if="progress < 100"
          type="line"
          :percentage="progress"
          :indicator-placement="'inside'"
          processing
          style="width:100%"
        />

        <n-statistic label="Progress">
          <template #prefix> % </template>
          <template #value> {{ progress.toFixed(0) }} </template>
        </n-statistic>

        <div v-if="details">
          <n-space vertical size="small">
            <n-text depth="3">
              Daily bars: {{ details.daily_bars || 0 }}
              <template v-if="details.expected?.daily_bars_total">/ {{ details.expected.daily_bars_total }}</template>
            </n-text>
            <n-text depth="3">
              Minute bars: {{ details.minute_bars || 0 }}
              <template v-if="details.expected?.minute_bars_total">/ {{ details.expected.minute_bars_total }}</template>
            </n-text>
            <n-text depth="3">
              Users: {{ details.users || 0 }}
              <template v-if="details.expected?.users_total">/ {{ details.expected.users_total }}</template>
            </n-text>
            <n-text depth="3">
              Runners: {{ details.runners || 0 }}
              <template v-if="details.expected?.runners_total">/ {{ details.expected.runners_total }}</template>
            </n-text>
            <n-text v-if="details.date_range?.start && details.date_range?.end" depth="3">
              Range: {{ new Date(details.date_range.start).toLocaleDateString() }} - {{ new Date(details.date_range.end).toLocaleDateString() }}
            </n-text>
            <n-text v-if="details.marker?.exists" depth="3">Marker: {{ details.marker?.path }}</n-text>
          </n-space>
        </div>
      </n-space>
    </n-card>
  </template>
  
  <script setup>
  import { computed } from 'vue'
  import { NCard, NSpace, NTag, NText, NProgress, NStatistic } from 'naive-ui'
  
  const props = defineProps({
    state: { type: String, default: 'idle' },
    progress: { type: Number, default: 0 },
    processed: { type: Number, default: 0 },
    total: { type: Number, default: 0 },
    details: { type: Object, default: null }
  })
  
  const isReady = computed(() => props.progress >= 100 || props.state === 'ready' || props.state === 'completed')

  const statusText = computed(() => {
    switch (props.state) {
      case 'importing': return 'Importing'
      case 'ready': return 'Ready'
      case 'completed': return 'Ready'
      case 'pending': return 'Pending'
      case 'error': return 'Error'
      default: return 'Idle'
    }
  })
  
  const tagType = computed(() => {
    switch (props.state) {
      case 'importing': return 'info'
      case 'ready': return 'success'
      case 'completed': return 'success'
      case 'pending': return 'warning'
      case 'error': return 'error'
      default: return 'default'
    }
  })
  </script>
  