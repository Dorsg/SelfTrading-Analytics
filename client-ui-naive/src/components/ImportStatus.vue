<template>
    <n-card title="Import Status" size="large">
      <n-space vertical size="large">
        <n-space align="center" justify="space-between">
          <n-tag :type="tagType" round>{{ statusText }}</n-tag>
          <n-text depth="3">
            {{ processed }}/{{ total }} files
          </n-text>
        </n-space>
  
        <n-progress
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
    total: { type: Number, default: 0 }
  })
  
  const statusText = computed(() => {
    switch (props.state) {
      case 'importing': return 'Importing'
      case 'ready': return 'Ready'
      case 'error': return 'Error'
      default: return 'Idle'
    }
  })
  
  const tagType = computed(() => {
    switch (props.state) {
      case 'importing': return 'info'
      case 'ready': return 'success'
      case 'error': return 'error'
      default: return 'default'
    }
  })
  </script>
  