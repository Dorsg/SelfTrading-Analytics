<template>
    <n-card title="Warnings & Errors (plain text)" size="large">
      <n-tabs type="line" :value="tab" @update:value="tab = $event">
        <n-tab-pane name="warnings" tab="Warnings">
          <n-input
            type="textarea"
            :value="warningsText"
            :autosize="{ minRows: 12, maxRows: 20 }"
            readonly
            placeholder="No warnings yet…"
          />
        </n-tab-pane>
        <n-tab-pane name="errors" tab="Errors">
          <n-input
            type="textarea"
            :value="errorsText"
            :autosize="{ minRows: 12, maxRows: 20 }"
            readonly
            placeholder="No errors yet…"
          />
        </n-tab-pane>
      </n-tabs>
    </n-card>
  </template>
  
  <script setup>
  import { computed, ref, watch } from 'vue'
  import { NCard, NTabs, NTabPane, NInput } from 'naive-ui'
  
  const props = defineProps({
    warnings: { type: Array, default: () => [] },
    errors: { type: Array, default: () => [] }
  })
  
  const tab = ref('warnings')
  
  const warningsText = computed(() => {
    return (props.warnings ?? []).join('\n')
  })
  
  const errorsText = computed(() => {
    return (props.errors ?? []).join('\n')
  })
  
  watch(() => [props.warnings, props.errors], () => {
    // Keep the textarea content reactive; no extra logic needed here
  })
  </script>
  