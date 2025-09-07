<template>
  <n-config-provider :theme-overrides="theme">
    <n-message-provider>
      <div class="app-shell">
        <header class="app-header">
          <n-space align="center" justify="space-between" class="w-full">
            <n-space align="center">
              <n-avatar size="large" round>SIM</n-avatar>
              <n-h2 style="margin:0">Simulator Console</n-h2>
            </n-space>
            <n-space align="center">
              <n-tag type="success" size="small" round>Naive-UI</n-tag>
            </n-space>
          </n-space>
        </header>

        <main class="app-main">
          <n-card size="large" embedded>
            <n-tabs type="line" :value="active" @update:value="active = $event">
              <n-tab-pane name="simulation" tab="Simulation">
                <SimulationView />
              </n-tab-pane>
              <n-tab-pane name="results" tab="Results">
                <ResultsView />
              </n-tab-pane>
            </n-tabs>
          </n-card>
        </main>
      </div>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup>
import { ref } from 'vue'
import { NConfigProvider, NMessageProvider, NCard, NTabs, NTabPane, NSpace, NAvatar, NH2, NTag } from 'naive-ui'
import SimulationView from '@/views/SimulationView.vue'
import ResultsView from '@/views/ResultsView.vue'

const active = ref('simulation')

const theme = {
  common: {
    fontWeightStrong: '600'
  },
  Card: {
    borderRadius: '16px'
  },
  Button: { borderRadius: '12px' },
  Tag: { borderRadius: '999px' }
}
</script>

<style scoped>
.app-shell {
  min-height: 100vh;
  background: linear-gradient(135deg, #0f172a 0%, #111827 100%);
  color: #f8fafc;
}
.app-header {
  padding: 16px 24px;
}
.app-main {
  padding: 0 24px 24px;
}
.w-full { width: 100%; }
</style>
