<!-- src/components/Dashboard.vue or equivalent -->

<template>
  <n-config-provider :theme="darkTheme">
    <n-message-provider>
      <n-layout>
        <n-layout-content class="content">
          <n-tabs v-model:value="activeTab" type="line" :bar-width="42" size="large" animated>
            <n-tab-pane name="progress">
              <template #tab>
                <n-icon :component="LayoutGrid" size="18" style="margin-right: 8px; vertical-align: -3px" />
                Progress
              </template>
              <ProgressTab />
            </n-tab-pane>

            <n-tab-pane name="results">
              <template #tab>
                <n-icon :component="ChartNoAxesCombined" size="18" style="margin-right: 8px; vertical-align: -3px" />
                Results
              </template>
              <ResultsTab />
            </n-tab-pane>
          </n-tabs>
        </n-layout-content>
      </n-layout>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup>
import { ref, watch } from "vue";
import { darkTheme } from "naive-ui";
import {
  LayoutGrid,
  ChartNoAxesCombined
} from 'lucide-vue-next';
import ProgressTab from "@/components/tabs/ProgressTab.vue";
import ResultsTab from "@/components/tabs/ResultsTab.vue";

const savedTab = localStorage.getItem("activeTab") || "progress";
const activeTab = ref(savedTab);
watch(activeTab, (v) => localStorage.setItem("activeTab", v));
</script>

<style scoped>
.content {
  padding: 24px;
}
.card {
  margin-top: 0;
}
</style>
