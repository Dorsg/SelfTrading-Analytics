<template>
    <n-card :title="title" size="large">
      <n-data-table
        :columns="columns"
        :data="rows"
        :bordered="false"
        :single-line="false"
        :row-class-name="rowClass"
      />
    </n-card>
  </template>
  
  <script setup>
  import { h } from 'vue'
  import { NCard, NDataTable, NProgress, NTag } from 'naive-ui'
  
  const props = defineProps({
    title: { type: String, required: true },
    rows: {
      type: Array,
      default: () => [] // [{ bucket, pct }]
    }
  })
  
  function barCell (pct) {
    const val = Math.round(Number(pct) || 0)
    return h(NProgress, {
      type: 'line',
      percentage: Math.min(100, Math.abs(val)),
      indicatorPlacement: 'inside',
      processing: true
    }, {
      default: () => `${val}%`
    })
  }
  
  const columns = [
    {
      title: 'Bucket',
      key: 'bucket',
      minWidth: 120,
      render (row) {
        return h(NTag, { type: 'default', round: true }, { default: () => row.bucket })
      }
    },
    {
      title: 'P&L (%)',
      key: 'pct',
      minWidth: 220,
      render (row) {
        return barCell(row.pct)
      }
    }
  ]
  
  function rowClass (row) {
    const v = Number(row.pct || 0)
    return v >= 0 ? 'pos' : 'neg'
  }
  </script>
  
  <style scoped>
  :deep(.n-data-table .n-data-table-tr.pos td) {
    background: rgba(16, 185, 129, 0.06);
  }
  :deep(.n-data-table .n-data-table-tr.neg td) {
    background: rgba(239, 68, 68, 0.06);
  }
  </style>
  