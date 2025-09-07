<template>
    <n-card title="Top Stocks by % Performance" size="large">
      <n-data-table
        :columns="columns"
        :data="items"
        :bordered="false"
        :pagination="pagination"
        :single-line="false"
      />
    </n-card>
  </template>
  
  <script setup>
  import { ref } from 'vue'
  import { NCard, NDataTable, NTag } from 'naive-ui'
  
  const props = defineProps({
    items: {
      type: Array,
      default: () => [] // [{ stock, time, timeframe, strategy, pct }]
    }
  })
  
  const pagination = ref({ pageSize: 10 })
  
  const columns = [
    { title: 'Stock', key: 'stock', minWidth: 120,
      render: (row) => row.stock
    },
    { title: 'Time', key: 'time', minWidth: 140,
      render: (row) => row.time
    },
    { title: 'Timeframe', key: 'timeframe', minWidth: 100,
      render: (row) => row.timeframe
    },
    { title: 'Strategy', key: 'strategy', minWidth: 160,
      render: (row) => row.strategy
    },
    { title: 'P&L (%)', key: 'pct', minWidth: 120,
      sorter: (a, b) => Number(a.pct) - Number(b.pct),
      render: (row) => {
        const val = Number(row.pct ?? 0).toFixed(2)
        const type = Number(row.pct ?? 0) >= 0 ? 'success' : 'error'
        return h(NTag, { type, round: true }, { default: () => `${val}%` })
      }
    }
  ]
  </script>
  