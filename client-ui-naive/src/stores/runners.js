/* eslint-disable camelcase */
import { defineStore } from 'pinia'
import {
  fetchRunnersAPI, createRunnerAPI, deleteRunnersAPI,
  activateRunnersAPI, deactivateRunnersAPI,
  fetchAccountSnapshot, fetchOpenPositions, fetchMarketStatus,
  invalidateCache
} from '@/services/dataManager'

/* ─── cfg helper ───────────────────────────────────────────── */
const XRTH_TRADING_ENABLED =
  String(import.meta.env.VITE_XRTH_TRADING_ENABLED ?? 'true').toLowerCase() !== 'false'

export const useRunnersStore = defineStore('runners', {
  state: () => ({
    rows         : [],
    openPositions: [],
    loading      : false,
    totalCash    : 0,
    lastUpdate   : null
  }),

  getters: {
    /* total $ reserved by **non-removed** runners */
    budgetSum (s) {
      return s.rows
        .filter(r => r.activation !== 'removed')
        .reduce((sum, r) => sum + (r.budget || 0), 0)
    },

    activeRunnerStocks (s) {
      return s.rows
        .filter(r => (r.activation || '').toLowerCase() !== 'removed')
        .map(r => r.stock.toUpperCase());
    },

    /* % of cash that is allocated to runners */
    cashRatio (s) {
      return s.totalCash
        ? Number(((this.budgetSum / s.totalCash) * 100).toFixed(2))
        : 0
    }
  },

  actions: {
    /* ───────── initial load ───────── */
    async init () {
      this.loading = true
      try {
        await Promise.all([
          this.refreshCash(),
          this.refreshRows(),
          this.refreshPositions()
        ])
      } finally {
        this.loading = false
      }
    },

    async refreshRows  () { this.rows         = await fetchRunnersAPI() },
    async refreshPositions () { this.openPositions = await fetchOpenPositions() },
    async refreshCash  () {
      this.totalCash   = (await fetchAccountSnapshot())?.total_cash_value ?? 0
    },

    /* ───────── helper – is trading allowed _now_ ? ───────── */
    async sessionActive () {
      const session = (await fetchMarketStatus())?.toLowerCase?.() ?? ''
      if (session === 'open') return true
      if (session === 'extended-hours') return XRTH_TRADING_ENABLED
      return false
    },

    /* ───────── CRUD helpers ───────── */

    /* ——— CREATE (allowed any time) ——— */
    async create (payload) {
      this.loading = true
      try {
        const saved = await createRunnerAPI(payload)
        if (saved) {
          this.rows.push(saved)
          this.lastUpdate = saved.created_at
          invalidateCache('accountSnapshot')
        }
        return saved
      } finally { this.loading = false }
    },

    /* ——— REMOVE (needs trading-allowed) ——— */
    async remove (ids) {
      if (!ids.length) return false
      if (!(await this.sessionActive())) return false     // trading-window guard
      this.loading = true
      try {
        const res       = await deleteRunnersAPI(ids)     // { removed:[…] } or 4xx
        const succeeded = res?.removed ?? []

        /* update local state only when API confirms the flat-sell finished */
        if (succeeded.length) {
          /* they are gone for good → drop rows */
          this.rows = this.rows.filter(r => !succeeded.includes(r.id))
          invalidateCache('accountSnapshot')
        }
        return succeeded.length === ids.length            // boolean to caller
      } finally {
        this.loading = false
      }
    },

    /* ——— INACTIVATE (needs trading-allowed) ——— */
    async inactivate (ids) {
      if (!ids.length) return false
      if (!(await this.sessionActive())) return false
      this.loading = true
      try {
        const res       = await deactivateRunnersAPI(ids) // { succeeded:[…], failed:[…] }
        const succeeded = res?.succeeded ?? res?.ids ?? []

        if (succeeded.length) {
          this.rows = this.rows.map(r =>
            succeeded.includes(r.id) ? { ...r, activation: 'inactive' } : r
          )
        }
        return succeeded.length === ids.length
      } finally {
        this.loading = false
      }
    },

    /* ——— ACTIVATE (never submits orders immediately) ——— */
    async activate (ids) {
      if (!ids.length) return false
      const ok = await activateRunnersAPI(ids)
      if (ok) {
        this.rows = this.rows.map(r =>
          ids.includes(r.id) ? { ...r, activation: 'active' } : r
        )
      }
      return ok
    }
  }
})
