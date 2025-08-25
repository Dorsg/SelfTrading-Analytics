/* eslint-disable camelcase */
import { ref, watch, computed } from 'vue';
import { useMessage } from 'naive-ui';
import { validateSymbolViaAPI } from '@/services/dataManager';


const EXTRA_FIELDS = {
  stopLoss: {
    key   : 'stopLoss',
    label : 'Stop Loss (%)',
    component      : 'n-input-number',
    componentProps : { step: 0.5, placeholder: '-5' },
    componentSlots : { suffix: '%' },
    rule: v =>
      (typeof v === 'number' && v < 0 && v > -10) ||
      new Error('Stop Loss must be between –10 and 0'),
    toPayloadKey: 'stop_loss',
  },
  
  maPeriod: {
    key   : 'maPeriod',
    label : 'MA Period (default 150)',
    component      : 'n-input-number',
    componentProps : { step: 1, placeholder: '150' },
    rule: v => (v == null || (Number.isInteger(v) && v >= 50 && v <= 300)) || new Error('MA must be 50..300'),
    toPayloadKey: 'ma_period',
  },

  tlLookback: {
    key   : 'tlLookback',
    label : 'Trendline Lookback (default 80)',
    component      : 'n-input-number',
    componentProps : { step: 1, placeholder: '80' },
    rule: v => (v == null || (Number.isInteger(v) && v >= 20 && v <= 400)) || new Error('TL lookback must be 20..400'),
    toPayloadKey: 'tl_lookback',
  },



  takeProfit: {
    key   : 'takeProfit',
    label : 'Take Profit (%)',
    component      : 'n-input-number',
    componentProps : { step: 0.5, placeholder: '3' },
    rule: v =>
      (typeof v === 'number' && v > 0 && v < 30) ||
      new Error('Take Profit must be between 0 and 30'),
    toPayloadKey: 'take_profit',
  },

  commissionRatio: {
    key   : 'commissionRatio',
    label : 'Commission Ratio (% of order)',
    component      : 'n-input-number',
    componentProps : { step: 0.01, placeholder: '0.1' },
    rule: v =>
      (typeof v === 'number' && v >= 0.01) ||
      new Error('Commission Ratio must be ≥ 0.01'),
    toPayloadKey: 'commission_ratio',
  },

  low: {
    key   : 'low',
    label : 'Low',
    component      : 'n-input-number',
    componentProps : { step: 0.01, placeholder: '50.00' },
    rule: v =>
      (typeof v === 'number' && v > 0) ||
      new Error('Low must be a positive number'),
    toPayloadKey: 'low',
  },

  high: {
    key   : 'high',
    label : 'High',
    component      : 'n-input-number',
    componentProps : { step: 0.01, placeholder: '150.00' },
    rule: v =>
      (typeof v === 'number' && v > 0) ||
      new Error('High must be a positive number'),
    toPayloadKey: 'high',
  },

  fibHigh: {
    key   : 'fibHigh',
    label : 'Fibonacci High',
    component      : 'n-input-number',
    componentProps : { step: 0.01, placeholder: '150.00' },
    rule: v =>
      (typeof v === 'number' && v > 0) ||
      new Error('Fibonacci High must be a positive number'),
    toPayloadKey: 'fib_high',
  },

  fibLow: {
    key   : 'fibLow',
    label : 'Fibonacci Low',
    component      : 'n-input-number',
    componentProps : { step: 0.01, placeholder: '100.00' },
    rule: v =>
      (typeof v === 'number' && v > 0) ||
      new Error('Fibonacci Low must be a positive number'),
    toPayloadKey: 'fib_low',
  },

  aboveBuy: {
    key   : 'aboveBuy',
    label : 'Above Buy Price',
    component      : 'n-input-number',
    componentProps : { step: 0.01, placeholder: '100.00' },
    rule: v =>
      (typeof v === 'number' && v > 0) ||
      new Error('Above Buy Price must be a positive number'),
    toPayloadKey: 'above_buy',
  },

  belowSell: {
    key   : 'belowSell',
    label : 'Below Sell Price',
    component      : 'n-input-number',
    componentProps : { step: 0.01, placeholder: '90.00' },
    rule: v =>
      (typeof v === 'number' && v > 0) ||
      new Error('Below Sell Price must be a positive number'),
    toPayloadKey: 'below_sell',
  },
};

/**
 * Strategy catalogue – reference EXTRA_FIELDS by name.
 */
const STRATEGIES = [
  {
    label      : 'below above',
    value      : 'below_above',
    extraFields: [
      EXTRA_FIELDS.aboveBuy,
      EXTRA_FIELDS.belowSell,
      EXTRA_FIELDS.takeProfit,
      EXTRA_FIELDS.stopLoss,
    ],
  },
  {
    label      : 'chatgpt 5 strategy',
    value      : 'chatgpt_5_strategy',
    extraFields: [
      EXTRA_FIELDS.takeProfit,
      EXTRA_FIELDS.stopLoss,
    ],
  },

  {
    label      : 'grok 4 strategy',
    value      : 'grok_4_strategy',
    extraFields: [
      EXTRA_FIELDS.takeProfit,
      EXTRA_FIELDS.stopLoss,
    ],
  },
  {
    label      : 'fibonacci yuval',
    value      : 'fibonacci_yuval',
    extraFields: [
      EXTRA_FIELDS.fibHigh,
      EXTRA_FIELDS.fibLow,
      EXTRA_FIELDS.takeProfit,
      EXTRA_FIELDS.stopLoss,
    ],
  },
];


export function useCreateRunnerForm (props, emit) {
  const formRef      = ref(null)
  const isSubmitting = ref(false)
  const formSize     = ref('medium')
  const message      = useMessage()

  function getDefaultForm () {
    const nowPlus1Min = Date.now() + 60 * 1000
    const base = {
      name        : '',
      strategy    : null,
      budget      : null,
      stock       : '',
      timeFrame   : null,
      startTime   : nowPlus1Min,
      endTime     : null,
      exitStrategy: []
    }
    // initialise every extra field to null so it's easy to skip when not provided
    Object.values(EXTRA_FIELDS).forEach(f => { base[f.key] = null })
    return base
  }

  const form = ref(getDefaultForm())

  const strategyOptions  = STRATEGIES.map(s => ({ label: s.label, value: s.value }))
  const timeFrameOptions = [
    { label: '5 minutes',  value: 5    },
    { label: '15 minutes', value: 15   },
    { label: '30 minutes', value: 30   },
    { label: '1 hour',     value: 60   },
    { label: '4 hours',    value: 240  },
    { label: 'Day',        value: 1440 }
  ]

  const tsOrNull = ts => ts ? new Date(ts).toISOString() : null

  const currentStrategyConfig = computed(() =>
    STRATEGIES.find(s => s.value === form.value.strategy) || { extraFields: [] }
  )

  // ───────────────── base (always-on) validation ─────────────────
  const baseRules = {
    name: [
      { required: true, message: 'Enter name', trigger: ['input', 'blur'] },
      {
        validator: (_r, v) => {
          if (!v) return true
          const exists = props.existingRunnerNames
            .some(r => r.name.toLowerCase() === v.toLowerCase())
          return exists ? new Error('Name already exists') : true
        },
        trigger: 'blur'
      }
    ],
    strategy : { required: true, message: 'Select a strategy', trigger: 'change' },
    budget: {
      type: 'number',
      required: true,
      validator: (_r, v) => {
        if (typeof v !== 'number' || v < 1000) {
          return new Error('Budget must be at least $1 000')
        }
        const available = props.totalCash - props.allRunnersBudgetSum
        if (v > available) {
          return new Error(`Budget exceeds available cash: $${available.toLocaleString()}`)
        }
        return true
      },
      trigger: ['blur', 'change']
    },
    stock: {
      required: true,
      async validator (_r, v) {
        if (!v) return Promise.reject('Stock symbol required')
        const sym = v.toUpperCase()
        if (props.existingRunnerStocks.includes(sym)) {
          return Promise.reject('Runner for this stock already exists')
        }
        if (props.openPositionStocks.includes(sym)) {
          return Promise.reject('An open position already exists on this stock')
        }
        const ok = await validateSymbolViaAPI(sym)
        return ok ? true : Promise.reject('Symbol not found')
      },
      trigger: ['blur', 'change']
    },
    timeFrame: { type: 'number', required: true, message: 'Select time frame', trigger: 'change' },
    startTime: {
      type: 'number',
      required: true,
      validator: (_r, v) => (v && v > Date.now()) || new Error('Start time must be in the future'),
      trigger: 'change'
    },
    endTime: {
      validator (_r, v) {
        const { exitStrategy, startTime } = form.value
        if (!exitStrategy.includes('expired date')) return true
        if (v == null) return new Error('End time is required')
        if (v < Date.now()) return new Error('End time must be in the future')
        if (startTime && v <= startTime) return new Error('End time must be after start time')
        return true
      },
      trigger: ['change', 'blur']
    },
    exitStrategy: {
      type: 'array',
      required: true,
      message: 'Select at least one exit strategy',
      trigger: 'change'
    }
  }

  // ───────────────── extra-field validation (with takeProfit optional) ─────────────────
  const rules = computed(() => {
    const extraRules = {}
    const isBelowAbove = form.value.strategy === 'below_above'
    const isFibonacci = form.value.strategy === 'fibonacci_yuval'

    currentStrategyConfig.value.extraFields.forEach(f => {
      const isTP = f.key === 'takeProfit'
      const ruleList = [{
        required: !isTP, // <-- make takeProfit NOT required
        validator: (_r, v) => {
          // allow empty/null/undefined for takeProfit
          if (isTP && (v === null || v === undefined || v === '')) return true
          return f.rule(v)
        },
        trigger: ['blur', 'change']
      }]

      // Cross-field validation: Below < Above (for below_above strategy)
      if (isBelowAbove && (f.key === 'belowSell' || f.key === 'aboveBuy')) {
        ruleList.push({
          validator: () => {
            const below = form.value.belowSell
            const above = form.value.aboveBuy
            if (typeof below !== 'number' || typeof above !== 'number') return true
            if (below >= above) {
              return f.key === 'belowSell'
                ? new Error('Below Sell must be less than Above Buy')
                : new Error('Above Buy must be greater than Below Sell')
            }
            return true
          },
          trigger: ['blur', 'change']
        })
      }

      // Cross-field validation: fibLow < fibHigh (for fibonacci_yuval strategy)
      if (isFibonacci && (f.key === 'fibLow' || f.key === 'fibHigh')) {
        ruleList.push({
          validator: () => {
            const fibLow = form.value.fibLow
            const fibHigh = form.value.fibHigh
            if (typeof fibLow !== 'number' || typeof fibHigh !== 'number') return true
            if (fibLow >= fibHigh) {
              return f.key === 'fibLow'
                ? new Error('Fibonacci Low must be less than Fibonacci High')
                : new Error('Fibonacci High must be greater than Fibonacci Low')
            }
            return true
          },
          trigger: ['blur', 'change']
        })
      }

      extraRules[f.key] = ruleList
    })

    return { ...baseRules, ...extraRules }
  })

  async function validateAndSubmit () {
    if (isSubmitting.value) return
    isSubmitting.value = true

    formRef.value?.validate(async errors => {
      if (errors) {
        message.error('Please fix the errors')
        isSubmitting.value = false
        return
      }

      // build core payload
      const payload = {
        name           : form.value.name,
        strategy       : form.value.strategy,
        budget         : form.value.budget,
        stock          : form.value.stock.toUpperCase(),
        time_frame     : form.value.timeFrame,
        exit_strategy  : form.value.exitStrategy.join(','),
        time_range_from: tsOrNull(form.value.startTime),
        time_range_to  : form.value.exitStrategy.includes('expired date')
          ? tsOrNull(form.value.endTime)
          : null,
        parameters     : {}
      }

      // populate strategy-specific parameters (skip null/undefined)
      currentStrategyConfig.value.extraFields.forEach(f => {
        const val = form.value[f.key]
        if (val !== null && val !== undefined && val !== '') {
          payload.parameters[f.toPayloadKey || f.key] = val
        }
      })

      emit('create', payload)
      message.success('Runner created')
      isSubmitting.value = false
    })
  }

  watch(() => form.value.exitStrategy, arr => {
    if (!arr.includes('expired date')) form.value.endTime = null
  })

  return {
    formRef, form, formSize, isSubmitting,
    strategyOptions, timeFrameOptions,
    rules, validateAndSubmit, currentStrategyConfig
  }
}