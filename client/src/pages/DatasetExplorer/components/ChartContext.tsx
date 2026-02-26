import React, { createContext, useContext } from 'react'
import type { Filter } from '../../../utils/filterHelpers'
import type { ColumnAggregation, ColumnMetadata, HistogramBin, NumericStats, SurvivalCurvePoint, BivariateAggregation, Table } from '../types'

export interface ChartContextValue {
  // Aggregation access
  getEffectiveCacheKeyForChart: (tableName: string, columnName: string) => string
  getAggregation: (tableName: string, field: string, cacheKey: string) => ColumnAggregation | undefined
  getBaselineAggregation: (tableName: string, field: string) => ColumnAggregation | undefined
  getMetricLabels: (aggregation?: ColumnAggregation) => { short: string; long: string }
  metricsMatch: (a?: ColumnAggregation, b?: ColumnAggregation) => boolean

  // Metadata
  getColumnMetadata: (tableName: string, field: string) => ColumnMetadata | undefined
  getTableDisplayNameByName: (tableName: string) => string | undefined
  formatMetricPath: (aggregation?: ColumnAggregation) => string | null

  // Count-by
  getCountByOptions: (tableName: string) => Array<{ value: string; label: string }>
  getCountByCacheKey: (tableName: string) => string
  handleCountByChange: (tableName: string, value: string) => void
  handleChartCountOverrideChange: (tableName: string, columnName: string, value: string) => void
  activeCountMenuKey: string | null
  setActiveCountMenuKey: React.Dispatch<React.SetStateAction<string | null>>

  // Colors
  getTableColor: (tableName: string) => string
  getCountIndicatorColor: (tableName: string, cacheKey: string) => string
  getCountByTableColor: (tableName: string, cacheKey: string) => string | null
  getCountByLabelFromCacheKey: (tableName: string, cacheKey: string) => string

  // Filter state
  activeFilterMenu: { tableName: string; columnName: string; countKey?: string } | null
  setActiveFilterMenu: React.Dispatch<React.SetStateAction<{ tableName: string; columnName: string; countKey?: string } | null>>
  filters: Filter[]
  setFilters: React.Dispatch<React.SetStateAction<Filter[]>>
  hasColumnFilter: (column: string, cacheKey?: string) => boolean
  isValueFiltered: (column: string, value: string | number, cacheKey?: string) => boolean
  normalizeFilterValue: (value: string | number | null | undefined) => string
  toggleFilter: (column: string, value: string | number, tableName?: string, cacheKey?: string) => void
  clearColumnFilter: (tableName: string, column: string, cacheKey?: string) => void
  removeColumnFilters: (prev: Filter[], column: string, countKey?: string) => Filter[]

  // Range filter
  rangeSelections: Record<string, Array<{ start: number; end: number }>>
  customRangeInputs: Record<string, { min: string; max: string }>
  toggleRangeFilter: (tableName: string, column: string, binStart: number, binEnd: number, cacheKey?: string) => void
  isRangeFiltered: (tableName: string, column: string, binStart: number, binEnd: number, cacheKey?: string) => boolean
  handleCustomRangeChange: (key: string, field: 'min' | 'max', value: string) => void
  applyCustomRange: (tableName: string, column: string, cacheKey?: string) => void
  updateColumnRanges: (tableName: string, column: string, updater: (prev: Array<{ start: number; end: number }>) => Array<{ start: number; end: number }>, cacheKey?: string) => void
  formatRangeValue: (value: number) => string

  // Histogram utility
  getDisplayHistogram: (histogram: HistogramBin[] | undefined, stats: NumericStats | undefined) => HistogramBin[]

  // View preferences
  showPercentageLabels: boolean
  toggleViewPreference: (tableName: string, field: string) => void
  getSurvivalViewPreference: (tableName: string, field: string) => 'histogram' | 'km'
  toggleSurvivalViewPreference: (tableName: string, field: string) => void

  // Survival
  findTable: (tableName: string) => Table | undefined
  findSurvivalStatusColumn: (tableName: string, timeColumn: string) => string | null
  ensureSurvivalCurve: (table: Table, timeColumn: string, statusColumn: string, cacheKey?: string) => void
  getSurvivalCurve: (tableName: string, timeColumn: string, statusColumn: string, cacheKey?: string) => SurvivalCurvePoint[] | undefined

  // Dashboard
  isOnDashboard: (tableName: string, field: string) => boolean
  toggleDashboard: (tableName: string, field: string) => void

  // Bivariate (2-variable) charts
  getBivariateSelection: (tableName: string, columnName: string) => string | undefined
  setBivariateSelection: (tableName: string, columnName: string, compareColumn?: string) => void
  getCategoricalColumns: (tableName: string) => ColumnMetadata[]
  getBivariateData: (tableName: string, xColumn: string, yColumn: string, cacheKey?: string) => BivariateAggregation | undefined
  ensureBivariateData: (table: Table, xColumn: string, yColumn: string, cacheKey?: string) => void
}

const ChartContext = createContext<ChartContextValue | null>(null)

export function useChartContext(): ChartContextValue {
  const ctx = useContext(ChartContext)
  if (!ctx) {
    throw new Error('useChartContext must be used within a ChartProvider')
  }
  return ctx
}

export function ChartProvider({ value, children }: { value: ChartContextValue; children: React.ReactNode }) {
  return <ChartContext.Provider value={value}>{children}</ChartContext.Provider>
}

export interface BaseChartProps {
  title: string
  tableName: string
  field: string
  tableColor?: string
  aggregationOverride?: ColumnAggregation
  cacheKeyOverride?: string
  countIndicatorOverride?: React.ReactNode
}
