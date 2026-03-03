import { render } from '@testing-library/react'
import { vi } from 'vitest'
import { ChartProvider, type ChartContextValue } from '../ChartContext'

export function createMockChartContext(overrides: Partial<ChartContextValue> = {}): ChartContextValue {
  return {
    getEffectiveCacheKeyForChart: vi.fn(() => 'rows'),
    getAggregation: vi.fn(() => undefined),
    getBaselineAggregation: vi.fn(() => undefined),
    getMetricLabels: vi.fn(() => ({ short: 'Rows', long: 'Row count' })),
    metricsMatch: vi.fn(() => true),
    getColumnMetadata: vi.fn(() => undefined),
    getTableDisplayNameByName: vi.fn(() => undefined),
    formatMetricPath: vi.fn(() => null),
    getCountByOptions: vi.fn(() => [{ value: 'rows', label: 'Rows' }]),
    getCountByCacheKey: vi.fn(() => 'rows'),
    handleCountByChange: vi.fn(),
    handleChartCountOverrideChange: vi.fn(),
    activeCountMenuKey: null,
    setActiveCountMenuKey: vi.fn(),
    getTableColor: vi.fn(() => '#2196F3'),
    getCountIndicatorColor: vi.fn(() => '#2196F3'),
    getCountByTableColor: vi.fn(() => null),
    getCountByLabelFromCacheKey: vi.fn(() => 'Rows'),
    activeFilterMenu: null,
    setActiveFilterMenu: vi.fn(),
    filters: [],
    setFilters: vi.fn(),
    hasColumnFilter: vi.fn(() => false),
    isValueFiltered: vi.fn(() => false),
    normalizeFilterValue: vi.fn((v) => String(v ?? '')),
    toggleFilter: vi.fn(),
    clearColumnFilter: vi.fn(),
    removeColumnFilters: vi.fn((prev) => prev),
    rangeSelections: {},
    customRangeInputs: {},
    toggleRangeFilter: vi.fn(),
    isRangeFiltered: vi.fn(() => false),
    handleCustomRangeChange: vi.fn(),
    applyCustomRange: vi.fn(),
    updateColumnRanges: vi.fn(),
    formatRangeValue: vi.fn((v) => String(v)),
    getDisplayHistogram: vi.fn((h) => h ?? []),
    showPercentageLabels: false,
    toggleViewPreference: vi.fn(),
    getSurvivalViewPreference: vi.fn((): 'histogram' | 'km' => 'histogram'),
    toggleSurvivalViewPreference: vi.fn(),
    findTable: vi.fn(() => undefined),
    findSurvivalStatusColumn: vi.fn(() => null),
    ensureSurvivalCurve: vi.fn(),
    getSurvivalCurve: vi.fn(() => undefined),
    isOnDashboard: vi.fn(() => false),
    toggleDashboard: vi.fn(),
    getBivariateSelection: vi.fn(() => undefined),
    setBivariateSelection: vi.fn(),
    getCategoricalColumns: vi.fn(() => []),
    getBivariateData: vi.fn(() => undefined),
    ensureBivariateData: vi.fn(),
    ...overrides,
  }
}

export function renderWithChartContext(
  ui: React.ReactElement,
  contextOverrides: Partial<ChartContextValue> = {}
) {
  const ctx = createMockChartContext(contextOverrides)
  const result = render(
    <ChartProvider value={ctx}>{ui}</ChartProvider>
  )
  return { ...result, ctx }
}
