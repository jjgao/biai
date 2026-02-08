import React from 'react'
import { MAX_PIE_CATEGORIES } from '../types'
import type { ColumnAggregation } from '../types'
import { PieChart } from './PieChart'
import { BarChart } from './BarChart'
import { TableViewChart } from './TableViewChart'
import { HistogramChart } from './HistogramChart'
import { SurvivalChart } from './SurvivalChart'
import { MapChart } from './MapChart'

interface ChartByTypeParams {
  title: string
  tableName: string
  columnName: string
  tableColor: string
  aggregation: ColumnAggregation
  cacheKey: string
  normalizedDisplayType: string
  metaDisplayType?: string
  countIndicatorOverride?: React.ReactNode
  getViewPreference: (tableName: string, columnName: string, categoryCount: number) => string
  getSurvivalViewPreference: (tableName: string, columnName: string) => 'histogram' | 'km'
  toggleSurvivalViewPreference: (tableName: string, columnName: string) => void
}

interface ChartByTypeResult {
  element: React.ReactNode
  gridColumn?: string
  gridRow?: string
}

export function getChartByType({
  title,
  tableName,
  columnName,
  tableColor,
  aggregation,
  cacheKey,
  normalizedDisplayType,
  metaDisplayType,
  countIndicatorOverride,
  getViewPreference,
  getSurvivalViewPreference,
  toggleSurvivalViewPreference,
}: ChartByTypeParams): ChartByTypeResult | null {
  if ((normalizedDisplayType === 'categorical' || metaDisplayType === 'survival_status') && aggregation.categories) {
    const categoryCount = aggregation.categories.length
    const viewPref = getViewPreference(tableName, columnName, categoryCount)
    const allowPie = categoryCount <= MAX_PIE_CATEGORIES

    if (viewPref === 'table') {
      return {
        element: <TableViewChart title={title} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={cacheKey} countIndicatorOverride={countIndicatorOverride} />,
        gridColumn: 'span 2',
        gridRow: 'span 2',
      }
    }

    if (allowPie) {
      return {
        element: <PieChart title={title} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={cacheKey} countIndicatorOverride={countIndicatorOverride} />,
      }
    }

    return {
      element: <BarChart title={title} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={cacheKey} countIndicatorOverride={countIndicatorOverride} />,
      gridColumn: 'span 2',
    }
  }

  if (metaDisplayType === 'survival_time') {
    const view = getSurvivalViewPreference(tableName, columnName)
    const toggleButton = (
      <button
        type="button"
        onClick={event => {
          event.stopPropagation()
          toggleSurvivalViewPreference(tableName, columnName)
        }}
        style={{
          border: 'none',
          background: '#f0f0f0',
          color: '#333',
          borderRadius: '50%',
          width: '20px',
          height: '20px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: '0.8rem',
          fontWeight: 700,
          cursor: 'pointer',
          lineHeight: 1
        }}
        title={view === 'km' ? 'Show histogram' : 'Show survival curve'}
      >
        {view === 'km' ? '📊' : '┐'}
      </button>
    )

    if (view === 'km') {
      return {
        element: <SurvivalChart title={title} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={cacheKey} countIndicatorOverride={countIndicatorOverride} extraActions={toggleButton} showHistogram={false} />,
        gridColumn: 'span 2',
        gridRow: 'span 2',
      }
    }

    return {
      element: <HistogramChart title={title} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={cacheKey} countIndicatorOverride={countIndicatorOverride} extraActions={toggleButton} />,
      gridColumn: 'span 2',
    }
  }

  if (normalizedDisplayType === 'numeric' && aggregation.histogram) {
    return {
      element: <HistogramChart title={title} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={cacheKey} countIndicatorOverride={countIndicatorOverride} />,
      gridColumn: 'span 2',
    }
  }

  if (aggregation.display_type === 'geographic' && aggregation.categories) {
    return {
      element: <MapChart title={title} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={cacheKey} countIndicatorOverride={countIndicatorOverride} />,
      gridColumn: 'span 4',
    }
  }

  return null
}
