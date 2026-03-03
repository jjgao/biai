import React from 'react'
import Plot from 'react-plotly.js'
import type { PlotMouseEvent, PlotSelectionEvent } from 'plotly.js'
import { TABLE_SCOPE_KEY } from '../types'
import type { BaseChartProps } from './ChartContext'
import { useChartContext } from './ChartContext'
import { ChartHeader } from './ChartHeader'
import { CountIndicator } from './CountIndicator'
import { FilterMenu } from './FilterMenu'
import { SqlViewButton } from './SqlViewerModal'
import { CompareColumnButton } from './CompareColumnButton'

export function BarChart({
  title,
  tableName,
  field,
  tableColor,
  aggregationOverride,
  cacheKeyOverride,
  countIndicatorOverride,
}: BaseChartProps) {
  const ctx = useChartContext()
  const cacheKey = cacheKeyOverride ?? ctx.getEffectiveCacheKeyForChart(tableName, field)
  const aggregation =
    aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
      ? aggregationOverride
      : ctx.getAggregation(tableName, field, cacheKey)

  const metadata = ctx.getColumnMetadata(tableName, field)
  const tableDisplayName = ctx.getTableDisplayNameByName(tableName) || tableName
  const tooltipParts = [
    metadata?.display_name || title,
    `ID: ${field}`,
    metadata?.description || '',
    `Table: ${tableDisplayName}`
  ]
  if (aggregation) {
    const pathLabel = ctx.formatMetricPath(aggregation)
    if (pathLabel) tooltipParts.push(pathLabel)
  }
  const tooltipText = tooltipParts.filter(Boolean).join('\n')

  const menuOpen =
    ctx.activeFilterMenu?.tableName === tableName &&
    ctx.activeFilterMenu.columnName === field &&
    ctx.activeFilterMenu.countKey === cacheKey
  const columnActive = ctx.hasColumnFilter(field, cacheKey)

  const actionButtons = (
    <>
      <SqlViewButton sql={aggregation?.sql} columnName={field} />
      <CompareColumnButton tableName={tableName} columnName={field} />
      <button
        type="button"
        onClick={event => {
          event.stopPropagation()
          ctx.toggleViewPreference(tableName, field)
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
          fontSize: '0.7rem',
          cursor: 'pointer',
          lineHeight: 1
        }}
        title="Switch to table view"
      >
        ⊞
      </button>
      <button
        type="button"
        onClick={event => {
          event.stopPropagation()
          ctx.toggleDashboard(tableName, field)
        }}
        style={{
          border: 'none',
          background: ctx.isOnDashboard(tableName, field) ? '#4CAF50' : '#f0f0f0',
          color: ctx.isOnDashboard(tableName, field) ? 'white' : '#333',
          borderRadius: '50%',
          width: '20px',
          height: '20px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: '0.7rem',
          cursor: 'pointer',
          lineHeight: 1
        }}
        title={ctx.isOnDashboard(tableName, field) ? 'Remove from dashboard' : 'Add to dashboard'}
      >
        {ctx.isOnDashboard(tableName, field) ? '✓' : '+'}
      </button>
      <button
        type="button"
        onClick={event => {
          event.stopPropagation()
          ctx.setActiveFilterMenu(prev =>
            prev && prev.tableName === tableName && prev.columnName === field && prev.countKey === cacheKey
              ? null
              : { tableName, columnName: field, countKey: cacheKey }
          )
        }}
        style={{
          border: 'none',
          background: menuOpen || columnActive ? '#1976D2' : '#f0f0f0',
          color: menuOpen || columnActive ? 'white' : '#333',
          borderRadius: '50%',
          width: '20px',
          height: '20px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: '0.75rem',
          cursor: 'pointer',
          lineHeight: 1
        }}
        title="Filter values"
      >
        ⚲
      </button>
    </>
  )

  const containerStyle: React.CSSProperties = {
    position: 'relative',
    background: 'white',
    padding: '0.5rem',
    borderRadius: '8px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    width: '358px',
    minHeight: '175px',
    boxSizing: 'border-box',
    flexShrink: 0,
    display: 'flex',
    flexDirection: 'column',
    border: tableColor ? `2px solid ${tableColor}20` : undefined
  }

  const countIndicator = countIndicatorOverride ?? (
    <CountIndicator
      menuKey={`${TABLE_SCOPE_KEY}:${tableName}.${field}`}
      indicatorColor={ctx.getCountIndicatorColor(tableName, cacheKey)}
      borderColor={ctx.getCountByTableColor(tableName, cacheKey)}
      label={ctx.getCountByLabelFromCacheKey(tableName, cacheKey)}
      options={ctx.getCountByOptions(tableName)}
      currentValue={cacheKey}
      buttonLabel={`Change count-by for ${tableName}.${field}`}
      onSelect={value => ctx.handleChartCountOverrideChange(tableName, field, value)}
    />
  )

  if (!aggregation || !aggregation.categories || aggregation.categories.length === 0) {
    const message = aggregation ? 'No data for current filters' : 'Loading data…'
    return (
      <div style={containerStyle}>
        <ChartHeader
          title={metadata?.display_name || title}
          tooltip={tooltipText}
          countIndicator={countIndicator}
          actions={actionButtons}
          isListColumn={metadata?.is_list_column || false}
        />
        <div
          style={{
            flex: 1,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: '#999',
            fontSize: '0.75rem',
            textAlign: 'center',
            padding: '0.5rem'
          }}
        >
          {message}
        </div>
      </div>
    )
  }

  const metricLabels = ctx.getMetricLabels(aggregation)
  const pathLabel = ctx.formatMetricPath(aggregation)
  const labels = aggregation.categories.map(c => c.display_value ?? (c.value === '' ? '(Empty)' : String(c.value)))
  const values = aggregation.categories.map(c => c.count)
  const filterValues = aggregation.categories.map(c => ctx.normalizeFilterValue(c.value))
  const totalCount = aggregation.total_rows ?? values.reduce((sum, val) => sum + val, 0)
  const percentTexts = values.map(val =>
    totalCount > 0 ? `${((val / totalCount) * 100).toFixed(1)}%` : '0%'
  )

  const baselineAggregation = ctx.getBaselineAggregation(tableName, field)
  const categoriesForMenu =
    ctx.metricsMatch(baselineAggregation, aggregation) && baselineAggregation?.categories?.length
      ? baselineAggregation.categories
      : aggregation.categories

  return (
    <div style={containerStyle}>
      <ChartHeader
        title={metadata?.display_name || title}
        tooltip={tooltipText}
        countIndicator={countIndicator}
        actions={actionButtons}
        isListColumn={metadata?.is_list_column || false}
      />
      <Plot
        data={[{
          type: 'bar',
          x: labels,
          y: values,
          marker: {
            color: filterValues.map(value =>
              ctx.isValueFiltered(field, value, cacheKey) ? '#1976D2' : '#2196F3'
            ),
            line: {
              color: filterValues.map(value =>
                ctx.isValueFiltered(field, value, cacheKey) ? '#000' : undefined
              ),
              width: filterValues.map(value =>
                ctx.isValueFiltered(field, value, cacheKey) ? 2 : 0
              )
            }
          },
          hovertemplate: `${['%{x}', `Count (${metricLabels.short}): %{y}`, 'Percent of total: %{text}']
            .concat(pathLabel ? [pathLabel] : [])
            .join('<br>')}<extra></extra>`,
          text: percentTexts,
          textposition: 'auto'
        }]}
        layout={{
          height: 135,
          margin: { t: 5, b: 40, l: 30, r: 5 },
          xaxis: { tickangle: -45, automargin: true, tickfont: { size: 9 } },
          yaxis: { title: metricLabels.long, automargin: true, tickfont: { size: 9 }, titlefont: { size: 10 } },
          paper_bgcolor: 'transparent',
          plot_bgcolor: 'transparent',
          dragmode: 'select',
          selectdirection: 'h'
        }}
        config={{
          displayModeBar: false,
          responsive: true,
          staticPlot: false,
          scrollZoom: false
        }}
        style={{ width: '348px', height: '135px', cursor: 'pointer' }}
        onClick={(event: PlotMouseEvent) => {
          const point = event.points?.[0]
          if (!point) return

          const pointIndex = point.pointIndex
          if (typeof pointIndex === 'number' && pointIndex >= 0 && pointIndex < filterValues.length) {
            const clickedValue = filterValues[pointIndex]
            ctx.toggleFilter(field, clickedValue, tableName, cacheKey)
          }
        }}
        onSelected={(event: PlotSelectionEvent) => {
          if (!event?.points || event.points.length === 0) return
          const selectedValues = event.points
            .map(p => p.pointIndex)
            .filter((idx): idx is number => typeof idx === 'number' && idx >= 0 && idx < filterValues.length)
            .map(idx => filterValues[idx])

          if (selectedValues.length > 0) {
            ctx.setFilters(prev => [
              ...ctx.removeColumnFilters(prev, field, cacheKey),
              { column: field, operator: 'in', value: selectedValues, tableName, countByKey: cacheKey }
            ])
          }
        }}
      />
      <FilterMenu tableName={tableName} columnName={field} categories={categoriesForMenu} cacheKeyOverride={cacheKey} />
    </div>
  )
}
