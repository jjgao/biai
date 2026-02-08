import React from 'react'
import Plot from 'react-plotly.js'
import type { PlotMouseEvent, PlotSelectionEvent } from 'plotly.js'
import { getStateCode, normalizeStateName } from '../../../data/us-states'
import { TABLE_SCOPE_KEY } from '../types'
import type { BaseChartProps } from './ChartContext'
import { useChartContext } from './ChartContext'
import { ChartHeader } from './ChartHeader'
import { CountIndicator } from './CountIndicator'
import { FilterMenu } from './FilterMenu'
import { TableViewChart } from './TableViewChart'

export function MapChart({
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

  if (!aggregation?.categories) return null

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
          fontSize: '0.7rem',
          cursor: 'pointer',
          lineHeight: 1
        }}
        title={columnActive ? 'Active filter' : 'Filter'}
      >
        ≡
      </button>
    </>
  )

  const containerStyle: React.CSSProperties = {
    position: 'relative',
    background: 'white',
    padding: '0.5rem',
    borderRadius: '8px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    width: '708px',
    minHeight: '400px',
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

  const metricLabels = ctx.getMetricLabels(aggregation)
  const pathLabel = ctx.formatMetricPath(aggregation)

  // Map state values to codes for Plotly choropleth
  const stateMap = new Map<string, { count: number, name: string, originalValues: string[] }>()

  aggregation.categories.forEach(category => {
    const stateValue = category.value === '' ? '(Empty)' : String(category.value)
    const normalizedName = normalizeStateName(stateValue)
    const stateCode = getStateCode(normalizedName)

    if (stateCode) {
      const existing = stateMap.get(stateCode)
      if (existing) {
        existing.count += category.count
        existing.originalValues.push(ctx.normalizeFilterValue(category.value))
      } else {
        stateMap.set(stateCode, {
          count: category.count,
          name: normalizedName,
          originalValues: [ctx.normalizeFilterValue(category.value)]
        })
      }
    }
  })

  const locationCodes: string[] = []
  const zValues: number[] = []
  const hoverTexts: string[] = []
  const filterValues: string[][] = []

  stateMap.forEach((data, code) => {
    locationCodes.push(code)
    zValues.push(data.count)
    hoverTexts.push(data.name)
    filterValues.push(data.originalValues)
  })

  if (locationCodes.length === 0) {
    // No valid US state data - fall back to categorical table view
    return <TableViewChart title={title} tableName={tableName} field={field} tableColor={tableColor} aggregationOverride={aggregationOverride} cacheKeyOverride={cacheKeyOverride} countIndicatorOverride={countIndicatorOverride} />
  }

  const totalCount = aggregation.total_rows ?? zValues.reduce((sum, val) => sum + val, 0)

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
          type: 'choropleth',
          locationmode: 'USA-states',
          locations: locationCodes,
          z: zValues,
          text: hoverTexts,
          hovertemplate: `${['%{text}', `Count (${metricLabels.short}): %{z}`, 'Percent: %{customdata}%']
            .concat(pathLabel ? [pathLabel] : [])
            .join('<br>')}<extra></extra>`,
          customdata: zValues.map(val =>
            totalCount > 0 ? ((val / totalCount) * 100).toFixed(1) : '0'
          ),
          colorscale: [
            [0, tableColor ? `${tableColor}40` : '#E3F2FD'],
            [1, tableColor || '#2196F3']
          ],
          marker: {
            line: {
              color: locationCodes.map((_code, idx) =>
                filterValues[idx].some(v => ctx.isValueFiltered(field, v, cacheKey)) ? '#000' : 'white'
              ),
              width: locationCodes.map((_code, idx) =>
                filterValues[idx].some(v => ctx.isValueFiltered(field, v, cacheKey)) ? 3 : 1
              )
            }
          },
          showscale: true,
          colorbar: {
            title: metricLabels.short,
            titleside: 'right',
            tickfont: { size: 10 },
            len: 0.7
          }
        }]}
        layout={{
          geo: {
            scope: 'usa',
            projection: { type: 'albers usa' },
            showlakes: true,
            lakecolor: 'rgb(255, 255, 255)'
          },
          height: 380,
          margin: { t: 5, b: 5, l: 5, r: 5 },
          paper_bgcolor: 'transparent',
          dragmode: false
        }}
        config={{
          displayModeBar: false,
          responsive: true,
          staticPlot: false,
          scrollZoom: false
        }}
        style={{ width: '698px', height: '380px', cursor: 'pointer' }}
        onClick={(event: PlotMouseEvent) => {
          const point = event.points?.[0]
          if (!point) return

          const pointIndex = point.pointIndex
          if (typeof pointIndex === 'number' && pointIndex >= 0 && pointIndex < filterValues.length) {
            const stateValues = filterValues[pointIndex]
            if (stateValues.length === 1) {
              ctx.toggleFilter(field, stateValues[0], tableName, cacheKey)
            } else {
              ctx.setFilters(prev => [
                ...ctx.removeColumnFilters(prev, field, cacheKey),
                { column: field, operator: 'in', value: stateValues, tableName, countByKey: cacheKey }
              ])
            }
          }
        }}
        onSelected={(event: PlotSelectionEvent) => {
          if (!event?.points || event.points.length === 0) return
          const selectedValues = event.points
            .map(p => p.pointIndex)
            .filter((idx): idx is number => typeof idx === 'number' && idx >= 0 && idx < filterValues.length)
            .flatMap(idx => filterValues[idx])

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
