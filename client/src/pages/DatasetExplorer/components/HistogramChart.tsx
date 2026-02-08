import React from 'react'
import Plot from 'react-plotly.js'
import type { PlotMouseEvent, PlotSelectionEvent } from 'plotly.js'
import { rangesEqual } from '../../../utils/filterHelpers'
import { TABLE_SCOPE_KEY } from '../types'
import type { BaseChartProps } from './ChartContext'
import { useChartContext } from './ChartContext'
import { ChartHeader } from './ChartHeader'
import { CountIndicator } from './CountIndicator'
import { NumericFilterMenu } from './NumericFilterMenu'

interface HistogramChartProps extends BaseChartProps {
  extraActions?: React.ReactNode
}

export function HistogramChart({
  title,
  tableName,
  field,
  tableColor,
  aggregationOverride,
  cacheKeyOverride,
  countIndicatorOverride,
  extraActions: _extraActions,
}: HistogramChartProps) {
  const ctx = useChartContext()
  const cacheKey = cacheKeyOverride ?? ctx.getEffectiveCacheKeyForChart(tableName, field)
  const aggregation =
    aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
      ? aggregationOverride
      : ctx.getAggregation(tableName, field, cacheKey)
  if (!aggregation?.numeric_stats) return null

  const rawHistogram = aggregation.histogram ?? []
  if (rawHistogram.length === 0) return null

  const metricLabels = ctx.getMetricLabels(aggregation)
  const pathLabel = ctx.formatMetricPath(aggregation)
  const metadata = ctx.getColumnMetadata(tableName, field)
  const tableDisplayName = ctx.getTableDisplayNameByName(tableName) || tableName

  const statsText = [
    `Mean: ${aggregation.numeric_stats.mean !== null ? aggregation.numeric_stats.mean.toFixed(2) : 'N/A'}`,
    `Median: ${aggregation.numeric_stats.median !== null ? aggregation.numeric_stats.median.toFixed(2) : 'N/A'}`,
    `Range: [${aggregation.numeric_stats.min !== null ? aggregation.numeric_stats.min.toFixed(2) : 'N/A'}, ${aggregation.numeric_stats.max !== null ? aggregation.numeric_stats.max.toFixed(2) : 'N/A'}]`
  ].join(' | ')

  const tooltipParts = [
    metadata?.display_name || title,
    `ID: ${field}`,
    metadata?.description || '',
    `Table: ${tableDisplayName}`
  ]
  if (pathLabel) tooltipParts.push(pathLabel)
  tooltipParts.push('', statsText)
  const tooltipText = tooltipParts.filter(Boolean).join('\n')

  const baselineAggregation = ctx.getBaselineAggregation(tableName, field)
  const histogramMatches = ctx.metricsMatch(baselineAggregation, aggregation)
  const menuHistogram = histogramMatches && baselineAggregation?.histogram?.length
    ? baselineAggregation.histogram
    : rawHistogram
  const menuStats = histogramMatches && baselineAggregation?.numeric_stats
    ? baselineAggregation.numeric_stats
    : aggregation.numeric_stats

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

  const displayHistogram = ctx.getDisplayHistogram(menuHistogram, menuStats)
  const binsForPlot = displayHistogram.length > 0 ? displayHistogram : menuHistogram

  const xValues = binsForPlot.map(bin => (bin.bin_start + bin.bin_end) / 2)
  const yValues = binsForPlot.map(baselineBin => {
    let totalCount = 0
    rawHistogram.forEach(filteredBin => {
      const overlapStart = Math.max(baselineBin.bin_start, filteredBin.bin_start)
      const overlapEnd = Math.min(baselineBin.bin_end, filteredBin.bin_end)
      if (overlapStart < overlapEnd) {
        const filteredBinWidth = filteredBin.bin_end - filteredBin.bin_start
        const overlapWidth = overlapEnd - overlapStart
        const overlapFraction = overlapWidth / filteredBinWidth
        totalCount += filteredBin.count * overlapFraction
      }
    })
    return totalCount
  })
  const totalMetricCount = aggregation.total_rows ?? rawHistogram.reduce((sum, bin) => sum + bin.count, 0)
  const sumY = yValues.reduce((sum, val) => sum + val, 0)
  const scalingFactor = sumY > 0 && totalMetricCount > 0 ? totalMetricCount / sumY : 1
  const adjustedYValues = scalingFactor < 1 ? yValues.map(val => val * scalingFactor) : yValues
  const roundedYValues = adjustedYValues.map(val => Math.max(0, val))
  const binWidth = binsForPlot[0] ? binsForPlot[0].bin_end - binsForPlot[0].bin_start : 1
  const totalCount = totalMetricCount > 0 ? totalMetricCount : roundedYValues.reduce((sum, val) => sum + val, 0)
  const percentTexts = roundedYValues.map(val =>
    totalCount > 0 ? `${((val / totalCount) * 100).toFixed(1)}%` : '0%'
  )
  const countTexts = roundedYValues.map(val =>
    val >= 1000 ? Math.round(val).toLocaleString() : Math.round(val).toString()
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
          x: xValues,
          y: roundedYValues,
          width: binWidth * 0.9,
          marker: {
            color: binsForPlot.map(bin =>
              ctx.isRangeFiltered(tableName, field, bin.bin_start, bin.bin_end, cacheKey) ? '#2E7D32' : '#4CAF50'
            ),
            line: {
              color: binsForPlot.map(bin =>
                ctx.isRangeFiltered(tableName, field, bin.bin_start, bin.bin_end, cacheKey) ? '#000' : undefined
              ),
              width: binsForPlot.map(bin =>
                ctx.isRangeFiltered(tableName, field, bin.bin_start, bin.bin_end, cacheKey) ? 2 : 0
              )
            }
          },
          hovertemplate: `${[
            'Range: [%{customdata[0]:.2f}, %{customdata[1]:.2f}]',
            `Count (${metricLabels.short}): %{y}`,
            'Percent of total: %{customdata[2]}'
          ]
            .concat(pathLabel ? [pathLabel] : [])
            .join('<br>')}<extra></extra>`,
          customdata: binsForPlot.map((bin, idx) => [bin.bin_start, bin.bin_end, percentTexts[idx]]),
          text: ctx.showPercentageLabels ? percentTexts : countTexts,
          textposition: 'auto'
        }]}
        layout={{
          height: 135,
          margin: { t: 5, b: 30, l: 30, r: 5 },
          xaxis: { title: field, automargin: true, tickfont: { size: 9 }, titlefont: { size: 10 } },
          yaxis: { title: metricLabels.long, automargin: true, tickfont: { size: 9 }, titlefont: { size: 10 } },
          paper_bgcolor: 'transparent',
          plot_bgcolor: 'transparent',
          bargap: 0.1,
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
          if (typeof pointIndex === 'number' && pointIndex >= 0 && pointIndex < binsForPlot.length) {
            const bin = binsForPlot[pointIndex]
            ctx.toggleRangeFilter(tableName, field, bin.bin_start, bin.bin_end, cacheKey)
          }
        }}
        onSelected={(event: PlotSelectionEvent) => {
          const rangeX = event?.range?.x
          if (!rangeX || rangeX.length < 2) return
          const [minX, maxX] = rangeX
          ctx.updateColumnRanges(tableName, field, prev => {
            const nextRange = { start: minX, end: maxX }
            const existingIndex = prev.findIndex(range => rangesEqual(range, nextRange))
            if (existingIndex >= 0) return prev
            return [...prev, nextRange]
          }, cacheKey)
        }}
      />
      <NumericFilterMenu tableName={tableName} columnName={field} histogram={displayHistogram} stats={menuStats} cacheKeyOverride={cacheKey} />
    </div>
  )
}
