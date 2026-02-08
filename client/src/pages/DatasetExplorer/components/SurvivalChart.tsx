import React from 'react'
import Plot from 'react-plotly.js'
import { TABLE_SCOPE_KEY } from '../types'
import type { BaseChartProps } from './ChartContext'
import { useChartContext } from './ChartContext'
import { ChartHeader } from './ChartHeader'
import { CountIndicator } from './CountIndicator'
import { NumericFilterMenu } from './NumericFilterMenu'

interface SurvivalChartProps extends BaseChartProps {
  extraActions?: React.ReactNode
  showHistogram?: boolean
}

export function SurvivalChart({
  title,
  tableName,
  field,
  tableColor,
  aggregationOverride,
  cacheKeyOverride,
  countIndicatorOverride,
  extraActions: _extraActions,
  showHistogram = true,
}: SurvivalChartProps) {
  const ctx = useChartContext()
  const cacheKey = cacheKeyOverride ?? ctx.getEffectiveCacheKeyForChart(tableName, field)
  const aggregation =
    aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
      ? aggregationOverride
      : ctx.getAggregation(tableName, field, cacheKey)
  const table = ctx.findTable(tableName)
  if (!aggregation || !table) return null

  const statusColumn = ctx.findSurvivalStatusColumn(tableName, field)
  if (statusColumn) {
    ctx.ensureSurvivalCurve(table, field, statusColumn, cacheKey)
  }
  const curve = statusColumn ? ctx.getSurvivalCurve(tableName, field, statusColumn, cacheKey) : undefined

  const metadata = ctx.getColumnMetadata(tableName, field)
  const tableDisplayName = ctx.getTableDisplayNameByName(tableName) || tableName
  const tooltipParts = [
    metadata?.display_name || title,
    `ID: ${field}`,
    metadata?.description || '',
    `Table: ${tableDisplayName}`
  ]
  const pathLabel = ctx.formatMetricPath(aggregation)
  if (pathLabel) tooltipParts.push(pathLabel)
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

  const displayHistogram = ctx.getDisplayHistogram(aggregation.histogram ?? [], aggregation.numeric_stats)
  const binsForPlot = displayHistogram.length > 0 ? displayHistogram : aggregation.histogram || []
  const histogramPlot =
    binsForPlot.length > 0 ? (
      <Plot
        data={[{
          type: 'bar',
          x: binsForPlot.map(bin => (bin.bin_start + bin.bin_end) / 2),
          y: binsForPlot.map(bin => bin.count),
          width: binsForPlot.map(bin => bin.bin_end - bin.bin_start),
          marker: { color: tableColor || '#2196F3', opacity: 0.7 }
        }]}
        layout={{
          height: 180,
          margin: { t: 20, b: 40, l: 50, r: 10 },
          xaxis: { title: metadata?.display_name || title, tickfont: { size: 9 } },
          yaxis: { title: ctx.getMetricLabels(aggregation).long, tickfont: { size: 9 } },
          paper_bgcolor: 'transparent',
          plot_bgcolor: 'transparent',
          bargap: 0
        }}
        config={{
          displayModeBar: false,
          responsive: true,
          staticPlot: false,
          scrollZoom: false
        }}
        style={{ width: '100%', height: '180px' }}
      />
    ) : (
      <div style={{ padding: '1rem', color: '#777', fontSize: '0.85rem' }}>
        No histogram data available
      </div>
    )

  const survivalPlot = statusColumn
    ? curve && curve.length > 0 ? (
      <Plot
        data={[{
          type: 'scatter',
          mode: 'lines',
          line: { shape: 'hv', color: tableColor || '#1976D2', width: 2 },
          x: curve.map(p => p.time),
          y: curve.map(p => p.survival),
          customdata: curve.map(p => [p.atRisk, p.events, p.censored]),
          hovertemplate: [
            'Time: %{x}',
            'Survival: %{y:.3f}',
            'At risk: %{customdata[0]}',
            'Events: %{customdata[1]}',
            'Censored: %{customdata[2]}'
          ].join('<br>') + '<extra></extra>'
        }]}
        layout={{
          height: 260,
          margin: { t: 20, b: 40, l: 50, r: 10 },
          xaxis: { title: metadata?.display_name || title, tickfont: { size: 10 } },
          yaxis: { title: 'Survival probability', range: [0, 1], tickfont: { size: 10 } },
          paper_bgcolor: 'transparent',
          plot_bgcolor: 'transparent',
          hovermode: 'closest'
        }}
        config={{
          displayModeBar: false,
          responsive: true,
          staticPlot: false,
          scrollZoom: false
        }}
        style={{ width: '100%', height: '260px' }}
      />
    ) : (
      <div style={{ padding: '1rem', color: '#777', fontSize: '0.85rem' }}>
        {curve ? 'No survival data for current filters' : 'Loading survival curve…'}
      </div>
    )
    : (
      <div style={{ padding: '1rem', color: '#777', fontSize: '0.85rem' }}>
        Add a survival status column to plot a Kaplan–Meier curve.
      </div>
    )

  const containerStyle: React.CSSProperties = {
    position: 'relative',
    background: 'white',
    padding: '0.5rem',
    borderRadius: '8px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    width: '100%',
    minHeight: showHistogram ? '420px' : '320px',
    boxSizing: 'border-box',
    flexShrink: 0,
    display: 'flex',
    flexDirection: 'column',
    border: tableColor ? `2px solid ${tableColor}20` : undefined
  }

  const menuStats = aggregation.numeric_stats
  const displayHistogramForMenu = ctx.getDisplayHistogram(aggregation.histogram ?? [], aggregation.numeric_stats)

  return (
    <div style={containerStyle}>
      <ChartHeader
        title={metadata?.display_name || title}
        tooltip={tooltipText}
        countIndicator={countIndicator}
        actions={actionButtons}
        isListColumn={metadata?.is_list_column || false}
      />
      <div style={{ display: 'grid', gridTemplateColumns: showHistogram ? '2fr 1fr' : '1fr', gap: '0.5rem', flex: 1 }}>
        <div style={{ background: '#fafafa', borderRadius: '6px', padding: '0.35rem' }}>
          {survivalPlot}
        </div>
        {showHistogram && (
          <div style={{ background: '#fafafa', borderRadius: '6px', padding: '0.35rem' }}>
            {histogramPlot}
          </div>
        )}
      </div>
      <NumericFilterMenu tableName={tableName} columnName={field} histogram={displayHistogramForMenu} stats={menuStats} cacheKeyOverride={cacheKey} />
    </div>
  )
}
