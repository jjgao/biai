import { useEffect } from 'react'
import Plot from 'react-plotly.js'
import { TABLE_SCOPE_KEY } from '../types'
import type { BaseChartProps } from './ChartContext'
import { useChartContext } from './ChartContext'
import { ChartHeader } from './ChartHeader'
import { CountIndicator } from './CountIndicator'
import { SqlViewButton } from './SqlViewerModal'

interface StackedBarChartProps extends Omit<BaseChartProps, 'field'> {
  xColumn: string
  yColumn: string
}

export function StackedBarChart({
  tableName,
  xColumn,
  yColumn,
  tableColor,
  cacheKeyOverride,
  countIndicatorOverride,
}: StackedBarChartProps) {
  const ctx = useChartContext()
  const cacheKey = cacheKeyOverride ?? ctx.getEffectiveCacheKeyForChart(tableName, xColumn)

  // Ensure bivariate data is loaded
  const table = ctx.findTable(tableName)
  useEffect(() => {
    if (table) {
      ctx.ensureBivariateData(table, xColumn, yColumn, cacheKey)
    }
  }, [table, xColumn, yColumn, cacheKey])

  const bivariateData = ctx.getBivariateData(tableName, xColumn, yColumn, cacheKey)

  const metadata = ctx.getColumnMetadata(tableName, xColumn)
  const yMetadata = ctx.getColumnMetadata(tableName, yColumn)
  const tableDisplayName = ctx.getTableDisplayNameByName(tableName) || tableName
  const displayTitle = `${metadata?.display_name || xColumn} vs ${yMetadata?.display_name || yColumn}`
  const tooltipParts = [
    displayTitle,
    `X: ${xColumn}`,
    `Y: ${yColumn}`,
    `Table: ${tableDisplayName}`
  ]
  const tooltipText = tooltipParts.filter(Boolean).join('\n')

  const actionButtons = (
    <>
      <SqlViewButton sql={bivariateData?.sql} columnName={`${xColumn} vs ${yColumn}`} />
      <button
        type="button"
        onClick={event => {
          event.stopPropagation()
          ctx.setBivariateSelection(tableName, xColumn, undefined)
        }}
        style={{
          border: 'none',
          background: '#FF5722',
          color: 'white',
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
        title="Remove comparison (back to single variable)"
      >
        ✕
      </button>
      <button
        type="button"
        onClick={event => {
          event.stopPropagation()
          ctx.toggleDashboard(tableName, xColumn)
        }}
        style={{
          border: 'none',
          background: ctx.isOnDashboard(tableName, xColumn) ? '#4CAF50' : '#f0f0f0',
          color: ctx.isOnDashboard(tableName, xColumn) ? 'white' : '#333',
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
        title={ctx.isOnDashboard(tableName, xColumn) ? 'Remove from dashboard' : 'Add to dashboard'}
      >
        {ctx.isOnDashboard(tableName, xColumn) ? '✓' : '+'}
      </button>
    </>
  )

  const containerStyle: React.CSSProperties = {
    position: 'relative',
    background: 'white',
    padding: '0.5rem',
    borderRadius: '8px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    width: '350px',
    minHeight: '350px',
    boxSizing: 'border-box',
    flexShrink: 0,
    display: 'flex',
    flexDirection: 'column',
    border: tableColor ? `2px solid ${tableColor}20` : undefined,
  }

  const countIndicator = countIndicatorOverride ?? (
    <CountIndicator
      menuKey={`${TABLE_SCOPE_KEY}:${tableName}.${xColumn}`}
      indicatorColor={ctx.getCountIndicatorColor(tableName, cacheKey)}
      borderColor={ctx.getCountByTableColor(tableName, cacheKey)}
      label={ctx.getCountByLabelFromCacheKey(tableName, cacheKey)}
      options={ctx.getCountByOptions(tableName)}
      currentValue={cacheKey}
      buttonLabel={`Change count-by for ${tableName}.${xColumn}`}
      onSelect={value => ctx.handleChartCountOverrideChange(tableName, xColumn, value)}
    />
  )

  if (!bivariateData) {
    return (
      <div style={containerStyle}>
        <ChartHeader
          title={displayTitle}
          tooltip={tooltipText}
          countIndicator={countIndicator}
          actions={actionButtons}
        />
        <div style={{
          flex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: '#999',
          fontSize: '0.8rem'
        }}>
          Loading...
        </div>
      </div>
    )
  }

  // Build Plotly traces - one trace per y-category (stacked)
  const { x_categories, y_categories, data } = bivariateData

  // Build a lookup map for quick access
  const dataMap: Record<string, Record<string, number>> = {}
  for (const point of data) {
    if (!dataMap[point.x]) dataMap[point.x] = {}
    dataMap[point.x][point.y] = point.count
  }

  // Color palette for y-categories (stacked segments)
  const colors = [
    '#4E79A7', '#F28E2B', '#E15759', '#76B7B2', '#59A14F',
    '#EDC948', '#B07AA1', '#FF9DA7', '#9C755F', '#BAB0AC', '#86BCB6',
  ]

  const traces = y_categories.map((yCat, idx) => ({
    type: 'bar' as const,
    name: yCat,
    x: x_categories,
    y: x_categories.map(xCat => dataMap[xCat]?.[yCat] ?? 0),
    marker: {
      color: colors[idx % colors.length],
    },
    hovertemplate: `%{x}<br>${yCat}: %{y}<extra></extra>`,
  }))

  const handleClick = (event: any) => {
    if (!event.points || event.points.length === 0) return
    const point = event.points[0]
    const xValue = point.x as string
    const yValue = y_categories[point.curveNumber]
    if (xValue) {
      const xRaw = xValue === '(Empty)' ? '' : xValue === '(N/A)' ? 'N/A' : xValue
      ctx.toggleFilter(xColumn, xRaw, tableName, cacheKey)
    }
    if (yValue) {
      const yRaw = yValue === '(Empty)' ? '' : yValue === '(N/A)' ? 'N/A' : yValue
      ctx.toggleFilter(yColumn, yRaw, tableName, cacheKey)
    }
  }

  return (
    <div style={containerStyle}>
      <ChartHeader
        title={displayTitle}
        tooltip={tooltipText}
        countIndicator={countIndicator}
        actions={actionButtons}
      />
      <div style={{ flex: 1, minHeight: 0 }}>
        <Plot
          data={traces}
          layout={{
            barmode: 'stack',
            width: 330,
            height: 300,
            margin: { t: 10, r: 10, b: 60, l: 50 },
            xaxis: {
              tickangle: x_categories.length > 5 ? -45 : 0,
              tickfont: { size: 10 },
              automargin: true,
            },
            yaxis: {
              tickfont: { size: 10 },
            },
            legend: {
              font: { size: 9 },
              orientation: 'h',
              y: -0.3,
              x: 0.5,
              xanchor: 'center',
            },
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
          }}
          config={{
            displayModeBar: false,
            responsive: true,
          }}
          onClick={handleClick}
          style={{ width: '100%', height: '100%' }}
        />
      </div>
    </div>
  )
}
