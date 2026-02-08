import React from 'react'
import { TABLE_SCOPE_KEY } from '../types'
import type { BaseChartProps } from './ChartContext'
import { useChartContext } from './ChartContext'
import { ChartHeader } from './ChartHeader'
import { CountIndicator } from './CountIndicator'
import { FilterMenu } from './FilterMenu'

export function TableViewChart({
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
    if (pathLabel) {
      tooltipParts.push(pathLabel)
    }
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
        title="Switch to chart view"
      >
        ◐
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

  const containerStyle: React.CSSProperties = {
    position: 'relative',
    background: 'white',
    padding: '0.5rem',
    borderRadius: '8px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    width: '358px',
    height: '358px',
    boxSizing: 'border-box',
    flexShrink: 0,
    display: 'flex',
    flexDirection: 'column',
    border: tableColor ? `2px solid ${tableColor}20` : undefined
  }

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

  const baselineAggregation = ctx.getBaselineAggregation(tableName, field)
  const categoriesForMenu =
    ctx.metricsMatch(baselineAggregation, aggregation) && baselineAggregation?.categories?.length
      ? baselineAggregation.categories
      : aggregation.categories

  const totalRows = aggregation.total_rows ?? aggregation.categories.reduce((sum, cat) => sum + cat.count, 0)

  const tableData = aggregation.categories.map(cat => ({
    category: cat.display_value ?? (cat.value === '' ? '(Empty)' : String(cat.value)),
    rawValue: cat.value,
    count: cat.count,
    percentage: totalRows > 0 ? (cat.count / totalRows) * 100 : 0
  }))

  const sortedData = [...tableData].sort((a, b) => b.count - a.count)
  const showLimit = 100
  const visibleData = sortedData.slice(0, showLimit)
  const hasMore = sortedData.length > showLimit

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
          overflowY: 'auto',
          flex: 1,
          minHeight: 0,
          fontSize: '0.75rem'
        }}
      >
        <table
          style={{
            width: '100%',
            borderCollapse: 'collapse',
            fontSize: '0.75rem'
          }}
        >
          <thead
            style={{
              position: 'sticky',
              top: 0,
              background: '#f5f5f5',
              borderBottom: '2px solid #ddd'
            }}
          >
            <tr>
              <th style={{ padding: '0.4rem 0.5rem', textAlign: 'left', fontWeight: 600 }}>
                Category
              </th>
              <th style={{ padding: '0.4rem 0.5rem', textAlign: 'right', fontWeight: 600 }}>
                Count ↓
              </th>
              <th style={{ padding: '0.4rem 0.5rem', textAlign: 'right', fontWeight: 600 }}>
                %
              </th>
            </tr>
          </thead>
          <tbody>
            {visibleData.map((row, idx) => {
              const rawValue = ctx.normalizeFilterValue(row.rawValue)
              const isFiltered = ctx.isValueFiltered(field, rawValue, cacheKey)
              return (
                <tr
                  key={idx}
                  onClick={() => ctx.toggleFilter(field, rawValue, tableName, cacheKey)}
                  style={{
                    cursor: 'pointer',
                    background: isFiltered ? '#E3F2FD' : idx % 2 === 0 ? 'white' : '#fafafa',
                    borderLeft: isFiltered ? '3px solid #1976D2' : '3px solid transparent'
                  }}
                  onMouseEnter={e => {
                    if (!isFiltered) e.currentTarget.style.background = '#f0f0f0'
                  }}
                  onMouseLeave={e => {
                    if (!isFiltered) e.currentTarget.style.background = idx % 2 === 0 ? 'white' : '#fafafa'
                  }}
                >
                  <td
                    style={{
                      padding: '0.4rem 0.5rem',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      maxWidth: '180px'
                    }}
                  >
                    {row.category}
                  </td>
                  <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>
                    {row.count.toLocaleString()}
                  </td>
                  <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>
                    {row.percentage.toFixed(1)}%
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
        {hasMore && (
          <div style={{ textAlign: 'center', padding: '0.5rem', fontSize: '0.7rem', color: '#666' }}>
            Showing first {showLimit} of {sortedData.length} categories
          </div>
        )}
      </div>
      <FilterMenu tableName={tableName} columnName={field} categories={categoriesForMenu} cacheKeyOverride={cacheKey} />
    </div>
  )
}
