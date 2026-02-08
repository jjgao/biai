import React from 'react'
import { findRelationshipPath } from '../../../utils/filterHelpers'
import type { ColumnAggregation, Table } from '../types'
import { getChartByType } from './renderChartByType'

interface TableSectionProps {
  table: Table
  tables: Table[]
  tableColor: string
  visibleAggregations: ColumnAggregation[]
  primaryAggregation: ColumnAggregation
  baselineRowCount: number | null
  hasTableFilters: boolean
  directFilterCount: number
  propagatedFilterCount: number
  propagatedFilters: Array<{ tableName?: string }>
  metricLabels: { short: string; long: string }
  countByValue: string
  parentOptions: Array<unknown>
  getTableColor: (tableName: string) => string
  getDisplayTitle: (tableName: string, columnName: string) => string
  getColumnMetadata: (tableName: string, columnName: string) => { display_type?: string; is_list_column?: boolean } | undefined
  getEffectiveCacheKeyForChart: (tableName: string, columnName: string) => string
  getCountByCacheKey: (tableName: string) => string
  getCountByLabelFromCacheKey: (tableName: string, cacheKey: string) => string
  getViewPreference: (tableName: string, columnName: string, categoryCount: number) => string
  getSurvivalViewPreference: (tableName: string, columnName: string) => 'histogram' | 'km'
  toggleSurvivalViewPreference: (tableName: string, columnName: string) => void
  addAllChartsToTable: (tableName: string) => void
  renderTabCountIndicator: (tableName: string, cacheKey: string) => React.ReactNode
}

export function TableSection({
  table,
  tables,
  tableColor,
  visibleAggregations,
  primaryAggregation,
  baselineRowCount,
  hasTableFilters,
  directFilterCount,
  propagatedFilterCount,
  propagatedFilters,
  metricLabels,
  countByValue,
  parentOptions,
  getDisplayTitle,
  getColumnMetadata,
  getEffectiveCacheKeyForChart,
  getCountByCacheKey,
  getCountByLabelFromCacheKey,
  getViewPreference,
  getSurvivalViewPreference,
  toggleSurvivalViewPreference,
  addAllChartsToTable,
  renderTabCountIndicator,
}: TableSectionProps) {
  const tableRowCount = primaryAggregation?.total_rows ?? table.rowCount ?? 0

  // Calculate maximum path length for transitive relationships (2+ hops only)
  let maxPathLength = 0
  if (propagatedFilterCount > 0 && tables.length > 0) {
    for (const filter of propagatedFilters) {
      if (filter.tableName) {
        const path = findRelationshipPath(table.name, filter.tableName, tables)
        if (path && path.length > 1) {
          const pathLength = path.length - 1
          if (pathLength >= 2) {
            maxPathLength = Math.max(maxPathLength, pathLength)
          }
        }
      }
    }
  }

  return (
    <div key={table.name} style={{ marginBottom: '2.5rem' }}>
      {/* Table Section Header */}
      <div style={{
        background: `linear-gradient(135deg, ${tableColor}15, ${tableColor}05)`,
        border: `2px solid ${tableColor}40`,
        borderRadius: '8px',
        padding: '0.75rem 1.25rem',
        marginBottom: '1rem',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '1rem'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          {parentOptions.length > 0 ? renderTabCountIndicator(table.name, countByValue) : (
            <div style={{
              background: tableColor,
              color: 'white',
              width: '8px',
              height: '40px',
              borderRadius: '4px'
            }} />
          )}
          <div>
            <h3 style={{
              margin: 0,
              fontSize: '1.1rem',
              fontWeight: 600,
              color: '#333'
            }}>
              {table.displayName || table.name}
            </h3>
            <div
              data-testid={`row-count-${table.name}`}
              style={{
                fontSize: '0.8rem',
                color: '#666',
                marginTop: '0.2rem'
              }}>
              {hasTableFilters && baselineRowCount !== null ? (
                <>
                  <span data-testid={`filtered-count-${table.name}`} style={{ color: '#E65100', fontWeight: 600 }}>
                    {tableRowCount.toLocaleString()}
                  </span>
                  <span style={{ color: '#999' }}> / </span>
                  <span data-testid={`total-count-${table.name}`}>{baselineRowCount.toLocaleString()}</span>
                  <span style={{
                    marginLeft: '0.3rem',
                    padding: '0.1rem 0.4rem',
                    background: '#FF9800',
                    color: 'white',
                    borderRadius: '8px',
                    fontSize: '0.7rem',
                    fontWeight: 600
                  }}>
                    {baselineRowCount > 0 ? ((tableRowCount / baselineRowCount) * 100).toFixed(1) : '0'}%
                  </span>
                  <span> {metricLabels.short} · {visibleAggregations.length} columns</span>
                  <span style={{ color: '#999', fontSize: '0.75rem' }}> (by {getCountByLabelFromCacheKey(table.name, countByValue)})</span>
                </>
              ) : (
                <>
                  <span data-testid={`total-count-${table.name}`}>{tableRowCount.toLocaleString()}</span> {metricLabels.short} · {visibleAggregations.length} columns
                  <span style={{ color: '#999', fontSize: '0.75rem' }}> (by {getCountByLabelFromCacheKey(table.name, countByValue)})</span>
                </>
              )}
            </div>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
          {/* Filter badges */}
          {directFilterCount > 0 && (
            <div
              style={{
                background: '#1976D2',
                color: 'white',
                fontSize: '0.7rem',
                padding: '0.3rem 0.6rem',
                borderRadius: '4px',
                fontWeight: 600
              }}
              title={`${directFilterCount} direct filter${directFilterCount > 1 ? 's' : ''} applied`}
            >
              {directFilterCount} filter{directFilterCount > 1 ? 's' : ''}
            </div>
          )}
          {propagatedFilterCount > 0 && (
            <div
              style={{
                background: '#64B5F6',
                color: 'white',
                fontSize: '0.7rem',
                padding: '0.3rem 0.6rem',
                borderRadius: '4px',
                fontWeight: 600,
                fontStyle: 'italic'
              }}
              title={`${propagatedFilterCount} filter${propagatedFilterCount > 1 ? 's' : ''} propagated from related tables${maxPathLength > 0 ? ` (max ${maxPathLength} hop${maxPathLength > 1 ? 's' : ''})` : ''}`}
            >
              +{propagatedFilterCount} linked{maxPathLength > 0 ? ` (${maxPathLength}-hop)` : ''}
            </div>
          )}
          {/* Add All Charts button */}
          <button
            onClick={() => addAllChartsToTable(table.name)}
            style={{
              padding: '0.3rem 0.6rem',
              background: '#4CAF50',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '0.7rem',
              fontWeight: 600,
              transition: 'background 0.2s'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = '#45a049'
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = '#4CAF50'
            }}
            title="Add all charts from this table to dashboard"
          >
            + Add All
          </button>
          <div style={{
            background: tableColor,
            color: 'white',
            fontSize: '0.7rem',
            padding: '0.3rem 0.6rem',
            borderRadius: '4px',
            fontWeight: 600
          }}>
            {table.name}
          </div>
        </div>
      </div>

      {/* Table Charts */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, 175px)',
        gridAutoRows: '175px',
        gap: '0.5rem',
        gridAutoFlow: 'dense'
      }}>
        {visibleAggregations.map(agg => {
          const displayTitle = getDisplayTitle(table.name, agg.column_name)
          const cacheKey = getEffectiveCacheKeyForChart(table.name, agg.column_name)
          const defaultKey = getCountByCacheKey(table.name)
          const aggregationForChart = cacheKey === defaultKey ? agg : undefined
          const columnMeta = getColumnMetadata(table.name, agg.column_name)
          const metaDisplayType = columnMeta?.display_type
          const normalizedDisplayType =
            agg?.normalized_display_type || agg?.display_type || metaDisplayType || ''

          const result = getChartByType({
            title: displayTitle,
            tableName: table.name,
            columnName: agg.column_name,
            tableColor,
            aggregation: aggregationForChart ?? agg,
            cacheKey,
            normalizedDisplayType,
            metaDisplayType,
            countIndicatorOverride: aggregationForChart ? undefined : undefined,
            getViewPreference,
            getSurvivalViewPreference,
            toggleSurvivalViewPreference,
          })

          if (!result) return null

          return (
            <div
              key={`${table.name}_${agg.column_name}`}
              style={{
                gridColumn: result.gridColumn,
                gridRow: result.gridRow,
              }}
            >
              {result.element}
            </div>
          )
        })}
      </div>
    </div>
  )
}
